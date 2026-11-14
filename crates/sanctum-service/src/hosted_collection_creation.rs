use sanctum_core::{Error, Result, Scope, ScopeId};
use sanctum_db::{Command, Unit, tenant_prefix};
use sanctum_storage::{Condition, Key, MAX_CONTROL_BYTES};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use uuid::Uuid;

use crate::{HostedCatalog, db};

#[derive(Deserialize, Serialize)]
struct Creation {
    collection: String,
    format: u8,
}

impl HostedCatalog {
    pub async fn create_collection(&self, scope: &Scope, operation: Option<Uuid>) -> Result<()> {
        if operation.is_some_and(|id| id.is_nil()) {
            return Err(Error::Invalid("request ID must be a nonzero UUID".into()));
        }
        let operation = operation.unwrap_or_else(Uuid::new_v4);
        let catalog = Unit::catalog(self.prefix.as_ref(), &scope.tenant)?;
        if let Some(outcome) = self.publication.receipt(&catalog, operation).await? {
            return collection_outcome(scope, &outcome);
        }
        if self.collection_retired(scope).await? {
            return Err(Error::Conflict("collection ID was retired".into()));
        }

        let tenant = tenant_prefix(self.prefix.as_ref(), &scope.tenant)?;
        let key = Key::parse(format!("{tenant}/catalog/creations/{operation}.json"))?;
        let body = serde_json::to_vec(&Creation {
            collection: scope.collection.to_string(),
            format: 1,
        })
        .map_err(|error| Error::Internal(Box::new(error)))?;
        match self
            .storage
            .put_control(&key, body.into(), Condition::Create)
            .await
        {
            Ok(_) => {}
            Err(Error::Conflict(_)) => match self.read_collection_creation(&key).await? {
                Some(creation) if creation.collection != scope.collection.as_str() => {
                    return Err(other_collection());
                }
                None => {
                    // Another instance may have activated the collection and removed the intent.
                    let outcome = self
                        .publication
                        .receipt(&catalog, operation)
                        .await?
                        .ok_or_else(|| {
                            Error::Busy("collection creation changed during admission".into())
                        })?;
                    return collection_outcome(scope, &outcome);
                }
                Some(_) => {}
            },
            Err(error) => return Err(error),
        }
        let outcome = self
            .finish_collection_creation(scope, &catalog, operation, &key)
            .await?;
        collection_outcome(scope, &outcome)
    }

    pub(super) async fn recover_collection_creation(
        &self,
        tenant: ScopeId,
        operation: Uuid,
        key: &Key,
    ) -> Result<bool> {
        let Some(creation) = self.read_collection_creation(key).await? else {
            return Ok(false);
        };
        let catalog = Unit::catalog(self.prefix.as_ref(), &tenant)?;
        let scope = Scope {
            tenant,
            collection: creation.collection.parse()?,
        };
        let outcome = self
            .finish_collection_creation(&scope, &catalog, operation, key)
            .await?;
        Ok(outcome == format!("created:{}", scope.collection))
    }

    async fn finish_collection_creation(
        &self,
        scope: &Scope,
        catalog: &Unit,
        operation: Uuid,
        key: &Key,
    ) -> Result<String> {
        let outcome = match self.publication.receipt(catalog, operation).await? {
            Some(outcome) => outcome,
            None => {
                self.publish_collection_creation(scope, catalog, operation)
                    .await?
            }
        };
        if let Err(error) = self.storage.delete(key).await
            && !matches!(error, Error::NotFound(_))
        {
            tracing::warn!(%error, %key, "could not remove settled collection creation");
        }
        Ok(outcome)
    }

    async fn publish_collection_creation(
        &self,
        scope: &Scope,
        catalog: &Unit,
        operation: Uuid,
    ) -> Result<String> {
        let collection = Unit::collection(self.prefix.as_ref(), scope)?;
        // Reuse an initial snapshot that committed before catalog activation.
        if !self.collection_retired(scope).await? {
            self.initialize_hosted_collection(&collection).await?;
        }

        let mut writer = self.publication.acquire(catalog).await?;
        let result = writer
            .apply(
                vec![Command {
                    operation,
                    payload: scope.collection.to_string(),
                }],
                |tx, collection| {
                    db::initialize_catalog(tx)?;
                    let outcome = match db::activate_collection(tx, &collection) {
                        Ok(true) => "created",
                        Ok(false) => "exists",
                        Err(Error::Conflict(_)) => "retired",
                        Err(error) => return Err(error),
                    };
                    Ok(format!("{outcome}:{collection}"))
                },
            )
            .await;
        let released = writer.release().await;
        let (_, receipts) = result?;
        released?;
        Ok(receipts
            .into_iter()
            .next()
            .expect("one creation command")
            .outcome)
    }

    async fn initialize_hosted_collection(&self, collection: &Unit) -> Result<()> {
        if self.publication.generation(collection).await?.is_some() {
            return Ok(());
        }
        let mut writer = match self.publication.acquire(collection).await {
            Ok(writer) => writer,
            // The collection can be deleted between the catalog check and acquisition.
            Err(Error::NotFound(_)) => return Ok(()),
            Err(error) => return Err(error),
        };
        let result = writer
            .apply(
                vec![Command {
                    operation: Uuid::nil(),
                    payload: (),
                }],
                |tx, ()| {
                    db::initialize_collection(tx)?;
                    Ok("initialized".into())
                },
            )
            .await;
        let released = writer.release().await;
        result?;
        released
    }

    async fn read_collection_creation(&self, key: &Key) -> Result<Option<Creation>> {
        let download = match self.storage.get(key).await {
            Ok(download) => download,
            Err(Error::NotFound(_)) => return Ok(None),
            Err(error) => return Err(error),
        };
        let size = download.object.length;
        if size > MAX_CONTROL_BYTES as u64 {
            return Err(Error::Internal(
                "collection creation intent is too large".into(),
            ));
        }
        let mut bytes = Vec::new();
        download
            .reader
            .take(size + 1)
            .read_to_end(&mut bytes)
            .await?;
        if bytes.len() as u64 != size {
            return Err(Error::Internal(
                "collection creation intent length mismatch".into(),
            ));
        }
        let intent: Creation =
            serde_json::from_slice(&bytes).map_err(|error| Error::Internal(Box::new(error)))?;
        if intent.format != 1 {
            return Err(Error::Internal(
                "unsupported collection creation intent format".into(),
            ));
        }
        Ok(Some(intent))
    }
}

pub(super) fn creation_key_scope(prefix: Option<&Key>, key: &Key) -> Option<(ScopeId, Uuid)> {
    let path = match prefix {
        Some(prefix) => key
            .as_str()
            .strip_prefix(prefix.as_str())?
            .strip_prefix('/')?,
        None => key.as_str(),
    };
    let (tenant, file) = path.split_once("/catalog/creations/")?;
    let operation: Uuid = file.strip_suffix(".json")?.parse().ok()?;
    (!operation.is_nil()).then_some((tenant.parse().ok()?, operation))
}

fn collection_outcome(scope: &Scope, outcome: &str) -> Result<()> {
    match outcome.split_once(':') {
        Some(("created", collection)) if collection == scope.collection.as_str() => Ok(()),
        Some(("exists", collection)) if collection == scope.collection.as_str() => Err(
            Error::Conflict(format!("collection {collection} already exists")),
        ),
        Some(("retired", collection)) if collection == scope.collection.as_str() => {
            Err(Error::Conflict("collection ID was retired".into()))
        }
        _ => Err(other_collection()),
    }
}

fn other_collection() -> Error {
    Error::Conflict("request ID already applies to a different collection".into())
}
