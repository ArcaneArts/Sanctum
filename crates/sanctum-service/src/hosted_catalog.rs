use std::{sync::Arc, time::Duration};

use sanctum_core::{Error, Result, Scope, ScopeId, UsageMeasurement, UsageScope};
use sanctum_db::{Command, Publication, Unit, collection_prefix, tenant_prefix};
use sanctum_storage::{Key, Storage};
use uuid::Uuid;

use crate::{Dispatch, db};

#[derive(Clone)]
pub struct HostedCatalog {
    pub(crate) storage: Arc<dyn Storage>,
    pub(crate) prefix: Option<Key>,
    pub(crate) max_source_bytes: u64,
    pub(crate) publication: Publication,
    pub(crate) dispatcher: Option<Arc<dyn Dispatch>>,
}

impl HostedCatalog {
    pub fn new(
        storage: Arc<dyn Storage>,
        prefix: Option<Key>,
        max_source_bytes: u64,
    ) -> Result<Self> {
        if max_source_bytes == 0 {
            return Err(Error::Invalid("max_source_bytes must be positive".into()));
        }
        let publication = Publication::new(storage.clone(), Duration::from_secs(300))?;
        Ok(Self {
            storage,
            prefix,
            max_source_bytes,
            publication,
            dispatcher: None,
        })
    }

    pub async fn create_collection(&self, scope: &Scope, operation: Option<Uuid>) -> Result<()> {
        if operation.is_some_and(|id| id.is_nil()) {
            return Err(Error::Invalid("request ID must be a nonzero UUID".into()));
        }
        let catalog = Unit::catalog(self.prefix.as_ref(), &scope.tenant)?;
        // A reused request ID must be checked before initializing another collection.
        if let Some(operation) = operation
            && let Some(outcome) = self.publication.receipt(&catalog, operation).await?
        {
            return collection_outcome(scope, &outcome);
        }

        let initialize = Unit::collection(self.prefix.as_ref(), scope)?;
        // A previous attempt can have published the collection before catalog activation.
        if self.publication.generation(&initialize).await?.is_none() {
            let mut writer = self.publication.acquire(&initialize).await?;
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
            let release = writer.release().await;
            result?;
            release?;
        }

        let mut writer = self.publication.acquire(&catalog).await?;
        let command = Command {
            operation: operation.unwrap_or_else(Uuid::new_v4),
            payload: scope.collection.to_string(),
        };
        let result = writer
            .apply(vec![command], |tx, collection| {
                db::initialize_catalog(tx)?;
                let outcome = if db::activate_collection(tx, &collection)? {
                    "created"
                } else {
                    "exists"
                };
                Ok(format!("{outcome}:{collection}"))
            })
            .await;
        let release = writer.release().await;
        let (_, receipts) = result?;
        release?;
        collection_outcome(scope, &receipts[0].outcome)
    }

    pub async fn list_collections(
        &self,
        tenant: &ScopeId,
        after: Option<&ScopeId>,
        limit: u32,
    ) -> Result<Vec<String>> {
        if !(1..=100).contains(&limit) {
            return Err(Error::Invalid(
                "collection page limit must be between 1 and 100".into(),
            ));
        }
        let scratch = tempfile::tempdir()?;
        let catalog = Unit::catalog(self.prefix.as_ref(), tenant)?;
        let Some(database) = self.publication.read(&catalog, scratch.path()).await? else {
            return Ok(Vec::new());
        };
        let after = after.map(ToString::to_string);
        tokio::task::spawn_blocking(move || {
            let _scratch = scratch;
            db::list_collections_page(database.path(), after.as_deref(), limit)
        })
        .await
        .map_err(|error| Error::Internal(Box::new(error)))?
    }

    pub async fn storage_usage(&self, scope: &UsageScope) -> Result<UsageMeasurement> {
        let prefix = match scope {
            UsageScope::Cluster => self.prefix.clone(),
            UsageScope::Tenant(tenant) => Some(tenant_prefix(self.prefix.as_ref(), tenant)?),
            UsageScope::Collection(scope) => {
                self.require_collection(scope).await?;
                Some(collection_prefix(self.prefix.as_ref(), scope)?)
            }
        };
        sanctum_storage::usage(self.storage.as_ref(), prefix.as_ref()).await
    }

    pub(crate) async fn require_collection(&self, scope: &Scope) -> Result<()> {
        self.with_catalog(scope, |path, collection| {
            db::require_collection(path, &collection)
        })
        .await
    }

    pub(crate) async fn with_catalog<T: Send + 'static>(
        &self,
        scope: &Scope,
        read: impl FnOnce(&std::path::Path, String) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let scratch = tempfile::tempdir()?;
        let unit = Unit::catalog(self.prefix.as_ref(), &scope.tenant)?;
        let database = self
            .publication
            .read(&unit, scratch.path())
            .await?
            .ok_or_else(|| Error::NotFound("tenant catalog was not found".into()))?;
        let collection = scope.collection.to_string();
        tokio::task::spawn_blocking(move || {
            let _scratch = scratch;
            read(database.path(), collection)
        })
        .await
        .map_err(|error| Error::Internal(Box::new(error)))?
    }
}

fn collection_outcome(scope: &Scope, outcome: &str) -> Result<()> {
    if outcome == format!("created:{}", scope.collection) {
        Ok(())
    } else if outcome == format!("exists:{}", scope.collection) {
        Err(Error::Conflict(format!(
            "collection {} already exists",
            scope.collection
        )))
    } else {
        Err(Error::Conflict(
            "request ID already applies to a different collection".into(),
        ))
    }
}
