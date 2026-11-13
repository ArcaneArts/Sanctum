use sanctum_core::{Error, Result, Scope, ScopeId};
use sanctum_db::{Command, Unit};
use uuid::Uuid;

use crate::{CollectionDetails, HostedCatalog, Service, db};

impl Service {
    pub fn list_collection_details(
        &self,
        tenant: &ScopeId,
        after: Option<&ScopeId>,
        limit: Option<u32>,
    ) -> Result<Vec<CollectionDetails>> {
        db::list_collection_details(
            &self.files.catalog_path(tenant),
            after.map(ScopeId::as_str),
            limit,
        )
    }

    pub fn update_collection(&self, scope: &Scope, name: &str) -> Result<CollectionDetails> {
        let name = validate_name(name)?;
        db::update_local_collection_details(
            &self.files.catalog_path(&scope.tenant),
            scope.collection.as_str(),
            name,
        )
    }
}

impl HostedCatalog {
    pub async fn list_collection_details(
        &self,
        tenant: &ScopeId,
        after: Option<&ScopeId>,
        limit: u32,
    ) -> Result<Vec<CollectionDetails>> {
        let unit = Unit::catalog(self.prefix.as_ref(), tenant)?;
        let Some(database) = self.snapshots.read(&self.publication, &unit).await? else {
            return Ok(Vec::new());
        };
        let after = after.map(ToString::to_string);
        tokio::task::spawn_blocking(move || {
            db::list_collection_details(database.path(), after.as_deref(), Some(limit))
        })
        .await
        .map_err(|error| Error::Internal(Box::new(error)))?
    }

    pub async fn update_collection(
        &self,
        scope: &Scope,
        name: String,
    ) -> Result<CollectionDetails> {
        let name = validate_name(&name)?.to_owned();
        let unit = Unit::catalog(self.prefix.as_ref(), &scope.tenant)?;
        let mut writer = self.publication.acquire_existing(&unit).await?;
        let result = writer
            .apply_stage(
                "update-collection",
                vec![Command {
                    operation: Uuid::new_v4(),
                    payload: (scope.collection.to_string(), name),
                }],
                |tx, (collection, name)| {
                    db::initialize_catalog(tx)?;
                    let row = db::update_collection_details(tx, &collection, &name)?;
                    serde_json::to_string(&row).map_err(|error| Error::Internal(Box::new(error)))
                },
            )
            .await;
        let released = writer.release().await;
        let (_, receipts) = result?;
        released?;
        serde_json::from_str(&receipts[0].outcome).map_err(|error| Error::Internal(Box::new(error)))
    }
}

fn validate_name(name: &str) -> Result<&str> {
    let name = name.trim();
    if name.is_empty() || name.len() > 256 || name.chars().any(char::is_control) {
        return Err(Error::Invalid(
            "collection name must be 1 to 256 non-control UTF-8 bytes".into(),
        ));
    }
    Ok(name)
}
