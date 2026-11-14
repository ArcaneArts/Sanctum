use std::{sync::Arc, time::Duration};

use sanctum_core::{ChunkSettings, Error, Result, Scope, ScopeId, UsageMeasurement, UsageScope};
use sanctum_db::{Command, Publication, SnapshotCache, Unit, collection_prefix, tenant_prefix};
use sanctum_storage::{Key, Storage};
use tokio::sync::Notify;
use uuid::Uuid;

use crate::{Dispatch, db};

pub const DEFAULT_SNAPSHOT_CACHE_BYTES: u64 = 2 * 1024 * 1024 * 1024;

#[derive(Clone)]
pub struct HostedCatalog {
    pub(crate) storage: Arc<dyn Storage>,
    pub(crate) prefix: Option<Key>,
    pub(crate) max_source_bytes: u64,
    pub(crate) chunking: ChunkSettings,
    pub(crate) publication: Publication,
    pub(crate) snapshots: Arc<SnapshotCache>,
    pub(crate) dispatcher: Option<Arc<dyn Dispatch>>,
    pub(crate) standalone_wake: Option<Arc<Notify>>,
    pub(crate) embedding: Option<Arc<dyn sanctum_embedding::Provider>>,
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
        let snapshots = Arc::new(SnapshotCache::new(DEFAULT_SNAPSHOT_CACHE_BYTES));
        let publication = Publication::new(storage.clone(), Duration::from_secs(300))?
            .with_snapshot_cache(snapshots.clone());
        Ok(Self {
            storage,
            prefix,
            max_source_bytes,
            chunking: ChunkSettings::default(),
            publication,
            snapshots,
            dispatcher: None,
            standalone_wake: None,
            embedding: None,
        })
    }

    pub fn with_chunking(mut self, settings: ChunkSettings) -> Result<Self> {
        settings.validate()?;
        self.chunking = settings;
        Ok(self)
    }

    pub fn with_embedding(
        mut self,
        embedding: Option<Arc<dyn sanctum_embedding::Provider>>,
    ) -> Self {
        self.embedding = embedding;
        self
    }

    pub fn with_snapshot_cache_bytes(mut self, max_bytes: u64) -> Self {
        self.snapshots = Arc::new(SnapshotCache::new(max_bytes));
        self.publication = self.publication.with_snapshot_cache(self.snapshots.clone());
        self
    }

    pub fn with_snapshot_page_deltas(mut self, enabled: bool) -> Self {
        self.publication = self.publication.with_page_deltas(enabled);
        self
    }

    pub async fn delete_collection(&self, scope: &Scope, request_id: Option<Uuid>) -> Result<()> {
        if request_id.is_some_and(|id| id.is_nil()) {
            return Err(Error::Invalid("request ID must be a nonzero UUID".into()));
        }
        let catalog = Unit::catalog(self.prefix.as_ref(), &scope.tenant)?;
        let collection = scope.collection.to_string();
        let unit = Unit::collection(self.prefix.as_ref(), scope)?;
        if let Some(operation) = request_id
            && let Some(saved) = self
                .publication
                .stage_receipt(&catalog, operation, "delete-collection")
                .await?
        {
            deletion_outcome(&collection, &saved)?;
            return self.publication.fence_deletion(&unit).await;
        }
        let mut writer = self.publication.acquire_existing(&catalog).await?;
        let result = writer
            .apply_stage(
                "delete-collection",
                vec![Command {
                    operation: request_id.unwrap_or_else(Uuid::new_v4),
                    payload: collection.clone(),
                }],
                |tx, collection| {
                    db::initialize_catalog(tx)?;
                    db::delete_catalog_collection(tx, &collection)?;
                    Ok(collection)
                },
            )
            .await;
        let release = writer.release().await;
        let (_, receipts) = result?;
        release?;
        deletion_outcome(&collection, &receipts[0].outcome)?;
        self.publication.fence_deletion(&unit).await
    }

    pub(crate) async fn collection_retired(&self, scope: &Scope) -> Result<bool> {
        match self
            .with_catalog(scope, |path, collection| {
                db::catalog_collection_deleted(path, &collection)
            })
            .await
        {
            Err(Error::NotFound(_)) => Ok(false),
            result => result,
        }
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
        Ok(self
            .list_collection_details(tenant, after, limit)
            .await?
            .into_iter()
            .map(|row| row.id)
            .collect())
    }

    pub async fn storage_usage(&self, scope: &UsageScope) -> Result<UsageMeasurement> {
        self.storage_usage_with_progress(scope, |_, _| Ok(())).await
    }

    pub async fn storage_usage_with_progress(
        &self,
        scope: &UsageScope,
        progress: impl FnMut(u64, u64) -> Result<()>,
    ) -> Result<UsageMeasurement> {
        let prefix = match scope {
            UsageScope::Cluster => self.prefix.clone(),
            UsageScope::Tenant(tenant) => Some(tenant_prefix(self.prefix.as_ref(), tenant)?),
            UsageScope::Collection(scope) => {
                self.require_collection(scope).await?;
                Some(collection_prefix(self.prefix.as_ref(), scope)?)
            }
        };
        sanctum_storage::usage_with_progress(self.storage.as_ref(), prefix.as_ref(), progress).await
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
        let unit = Unit::catalog(self.prefix.as_ref(), &scope.tenant)?;
        let database = self
            .snapshots
            .read(&self.publication, &unit)
            .await?
            .ok_or_else(|| Error::NotFound("tenant catalog was not found".into()))?;
        let collection = scope.collection.to_string();
        tokio::task::spawn_blocking(move || read(database.path(), collection))
            .await
            .map_err(|error| Error::Internal(Box::new(error)))?
    }
}

fn deletion_outcome(collection: &str, saved: &str) -> Result<()> {
    if collection == saved {
        Ok(())
    } else {
        Err(Error::Conflict(
            "request ID already applies to a different collection deletion".into(),
        ))
    }
}
