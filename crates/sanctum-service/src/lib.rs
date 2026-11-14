use std::{
    path::Path,
    sync::{Arc, Mutex, RwLock},
};

pub use context::ChunkContext;
pub use database_sessions::{DatabaseSession, DatabaseTarget};
pub use db::metadata::{
    Change, ChunkMetadata, Edit as MetadataEdit, Entry as MetadataEntry,
    Mutation as MetadataMutation, RevisionTarget,
};
pub use db::{metadata_filter, metadata_indexes, metadata_query, snapshot as database_snapshot};
pub use hit_metadata::{HitMetadata, HitMetadataEntry};
use sanctum_core::{
    ChunkSettings, Error, ExternalSource, Result, Scope, ScopeId, UsageMeasurement, UsageScope,
};
use sanctum_db_sqlite::{self as db, CollectionDb};
pub use sanctum_db_sqlite::{Chunk, ChunkRef, CollectionDetails, Revision, RevisionContent};
use sanctum_storage_fs::LocalFiles;
use tempfile::NamedTempFile;
use uuid::Uuid;

mod collection_details;
mod context;
mod database_sessions;
mod embedding;
mod git_import;
mod hit_metadata;
mod hosted_catalog;
mod hosted_database;
mod hosted_dispatch;
mod hosted_embedding;
mod hosted_ingestion;
mod hosted_lifecycle;
mod hosted_metadata;
mod hosted_rebuild;
mod hosted_retrieval;
mod hosted_snapshots;
mod local_import;
mod local_jobs;
mod local_writes;
mod maintenance;
mod managed_metadata_indexes;
mod pending_scan;
mod source_inspection;
mod url_import;
mod watched_folders;
mod worker;
pub use embedding::{CollectionSearchResult, SearchMode, run_embedding_worker};
pub use git_import::{GitImportEvent, GitRepository};
pub use hosted_catalog::{DEFAULT_SNAPSHOT_CACHE_BYTES, HostedCatalog};
pub use hosted_database::HostedDatabaseSession;
pub use hosted_dispatch::{DISPATCH_SHARDS, Dispatch, ReconcilePage, Task, TaskStage};
pub use hosted_ingestion::{
    HostedActivityPage, HostedEmbeddingWork, HostedJob, HostedJobState, HostedUpload, UploadSweep,
};
pub use hosted_rebuild::RebuildPage;
pub use local_import::{ImportEvent, ImportOptions, ImportState};
pub use pending_scan::{PendingText, ScanOptions, ScanPage, ScanStop};
pub use sanctum_db_sqlite::embedding::{ChunkEmbeddingState, EmbeddingStatus};
pub use watched_folders::{WatchedFolder, run_watched_folders};
pub use worker::{run_local_maintenance, run_local_worker};

use db::queue::LocalQueue;
pub use db::queue::{Job, JobState};

fn validate_change_limit(limit: u32) -> Result<()> {
    if !(1..=200).contains(&limit) {
        return Err(Error::Invalid(
            "change page limit must be between 1 and 200".into(),
        ));
    }
    Ok(())
}

#[derive(Clone)]
pub struct Service {
    files: Arc<LocalFiles>,
    queue: LocalQueue,
    max_source_bytes: u64,
    chunking: ChunkSettings,
    url_importer: url_import::UrlImporter,
    git_imports: git_import::ActiveImports,
    database_sessions: database_sessions::DatabaseSessions,
    retrieval: Arc<RwLock<()>>,
    local_writes: Arc<local_writes::LocalWrites>,
    upload_requests: Arc<[Mutex<()>; 64]>,
    embedding: Option<Arc<dyn sanctum_embedding::Provider>>,
}

#[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct TextUpload {
    pub name: String,
    pub external: Option<ExternalSource>,
    pub replacement: Option<Replacement>,
    pub retain_original: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub initial_metadata: Vec<MetadataMutation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub initial_chunk_metadata: Vec<ChunkMetadata>,
}

#[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Replacement {
    pub source: Option<Uuid>,
    pub expected_revision: Option<Uuid>,
}

impl TextUpload {
    fn validate(&mut self) -> Result<()> {
        db::metadata::validate_initial_metadata(
            &self.initial_metadata,
            &self.initial_chunk_metadata,
        )?;
        let by_key = |left: &MetadataMutation, right: &MetadataMutation| {
            (&left.namespace, &left.key).cmp(&(&right.namespace, &right.key))
        };
        self.initial_metadata.sort_unstable_by(by_key);
        self.initial_chunk_metadata
            .sort_unstable_by_key(|chunk| chunk.ordinal);
        for chunk in &mut self.initial_chunk_metadata {
            chunk.entries.sort_unstable_by(by_key);
        }
        if self.name.trim().is_empty() || self.name.len() > 1024 {
            return Err(Error::Invalid(
                "source name must contain 1 to 1024 bytes".into(),
            ));
        }
        if let Some(external) = &self.external
            && (external.namespace.trim().is_empty()
                || external.namespace.len() > 256
                || external.id.trim().is_empty()
                || external.id.len() > 4096)
        {
            return Err(Error::Invalid(
                "external source requires a namespace and ID within their size limits".into(),
            ));
        }
        if let Some(replacement) = &self.replacement
            && replacement.source.is_none()
            && self.external.is_none()
        {
            return Err(Error::Invalid(
                "replacement requires a source ID or external reference".into(),
            ));
        }
        Ok(())
    }
}

pub struct StagedUpload {
    service: Service,
    scope: Scope,
    upload: TextUpload,
    file: NamedTempFile,
}

#[derive(Debug)]
pub struct Hit {
    pub chunk: Chunk,
    pub excerpt: String,
    pub score: Option<f64>,
    pub database_generation: Option<Uuid>,
    pub metadata: HitMetadata,
    pub embedding_state: ChunkEmbeddingState,
}

impl Service {
    pub fn activity(
        &self,
        tenant: &ScopeId,
        collection: Option<&ScopeId>,
        before: Option<Uuid>,
        limit: u32,
    ) -> Result<(Vec<Job>, Option<Uuid>)> {
        if !(1..=100).contains(&limit) {
            return Err(Error::Invalid(
                "activity limit must be between 1 and 100".into(),
            ));
        }
        let mut jobs = self.queue.activity(tenant, collection, before, limit + 1)?;
        let next = if jobs.len() > limit as usize {
            jobs.pop();
            jobs.last().map(|job| job.id)
        } else {
            None
        };
        Ok((jobs, next))
    }

    pub fn embedding_status(
        &self,
        scope: &Scope,
        source_revision: Option<(Uuid, Uuid)>,
    ) -> Result<EmbeddingStatus> {
        self.database(scope)?
            .embedding_status(self.embedding_profile(), source_revision)
    }

    pub fn storage_usage(&self, scope: &UsageScope) -> Result<UsageMeasurement> {
        if let UsageScope::Collection(collection) = scope {
            self.require_collection(collection)?;
        }
        self.files.usage(scope)
    }
    pub fn metadata(
        &self,
        scope: &Scope,
        target: Option<RevisionTarget>,
        edits: Option<&[MetadataMutation]>,
    ) -> Result<Vec<MetadataEntry>> {
        match target {
            Some(target) if edits.is_some() => {
                let _reader = self
                    .retrieval
                    .read()
                    .map_err(|_| Error::Busy("retrieval lock is unavailable".into()))?;
                self.local_writes
                    .run(scope, || self.database(scope)?.metadata(target, edits))
            }
            Some(target) => self.database(scope)?.metadata(target, None),
            None => {
                self.require_collection(scope)?;
                db::metadata::catalog_metadata(
                    &self.files.catalog_path(&scope.tenant),
                    scope.collection.as_str(),
                    edits,
                )
            }
        }
    }

    pub fn query_metadata(
        &self,
        tenant: &ScopeId,
        collection: Option<ScopeId>,
        query: metadata_query::Query,
    ) -> Result<metadata_query::Page> {
        use metadata_filter::Level;
        match (query.level, collection) {
            (Level::Collection, None) => {
                metadata_query::catalog(&self.files.catalog_path(tenant), query)
            }
            (Level::Source | Level::Chunk, Some(collection)) => {
                let scope = Scope {
                    tenant: tenant.clone(),
                    collection,
                };
                let database = self.database(&scope)?;
                let values = self.metadata(&scope, None, None)?;
                database.query_metadata(scope.collection.as_str(), query, &values)
            }
            _ => Err(Error::Invalid(
                "collection is required only for source or chunk metadata queries".into(),
            )),
        }
    }

    pub fn change_cursor(&self, scope: &Scope) -> Result<u64> {
        self.database(scope)?.change_cursor()
    }

    pub fn changes(&self, scope: &Scope, after: u64, limit: u32) -> Result<Vec<Change>> {
        validate_change_limit(limit)?;
        self.database(scope)?.changes(after, limit)
    }

    pub fn open(root: &Path, max_source_bytes: u64) -> Result<Self> {
        Self::open_with_embedding(root, max_source_bytes, None)
    }

    pub fn open_with_embedding(
        root: &Path,
        max_source_bytes: u64,
        embedding: Option<Arc<dyn sanctum_embedding::Provider>>,
    ) -> Result<Self> {
        if !(1..=i64::MAX as u64).contains(&max_source_bytes) {
            return Err(Error::Invalid(
                "max_source_bytes is outside the supported range".into(),
            ));
        }
        let files = Arc::new(LocalFiles::open(root)?);
        let queue = LocalQueue::open(files.runtime_path())?;
        let database_sessions = database_sessions::DatabaseSessions::open(files.root());
        let service = Self {
            files,
            queue,
            max_source_bytes,
            chunking: ChunkSettings::default(),
            url_importer: url_import::UrlImporter::new()?,
            git_imports: Default::default(),
            database_sessions,
            retrieval: Arc::new(RwLock::new(())),
            local_writes: Arc::default(),
            upload_requests: Arc::new(std::array::from_fn(|_| Mutex::new(()))),
            embedding,
        };
        if let Err(error) = service.reclaim_deleted_collections() {
            tracing::warn!(%error, "could not reclaim all deleted local collections");
        }
        if let Err(error) = service.reclaim_startup_orphans() {
            tracing::warn!(%error, "could not reclaim all unreferenced local uploads");
        }
        Ok(service)
    }

    pub fn with_chunking(mut self, settings: ChunkSettings) -> Result<Self> {
        settings.validate()?;
        self.chunking = settings;
        Ok(self)
    }

    pub fn max_source_bytes(&self) -> u64 {
        self.max_source_bytes
    }

    pub fn create_collection(&self, scope: &Scope) -> Result<()> {
        let _reader = self
            .retrieval
            .read()
            .map_err(|_| Error::Busy("retrieval lock is unavailable".into()))?;
        db::create_collection(
            &self.files.catalog_path(&scope.tenant),
            &self.files.database_path(scope),
            scope.collection.as_str(),
        )
    }

    pub fn list_collections(&self, tenant: &ScopeId) -> Result<Vec<String>> {
        db::list_collections(&self.files.catalog_path(tenant))
    }

    pub fn list_collections_page(
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
        db::list_collections_page(
            &self.files.catalog_path(tenant),
            after.map(ScopeId::as_str),
            limit,
        )
    }

    pub fn stage_upload(&self, scope: Scope, mut upload: TextUpload) -> Result<StagedUpload> {
        let _reader = self
            .retrieval
            .read()
            .map_err(|_| Error::Busy("retrieval lock is unavailable".into()))?;
        upload.validate()?;
        self.require_collection(&scope)?;
        let file = self.files.stage(&scope)?;
        Ok(StagedUpload {
            service: self.clone(),
            scope,
            upload,
            file,
        })
    }

    pub fn fetch(&self, scope: &Scope, reference: &ChunkRef) -> Result<Hit> {
        let _reader = self
            .retrieval
            .read()
            .map_err(|_| Error::Busy("retrieval lock is unavailable".into()))?;
        self.database(scope)?.read_snapshot(|db| {
            let chunk = db.fetch(reference)?;
            let state = db.chunk_embedding_state(self.embedding_profile(), &chunk.hash)?;
            self.hit(scope, chunk, state)
        })
    }

    fn require_collection(&self, scope: &Scope) -> Result<()> {
        db::require_collection(
            &self.files.catalog_path(&scope.tenant),
            scope.collection.as_str(),
        )
    }

    fn database(&self, scope: &Scope) -> Result<CollectionDb> {
        self.require_collection(scope)?;
        CollectionDb::open(&self.files.database_path(scope))
    }

    fn hit(&self, scope: &Scope, chunk: Chunk, state: ChunkEmbeddingState) -> Result<Hit> {
        let excerpt = self.read_excerpt(scope, &chunk)?;
        Ok(Hit::new(chunk, excerpt, state))
    }

    fn read_excerpt(&self, scope: &Scope, chunk: &Chunk) -> Result<String> {
        let bytes = self.files.read_chunk(
            scope,
            chunk.reference.source,
            chunk.reference.revision,
            chunk.range,
        )?;
        verified_excerpt(chunk, &bytes)
    }
}

impl Hit {
    fn new(chunk: Chunk, excerpt: String, embedding_state: ChunkEmbeddingState) -> Self {
        Self {
            chunk,
            excerpt,
            score: None,
            database_generation: None,
            metadata: HitMetadata::default(),
            embedding_state,
        }
    }
}

fn verified_excerpt(chunk: &Chunk, bytes: &[u8]) -> Result<String> {
    if blake3::hash(bytes).to_hex().as_str() != chunk.hash {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "stored chunk does not match its fingerprint",
        )));
    }
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|error| Error::Internal(Box::new(error)))
}

impl StagedUpload {
    pub fn reopen(&self) -> Result<std::fs::File> {
        Ok(self.file.reopen()?)
    }
}
