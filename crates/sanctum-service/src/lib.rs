use std::{
    path::Path,
    sync::{Arc, RwLock},
};

pub use database_sessions::{DatabaseSession, DatabaseTarget};
pub use db::metadata::{
    Change, Edit as MetadataEdit, Entry as MetadataEntry, Mutation as MetadataMutation,
    RevisionTarget,
};
pub use db::{metadata_filter, metadata_query, snapshot as database_snapshot};
use sanctum_core::{Error, ExternalSource, Result, Scope, ScopeId, UsageMeasurement, UsageScope};
use sanctum_db_sqlite::{self as db, CollectionDb};
pub use sanctum_db_sqlite::{Chunk, ChunkRef, Revision, RevisionContent};
use sanctum_storage_fs::LocalFiles;
use tempfile::NamedTempFile;
use uuid::Uuid;

mod database_sessions;
mod embedding;
mod git_import;
mod local_import;
mod local_jobs;
mod maintenance;
mod url_import;
mod watched_folders;
mod worker;
pub use embedding::{CollectionSearchResult, SearchMode, run_embedding_worker};
pub use git_import::{GitImportEvent, GitRepository};
pub use local_import::{ImportEvent, ImportOptions, ImportState};
pub use sanctum_db_sqlite::embedding::EmbeddingStatus;
pub use watched_folders::{WatchedFolder, run_watched_folders};
pub use worker::{run_local_maintenance, run_local_worker};

use db::queue::LocalQueue;
pub use db::queue::{Job, JobState};

#[derive(Clone)]
pub struct Service {
    files: Arc<LocalFiles>,
    queue: LocalQueue,
    max_source_bytes: u64,
    url_importer: url_import::UrlImporter,
    git_imports: git_import::ActiveImports,
    database_sessions: database_sessions::DatabaseSessions,
    retrieval: Arc<RwLock<()>>,
    embedding: Option<Arc<dyn sanctum_embedding::Provider>>,
}

pub struct TextUpload {
    pub name: String,
    pub external: Option<ExternalSource>,
    pub replacement: Option<Replacement>,
    pub retain_original: bool,
}

pub struct Replacement {
    pub source: Option<Uuid>,
    pub expected_revision: Option<Uuid>,
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
            Some(target) => self.database(scope)?.metadata(target, edits),
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
        if !(1..=200).contains(&limit) {
            return Err(Error::Invalid(
                "change page limit must be between 1 and 200".into(),
            ));
        }
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
            url_importer: url_import::UrlImporter::new()?,
            git_imports: Default::default(),
            database_sessions,
            retrieval: Arc::new(RwLock::new(())),
            embedding,
        };
        if let Err(error) = service.reclaim_startup_orphans() {
            tracing::warn!(%error, "could not reclaim all unreferenced local uploads");
        }
        Ok(service)
    }

    pub fn max_source_bytes(&self) -> u64 {
        self.max_source_bytes
    }

    pub fn create_collection(&self, scope: &Scope) -> Result<()> {
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

    pub fn stage_upload(&self, scope: Scope, upload: TextUpload) -> Result<StagedUpload> {
        if upload.name.trim().is_empty() || upload.name.len() > 1024 {
            return Err(Error::Invalid(
                "source name must contain 1 to 1024 bytes".into(),
            ));
        }
        if let Some(external) = &upload.external
            && (external.namespace.trim().is_empty()
                || external.namespace.len() > 256
                || external.id.trim().is_empty()
                || external.id.len() > 4096)
        {
            return Err(Error::Invalid(
                "external source requires a namespace and ID within their size limits".into(),
            ));
        }
        if let Some(replacement) = &upload.replacement
            && replacement.source.is_none()
            && upload.external.is_none()
        {
            return Err(Error::Invalid(
                "replacement requires a source ID or external reference".into(),
            ));
        }
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
        self.hit(scope, self.database(scope)?.fetch(reference)?)
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

    fn hit(&self, scope: &Scope, chunk: Chunk) -> Result<Hit> {
        let bytes = self.files.read_chunk(
            scope,
            chunk.reference.source,
            chunk.reference.revision,
            chunk.range,
        )?;
        if blake3::hash(&bytes).to_hex().as_str() != chunk.hash {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "stored chunk does not match its fingerprint",
            )));
        }
        let excerpt = String::from_utf8(bytes).map_err(|error| Error::Internal(Box::new(error)))?;
        Ok(Hit {
            chunk,
            excerpt,
            score: None,
        })
    }
}

impl StagedUpload {
    pub fn reopen(&self) -> Result<std::fs::File> {
        Ok(self.file.reopen()?)
    }
}
