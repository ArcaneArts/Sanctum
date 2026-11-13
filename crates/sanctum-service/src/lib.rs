use std::{
    io::{Seek, SeekFrom},
    path::Path,
    sync::Arc,
};

use sanctum_chunk::index_utf8;
use sanctum_core::{Error, ExternalSource, Result, Scope, ScopeId};
use sanctum_db_sqlite::{self as db, CollectionDb};
pub use sanctum_db_sqlite::{Chunk, ChunkRef, Revision};
use sanctum_storage_fs::LocalFiles;
use tempfile::NamedTempFile;
use uuid::Uuid;

#[derive(Clone)]
pub struct Service {
    files: Arc<LocalFiles>,
    max_source_bytes: u64,
}

pub struct TextUpload {
    pub name: String,
    pub external: Option<ExternalSource>,
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
}

impl Service {
    pub fn open(root: &Path, max_source_bytes: u64) -> Result<Self> {
        if !(1..=i64::MAX as u64).contains(&max_source_bytes) {
            return Err(Error::Invalid(
                "max_source_bytes is outside the supported range".into(),
            ));
        }
        Ok(Self {
            files: Arc::new(LocalFiles::open(root)?),
            max_source_bytes,
        })
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
        self.require_collection(&scope)?;
        let file = self.files.stage(&scope)?;
        Ok(StagedUpload {
            service: self.clone(),
            scope,
            upload,
            file,
        })
    }

    pub fn search(&self, scope: &Scope, query: &str, limit: u32) -> Result<Vec<Hit>> {
        if query.trim().is_empty() || query.len() > 4096 || query.as_bytes().contains(&0) {
            return Err(Error::Invalid(
                "search requires a text query of at most 4096 bytes".into(),
            ));
        }
        if !(1..=100).contains(&limit) {
            return Err(Error::Invalid(
                "search limit must be between 1 and 100".into(),
            ));
        }
        let expression = query
            .split_whitespace()
            .map(|word| format!("\"{}\"", word.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(" AND ");
        self.database(scope)?
            .search(&expression, limit)?
            .into_iter()
            .map(|chunk| self.hit(scope, chunk))
            .collect()
    }

    pub fn fetch(&self, scope: &Scope, reference: &ChunkRef) -> Result<Hit> {
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
        Ok(Hit { chunk, excerpt })
    }
}

impl StagedUpload {
    pub fn reopen(&self) -> Result<std::fs::File> {
        Ok(self.file.reopen()?)
    }

    pub fn commit(mut self) -> Result<Revision> {
        let mut input = self.file.reopen()?;
        input.seek(SeekFrom::Start(0))?;
        if input.metadata()?.len() > self.service.max_source_bytes {
            return Err(Error::Invalid(
                "source exceeds the configured upload limit".into(),
            ));
        }

        let source = Uuid::new_v4();
        let revision = Uuid::new_v4();
        let mut database = self.service.database(&self.scope)?;
        let writer = database.write()?;
        let mut ordinal = 0;
        let result = index_utf8(&mut input, |range, text| {
            let reference = ChunkRef {
                source,
                revision,
                ordinal,
            };
            writer.add_chunk(
                &reference,
                range,
                blake3::hash(text.as_bytes()).to_hex().as_str(),
                text,
            )?;
            ordinal += 1;
            Ok(())
        })?;
        drop(input);
        let data = Revision {
            source,
            revision,
            name: std::mem::take(&mut self.upload.name),
            external: self.upload.external.take(),
            bytes: result.bytes,
            chunks: result.chunks,
            hash: result.hash,
        };
        writer.register(&data)?;
        self.service
            .files
            .publish(&self.scope, source, revision, self.file)?;
        writer.commit()?;
        Ok(data)
    }
}
