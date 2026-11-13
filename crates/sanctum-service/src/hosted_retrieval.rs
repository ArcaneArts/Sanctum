use sanctum_core::{Error, Result, Scope};
use sanctum_db::{Unit, collection_prefix};
use sanctum_storage::Key;

use crate::embedding::{SearchResult, lexical_expression};
use crate::{Chunk, ChunkRef, Hit, HostedCatalog, SearchMode, db, metadata_filter::Filter};

impl HostedCatalog {
    pub async fn fetch(&self, scope: &Scope, reference: ChunkRef) -> Result<Hit> {
        let (chunk, key) = self
            .with_collection(scope, move |database| {
                locate(database, database.fetch(&reference)?)
            })
            .await?;
        self.read_hit(scope, chunk, key).await
    }

    pub async fn retrieve(
        &self,
        scope: Scope,
        query: String,
        limit: u32,
        filter: Option<Filter>,
        mode: SearchMode,
    ) -> Result<SearchResult> {
        let expression = lexical_expression(&query, limit)?;
        if let Some(filter) = &filter {
            filter.validate()?;
        }
        if mode == SearchMode::Semantic {
            return Err(Error::Busy("embedding_disabled".into()));
        }
        let metadata = if filter
            .as_ref()
            .is_some_and(Filter::uses_collection_metadata)
        {
            self.with_catalog(&scope, |path, collection| {
                db::metadata::catalog_metadata(path, &collection, None)
            })
            .await?
        } else {
            Vec::new()
        };
        let (chunks, coverage) = self
            .with_collection(&scope, move |database| {
                let coverage = database.embedding_coverage(None)?;
                let chunks = database
                    .search(&expression, limit, filter.as_ref(), &metadata)?
                    .into_iter()
                    .map(|chunk| locate(database, chunk))
                    .collect::<Result<Vec<_>>>()?;
                Ok((chunks, coverage))
            })
            .await?;
        let mut hits = Vec::with_capacity(chunks.len());
        for (chunk, key) in chunks {
            hits.push(self.read_hit(&scope, chunk, key).await?);
        }
        Ok(SearchResult {
            hits,
            coverage,
            profile: None,
            mode: SearchMode::Lexical,
            degraded: (mode == SearchMode::Hybrid).then_some("embedding_disabled"),
        })
    }

    async fn with_collection<T: Send + 'static>(
        &self,
        scope: &Scope,
        read: impl FnOnce(&db::CollectionDb) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.require_collection(scope).await?;
        let directory = tempfile::tempdir()?;
        let unit = Unit::collection(self.prefix.as_ref(), scope)?;
        let database = self
            .publication
            .read(&unit, directory.path())
            .await?
            .ok_or_else(|| Error::NotFound("collection database was not found".into()))?;
        tokio::task::spawn_blocking(move || {
            let _directory = directory;
            read(&db::CollectionDb::open(database.path())?)
        })
        .await
        .map_err(|error| Error::Internal(Box::new(error)))?
    }

    async fn read_hit(&self, scope: &Scope, chunk: Chunk, key: String) -> Result<Hit> {
        let collection = collection_prefix(self.prefix.as_ref(), scope)?;
        let key = Key::parse(key)?;
        if !key
            .as_str()
            .strip_prefix(collection.as_str())
            .is_some_and(|suffix| suffix.starts_with('/'))
        {
            return Err(Error::Internal(
                "canonical text key escapes its collection".into(),
            ));
        }
        let bytes = self.storage.read_range(&key, chunk.range).await?;
        Hit::from_bytes(chunk, &bytes)
    }
}

fn locate(database: &db::CollectionDb, chunk: Chunk) -> Result<(Chunk, String)> {
    let key = database.hosted_text_key(chunk.reference.source, chunk.reference.revision)?;
    Ok((chunk, key))
}
