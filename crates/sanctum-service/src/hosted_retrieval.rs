use std::sync::Arc;

use sanctum_core::{Error, Result, Scope};
use sanctum_db::{Unit, collection_prefix};
use sanctum_embedding::{Purpose, embed};
use sanctum_storage::Key;
use uuid::Uuid;

use crate::embedding::{PreparedQuery, SearchResult, fuse, lexical_expression, limit_hits};
use crate::{Chunk, ChunkRef, Hit, HostedCatalog, SearchMode, db, metadata_filter::Filter};

impl HostedCatalog {
    pub async fn fetch(&self, scope: &Scope, reference: ChunkRef) -> Result<Hit> {
        let ((chunk, key), generation) = self
            .with_collection(scope, move |database| {
                locate(database, database.fetch(&reference)?)
            })
            .await?;
        self.read_hit(scope, chunk, key, generation).await
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
        let mut prepared = PreparedQuery {
            expression,
            profile: self
                .embedding
                .as_ref()
                .map(|provider| provider.profile_id().to_owned()),
            vector: None,
            degraded: None,
        };
        if mode != SearchMode::Lexical {
            if let Some(provider) = &self.embedding {
                let profile = provider.profile_id().to_owned();
                let (available, _) = self
                    .with_collection(&scope, move |db| db.has_embeddings(&profile))
                    .await?;
                if available {
                    match embed(provider.as_ref(), Purpose::Query, &[query]).await {
                        Ok(mut vectors) => prepared.vector = vectors.pop().map(Arc::from),
                        Err(error) => {
                            tracing::warn!(%error, "hosted query embedding unavailable");
                            prepared.degraded = Some("embedding_provider_unavailable");
                        }
                    }
                }
            } else {
                prepared.degraded = Some("embedding_disabled");
            }
        }
        let profile = prepared.profile.clone();
        let metadata = if filter
            .as_ref()
            .is_some_and(Filter::uses_collection_metadata)
        {
            self.with_catalog(&scope, |path, collection| {
                db::metadata::read_catalog_metadata(path, &collection)
            })
            .await?
        } else {
            Vec::new()
        };
        let ((chunks, coverage, actual_mode, degraded), generation) = self
            .with_collection(&scope, move |database| {
                let coverage = database.embedding_coverage(prepared.profile.as_deref())?;
                let (actual_mode, degraded) = prepared.strategy(mode, coverage)?;
                let candidate_limit = if actual_mode == SearchMode::Lexical {
                    limit
                } else {
                    (limit * 4).max(32)
                };
                let lexical = if actual_mode == SearchMode::Semantic {
                    Vec::new()
                } else {
                    database.search(
                        &prepared.expression,
                        candidate_limit,
                        filter.as_ref(),
                        &metadata,
                    )?
                };
                let semantic = match (&prepared.vector, &prepared.profile) {
                    (Some(query), Some(profile)) if actual_mode != SearchMode::Lexical => database
                        .search_vector(
                            profile,
                            query.clone(),
                            candidate_limit,
                            filter.as_ref(),
                            &metadata,
                        )?,
                    _ => Vec::new(),
                };
                let ranked = match actual_mode {
                    SearchMode::Lexical => lexical.into_iter().map(|chunk| (chunk, None)).collect(),
                    SearchMode::Semantic => semantic
                        .into_iter()
                        .map(|hit| (hit.chunk, Some(hit.score)))
                        .collect(),
                    SearchMode::Hybrid => fuse(lexical, semantic),
                };
                let chunks = limit_hits(ranked, actual_mode, limit)
                    .map(|(chunk, score)| Ok((locate(database, chunk)?, score)))
                    .collect::<Result<Vec<_>>>()?;
                Ok((chunks, coverage, actual_mode, degraded))
            })
            .await?;
        let mut hits = Vec::with_capacity(chunks.len());
        for ((chunk, key), score) in chunks {
            let mut hit = self.read_hit(&scope, chunk, key, generation).await?;
            hit.score = score;
            hits.push(hit);
        }
        Ok(SearchResult {
            hits,
            coverage,
            profile,
            mode: actual_mode,
            degraded,
        })
    }

    pub(crate) async fn with_collection<T: Send + 'static>(
        &self,
        scope: &Scope,
        read: impl FnOnce(&db::CollectionDb) -> Result<T> + Send + 'static,
    ) -> Result<(T, Uuid)> {
        self.require_collection(scope).await?;
        let unit = Unit::collection(self.prefix.as_ref(), scope)?;
        let database = self
            .snapshots
            .read(&self.publication, &unit)
            .await?
            .ok_or_else(|| Error::NotFound("collection database was not found".into()))?;
        tokio::task::spawn_blocking(move || {
            let value = read(&db::CollectionDb::open_read_only(database.path())?)?;
            Ok((value, database.generation.id))
        })
        .await
        .map_err(|error| Error::Internal(Box::new(error)))?
    }

    pub(crate) async fn read_hit(
        &self,
        scope: &Scope,
        chunk: Chunk,
        key: String,
        generation: Uuid,
    ) -> Result<Hit> {
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
        let mut hit = Hit::from_bytes(chunk, &bytes)?;
        hit.database_generation = Some(generation);
        Ok(hit)
    }
}

pub(crate) fn locate(database: &db::CollectionDb, chunk: Chunk) -> Result<(Chunk, String)> {
    let key = database.hosted_text_key(chunk.reference.source, chunk.reference.revision)?;
    Ok((chunk, key))
}
