use std::sync::Arc;

use sanctum_core::{Error, Result, Scope, ScopeId};
use sanctum_db::{Unit, collection_prefix};
use sanctum_embedding::{Purpose, embed};
use sanctum_storage::Key;
use uuid::Uuid;

use crate::embedding::{
    CollectionSearchResult, Coverage, PreparedQuery, SearchResult,
    collections::{Candidates, collection_scopes, merge, rank_collection},
    fuse, lexical_expression, limit_hits,
};
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
        let prepared = self
            .prepare_hosted_query(
                std::slice::from_ref(&scope),
                query,
                limit,
                filter.as_ref(),
                mode,
            )
            .await?;
        let profile = prepared.profile.clone();
        let metadata = self.search_metadata(&scope, filter.as_ref()).await?;
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

    pub async fn retrieve_collections(
        &self,
        tenant: ScopeId,
        collections: Vec<ScopeId>,
        query: String,
        limit: u32,
        filter: Option<Filter>,
        requested: SearchMode,
    ) -> Result<CollectionSearchResult> {
        let scopes = collection_scopes(tenant, collections)?;
        let prepared = self
            .prepare_hosted_query(&scopes, query, limit, filter.as_ref(), requested)
            .await?;
        let candidate_limit = (limit * 4).max(32);
        let filter = filter.map(Arc::new);
        let mut all = Vec::with_capacity(scopes.len());
        let mut coverage = Coverage::default();
        for scope in scopes {
            let metadata = self.search_metadata(&scope, filter.as_deref()).await?;
            let filter = filter.clone();
            let query = prepared.clone();
            let ((ranked, current), generation) = self
                .with_collection(&scope, move |database| {
                    let (ranked, current) = rank_collection(
                        database,
                        &query,
                        candidate_limit,
                        filter.as_deref(),
                        &metadata,
                        requested,
                    )?;
                    let located = ranked
                        .into_iter()
                        .map(|chunk| locate(database, chunk))
                        .collect::<Result<Vec<_>>>()?;
                    Ok((located, current))
                })
                .await?;
            coverage.total += current.total;
            coverage.embedded += current.embedded;
            coverage.failed += current.failed;
            all.push(Candidates {
                scope,
                coverage: current,
                ranked: ranked
                    .into_iter()
                    .map(|(chunk, key)| (chunk, (key, generation)))
                    .collect(),
            });
        }
        let (mode, degraded) = prepared.strategy(requested, coverage)?;
        let mut hits = Vec::with_capacity(limit as usize);
        for (scope, chunk, (key, generation), score) in merge(all, limit) {
            let mut hit = self.read_hit(&scope, chunk, key, generation).await?;
            hit.score = Some(score);
            hits.push((scope, hit));
        }
        Ok(CollectionSearchResult {
            hits,
            mode,
            coverage,
            profile: prepared.profile,
            degraded,
        })
    }

    async fn prepare_hosted_query(
        &self,
        scopes: &[Scope],
        query: String,
        limit: u32,
        filter: Option<&Filter>,
        mode: SearchMode,
    ) -> Result<PreparedQuery> {
        let expression = lexical_expression(&query, limit)?;
        if let Some(filter) = filter {
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
                let mut available = false;
                for scope in scopes {
                    let profile = profile.clone();
                    let (has_embeddings, _) = self
                        .with_collection(scope, move |db| db.has_embeddings(&profile))
                        .await?;
                    if has_embeddings {
                        available = true;
                        break;
                    }
                }
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
        Ok(prepared)
    }

    async fn search_metadata(
        &self,
        scope: &Scope,
        filter: Option<&Filter>,
    ) -> Result<Vec<db::metadata::Entry>> {
        if filter.is_some_and(Filter::uses_collection_metadata) {
            self.with_catalog(scope, |path, collection| {
                db::metadata::read_catalog_metadata(path, &collection)
            })
            .await
        } else {
            Ok(Vec::new())
        }
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
