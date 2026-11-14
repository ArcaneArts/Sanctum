use std::{sync::Arc, time::Instant};

use sanctum_core::{Error, Result, Scope, ScopeId};
use sanctum_db::{Unit, collection_prefix};
use sanctum_db_sqlite::vector_index::VectorIndex;
use sanctum_embedding::Purpose;
use sanctum_storage::Key;
use uuid::Uuid;

use crate::embedding::{
    CollectionSearchResult, Coverage, PreparedQuery, SearchResult,
    collections::{Candidates, collection_scopes, merge, rank_collection},
    fuse, limit_hits, query_scope,
};
use crate::{
    Chunk, ChunkEmbeddingState, ChunkRef, Hit, HostedCatalog, SearchMode, db,
    hit_metadata::metadata_for_hit, metadata_filter::Filter, telemetry, verified_excerpt,
};

impl HostedCatalog {
    pub(crate) fn embedding_profile(&self) -> Option<&str> {
        self.embedding
            .as_ref()
            .map(|provider| provider.profile_id())
    }

    pub async fn fetch(&self, scope: &Scope, reference: ChunkRef) -> Result<Hit> {
        let profile = self.embedding_profile().map(str::to_owned);
        let ((located, state), generation) = self
            .with_collection(scope, move |database| {
                let chunk = database.fetch(&reference)?;
                let state = database.chunk_embedding_state(profile.as_deref(), &chunk.hash)?;
                Ok((locate(database, chunk)?, state))
            })
            .await?;
        self.read_hit(scope, located, state, generation).await
    }

    pub async fn retrieve(
        &self,
        scope: Scope,
        query: String,
        limit: u32,
        filter: Option<Filter>,
        mode: SearchMode,
    ) -> Result<SearchResult> {
        let start = Instant::now();
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
                let lexical = prepared.lexical(
                    database,
                    mode,
                    semantic.is_empty(),
                    candidate_limit,
                    filter.as_ref(),
                    &metadata,
                )?;
                let ranked = match actual_mode {
                    SearchMode::Lexical => lexical.into_iter().map(|chunk| (chunk, None)).collect(),
                    SearchMode::Semantic => semantic
                        .into_iter()
                        .map(|hit| (hit.chunk, Some(hit.score)))
                        .collect(),
                    SearchMode::Hybrid => fuse(lexical, semantic),
                };
                let chunks = limit_hits(ranked, actual_mode, limit)
                    .map(|(chunk, score)| {
                        let fields = metadata_for_hit(
                            database,
                            &chunk.reference,
                            filter.as_ref(),
                            &metadata,
                        )?;
                        let state = database
                            .chunk_embedding_state(prepared.profile.as_deref(), &chunk.hash)?;
                        Ok((locate(database, chunk)?, score, fields, state))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok((chunks, coverage, actual_mode, degraded))
            })
            .await?;
        let mut hits = Vec::with_capacity(chunks.len());
        for (located, score, fields, state) in chunks {
            let mut hit = self.read_hit(&scope, located, state, generation).await?;
            hit.score = score;
            hit.metadata = fields;
            hits.push(hit);
        }
        telemetry::search(
            &scope.tenant,
            Some(&scope.collection),
            start.elapsed(),
            actual_mode,
            coverage,
            hits.len(),
            degraded,
        );
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
        let start = Instant::now();
        let scopes = collection_scopes(tenant.clone(), collections)?;
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
                        .map(|chunk| {
                            let fields = metadata_for_hit(
                                database,
                                &chunk.reference,
                                filter.as_deref(),
                                &metadata,
                            )?;
                            let state = database
                                .chunk_embedding_state(query.profile.as_deref(), &chunk.hash)?;
                            Ok((locate(database, chunk)?, fields, state))
                        })
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
                    .map(|((chunk, key), fields, state)| (chunk, (key, generation, fields, state)))
                    .collect(),
            });
        }
        let (mode, degraded) = prepared.strategy(requested, coverage)?;
        let mut hits = Vec::with_capacity(limit as usize);
        for (scope, chunk, (key, generation, fields, state), score) in merge(all, limit) {
            let mut hit = self
                .read_hit(&scope, (chunk, key), state, generation)
                .await?;
            hit.score = Some(score);
            hit.metadata = fields;
            hits.push((scope, hit));
        }
        telemetry::search(
            &tenant,
            None,
            start.elapsed(),
            mode,
            coverage,
            hits.len(),
            degraded,
        );
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
        let mut prepared =
            PreparedQuery::new(&query, limit, self.embedding_profile().map(str::to_owned))?;
        if let Some(filter) = filter {
            filter.validate()?;
        }
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
                    let (tenant, collection) = query_scope(scopes);
                    match telemetry::embed(
                        &tenant,
                        collection.as_ref(),
                        provider.as_ref(),
                        Purpose::Query,
                        &[query],
                    )
                    .await
                    {
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
        self.with_owned_collection(scope, move |db| read(&db)).await
    }

    pub(crate) async fn with_owned_collection<T: Send + 'static>(
        &self,
        scope: &Scope,
        read: impl FnOnce(db::CollectionDb) -> Result<T> + Send + 'static,
    ) -> Result<(T, Uuid)> {
        self.require_collection(scope).await?;
        let unit = Unit::collection(self.prefix.as_ref(), scope)?;
        let database = self
            .snapshots
            .read(&self.publication, &unit)
            .await?
            .ok_or_else(|| Error::NotFound("collection database was not found".into()))?;
        tokio::task::spawn_blocking(move || {
            let value = read(db::CollectionDb::open_read_only(database.path())?)?;
            Ok((value, database.generation.id))
        })
        .await
        .map_err(|error| Error::Internal(Box::new(error)))?
    }

    async fn read_hit(
        &self,
        scope: &Scope,
        (chunk, key): (Chunk, String),
        state: ChunkEmbeddingState,
        generation: Uuid,
    ) -> Result<Hit> {
        let excerpt = self.read_excerpt(scope, &chunk, key).await?;
        let mut hit = Hit::new(chunk, excerpt, state);
        hit.database_generation = Some(generation);
        Ok(hit)
    }

    pub(crate) async fn read_excerpt(
        &self,
        scope: &Scope,
        chunk: &Chunk,
        key: String,
    ) -> Result<String> {
        let key = self.checked_text_key(scope, key)?;
        let start = Instant::now();
        let bytes = self.storage.read_range(&key, chunk.range).await?;
        let excerpt = verified_excerpt(chunk, &bytes)?;
        tracing::trace!(
            event = "source_range_read",
            tenant = %scope.tenant,
            collection = %scope.collection,
            bytes = bytes.len(),
            duration_ms = start.elapsed().as_secs_f64() * 1_000.0,
            "read and verified a source range"
        );
        Ok(excerpt)
    }

    pub(crate) fn checked_text_key(&self, scope: &Scope, key: String) -> Result<Key> {
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
        Ok(key)
    }
}

pub(crate) fn locate(database: &db::CollectionDb, chunk: Chunk) -> Result<(Chunk, String)> {
    let key = database.hosted_text_key(chunk.reference.source, chunk.reference.revision)?;
    Ok((chunk, key))
}
