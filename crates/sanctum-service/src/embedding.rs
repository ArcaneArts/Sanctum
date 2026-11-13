use std::{collections::HashMap, sync::Arc, time::Duration};

use sanctum_core::{ByteRange, Error, Result, Scope};
use sanctum_db_sqlite::embedding::ScoredChunk;
use sanctum_embedding::{EmbedError, Provider, Purpose, embed};
use tokio::sync::watch;

use crate::{Chunk, ChunkRef, Hit, Service, metadata_filter::Filter};
pub use sanctum_db_sqlite::embedding::Coverage;

mod collections;
pub use collections::CollectionSearchResult;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SearchMode {
    Lexical,
    Semantic,
    Hybrid,
}

pub struct SearchResult {
    pub hits: Vec<Hit>,
    pub mode: SearchMode,
    pub coverage: Coverage,
    pub profile: Option<String>,
    pub degraded: Option<&'static str>,
}

struct PreparedQuery {
    expression: String,
    profile: Option<String>,
    vector: Option<Arc<[f32]>>,
    degraded: Option<&'static str>,
}

impl PreparedQuery {
    fn strategy(
        &self,
        requested: SearchMode,
        coverage: Coverage,
    ) -> Result<(SearchMode, Option<&'static str>)> {
        if self.vector.is_some() && coverage.embedded > 0 {
            return Ok((
                requested,
                (coverage.embedded < coverage.total).then_some("embedding_coverage_partial"),
            ));
        }
        if requested == SearchMode::Lexical {
            return Ok((requested, None));
        }
        let degraded = self.degraded.unwrap_or("embeddings_unavailable");
        if requested == SearchMode::Semantic {
            return Err(Error::Busy(degraded.into()));
        }
        Ok((SearchMode::Lexical, Some(degraded)))
    }
}

struct Batch {
    hashes: Vec<String>,
    texts: Vec<String>,
}

struct BatchOutcome {
    hashes: Vec<String>,
    result: std::result::Result<Vec<Vec<f32>>, EmbedError>,
}

impl Service {
    pub(crate) fn embedding_profile(&self) -> Option<&str> {
        self.embedding
            .as_ref()
            .map(|provider| provider.profile_id())
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
            .prepare_query(vec![scope.clone()], query, limit, filter.as_ref(), mode)
            .await?;

        let service = self.clone();
        blocking(move || {
            let _reader = service
                .retrieval
                .read()
                .map_err(|_| Error::Busy("retrieval lock is unavailable".into()))?;
            let metadata = match &filter {
                Some(filter) if filter.uses_collection_metadata() => {
                    service.metadata(&scope, None, None)?
                }
                _ => Vec::new(),
            };
            let db = service.database(&scope)?;
            let (ranked, actual_mode, coverage, degraded) = db.read_snapshot(|db| {
                let coverage = db.embedding_coverage(prepared.profile.as_deref())?;
                let (actual_mode, degraded) = prepared.strategy(mode, coverage)?;
                let candidate_limit = if actual_mode == SearchMode::Lexical {
                    limit
                } else {
                    (limit * 4).max(32)
                };
                let semantic = match (&prepared.vector, &prepared.profile) {
                    (Some(query), Some(profile)) if actual_mode != SearchMode::Lexical => db
                        .search_vector(
                            profile,
                            query.clone(),
                            candidate_limit,
                            filter.as_ref(),
                            &metadata,
                        )?,
                    _ => Vec::new(),
                };
                let lexical = if actual_mode == SearchMode::Semantic {
                    Vec::new()
                } else {
                    db.search(
                        &prepared.expression,
                        candidate_limit,
                        filter.as_ref(),
                        &metadata,
                    )?
                };
                let ranked = match actual_mode {
                    SearchMode::Lexical => lexical.into_iter().map(|chunk| (chunk, None)).collect(),
                    SearchMode::Semantic => semantic
                        .into_iter()
                        .map(|hit| (hit.chunk, Some(hit.score)))
                        .collect(),
                    SearchMode::Hybrid => fuse(lexical, semantic),
                };
                Ok((ranked, actual_mode, coverage, degraded))
            })?;
            let mut hits = Vec::with_capacity(limit as usize);
            let mut ranges: HashMap<_, Vec<ByteRange>> = HashMap::new();
            for (chunk, score) in ranked {
                if actual_mode != SearchMode::Lexical {
                    let ranges = ranges
                        .entry((chunk.reference.source, chunk.reference.revision))
                        .or_default();
                    if ranges
                        .iter()
                        .any(|range| range.start < chunk.range.end && chunk.range.start < range.end)
                    {
                        continue;
                    }
                    ranges.push(chunk.range);
                }
                let mut hit = service.hit(&scope, chunk)?;
                hit.score = score;
                hits.push(hit);
                if hits.len() == limit as usize {
                    break;
                }
            }
            Ok(SearchResult {
                hits,
                mode: actual_mode,
                coverage,
                profile: prepared.profile,
                degraded,
            })
        })
        .await
    }

    async fn prepare_query(
        &self,
        scopes: Vec<Scope>,
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
            profile: self.embedding_profile().map(str::to_owned),
            vector: None,
            degraded: None,
        };
        if mode == SearchMode::Lexical {
            return Ok(prepared);
        }
        let Some(provider) = self.embedding.clone() else {
            prepared.degraded = Some("embedding_disabled");
            return Ok(prepared);
        };
        let service = self.clone();
        let profile = provider.profile_id().to_owned();
        let available = blocking(move || {
            let mut available = false;
            for scope in scopes {
                let database = service.database(&scope)?;
                if !available {
                    available = database.has_embeddings(&profile)?;
                }
            }
            Ok(available)
        })
        .await?;
        if !available {
            prepared.degraded = Some("embeddings_unavailable");
            return Ok(prepared);
        }
        match embed(provider.as_ref(), Purpose::Query, &[query]).await {
            Ok(mut vectors) => prepared.vector = vectors.pop().map(Arc::from),
            Err(error) => {
                tracing::warn!(%error, "query embedding unavailable");
                prepared.degraded = Some("embedding_provider_unavailable");
            }
        }
        Ok(prepared)
    }

    fn embedding_batch(&self, scope: &Scope, profile: &str) -> Result<Batch> {
        let _reader = self
            .retrieval
            .read()
            .map_err(|_| Error::Busy("retrieval lock is unavailable".into()))?;
        let chunks = self.database(scope)?.pending_embeddings(profile)?;
        let mut batch = Batch {
            hashes: Vec::new(),
            texts: Vec::new(),
        };
        for chunk in chunks {
            let hit = self.hit(scope, chunk)?;
            batch.hashes.push(hit.chunk.hash);
            batch.texts.push(hit.excerpt);
        }
        Ok(batch)
    }

    fn save_embedding_batch(
        &self,
        scope: &Scope,
        profile: &str,
        outcomes: Vec<BatchOutcome>,
    ) -> Result<()> {
        let mut database = self.database(scope)?;
        let writer = database.write()?;
        let mut published = false;
        for outcome in outcomes {
            match outcome.result {
                Ok(vectors) => {
                    for (hash, vector) in outcome.hashes.iter().zip(vectors) {
                        writer.store_embedding(profile, hash, &vector)?;
                    }
                    published = true;
                }
                Err(error) => {
                    let retry = matches!(error, EmbedError::Transient(_));
                    let message = error.to_string();
                    for hash in outcome.hashes {
                        writer.embedding_failure(profile, &hash, &message, retry)?;
                    }
                }
            }
        }
        if published {
            writer.release_retirements(Some(profile))?;
        }
        writer.commit()
    }
}

pub async fn run_embedding_worker(service: Service, mut stop: watch::Receiver<bool>) {
    let Some(provider) = service.embedding.clone() else {
        return;
    };
    let profile = provider.profile_id().to_owned();
    while !*stop.borrow_and_update() {
        let current = service.clone();
        let scopes = blocking(move || {
            if current.queue_paused()? {
                Ok(Vec::new())
            } else {
                current.local_scopes()
            }
        })
        .await;
        let mut did_work = false;
        match scopes {
            Ok(scopes) => {
                for scope in scopes {
                    if *stop.borrow() {
                        return;
                    }
                    let current = service.clone();
                    let scoped = scope.clone();
                    let selected = profile.clone();
                    let batch = blocking(move || current.embedding_batch(&scoped, &selected)).await;
                    match batch {
                        Ok(batch) if !batch.texts.is_empty() => {
                            let outcomes = tokio::select! {
                                outcomes = embed_batch(provider.as_ref(), batch) => outcomes,
                                _ = stop.changed() => return,
                            };
                            let current = service.clone();
                            let selected = profile.clone();
                            if let Err(error) = blocking(move || {
                                current.save_embedding_batch(&scope, &selected, outcomes)
                            })
                            .await
                            {
                                tracing::warn!(%error, "embedding batch could not be saved");
                            } else {
                                did_work = true;
                            }
                        }
                        Ok(_) => {}
                        Err(error) => {
                            tracing::warn!(%error, "could not read pending embedding inputs")
                        }
                    }
                }
            }
            Err(error) => tracing::warn!(%error, "could not enumerate embedding work"),
        }
        if !did_work {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                changed = stop.changed() => if changed.is_err() { return; }
            }
        }
    }
}

async fn embed_batch(provider: &dyn Provider, batch: Batch) -> Vec<BatchOutcome> {
    let mut pending: Vec<_> = std::iter::once(0..batch.texts.len()).collect();
    let mut outcomes = Vec::new();
    while let Some(range) = pending.pop() {
        let result = embed(provider, Purpose::Document, &batch.texts[range.clone()]).await;
        if matches!(result, Err(EmbedError::Input(_))) && range.len() > 1 {
            let mid = range.start + range.len() / 2;
            pending.push(mid..range.end);
            pending.push(range.start..mid);
        } else {
            outcomes.push(BatchOutcome {
                hashes: batch.hashes[range].to_vec(),
                result,
            });
        }
    }
    outcomes
}

pub(crate) fn lexical_expression(query: &str, limit: u32) -> Result<String> {
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
    Ok(query
        .split_whitespace()
        .map(|word| format!("\"{}\"", word.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(" AND "))
}

async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| Error::Internal(Box::new(error)))?
}

fn fuse(lexical: Vec<Chunk>, semantic: Vec<ScoredChunk>) -> Vec<(Chunk, Option<f64>)> {
    let mut ranks: HashMap<ChunkRef, (Chunk, f64)> = HashMap::new();
    for list in [lexical, semantic.into_iter().map(|hit| hit.chunk).collect()] {
        for (rank, chunk) in list.into_iter().enumerate() {
            let contribution = 1.0 / (61 + rank) as f64;
            ranks
                .entry(chunk.reference.clone())
                .and_modify(|entry| entry.1 += contribution)
                .or_insert((chunk, contribution));
        }
    }
    let mut results: Vec<_> = ranks.into_values().collect();
    results.sort_by(|(left, lhs), (right, rhs)| {
        rhs.total_cmp(lhs)
            .then_with(|| left.reference.source.cmp(&right.reference.source))
            .then_with(|| left.reference.revision.cmp(&right.reference.revision))
            .then_with(|| left.reference.ordinal.cmp(&right.reference.ordinal))
    });
    results
        .into_iter()
        .map(|(chunk, score)| (chunk, Some(score)))
        .collect()
}
