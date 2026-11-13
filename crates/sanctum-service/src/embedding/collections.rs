use std::collections::{HashMap, HashSet};

use sanctum_core::{ByteRange, Error, Result, Scope, ScopeId};

use super::{Coverage, PreparedQuery, SearchMode, blocking, fuse};
use crate::{
    Chunk, ChunkEmbeddingState, Hit, HitMetadata, Service, db, hit_metadata::metadata_for_hit,
    metadata_filter::Filter,
};

const MAX_COLLECTIONS: usize = 8;
const RANK_CONSTANT: usize = 60;

pub struct CollectionSearchResult {
    pub hits: Vec<(Scope, Hit)>,
    pub mode: SearchMode,
    pub coverage: Coverage,
    pub profile: Option<String>,
    pub degraded: Option<&'static str>,
}

pub(crate) struct Candidates<T> {
    pub(crate) scope: Scope,
    pub(crate) coverage: Coverage,
    pub(crate) ranked: Vec<(Chunk, T)>,
}

impl Service {
    pub async fn retrieve_collections(
        &self,
        tenant: ScopeId,
        collections: Vec<ScopeId>,
        query: String,
        limit: u32,
        filter: Option<Filter>,
        mode: SearchMode,
    ) -> Result<CollectionSearchResult> {
        let scopes = collection_scopes(tenant, collections)?;
        let prepared = self
            .prepare_query(scopes.clone(), query, limit, filter.as_ref(), mode)
            .await?;
        let service = self.clone();
        blocking(move || service.rank_collections(scopes, prepared, limit, filter.as_ref(), mode))
            .await
    }

    fn rank_collections(
        &self,
        scopes: Vec<Scope>,
        prepared: PreparedQuery,
        limit: u32,
        filter: Option<&Filter>,
        requested: SearchMode,
    ) -> Result<CollectionSearchResult> {
        let _reader = self
            .retrieval
            .read()
            .map_err(|_| Error::Busy("retrieval lock is unavailable".into()))?;
        let candidate_limit = (limit * 4).max(32);
        let mut coverage = Coverage::default();
        let mut all = Vec::with_capacity(scopes.len());
        for scope in scopes {
            let list =
                self.collection_candidates(scope, &prepared, candidate_limit, filter, requested)?;
            coverage.total += list.coverage.total;
            coverage.embedded += list.coverage.embedded;
            coverage.failed += list.coverage.failed;
            all.push(list);
        }
        let (mode, degraded) = prepared.strategy(requested, coverage)?;
        let hits = merge(all, limit)
            .map(|(scope, chunk, (metadata, state), score)| {
                let mut hit = self.hit(&scope, chunk, state)?;
                hit.metadata = metadata;
                hit.score = Some(score);
                Ok((scope, hit))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(CollectionSearchResult {
            hits,
            mode,
            coverage,
            profile: prepared.profile,
            degraded,
        })
    }

    fn collection_candidates(
        &self,
        scope: Scope,
        prepared: &PreparedQuery,
        limit: u32,
        filter: Option<&Filter>,
        mode: SearchMode,
    ) -> Result<Candidates<(HitMetadata, ChunkEmbeddingState)>> {
        let metadata = match filter {
            Some(filter) if filter.uses_collection_metadata() => {
                self.metadata(&scope, None, None)?
            }
            _ => Vec::new(),
        };
        let database = self.database(&scope)?;
        database.read_snapshot(|db| {
            let (ranked, coverage) = rank_collection(db, prepared, limit, filter, &metadata, mode)?;
            let ranked = ranked
                .into_iter()
                .map(|chunk| {
                    let fields = metadata_for_hit(db, &chunk.reference, filter, &metadata)?;
                    let state =
                        db.chunk_embedding_state(prepared.profile.as_deref(), &chunk.hash)?;
                    Ok((chunk, (fields, state)))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Candidates {
                scope,
                coverage,
                ranked,
            })
        })
    }
}

pub(crate) fn rank_collection(
    database: &db::CollectionDb,
    prepared: &PreparedQuery,
    limit: u32,
    filter: Option<&Filter>,
    metadata: &[db::metadata::Entry],
    mode: SearchMode,
) -> Result<(Vec<Chunk>, Coverage)> {
    let coverage = database.embedding_coverage(prepared.profile.as_deref())?;
    let lexical = if mode != SearchMode::Semantic {
        database.search(&prepared.expression, limit, filter, metadata)?
    } else {
        Vec::new()
    };
    let semantic = match (&prepared.vector, &prepared.profile) {
        (Some(vector), Some(profile)) if coverage.embedded > 0 => {
            database.search_vector(profile, vector.clone(), limit, filter, metadata)?
        }
        _ => Vec::new(),
    };
    let ranked = match mode {
        SearchMode::Hybrid if !semantic.is_empty() => fuse(lexical, semantic)
            .into_iter()
            .map(|(chunk, _)| chunk)
            .collect(),
        SearchMode::Semantic => semantic.into_iter().map(|hit| hit.chunk).collect(),
        _ => lexical,
    };
    Ok((ranked, coverage))
}

pub(crate) fn collection_scopes(tenant: ScopeId, collections: Vec<ScopeId>) -> Result<Vec<Scope>> {
    if !(1..=MAX_COLLECTIONS).contains(&collections.len()) {
        return Err(Error::Invalid(
            "select between one and eight collections".into(),
        ));
    }
    let unique: HashSet<_> = collections.iter().collect();
    if unique.len() != collections.len() {
        return Err(Error::Invalid("collection IDs must be unique".into()));
    }
    let mut scopes: Vec<_> = collections
        .into_iter()
        .map(|collection| Scope {
            tenant: tenant.clone(),
            collection,
        })
        .collect();
    scopes.sort_by(|left, right| left.collection.as_str().cmp(right.collection.as_str()));
    Ok(scopes)
}

pub(crate) fn merge<T>(
    collections: Vec<Candidates<T>>,
    limit: u32,
) -> impl Iterator<Item = (Scope, Chunk, T, f64)> {
    let mut ranked: Vec<_> = collections
        .into_iter()
        .flat_map(|collection| {
            let scope = collection.scope;
            collection
                .ranked
                .into_iter()
                .enumerate()
                .map(move |(index, (chunk, extra))| (index, scope.clone(), chunk, extra))
        })
        .collect();
    ranked.sort_unstable_by(|(left, left_scope, _, _), (right, right_scope, _, _)| {
        left.cmp(right).then_with(|| {
            left_scope
                .collection
                .as_str()
                .cmp(right_scope.collection.as_str())
        })
    });
    let mut ranges: HashMap<_, Vec<ByteRange>> = HashMap::new();
    ranked
        .into_iter()
        .filter_map(move |(rank, scope, chunk, extra)| {
            let accepted = ranges
                .entry((
                    scope.collection.clone(),
                    chunk.reference.source,
                    chunk.reference.revision,
                ))
                .or_default();
            if accepted
                .iter()
                .any(|range| range.start < chunk.range.end && chunk.range.start < range.end)
            {
                return None;
            }
            accepted.push(chunk.range);
            let score = 1.0 / (RANK_CONSTANT + rank + 1) as f64;
            Some((scope, chunk, extra, score))
        })
        .take(limit as usize)
}
