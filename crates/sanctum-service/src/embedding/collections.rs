use std::collections::{HashMap, HashSet};

use sanctum_core::{ByteRange, Error, Result, Scope, ScopeId};

use super::{Coverage, PreparedQuery, SearchMode, blocking, fuse};
use crate::{Chunk, Hit, Service, metadata_filter::Filter};

const MAX_COLLECTIONS: usize = 8;
const RANK_CONSTANT: usize = 60;

pub struct CollectionSearchResult {
    pub hits: Vec<(Scope, Hit)>,
    pub mode: SearchMode,
    pub coverage: Coverage,
    pub profile: Option<String>,
    pub degraded: Option<&'static str>,
}

struct Candidates {
    scope: Scope,
    coverage: Coverage,
    ranked: Vec<Chunk>,
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
        let hits = merge(self, all, limit)?;
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
    ) -> Result<Candidates> {
        let metadata = match filter {
            Some(filter) if filter.uses_collection_metadata() => {
                self.metadata(&scope, None, None)?
            }
            _ => Vec::new(),
        };
        let database = self.database(&scope)?;
        let (ranked, coverage) = database.read_snapshot(|db| {
            let coverage = db.embedding_coverage(prepared.profile.as_deref())?;
            let lexical = if mode != SearchMode::Semantic {
                db.search(&prepared.expression, limit, filter, &metadata)?
            } else {
                Vec::new()
            };
            let semantic = match (&prepared.vector, &prepared.profile) {
                (Some(vector), Some(profile)) if coverage.embedded > 0 => {
                    db.search_vector(profile, vector.clone(), limit, filter, &metadata)?
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
        })?;
        Ok(Candidates {
            scope,
            coverage,
            ranked,
        })
    }
}

fn merge(service: &Service, collections: Vec<Candidates>, limit: u32) -> Result<Vec<(Scope, Hit)>> {
    let mut ranked: Vec<_> = collections
        .into_iter()
        .flat_map(|collection| {
            let scope = collection.scope;
            collection
                .ranked
                .into_iter()
                .enumerate()
                .map(move |(index, chunk)| (index, scope.clone(), chunk))
        })
        .collect();
    ranked.sort_unstable_by(|(left, left_scope, _), (right, right_scope, _)| {
        left.cmp(right).then_with(|| {
            left_scope
                .collection
                .as_str()
                .cmp(right_scope.collection.as_str())
        })
    });
    let mut hits = Vec::with_capacity(limit as usize);
    let mut ranges: HashMap<_, Vec<ByteRange>> = HashMap::new();
    for (rank, scope, chunk) in ranked {
        let ranges = ranges
            .entry((
                scope.collection.clone(),
                chunk.reference.source,
                chunk.reference.revision,
            ))
            .or_default();
        if ranges
            .iter()
            .any(|range| range.start < chunk.range.end && chunk.range.start < range.end)
        {
            continue;
        }
        ranges.push(chunk.range);
        let mut hit = service.hit(&scope, chunk)?;
        hit.score = Some(1.0 / (RANK_CONSTANT + rank + 1) as f64);
        hits.push((scope, hit));
        if hits.len() == limit as usize {
            break;
        }
    }
    Ok(hits)
}
