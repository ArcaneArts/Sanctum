use std::time::Instant;

use sanctum_core::{ByteRange, Error, Result, Scope};
use sanctum_storage::{Key, range_len};
use uuid::Uuid;

use crate::{
    Chunk, ChunkEmbeddingState, Hit, HostedCatalog, hit_metadata::HitMetadata, verified_excerpt,
};

const MAX_COMBINED_RANGE: u64 = 64 * 1024;

pub(crate) struct RankedHit {
    pub(crate) scope: Scope,
    pub(crate) chunk: Chunk,
    pub(crate) generation: Uuid,
    pub(crate) score: Option<f64>,
    pub(crate) metadata: HitMetadata,
    pub(crate) state: ChunkEmbeddingState,
}

impl HostedCatalog {
    pub(crate) async fn read_ranked_hits(
        &self,
        hits: Vec<(String, RankedHit)>,
    ) -> Result<Vec<(Scope, Hit)>> {
        let mut ordered = hits
            .into_iter()
            .enumerate()
            .map(|(rank, (key, hit))| {
                let key = self.checked_text_key(&hit.scope, key)?;
                range_len(hit.chunk.range)?;
                Ok((key, rank, hit))
            })
            .collect::<Result<Vec<_>>>()?;
        ordered.sort_unstable_by(|(left_key, _, left), (right_key, _, right)| {
            left_key
                .cmp(right_key)
                .then_with(|| left.chunk.range.start.cmp(&right.chunk.range.start))
        });
        let mut ordered = ordered.into_iter().peekable();
        let mut results = Vec::with_capacity(ordered.len());
        while let Some((key, rank, hit)) = ordered.next() {
            let mut range = hit.chunk.range;
            let mut group = vec![(rank, hit)];
            while let Some((next_key, _, next)) = ordered.peek()
                && *next_key == key
                && let Some(combined) = combine(range, next.chunk.range)
            {
                let (_, rank, hit) = ordered.next().expect("next hit was present");
                group.push((rank, hit));
                range = combined;
            }
            results.extend(self.read_hit_range(&key, range, group).await?);
        }
        results.sort_unstable_by_key(|(rank, _)| *rank);
        Ok(results.into_iter().map(|(_, hit)| hit).collect())
    }

    async fn read_hit_range(
        &self,
        key: &Key,
        range: ByteRange,
        group: Vec<(usize, RankedHit)>,
    ) -> Result<Vec<(usize, (Scope, Hit))>> {
        let scope = &group[0].1.scope;
        let requested_bytes: u64 = group
            .iter()
            .map(|(_, hit)| hit.chunk.range.end - hit.chunk.range.start)
            .sum();
        let bytes = self
            .read_source_range(scope, key, range, requested_bytes, group.len())
            .await?;
        let bytes = bytes.as_ref();
        group
            .into_iter()
            .map(|(rank, hit)| {
                let chunk_range = hit.chunk.range;
                let start = (chunk_range.start - range.start) as usize;
                let end = (chunk_range.end - range.start) as usize;
                let excerpt = verified_excerpt(&hit.chunk, &bytes[start..end])?;
                let mut response = Hit::new(hit.chunk, excerpt, hit.state);
                response.database_generation = Some(hit.generation);
                response.score = hit.score;
                response.metadata = hit.metadata;
                Ok((rank, (hit.scope, response)))
            })
            .collect()
    }

    pub(crate) async fn read_excerpt(
        &self,
        scope: &Scope,
        chunk: &Chunk,
        key: String,
    ) -> Result<String> {
        let key = self.checked_text_key(scope, key)?;
        let bytes = self
            .read_source_range(scope, &key, chunk.range, chunk.range.length()?, 1)
            .await?;
        verified_excerpt(chunk, bytes.as_ref())
    }

    async fn read_source_range(
        &self,
        scope: &Scope,
        key: &Key,
        range: ByteRange,
        requested_bytes: u64,
        excerpts: usize,
    ) -> Result<impl AsRef<[u8]> + use<>> {
        let start = Instant::now();
        let bytes = self.storage.read_range(key, range).await?;
        if bytes.len() != range_len(range)? {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "stored source did not return the requested byte range",
            )));
        }
        tracing::trace!(
            event = "source_range_read", tenant = %scope.tenant, collection = %scope.collection,
            requested_bytes, bytes = bytes.len(), excerpts,
            duration_ms = start.elapsed().as_secs_f64() * 1_000.0,
            "read a source range for excerpts"
        );
        Ok(bytes)
    }
}

fn combine(first: ByteRange, next: ByteRange) -> Option<ByteRange> {
    let end = first.end.max(next.end);
    (next.start <= first.end && end - first.start <= MAX_COMBINED_RANGE).then_some(ByteRange {
        start: first.start,
        end,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn combined_ranges_cover_only_touching_bytes_and_stop_at_the_budget() {
        let range = |start, end| ByteRange { start, end };
        let first = range(10, 20);
        assert_eq!(combine(first, range(18, 24)), Some(range(10, 24)));
        assert_eq!(combine(first, range(20, 24)), Some(range(10, 24)));
        assert_eq!(combine(first, range(21, 24)), None);
        let limit = 10 + MAX_COMBINED_RANGE;
        assert_eq!(combine(first, range(20, limit)), Some(range(10, limit)));
        assert_eq!(combine(first, range(20, limit + 1)), None);
    }
}
