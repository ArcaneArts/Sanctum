use rusqlite::{OptionalExtension, params, params_from_iter};
use sanctum_core::{Error, Result};
use uuid::Uuid;

use super::{
    Chunk, CollectionDb, RevisionWrite, database_error, query_all, row_chunk, unsigned_column,
};

#[derive(Clone, Copy, Debug, Default)]
pub struct Coverage {
    pub total: u64,
    pub embedded: u64,
    pub failed: u64,
}

pub struct EmbeddingStatus {
    pub profile: Option<String>,
    pub coverage: Coverage,
    pub retrying: u64,
    pub next_retry: Option<u64>,
    pub sample_error: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChunkEmbeddingState {
    Disabled,
    Pending,
    Ready,
    Retrying,
    Failed,
}

pub struct PendingEmbedding {
    pub chunk: Chunk,
    pub retry: Option<EmbeddingRetry>,
}

#[derive(Clone, Copy)]
pub struct EmbeddingRetry {
    attempts: i64,
    at: i64,
}

pub use crate::vector_index::ScoredChunk;

impl CollectionDb {
    pub fn embedding_status(
        &self,
        profile: Option<&str>,
        source_revision: Option<(Uuid, Uuid)>,
    ) -> Result<EmbeddingStatus> {
        self.read_snapshot(|db| {
            let source = source_revision.map(|(source, _)| source.to_string());
            if let Some((_, revision)) = source_revision {
                let current: Option<String> =
                    db.0.query_row(
                        "SELECT current_revision FROM sources WHERE id = ?1 AND deleted = 0",
                        [&source],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(database_error)?;
                match current {
                    None => return Err(Error::NotFound("source was not found".into())),
                    Some(current) if current != revision.to_string() => {
                        return Err(Error::StaleRevision(
                            "embedding status requires the current revision".into(),
                        ));
                    }
                    _ => {}
                }
            }
            db.read_embedding_status(profile, source.as_deref())
        })
    }

    pub(super) fn read_embedding_status(
        &self,
        profile: Option<&str>,
        source: Option<&str>,
    ) -> Result<EmbeddingStatus> {
        let condition = if source.is_some() {
            " AND s.id = ?2"
        } else {
            ""
        };
        let sql = format!(
                "SELECT count(*), count(e.vector), count(e.error), count(e.retry_at),
                        min(e.retry_at), min(e.error)
                 FROM chunks c JOIN sources s ON s.id = c.source_id AND s.current_revision = c.revision_id
                 LEFT JOIN embedding_cache e ON e.profile = ?1 AND e.input_hash = c.content_hash
                 WHERE s.deleted = 0{condition}"
            );
        let mut arguments = vec![profile];
        if let Some(source) = source {
            arguments.push(Some(source));
        }
        self.0
            .query_row(&sql, params_from_iter(arguments), |row| {
                Ok(EmbeddingStatus {
                    profile: profile.map(str::to_owned),
                    coverage: Coverage {
                        total: unsigned_column(row, 0)?,
                        embedded: unsigned_column(row, 1)?,
                        failed: unsigned_column(row, 2)?,
                    },
                    retrying: unsigned_column(row, 3)?,
                    next_retry: row
                        .get::<_, Option<i64>>(4)?
                        .map(|time| {
                            u64::try_from(time)
                                .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(4, time))
                        })
                        .transpose()?,
                    sample_error: row.get(5)?,
                })
            })
            .map_err(database_error)
    }

    pub fn chunk_embedding_state(
        &self,
        profile: Option<&str>,
        hash: &str,
    ) -> Result<ChunkEmbeddingState> {
        use ChunkEmbeddingState::{Disabled, Failed, Pending, Ready, Retrying};
        let Some(profile) = profile else {
            return Ok(Disabled);
        };
        let state = self
            .0
            .prepare_cached(
                "SELECT vector IS NOT NULL, retry_at IS NOT NULL, error IS NOT NULL
                 FROM embedding_cache WHERE profile = ?1 AND input_hash = ?2",
            )
            .map_err(database_error)?
            .query_row([profile, hash], |row| {
                Ok(match (row.get(0)?, row.get(1)?, row.get(2)?) {
                    (true, _, _) => Ready,
                    (_, true, _) => Retrying,
                    (_, _, true) => Failed,
                    _ => Pending,
                })
            })
            .optional()
            .map_err(database_error)?;
        Ok(state.unwrap_or(Pending))
    }

    pub fn has_embeddings(&self, profile: &str) -> Result<bool> {
        self.0
            .query_row(
                "SELECT EXISTS (
                SELECT 1 FROM embedding_cache e
                JOIN chunks c ON c.content_hash = e.input_hash
                JOIN sources s ON s.id = c.source_id AND s.current_revision = c.revision_id
                WHERE e.profile = ?1 AND e.vector IS NOT NULL AND s.deleted = 0
            )",
                [profile],
                |row| row.get(0),
            )
            .map_err(database_error)
    }

    pub fn embedding_coverage(&self, profile: Option<&str>) -> Result<Coverage> {
        let Some(profile) = profile else {
            let total = self
                .0
                .query_row(
                    "SELECT COALESCE(sum(r.chunk_count), 0)
                 FROM sources s JOIN revisions r ON r.source_id = s.id AND r.id = s.current_revision
                 WHERE s.deleted = 0",
                    [],
                    |row| unsigned_column(row, 0),
                )
                .map_err(database_error)?;
            return Ok(Coverage {
                total,
                ..Coverage::default()
            });
        };
        self.0.query_row(
            "SELECT count(*), count(e.vector), count(e.error)
             FROM chunks c JOIN sources s ON s.id = c.source_id AND s.current_revision = c.revision_id
             LEFT JOIN embedding_cache e ON e.profile = ?1 AND e.input_hash = c.content_hash
             WHERE s.deleted = 0",
            [profile], |row| Ok(Coverage {
                total: unsigned_column(row, 0)?,
                embedded: unsigned_column(row, 1)?,
                failed: unsigned_column(row, 2)?,
            }),
        ).map_err(database_error)
    }

    pub fn pending_embeddings(&self, profile: &str, limit: u32) -> Result<Vec<PendingEmbedding>> {
        query_all(
            &self.0,
            "SELECT c.source_id, c.revision_id, c.ordinal, s.name, s.external_namespace,
                    s.external_id, c.start_byte, c.end_byte, c.content_hash, e.attempts, e.retry_at
             FROM chunks c JOIN sources s ON s.id = c.source_id AND s.current_revision = c.revision_id
             LEFT JOIN embedding_cache e ON e.profile = ?1 AND e.input_hash = c.content_hash
             WHERE s.deleted = 0 AND
                 (e.input_hash IS NULL OR (e.vector IS NULL AND e.retry_at <= unixepoch()))
             GROUP BY c.content_hash ORDER BY min(c.id) LIMIT ?2",
            params![profile, limit], |row| {
                let retry = row.get::<_, Option<i64>>(9)?.map(|attempts| {
                    Ok::<_, rusqlite::Error>(EmbeddingRetry { attempts, at: row.get(10)? })
                }).transpose()?;
                Ok(PendingEmbedding { chunk: row_chunk(row)?, retry })
            },
        )
    }
}

impl<T: std::ops::Deref<Target = rusqlite::Connection>> RevisionWrite<T> {
    pub fn cached_revision_chunks(
        &self,
        source: Uuid,
        revision: Uuid,
        profile: &str,
    ) -> Result<u64> {
        self.0
            .query_row(
                "SELECT count(*) FROM chunks c JOIN embedding_cache e
                    ON e.profile = ?3 AND e.input_hash = c.content_hash AND e.vector IS NOT NULL
                 WHERE c.source_id = ?1 AND c.revision_id = ?2",
                params![source.to_string(), revision.to_string(), profile],
                |row| unsigned_column(row, 0),
            )
            .map_err(database_error)
    }

    pub fn store_embedding(&self, profile: &str, hash: &str, vector: &[f32]) -> Result<()> {
        let bytes: Vec<u8> = vector
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect();
        self.0
            .execute(
                "INSERT INTO embedding_cache (profile, input_hash, vector)
             SELECT ?1, ?2, ?3 WHERE EXISTS (
               SELECT 1 FROM chunks c JOIN sources s ON s.id = c.source_id
                 AND s.current_revision = c.revision_id
               WHERE s.deleted = 0 AND c.content_hash = ?2
             )
             ON CONFLICT (profile, input_hash) DO UPDATE SET
               vector = excluded.vector, error = NULL, attempts = 0, retry_at = NULL
             WHERE embedding_cache.vector IS NULL",
                params![profile, hash, bytes],
            )
            .map_err(database_error)?;
        Ok(())
    }

    pub fn embedding_failure(
        &self,
        profile: &str,
        hash: &str,
        selected: Option<EmbeddingRetry>,
        message: &str,
        transient: bool,
    ) -> Result<()> {
        // Several workers can prepare the same input outside the writer lease.
        // Count a failure only if its observed retry state is still current.
        self.0
            .execute(
                "INSERT INTO embedding_cache (profile, input_hash, error, attempts, retry_at)
             SELECT ?1, ?2, ?3, 1, CASE WHEN ?4 THEN unixepoch() + 5 END
             WHERE EXISTS (
               SELECT 1 FROM chunks c JOIN sources s ON s.id = c.source_id
                 AND s.current_revision = c.revision_id
               WHERE s.deleted = 0 AND c.content_hash = ?2
             )
             AND (?5 IS NULL OR EXISTS (
               SELECT 1 FROM embedding_cache
               WHERE profile = ?1 AND input_hash = ?2
             ))
             ON CONFLICT (profile, input_hash) DO UPDATE SET
               error = excluded.error, attempts = min(embedding_cache.attempts + 1, 10),
               retry_at = CASE WHEN ?4 THEN
                 unixepoch() + min(300, 5 * (1 << embedding_cache.attempts))
               END
             WHERE embedding_cache.vector IS NULL
               AND embedding_cache.attempts = ?5 AND embedding_cache.retry_at = ?6",
                params![
                    profile,
                    hash,
                    message,
                    transient,
                    selected.map(|retry| retry.attempts),
                    selected.map(|retry| retry.at),
                ],
            )
            .map_err(database_error)?;
        Ok(())
    }

    pub fn release_retirements(&self, profile: Option<&str>) -> Result<()> {
        self.0.execute(
            "UPDATE revisions AS r SET retire_at = unixepoch() + ?2
             WHERE retire_at IS NULL AND EXISTS (
                SELECT 1 FROM sources s WHERE s.id = r.source_id AND s.deleted = 0
                 AND s.current_revision <> r.id AND (?1 IS NULL OR NOT EXISTS (
                     SELECT 1 FROM chunks c LEFT JOIN embedding_cache e
                        ON e.profile = ?1 AND e.input_hash = c.content_hash AND e.vector IS NOT NULL
                     WHERE c.source_id = s.id AND c.revision_id = s.current_revision AND e.input_hash IS NULL
                 ))
             )",
            params![profile, super::lifecycle::RETIRE_GRACE_SECONDS],
        ).map_err(database_error)?;
        Ok(())
    }

    pub fn prune_unreferenced_embeddings(&self) -> Result<()> {
        self.0
            .execute(
                "DELETE FROM embedding_cache WHERE NOT EXISTS (
                SELECT 1 FROM chunks WHERE chunks.content_hash = embedding_cache.input_hash
             )",
                [],
            )
            .map_err(database_error)?;
        Ok(())
    }
}

/// Writes a provider batch inside the caller's fenced collection transaction.
pub fn apply_hosted_embeddings(
    transaction: &rusqlite::Transaction<'_>,
    profile: &str,
    batch: impl FnOnce(&RevisionWrite<&rusqlite::Connection>) -> Result<()>,
) -> Result<()> {
    let writer = RevisionWrite(&**transaction);
    batch(&writer)?;
    writer.release_retirements(Some(profile))
}
