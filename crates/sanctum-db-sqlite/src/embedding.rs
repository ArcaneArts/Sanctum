use rusqlite::{OptionalExtension, functions::FunctionFlags, params, params_from_iter};
use sanctum_core::{Error, Result};
use std::sync::Arc;
use uuid::Uuid;

use super::{
    Chunk, CollectionDb, RevisionWrite, database_error, metadata, metadata_filter, query_all,
    row_chunk, unsigned_column,
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

pub struct ScoredChunk {
    pub chunk: Chunk,
    pub score: f64,
}

impl CollectionDb {
    pub fn embedding_status(
        &self,
        profile: Option<&str>,
        source_revision: Option<(Uuid, Uuid)>,
    ) -> Result<EmbeddingStatus> {
        self.read_snapshot(|db| {
            let source = source_revision.map(|(source, _)| source.to_string());
            if let Some((_, revision)) = source_revision {
                let current: Option<String> = db.0.query_row(
                    "SELECT current_revision FROM sources WHERE id = ?1 AND deleted = 0",
                    [&source], |row| row.get(0),
                ).optional().map_err(database_error)?;
                match current {
                    None => return Err(Error::NotFound("source was not found".into())),
                    Some(current) if current != revision.to_string() => {
                        return Err(Error::StaleRevision("embedding status requires the current revision".into()));
                    }
                    _ => {}
                }
            }
            let condition = if source.is_some() { " AND s.id = ?2" } else { "" };
            let sql = format!(
                "SELECT count(*), count(e.vector), count(e.error), count(e.retry_at),
                        min(e.retry_at), min(e.error)
                 FROM chunks c JOIN sources s ON s.id = c.source_id AND s.current_revision = c.revision_id
                 LEFT JOIN embedding_cache e ON e.profile = ?1 AND e.input_hash = c.content_hash
                 WHERE s.deleted = 0{condition}"
            );
            let mut arguments = vec![profile];
            if let Some(source) = source.as_deref() {
                arguments.push(Some(source));
            }
            db.0.query_row(&sql, params_from_iter(arguments), |row| {
                Ok(EmbeddingStatus {
                    profile: profile.map(str::to_owned),
                    coverage: Coverage {
                        total: unsigned_column(row, 0)?,
                        embedded: unsigned_column(row, 1)?,
                        failed: unsigned_column(row, 2)?,
                    },
                    retrying: unsigned_column(row, 3)?,
                    next_retry: row.get::<_, Option<i64>>(4)?
                        .map(|time| u64::try_from(time)
                            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(4, time)))
                        .transpose()?,
                    sample_error: row.get(5)?,
                })
            }).map_err(database_error)
        })
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

    pub fn pending_embeddings(&self, profile: &str, limit: u32) -> Result<Vec<Chunk>> {
        query_all(
            &self.0,
            "SELECT c.source_id, c.revision_id, c.ordinal, s.name, s.external_namespace,
                    s.external_id, c.start_byte, c.end_byte, c.content_hash
             FROM chunks c JOIN sources s ON s.id = c.source_id AND s.current_revision = c.revision_id
             LEFT JOIN embedding_cache e ON e.profile = ?1 AND e.input_hash = c.content_hash
             WHERE s.deleted = 0 AND (e.input_hash IS NULL OR e.retry_at <= unixepoch())
             GROUP BY c.content_hash ORDER BY min(c.id) LIMIT ?2",
            params![profile, limit], row_chunk,
        )
    }

    pub fn search_vector(
        &self,
        profile: &str,
        query: Arc<[f32]>,
        limit: u32,
        filter: Option<&metadata_filter::Filter>,
        collection_metadata: &[metadata::Entry],
    ) -> Result<Vec<ScoredChunk>> {
        self.0
            .create_scalar_function(
                "sanctum_cosine",
                1,
                FunctionFlags::SQLITE_UTF8
                    | FunctionFlags::SQLITE_DETERMINISTIC
                    | FunctionFlags::SQLITE_DIRECTONLY,
                move |context| {
                    let bytes = context.get_raw(0).as_blob()?;
                    if bytes.len() != query.len() * 4 {
                        return Err(rusqlite::Error::UserFunctionError(
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "stored vector has an invalid length",
                            )
                            .into(),
                        ));
                    }
                    let score: f64 = bytes
                        .chunks_exact(4)
                        .zip(query.iter())
                        .map(|(bytes, &value)| {
                            f64::from(f32::from_le_bytes(
                                bytes.try_into().expect("four vector bytes"),
                            )) * f64::from(value)
                        })
                        .sum();
                    if !score.is_finite() {
                        return Err(rusqlite::Error::UserFunctionError(
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "stored vector is nonfinite",
                            )
                            .into(),
                        ));
                    }
                    Ok(score.clamp(-1.0, 1.0))
                },
            )
            .map_err(database_error)?;
        let mut sql = String::from(
            "SELECT c.source_id, c.revision_id, c.ordinal, s.name, s.external_namespace,
                    s.external_id, c.start_byte, c.end_byte, c.content_hash, sanctum_cosine(e.vector)
             FROM chunks c JOIN sources s ON s.id = c.source_id AND s.current_revision = c.revision_id
             JOIN embedding_cache e ON e.profile = ?1 AND e.input_hash = c.content_hash
             WHERE s.deleted = 0 AND e.vector IS NOT NULL",
        );
        let mut arguments = vec![profile.to_owned().into()];
        if let Some(filter) = filter {
            metadata_filter::append_condition(
                &self.0,
                &mut sql,
                &mut arguments,
                filter,
                collection_metadata,
            )?;
        }
        sql.push_str(" ORDER BY 10 DESC, c.id LIMIT ?");
        arguments.push(i64::from(limit).into());
        query_all(&self.0, &sql, params_from_iter(arguments), |row| {
            Ok(ScoredChunk {
                chunk: row_chunk(row)?,
                score: row.get(9)?,
            })
        })
    }
}

impl<T: std::ops::Deref<Target = rusqlite::Connection>> RevisionWrite<T> {
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
               vector = excluded.vector, error = NULL, attempts = 0, retry_at = NULL",
                params![profile, hash, bytes],
            )
            .map_err(database_error)?;
        Ok(())
    }

    pub fn embedding_failure(
        &self,
        profile: &str,
        hash: &str,
        message: &str,
        transient: bool,
    ) -> Result<()> {
        self.0
            .execute(
                "INSERT INTO embedding_cache (profile, input_hash, error, attempts, retry_at)
             SELECT ?1, ?2, ?3, 1, CASE WHEN ?4 THEN unixepoch() + 5 END
             WHERE EXISTS (
               SELECT 1 FROM chunks c JOIN sources s ON s.id = c.source_id
                 AND s.current_revision = c.revision_id
               WHERE s.deleted = 0 AND c.content_hash = ?2
             )
             ON CONFLICT (profile, input_hash) DO UPDATE SET
               error = excluded.error, attempts = min(embedding_cache.attempts + 1, 10),
               retry_at = CASE WHEN ?4 THEN
                 unixepoch() + min(300, 5 * (1 << embedding_cache.attempts))
               END
             WHERE embedding_cache.vector IS NULL",
                params![profile, hash, message, transient],
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
