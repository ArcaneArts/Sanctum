use std::sync::Arc;

use rusqlite::{Connection, functions::FunctionFlags, params_from_iter, types::Value};
use sanctum_core::Result;

use super::{Chunk, CollectionDb, database_error, metadata, metadata_filter, query_all, row_chunk};

pub struct ScoredChunk {
    pub chunk: Chunk,
    pub score: f64,
}

/// Ranks active chunks for one embedding profile after applying metadata filters.
pub trait VectorIndex {
    fn search_vector(
        &self,
        profile: &str,
        query: Arc<[f32]>,
        limit: u32,
        filter: Option<&metadata_filter::Filter>,
        collection_metadata: &[metadata::Entry],
    ) -> Result<Vec<ScoredChunk>>;
}

impl VectorIndex for CollectionDb {
    fn search_vector(
        &self,
        profile: &str,
        query: Arc<[f32]>,
        limit: u32,
        filter: Option<&metadata_filter::Filter>,
        collection_metadata: &[metadata::Entry],
    ) -> Result<Vec<ScoredChunk>> {
        register_dot_product(&self.0, query)?;
        search_candidates(
            &self.0,
            "sanctum_cosine(e.vector)",
            vec![profile.to_owned().into()],
            limit,
            filter,
            collection_metadata,
        )
    }
}

fn search_candidates(
    connection: &Connection,
    similarity: &str,
    mut arguments: Vec<Value>,
    limit: u32,
    filter: Option<&metadata_filter::Filter>,
    collection_metadata: &[metadata::Entry],
) -> Result<Vec<ScoredChunk>> {
    let mut sql = format!(
            "SELECT c.source_id, c.revision_id, c.ordinal, s.name, s.external_namespace,
                    s.external_id, c.start_byte, c.end_byte, c.content_hash, {similarity}
             FROM chunks c JOIN sources s ON s.id = c.source_id AND s.current_revision = c.revision_id
             JOIN embedding_cache e ON e.profile = ?1 AND e.input_hash = c.content_hash
             WHERE s.deleted = 0 AND e.vector IS NOT NULL",
        );
    if let Some(filter) = filter {
        metadata_filter::append_condition(
            connection,
            &mut sql,
            &mut arguments,
            filter,
            collection_metadata,
        )?;
    }
    sql.push_str(" ORDER BY 10 DESC, c.id LIMIT ?");
    arguments.push(i64::from(limit).into());
    query_all(connection, &sql, params_from_iter(arguments), |row| {
        Ok(ScoredChunk {
            chunk: row_chunk(row)?,
            score: row.get(9)?,
        })
    })
}

fn register_dot_product(connection: &Connection, query: Arc<[f32]>) -> Result<()> {
    connection
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
        .map_err(database_error)
}

#[cfg(test)]
mod tests;
