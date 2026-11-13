use rusqlite::{OptionalExtension, Row, params};
use sanctum_core::{Error, ExternalSource, Result};
use uuid::Uuid;

use super::{CollectionDb, Revision, database_error, query_all, row_revision_content, row_uuid};

const CURRENT: &str =
    "SELECT s.id, s.current_revision, s.name, s.external_namespace, s.external_id,
    r.content_hash, r.chunk_count, r.bytes, COALESCE(r.original_hash, r.content_hash),
    r.source_encoding, r.extractor, r.retain_original, r.url_provenance, r.git_provenance, r.chunker
    FROM sources s JOIN revisions r ON r.source_id = s.id AND r.id = s.current_revision
    WHERE s.deleted = 0";

impl CollectionDb {
    pub fn list_sources(&self, after: Option<Uuid>, limit: u32) -> Result<Vec<Revision>> {
        query_all(
            &self.0,
            &format!("{CURRENT} AND s.id > ?1 ORDER BY s.id LIMIT ?2"),
            params![after.map(|id| id.to_string()).unwrap_or_default(), limit],
            current_revision,
        )
    }

    pub fn source(&self, id: Uuid, expected: Option<Uuid>) -> Result<Revision> {
        let current = self
            .0
            .query_row(
                &format!("{CURRENT} AND s.id = ?1"),
                [id.to_string()],
                current_revision,
            )
            .optional()
            .map_err(database_error)?
            .ok_or_else(|| Error::NotFound("source was not found".into()))?;
        if expected.is_some_and(|id| id != current.revision) {
            return Err(Error::StaleRevision("source revision has changed".into()));
        }
        Ok(current)
    }
}

fn current_revision(row: &Row<'_>) -> rusqlite::Result<Revision> {
    let external = match (row.get(3)?, row.get(4)?) {
        (Some(namespace), Some(id)) => Some(ExternalSource { namespace, id }),
        _ => None,
    };
    Ok(Revision {
        source: row_uuid(row, 0)?,
        revision: row_uuid(row, 1)?,
        name: row.get(2)?,
        external,
        content: row_revision_content(row, 5)?,
    })
}
