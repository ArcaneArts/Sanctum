use rusqlite::{OptionalExtension, Row, params};
use sanctum_core::{Error, ExternalSource, Result};
use uuid::Uuid;

use super::{
    CollectionDb, Revision, database_error, embedding::EmbeddingStatus, query_all,
    row_revision_content, row_uuid, schema_version, unsigned_column,
};

pub struct SourceStatus {
    pub revision: Revision,
    pub embedding: EmbeddingStatus,
    pub retained_revisions: u64,
    pub pending_revision_cleanup: u64,
    pub pending_object_cleanup: Option<u64>,
}

impl SourceStatus {
    pub fn ready(&self) -> bool {
        self.embedding.profile.is_none()
            || self.embedding.coverage.embedded == self.embedding.coverage.total
    }
}

const CURRENT: &str =
    "SELECT s.id, s.current_revision, s.name, s.external_namespace, s.external_id,
    r.content_hash, r.chunk_count, r.bytes, COALESCE(r.original_hash, r.content_hash),
    r.source_encoding, r.extractor, r.retain_original, r.url_provenance, r.git_provenance, r.chunker
    FROM sources s JOIN revisions r ON r.source_id = s.id AND r.id = s.current_revision
    WHERE s.deleted = 0";

impl CollectionDb {
    pub fn readable_revision_bytes(&self, source: Uuid, revision: Uuid) -> Result<u64> {
        let bytes = self
            .0
            .query_row(
                "SELECT r.bytes FROM sources s JOIN revisions r ON r.source_id = s.id
                 WHERE s.id = ?1 AND r.id = ?2 AND s.deleted = 0",
                params![source.to_string(), revision.to_string()],
                |row| unsigned_column(row, 0),
            )
            .optional()
            .map_err(database_error)?;
        if let Some(bytes) = bytes {
            return Ok(bytes);
        }
        self.check_revision_staleness(source, revision)?;
        Err(Error::NotFound("source revision was not found".into()))
    }

    pub fn source_status(
        &self,
        id: Uuid,
        expected: Option<Uuid>,
        profile: Option<&str>,
    ) -> Result<SourceStatus> {
        self.read_snapshot(|db| {
            let revision = db.source(id, expected)?;
            let source = id.to_string();
            let embedding = db.read_embedding_status(profile, Some(&source))?;
            let (retained_revisions, pending_revision_cleanup) =
                db.0.query_row(
                    "SELECT
                    (SELECT count(*) FROM revisions WHERE source_id = ?1 AND id <> ?2),
                    (SELECT count(*) FROM revision_garbage WHERE source_id = ?1)",
                    params![source, revision.revision.to_string()],
                    |row| Ok((unsigned_column(row, 0)?, unsigned_column(row, 1)?)),
                )
                .map_err(database_error)?;
            let pending_object_cleanup = if schema_version(&db.0)? >= 12 {
                db.0.query_row(
                    "SELECT CASE WHEN EXISTS (
                        SELECT 1 FROM hosted_object_garbage WHERE source_id IS NULL
                     ) THEN NULL ELSE (
                        SELECT count(*) FROM hosted_object_garbage WHERE source_id = ?1
                     ) END",
                    [&source],
                    |row| match row.get_ref(0)? {
                        rusqlite::types::ValueRef::Null => Ok(None),
                        _ => unsigned_column(row, 0).map(Some),
                    },
                )
                .map_err(database_error)?
            } else {
                None
            };
            Ok(SourceStatus {
                revision,
                embedding,
                retained_revisions,
                pending_revision_cleanup,
                pending_object_cleanup,
            })
        })
    }

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
