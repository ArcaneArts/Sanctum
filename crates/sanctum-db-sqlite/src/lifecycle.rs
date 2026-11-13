use rusqlite::{OptionalExtension, params};
use sanctum_core::{Error, ExternalSource, Result};
use uuid::Uuid;

use super::{CollectionDb, Revision, RevisionWriter, database_error, query_all};

pub const RETIRE_GRACE_SECONDS: i64 = 300;
const DELETION_RECEIPT_SECONDS: i64 = 24 * 60 * 60;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RevisionKey {
    pub source: Uuid,
    pub revision: Uuid,
}

pub struct SourceIdentity {
    pub source: Uuid,
    pub revision: Uuid,
    pub external: Option<ExternalSource>,
    pub hash: String,
}

impl CollectionDb {
    pub fn garbage(&self) -> Result<Vec<RevisionKey>> {
        query_all(
            &self.0,
            "SELECT source_id, revision_id FROM revision_garbage LIMIT 256",
            [],
            key,
        )
    }

    pub fn acknowledge_garbage(&self, revision: RevisionKey) -> Result<()> {
        self.0
            .execute(
                "DELETE FROM revision_garbage WHERE source_id = ?1 AND revision_id = ?2",
                params![revision.source.to_string(), revision.revision.to_string()],
            )
            .map_err(database_error)?;
        Ok(())
    }

    pub fn contains_revision(&self, revision: RevisionKey) -> Result<bool> {
        Ok(self
            .revision_summary(revision.source, revision.revision)?
            .is_some())
    }

    pub fn has_retirement_work(&self) -> Result<bool> {
        self.0
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM revisions WHERE retire_at <= unixepoch())
                 OR EXISTS(SELECT 1 FROM revision_garbage)
                 OR EXISTS(SELECT 1 FROM deleted_sources d WHERE d.expires_at <= unixepoch()
                     AND NOT EXISTS(SELECT 1 FROM sources WHERE id = d.id))",
                [],
                |row| row.get(0),
            )
            .map_err(database_error)
    }
}

impl RevisionWriter<'_> {
    pub fn find_source(
        &self,
        id: Option<Uuid>,
        external: Option<&ExternalSource>,
    ) -> Result<SourceIdentity> {
        let namespace = external.map(|value| value.namespace.as_str());
        let external_id = external.map(|value| value.id.as_str());
        let source = self
            .0
            .query_row(
                "SELECT s.id, s.current_revision, s.external_namespace, s.external_id, r.content_hash
             FROM sources s JOIN revisions r ON r.source_id = s.id AND r.id = s.current_revision
             WHERE s.deleted = 0 AND
                ((?1 IS NOT NULL AND s.id = ?1) OR
                 (?1 IS NULL AND s.external_namespace = ?2 AND s.external_id = ?3))",
                params![id.map(|value| value.to_string()), namespace, external_id],
                |row| {
                    let reference = key(row)?;
                    let external = match (row.get(2)?, row.get(3)?) {
                        (Some(namespace), Some(id)) => Some(ExternalSource { namespace, id }),
                        _ => None,
                    };
                    Ok(SourceIdentity {
                        source: reference.source,
                        revision: reference.revision,
                        external,
                        hash: row.get(4)?,
                    })
                },
            )
            .optional()
            .map_err(database_error)?
            .ok_or_else(|| Error::NotFound("source was not found".into()))?;
        Ok(source)
    }

    pub(super) fn replace_current(&self, data: &Revision) -> Result<()> {
        let previous = self.find_source(Some(data.source), None)?;
        self.0
            .execute(
                "UPDATE sources SET current_revision = ?2, name = ?3 WHERE id = ?1",
                params![
                    data.source.to_string(),
                    data.revision.to_string(),
                    data.name
                ],
            )
            .map_err(database_error)?;
        // Only the lexical stage is configured so far. Embedding readiness will
        // own this transition when an embedding profile is active.
        self.0
            .execute(
                "UPDATE revisions SET retire_at = unixepoch() + ?3
             WHERE source_id = ?1 AND id = ?2",
                params![
                    previous.source.to_string(),
                    previous.revision.to_string(),
                    RETIRE_GRACE_SECONDS
                ],
            )
            .map_err(database_error)?;
        Ok(())
    }

    pub(super) fn reject_deleted(&self, source: Uuid) -> Result<()> {
        let deleted: bool = self
            .0
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM deleted_sources WHERE id = ?1)",
                [source.to_string()],
                |row| row.get(0),
            )
            .map_err(database_error)?;
        if deleted {
            return Err(Error::Conflict("source was deleted".into()));
        }
        Ok(())
    }

    pub fn delete_source(&self, source: Uuid, expected: Option<Uuid>) -> Result<SourceIdentity> {
        let current = self.find_source(Some(source), None)?;
        if expected.is_some_and(|revision| revision != current.revision) {
            return Err(Error::StaleRevision(
                "the current source revision has changed".into(),
            ));
        }
        self.0
            .execute(
                "INSERT INTO deleted_sources (id, expires_at) VALUES (?1, unixepoch() + ?2)",
                params![source.to_string(), DELETION_RECEIPT_SECONDS],
            )
            .map_err(database_error)?;
        self.0.execute(
            "UPDATE sources SET deleted = 1, external_namespace = NULL, external_id = NULL WHERE id = ?1",
            [source.to_string()],
        ).map_err(database_error)?;
        self.0
            .execute(
                "UPDATE revisions SET retire_at = unixepoch() + ?2 WHERE source_id = ?1",
                params![source.to_string(), RETIRE_GRACE_SECONDS],
            )
            .map_err(database_error)?;
        Ok(current)
    }

    pub fn retirement_candidates(&self) -> Result<Vec<RevisionKey>> {
        query_all(
            &self.0,
            "SELECT r.source_id, r.id FROM revisions r
            JOIN sources s ON s.id = r.source_id
            WHERE r.retire_at <= unixepoch()
            ORDER BY (r.id = s.current_revision), r.retire_at LIMIT 256",
            [],
            key,
        )
    }

    pub fn expire_deletion_receipts(&self) -> Result<()> {
        self.0
            .execute(
                "DELETE FROM deleted_sources WHERE id IN (
               SELECT d.id FROM deleted_sources d WHERE d.expires_at <= unixepoch()
                 AND NOT EXISTS(SELECT 1 FROM sources WHERE id = d.id) LIMIT 256
             )",
                [],
            )
            .map_err(database_error)?;
        Ok(())
    }

    pub fn retire(&self, revision: RevisionKey) -> Result<()> {
        let source = revision.source.to_string();
        let rev = revision.revision.to_string();
        let is_held: bool = self
            .0
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sources WHERE id = ?1 AND current_revision = ?2
              AND (deleted = 0 OR EXISTS (
                  SELECT 1 FROM revisions WHERE source_id = ?1 AND id <> ?2
              )))",
                params![source, rev],
                |row| row.get(0),
            )
            .map_err(database_error)?;
        if is_held {
            return Ok(());
        }
        self.0
            .execute(
                "INSERT INTO revision_garbage (source_id, revision_id) VALUES (?1, ?2)
                ON CONFLICT DO NOTHING",
                params![source, rev],
            )
            .map_err(database_error)?;
        self.0
            .execute(
                "DELETE FROM chunks_fts WHERE rowid IN (
                SELECT id FROM chunks WHERE source_id = ?1 AND revision_id = ?2
             )",
                params![source, rev],
            )
            .map_err(database_error)?;
        self.0
            .execute(
                "DELETE FROM chunks WHERE source_id = ?1 AND revision_id = ?2",
                params![source, rev],
            )
            .map_err(database_error)?;
        self.0
            .execute(
                "DELETE FROM revisions WHERE source_id = ?1 AND id = ?2",
                params![source, rev],
            )
            .map_err(database_error)?;
        self.0
            .execute(
                "DELETE FROM sources WHERE id = ?1 AND deleted = 1
            AND NOT EXISTS (SELECT 1 FROM revisions WHERE source_id = sources.id)",
                [source],
            )
            .map_err(database_error)?;
        Ok(())
    }
}

fn key(row: &rusqlite::Row<'_>) -> rusqlite::Result<RevisionKey> {
    let uuid = |index| {
        let text: String = row.get(index)?;
        Uuid::parse_str(&text).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                index,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })
    };
    Ok(RevisionKey {
        source: uuid(0)?,
        revision: uuid(1)?,
    })
}
