use rusqlite::{Transaction, params};
use sanctum_core::{Error, ExternalSource, Result};
use uuid::Uuid;

use crate::{RevisionContent, database_error, initialize_collection, lifecycle::find_source};

pub enum Target {
    Create(Uuid),
    Replace {
        source: Option<Uuid>,
        expected_revision: Option<Uuid>,
    },
}

pub struct RevisionIntent {
    pub operation: Uuid,
    pub revision: Uuid,
    pub target: Target,
    pub external: Option<ExternalSource>,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Registration {
    pub source: Uuid,
    pub external: Option<ExternalSource>,
    pub update_sequence: u64,
}

pub fn register_revision(
    transaction: &Transaction<'_>,
    intent: RevisionIntent,
) -> Result<Registration> {
    initialize_collection(transaction)?;
    let (source, external, creating) = match intent.target {
        Target::Create(source) => {
            if let Some(external) = &intent.external {
                require_unclaimed(transaction, external)?;
            }
            (source, intent.external, true)
        }
        Target::Replace {
            source,
            expected_revision,
        } => {
            let current = find_source(transaction, source, intent.external.as_ref())?;
            if expected_revision.is_some_and(|revision| revision != current.revision) {
                return Err(Error::StaleRevision(
                    "the current source revision has changed".into(),
                ));
            }
            if intent
                .external
                .as_ref()
                .is_some_and(|external| Some(external) != current.external.as_ref())
            {
                return Err(Error::Conflict(
                    "replacement must preserve the external source reference".into(),
                ));
            }
            (current.source, current.external, false)
        }
    };

    let update_sequence = transaction
        .query_row(
            "INSERT INTO hosted_revision_intents (source_id, operation_id, revision_id,
                update_sequence, creating, external_namespace, external_id)
             VALUES (?1, ?2, ?3, 1, ?4, ?5, ?6)
             ON CONFLICT (source_id) DO UPDATE SET
                operation_id = excluded.operation_id, revision_id = excluded.revision_id,
                update_sequence = update_sequence + 1, creating = excluded.creating,
                external_namespace = excluded.external_namespace, external_id = excluded.external_id
             RETURNING update_sequence",
            params![
                source.to_string(),
                intent.operation.to_string(),
                intent.revision.to_string(),
                creating,
                external
                    .as_ref()
                    .map(|reference| reference.namespace.as_str()),
                external.as_ref().map(|reference| reference.id.as_str()),
            ],
            |row| crate::unsigned_column(row, 0),
        )
        .map_err(database_error)?;
    Ok(Registration {
        source,
        external,
        update_sequence,
    })
}

fn require_unclaimed(transaction: &Transaction<'_>, external: &ExternalSource) -> Result<()> {
    let claimed = transaction
        .query_row(
            "SELECT EXISTS (
                SELECT 1 FROM sources WHERE deleted = 0 AND external_namespace = ?1 AND external_id = ?2
                UNION ALL
                SELECT 1 FROM hosted_revision_intents WHERE creating = 1 AND external_namespace = ?1 AND external_id = ?2
            )",
            params![external.namespace, external.id],
            |row| row.get::<_, bool>(0),
        )
        .map_err(database_error)?;
    if claimed {
        return Err(Error::Conflict("external source already exists".into()));
    }
    Ok(())
}

pub struct RevisionObjects {
    pub text_key: String,
    pub original_key: Option<String>,
}

impl crate::CollectionDb {
    pub fn hosted_text_key(&self, source: Uuid, revision: Uuid) -> Result<String> {
        use rusqlite::OptionalExtension;
        self.0
            .query_row(
                "SELECT text_key FROM hosted_revision_objects WHERE source_id = ?1 AND revision_id = ?2",
                params![source.to_string(), revision.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(database_error)?
            .ok_or_else(|| Error::Internal("hosted revision has no canonical text object".into()))
    }
}

pub fn is_registered(
    tx: &Transaction<'_>,
    operation: Uuid,
    revision: Uuid,
    registration: &Registration,
) -> Result<bool> {
    tx.query_row(
        "SELECT EXISTS (SELECT 1 FROM hosted_revision_intents
         WHERE source_id = ?1 AND operation_id = ?2 AND revision_id = ?3 AND update_sequence = ?4)",
        params![
            registration.source.to_string(),
            operation.to_string(),
            revision.to_string(),
            crate::sql_integer(registration.update_sequence)?,
        ],
        |row| row.get(0),
    )
    .map_err(database_error)
}

pub fn release_intent(tx: &Transaction<'_>, source: Uuid) -> Result<()> {
    // Preserve the last sequence even when a failed creation releases its external ID.
    tx.execute(
        "UPDATE hosted_revision_intents SET creating = 0, external_namespace = NULL,
         external_id = NULL WHERE source_id = ?1 AND creating = 1",
        [source.to_string()],
    )
    .map_err(database_error)?;
    Ok(())
}

pub fn release_canceled_intent(
    tx: &Transaction<'_>,
    operation: Uuid,
    revision: Uuid,
    source: Uuid,
) -> Result<()> {
    initialize_collection(tx)?;
    // Keep the sequence, and leave any newer intent or unrelated source alone.
    tx.execute(
        "UPDATE hosted_revision_intents SET creating = 0, external_namespace = NULL,
         external_id = NULL WHERE source_id = ?1 AND operation_id = ?2 AND revision_id = ?3 AND creating = 1",
        params![source.to_string(), operation.to_string(), revision.to_string()],
    )
    .map_err(database_error)?;
    Ok(())
}

pub fn published_revision(
    tx: &Transaction<'_>,
    source: Uuid,
    revision: Uuid,
) -> Result<Option<RevisionContent>> {
    crate::revision_summary(tx, source, revision)
}

pub fn publish_revision<'tx>(
    tx: &'tx Transaction<'_>,
    revision: &crate::Revision,
    objects: &RevisionObjects,
    replacement: bool,
    profile: Option<&str>,
    initial_metadata: (
        &[crate::metadata::Mutation],
        &[crate::metadata::ChunkMetadata],
    ),
    add_chunks: impl FnOnce(&crate::RevisionWrite<&rusqlite::Connection>) -> Result<()>,
) -> Result<crate::RevisionWrite<&'tx rusqlite::Connection>> {
    let writer = crate::RevisionWrite(&**tx);
    add_chunks(&writer)?;
    writer.register(revision, replacement, profile)?;
    writer.set_initial_metadata(revision, initial_metadata.0, initial_metadata.1)?;
    tx.execute(
        "INSERT INTO hosted_revision_objects (source_id, revision_id, text_key, original_key)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            revision.source.to_string(),
            revision.revision.to_string(),
            objects.text_key,
            objects.original_key
        ],
    )
    .map_err(database_error)?;
    release_intent(tx, revision.source)?;
    Ok(writer)
}

pub fn delete_source(tx: &Transaction<'_>, source: Uuid, expected: Option<Uuid>) -> Result<Uuid> {
    initialize_collection(tx)?;
    let writer = crate::RevisionWrite(&**tx);
    let deleted = writer.delete_source(source, expected)?;
    tx.execute(
        "DELETE FROM hosted_revision_intents WHERE source_id = ?1",
        [source.to_string()],
    )
    .map_err(database_error)?;
    Ok(deleted.revision)
}

const HOSTED_READER_GRACE_SECONDS: i64 = 900;
const HOSTED_DELETE_RETRY_SECONDS: i64 = 300;

pub fn retire_revisions(tx: &Transaction<'_>, profile: Option<&str>) -> Result<()> {
    initialize_collection(tx)?;
    let writer = crate::RevisionWrite(&**tx);
    writer.release_retirements(profile)?;
    for revision in writer.retirement_candidates(profile)? {
        if writer.revision_held(revision)? {
            continue;
        }
        let source = revision.source.to_string();
        let id = revision.revision.to_string();
        tx.execute(
            "INSERT OR IGNORE INTO hosted_object_garbage (object_key, delete_after, source_id)
             SELECT text_key, unixepoch() + ?3, source_id FROM hosted_revision_objects
               WHERE source_id = ?1 AND revision_id = ?2
             UNION
             SELECT original_key, unixepoch() + ?3, source_id FROM hosted_revision_objects
               WHERE source_id = ?1 AND revision_id = ?2 AND original_key IS NOT NULL",
            params![source, id, HOSTED_READER_GRACE_SECONDS],
        )
        .map_err(database_error)?;
        writer.retire(revision)?;
        tx.execute(
            "DELETE FROM revision_garbage WHERE source_id = ?1 AND revision_id = ?2",
            params![source, id],
        )
        .map_err(database_error)?;
    }
    writer.expire_deletion_receipts()?;
    writer.prune_unreferenced_embeddings()
}

pub fn settle_object_deletions(
    tx: &Transaction<'_>,
    objects: &[String],
    failures: &[String],
) -> Result<()> {
    let mut delete = tx
        .prepare("DELETE FROM hosted_object_garbage WHERE object_key = ?1")
        .map_err(database_error)?;
    for key in objects {
        delete.execute([key]).map_err(database_error)?;
    }
    let mut defer = tx
        .prepare(
            "UPDATE hosted_object_garbage SET delete_after = unixepoch() + ?2
             WHERE object_key = ?1 AND delete_after <= unixepoch()",
        )
        .map_err(database_error)?;
    for key in failures {
        defer
            .execute(params![key, HOSTED_DELETE_RETRY_SECONDS])
            .map_err(database_error)?;
    }
    Ok(())
}

impl crate::CollectionDb {
    pub fn has_hosted_retirement_work(&self, profile: Option<&str>) -> Result<bool> {
        self.0.query_row(
            "SELECT EXISTS (
                SELECT 1 FROM revisions r JOIN sources s ON s.id = r.source_id
                WHERE (s.deleted = 1 AND r.retire_at <= unixepoch())
                  OR (s.deleted = 0 AND r.id <> s.current_revision
                    AND (r.retire_at IS NULL OR r.retire_at <= unixepoch())
                    AND (?1 IS NULL OR NOT EXISTS (
                        SELECT 1 FROM chunks c LEFT JOIN embedding_cache e
                          ON e.profile = ?1 AND e.input_hash = c.content_hash AND e.vector IS NOT NULL
                        WHERE c.source_id = s.id AND c.revision_id = s.current_revision AND e.input_hash IS NULL
                    )))
            ) OR EXISTS (
                SELECT 1 FROM deleted_sources d WHERE d.expires_at <= unixepoch()
                 AND NOT EXISTS (SELECT 1 FROM sources WHERE id = d.id)
            )",
            [profile],
            |row| row.get(0),
        ).map_err(database_error)
    }

    pub fn has_hosted_revision_object(
        &self,
        source: Uuid,
        revision: Uuid,
        key: &str,
    ) -> Result<bool> {
        let referenced = self
            .0
            .query_row(
                "SELECT EXISTS (SELECT 1 FROM hosted_revision_objects
              WHERE source_id = ?1 AND revision_id = ?2 AND (text_key = ?3 OR original_key = ?3))",
                params![source.to_string(), revision.to_string(), key],
                |row| row.get(0),
            )
            .map_err(database_error)?;
        if referenced || crate::schema_version(&self.0)? < 10 {
            return Ok(referenced);
        }
        self.0
            .query_row(
                "SELECT EXISTS (SELECT 1 FROM hosted_object_garbage WHERE object_key = ?1)",
                [key],
                |row| row.get(0),
            )
            .map_err(database_error)
    }

    pub fn due_hosted_objects(&self) -> Result<Vec<String>> {
        if crate::schema_version(&self.0)? < 10 {
            return Ok(Vec::new());
        }
        crate::query_all(
            &self.0,
            "SELECT object_key FROM hosted_object_garbage WHERE delete_after <= unixepoch()
             ORDER BY delete_after, object_key LIMIT 64",
            [],
            |row| row.get(0),
        )
    }
}
