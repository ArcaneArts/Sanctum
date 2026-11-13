use rusqlite::{Transaction, params};
use sanctum_core::{Error, ExternalSource, Result};
use uuid::Uuid;

use crate::{database_error, initialize_collection, lifecycle::find_source};

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

pub fn publish_revision(
    tx: &Transaction<'_>,
    revision: &crate::Revision,
    objects: &RevisionObjects,
    replacement: bool,
    profile: Option<&str>,
    add_chunks: impl FnOnce(&crate::RevisionWrite<&rusqlite::Connection>) -> Result<()>,
) -> Result<()> {
    let writer = crate::RevisionWrite(&**tx);
    add_chunks(&writer)?;
    writer.register(revision, replacement, profile)?;
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
    Ok(())
}
