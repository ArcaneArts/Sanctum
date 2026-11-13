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
