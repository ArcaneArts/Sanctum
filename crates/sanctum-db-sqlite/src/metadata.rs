use std::path::Path;

use rusqlite::{
    Connection, OptionalExtension, Transaction, TransactionBehavior, params, params_from_iter,
    types::Value as SqlValue,
};
use sanctum_core::{Error, Result};
use serde_json::Value;
use uuid::Uuid;

use super::{
    CATALOG_MIGRATIONS, CollectionDb, database_error, migrate, open, query_all, sql_integer,
    unsigned_column,
};

const VALUE_LIMIT: usize = 64 * 1024;
const CHANGE_RETENTION: i64 = 10_000;

#[derive(Clone, Copy)]
pub struct RevisionTarget {
    pub source: Uuid,
    pub revision: Uuid,
    pub ordinal: Option<u64>,
}

pub enum Edit {
    Set(Value),
    Patch(Value),
    Remove,
}

pub struct Mutation {
    pub namespace: String,
    pub key: String,
    pub edit: Edit,
}

pub struct Entry {
    pub namespace: String,
    pub key: String,
    pub json: String,
}

pub struct Change {
    pub cursor: u64,
    pub kind: String,
    pub source: String,
    pub revision: String,
    pub previous_revision: Option<String>,
    pub ordinal: Option<u64>,
}

pub fn catalog_metadata(
    path: &Path,
    collection: &str,
    edits: Option<&[Mutation]>,
) -> Result<Vec<Entry>> {
    let mut connection = open(path, false)?;
    migrate(&mut connection, CATALOG_MIGRATIONS)?;
    let transaction = connection
        .transaction_with_behavior(if edits.is_some() {
            TransactionBehavior::Immediate
        } else {
            TransactionBehavior::Deferred
        })
        .map_err(database_error)?;
    let entries = catalog_entries(&transaction, collection, edits)?;
    transaction.commit().map_err(database_error)?;
    Ok(entries)
}

pub fn read_catalog_metadata(path: &Path, collection: &str) -> Result<Vec<Entry>> {
    let connection = super::open_read_only(path)?;
    catalog_entries(&connection, collection, None)
}

pub fn mutate_catalog_metadata(
    transaction: &Transaction<'_>,
    collection: &str,
    edits: &[Mutation],
) -> Result<Vec<Entry>> {
    catalog_entries(transaction, collection, Some(edits))
}

fn catalog_entries(
    connection: &Connection,
    collection: &str,
    edits: Option<&[Mutation]>,
) -> Result<Vec<Entry>> {
    let exists: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM collections WHERE id = ?1)",
            [collection],
            |row| row.get(0),
        )
        .map_err(database_error)?;
    if !exists {
        return Err(Error::NotFound(format!(
            "collection {collection} was not found"
        )));
    }
    apply(
        connection,
        "collection_metadata",
        "collection = ?1",
        "collection",
        vec![collection.to_owned().into()],
        edits,
    )
}

impl CollectionDb {
    pub fn metadata(
        &mut self,
        target: RevisionTarget,
        edits: Option<&[Mutation]>,
    ) -> Result<Vec<Entry>> {
        let transaction = self
            .0
            .transaction_with_behavior(if edits.is_some() {
                TransactionBehavior::Immediate
            } else {
                TransactionBehavior::Deferred
            })
            .map_err(database_error)?;
        let entries = revision_entries(&transaction, target, edits)?;
        transaction.commit().map_err(database_error)?;
        Ok(entries)
    }

    pub fn read_metadata(&self, target: RevisionTarget) -> Result<Vec<Entry>> {
        revision_entries(&self.0, target, None)
    }

    pub fn change_cursor(&self) -> Result<u64> {
        self.0
            .query_row(
                "SELECT COALESCE(max(cursor), 0) FROM source_changes",
                [],
                |row| unsigned_column(row, 0),
            )
            .map_err(database_error)
    }

    pub fn changes(&mut self, after: u64, limit: u32) -> Result<Vec<Change>> {
        let after = sql_integer(after)?;
        let snapshot = self.0.transaction().map_err(database_error)?;
        let (oldest, newest): (Option<i64>, i64) = snapshot
            .query_row(
                "SELECT (SELECT min(cursor) FROM source_changes),
                    COALESCE((SELECT max(cursor) FROM source_changes), 0)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(database_error)?;
        if after > newest {
            return Err(Error::Invalid(
                "change cursor is ahead of the collection".into(),
            ));
        }
        if oldest.is_some_and(|oldest| after < oldest - 1) {
            return Err(Error::StaleRevision(
                "change cursor expired; rescan the collection".into(),
            ));
        }
        let result = query_all(
            &snapshot,
            "SELECT cursor, kind, source, revision, previous_revision, ordinal FROM source_changes
             WHERE cursor > ?1 ORDER BY cursor LIMIT ?2",
            params![after, limit],
            |row| {
                Ok(Change {
                    cursor: unsigned_column(row, 0)?,
                    kind: row.get(1)?,
                    source: row.get(2)?,
                    revision: row.get(3)?,
                    previous_revision: row.get(4)?,
                    ordinal: row.get::<_, Option<i64>>(5)?.map(|value| value as u64),
                })
            },
        )?;
        snapshot.commit().map_err(database_error)?;
        Ok(result)
    }
}

pub fn mutate_revision_metadata(
    transaction: &Transaction<'_>,
    target: RevisionTarget,
    edits: &[Mutation],
) -> Result<Vec<Entry>> {
    revision_entries(transaction, target, Some(edits))
}

fn revision_entries(
    connection: &Connection,
    target: RevisionTarget,
    edits: Option<&[Mutation]>,
) -> Result<Vec<Entry>> {
    let source = target.source.to_string();
    let revision = target.revision.to_string();
    let current: Option<String> = connection
        .query_row(
            "SELECT current_revision FROM sources WHERE id = ?1 AND deleted = 0",
            [&source],
            |row| row.get(0),
        )
        .optional()
        .map_err(database_error)?;
    match current {
        None => return Err(Error::NotFound("source was not found".into())),
        Some(current) if current != revision => {
            return Err(Error::StaleRevision(
                "metadata target is not the current revision".into(),
            ));
        }
        _ => {}
    }
    let ordinal = target.ordinal.map(sql_integer).transpose()?;
    if let Some(ordinal) = ordinal {
        let exists: bool = connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM chunks WHERE source_id = ?1 AND revision_id = ?2 AND ordinal = ?3)",
                params![source, revision, ordinal], |row| row.get(0),
            ).map_err(database_error)?;
        if !exists {
            return Err(Error::NotFound("chunk was not found".into()));
        }
    }
    let entries = apply(
        connection,
        "revision_metadata",
        "source = ?1 AND revision = ?2 AND ordinal = ?3",
        "source, revision, ordinal",
        vec![source.into(), revision.into(), ordinal.unwrap_or(-1).into()],
        edits,
    )?;
    if edits.is_some() {
        record_change(
            connection,
            "metadata_changed",
            target.source,
            target.revision,
            None,
            ordinal,
        )?;
    }
    Ok(entries)
}

pub(super) fn record_change(
    connection: &Connection,
    kind: &str,
    source: Uuid,
    revision: Uuid,
    previous: Option<Uuid>,
    ordinal: Option<i64>,
) -> Result<()> {
    let cursor: i64 = connection
        .query_row(
            "INSERT INTO source_changes (kind, source, revision, previous_revision, ordinal)
         VALUES (?1, ?2, ?3, ?4, ?5) RETURNING cursor",
            params![
                kind,
                source.to_string(),
                revision.to_string(),
                previous.map(|id| id.to_string()),
                ordinal
            ],
            |row| row.get(0),
        )
        .map_err(database_error)?;
    connection
        .execute(
            "DELETE FROM source_changes WHERE cursor <= ?1",
            [cursor - CHANGE_RETENTION],
        )
        .map_err(database_error)?;
    Ok(())
}

fn apply(
    connection: &Connection,
    table: &str,
    predicate: &str,
    columns: &str,
    target: Vec<SqlValue>,
    edits: Option<&[Mutation]>,
) -> Result<Vec<Entry>> {
    if let Some(edits) = edits {
        if !(1..=32).contains(&edits.len()) {
            return Err(Error::Invalid(
                "metadata batch must contain 1 to 32 mutations".into(),
            ));
        }
        let select =
            format!("SELECT value FROM {table} WHERE {predicate} AND namespace = ? AND key = ?");
        let placeholders = vec!["?"; target.len() + 3].join(", ");
        let upsert = format!(
            "INSERT INTO {table} ({columns}, namespace, key, value)
            VALUES ({placeholders}) ON CONFLICT DO UPDATE SET value = excluded.value"
        );
        let delete = format!("DELETE FROM {table} WHERE {predicate} AND namespace = ? AND key = ?");
        for edit in edits {
            if !valid_name(&edit.namespace) || !valid_name(&edit.key) {
                return Err(Error::Invalid(
                    "metadata namespace and key must be 1 to 128 non-control UTF-8 bytes".into(),
                ));
            }
            let mut args = target.clone();
            args.push(edit.namespace.clone().into());
            args.push(edit.key.clone().into());
            let value = match &edit.edit {
                Edit::Remove => {
                    connection
                        .execute(&delete, params_from_iter(&args))
                        .map_err(database_error)?;
                    continue;
                }
                Edit::Set(value) => value.clone(),
                Edit::Patch(patch) => {
                    let old: Option<String> = connection
                        .query_row(&select, params_from_iter(&args), |row| row.get(0))
                        .optional()
                        .map_err(database_error)?;
                    let mut value = old
                        .as_deref()
                        .map(serde_json::from_str)
                        .transpose()
                        .map_err(json_error)?
                        .unwrap_or(Value::Null);
                    merge_patch(&mut value, patch);
                    value
                }
            };
            let json = serde_json::to_string(&value).map_err(json_error)?;
            if json.len() > VALUE_LIMIT {
                return Err(Error::Invalid("metadata value exceeds 64 KiB".into()));
            }
            args.push(json.into());
            connection
                .execute(&upsert, params_from_iter(&args))
                .map_err(database_error)?;
        }
    }
    let entries = query_all(
        connection,
        &format!(
            "SELECT namespace, key, value FROM {table}
            WHERE {predicate} ORDER BY namespace, key LIMIT 65"
        ),
        params_from_iter(&target),
        |row| {
            Ok(Entry {
                namespace: row.get(0)?,
                key: row.get(1)?,
                json: row.get(2)?,
            })
        },
    )?;
    if entries.len() > 64
        || entries.iter().map(|entry| entry.json.len()).sum::<usize>() > 512 * 1024
    {
        return Err(Error::Invalid(
            "metadata target exceeds its entry or total size limit".into(),
        ));
    }
    Ok(entries)
}

pub(super) fn valid_name(name: &str) -> bool {
    !name.trim().is_empty() && name.len() <= 128 && !name.chars().any(char::is_control)
}

fn merge_patch(value: &mut Value, patch: &Value) {
    let Value::Object(patch) = patch else {
        *value = patch.clone();
        return;
    };
    if !value.is_object() {
        *value = Value::Object(Default::default());
    }
    let object = value.as_object_mut().expect("merge target is an object");
    for (key, patch) in patch {
        if patch.is_null() {
            object.remove(key);
        } else {
            merge_patch(object.entry(key.clone()).or_insert(Value::Null), patch);
        }
    }
}

fn json_error(error: serde_json::Error) -> Error {
    Error::Internal(Box::new(error))
}
