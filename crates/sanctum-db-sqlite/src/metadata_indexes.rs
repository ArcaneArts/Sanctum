use std::path::Path;

use rusqlite::{Connection, Transaction, TransactionBehavior, params};
use sanctum_core::{Error, Result, ScopeId};

use crate::{
    CATALOG_MIGRATIONS, CollectionDb, database_error,
    metadata_filter::{Level, validate_field},
    migrate, open, open_read_only, query_all,
};

const MAX_INDEXES: usize = 32;

#[derive(Debug, PartialEq, Eq)]
pub struct Index {
    pub name: ScopeId,
    pub level: Level,
    pub namespace: String,
    pub key: String,
}

pub enum Edit {
    Create(Index),
    Drop(ScopeId),
}

impl Edit {
    pub fn validate(&self, is_collection_database: bool) -> Result<()> {
        if let Self::Create(index) = self {
            if is_collection_database == (index.level == Level::Collection) {
                return Err(Error::Invalid(
                    "the catalog supports collection indexes; collection databases support source and chunk indexes".into(),
                ));
            }
            validate_field(&index.namespace, &index.key, &[])?;
        }
        Ok(())
    }
}

impl CollectionDb {
    pub fn metadata_indexes(&self) -> Result<Vec<Index>> {
        list(&self.0)
    }

    pub fn edit_metadata_index(&mut self, edit: Edit) -> Result<()> {
        edit.validate(true)?;
        transact(&mut self.0, edit)
    }
}

pub fn list_catalog(path: &Path) -> Result<Vec<Index>> {
    require_catalog(path)?;
    list(&open_read_only(path)?)
}

pub fn edit_catalog(path: &Path, edit: Edit) -> Result<()> {
    edit.validate(false)?;
    require_catalog(path)?;
    let mut connection = open(path, false)?;
    migrate(&mut connection, CATALOG_MIGRATIONS)?;
    transact(&mut connection, edit)
}

fn require_catalog(path: &Path) -> Result<()> {
    if path.try_exists()? {
        Ok(())
    } else {
        Err(Error::NotFound("tenant catalog was not found".into()))
    }
}

fn transact(connection: &mut Connection, edit: Edit) -> Result<()> {
    let tx = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(database_error)?;
    apply(&tx, edit)?;
    tx.commit().map_err(database_error)
}

pub fn apply(tx: &Transaction<'_>, edit: Edit) -> Result<()> {
    match edit {
        Edit::Create(index) => create(tx, index),
        Edit::Drop(name) => {
            let removed = tx
                .execute(
                    "DELETE FROM metadata_indexes WHERE name = ?1",
                    [name.as_str()],
                )
                .map_err(database_error)?;
            if removed == 0 {
                return Err(Error::NotFound(format!(
                    "metadata index {name} was not found"
                )));
            }
            tx.execute_batch(&format!("DROP INDEX \"{}\";", physical_name(&name)))
                .map_err(database_error)
        }
    }
}

pub(super) fn list(connection: &Connection) -> Result<Vec<Index>> {
    // An existing hosted generation may predate the index registry.
    let exists: bool = connection
        .query_row(
            "SELECT EXISTS (SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'metadata_indexes')",
            [],
            |row| row.get(0),
        )
        .map_err(database_error)?;
    if !exists {
        return Ok(Vec::new());
    }
    query_all(
        connection,
        "SELECT name, level, namespace, key FROM metadata_indexes ORDER BY name",
        [],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get(2)?,
                row.get(3)?,
            ))
        },
    )?
    .into_iter()
    .map(|(name, level, namespace, key)| {
        let level = match level.as_str() {
            "collection" => Level::Collection,
            "source" => Level::Source,
            "chunk" => Level::Chunk,
            _ => {
                return Err(Error::Internal(
                    "invalid stored metadata index level".into(),
                ));
            }
        };
        Ok(Index {
            name: name.parse()?,
            level,
            namespace,
            key,
        })
    })
    .collect()
}

fn create(tx: &Transaction<'_>, index: Index) -> Result<()> {
    validate_field(&index.namespace, &index.key, &[])?;
    let indexes = list(tx)?;
    if indexes.iter().any(|current| current == &index) {
        return Ok(());
    }
    if indexes.iter().any(|current| {
        current.name == index.name
            || (current.level == index.level
                && current.namespace == index.namespace
                && current.key == index.key)
    }) {
        return Err(Error::Conflict(
            "metadata index name or field is already registered".into(),
        ));
    }
    if indexes.len() >= MAX_INDEXES {
        return Err(Error::Invalid(format!(
            "a database supports at most {MAX_INDEXES} named metadata indexes"
        )));
    }

    let (level, target, ordinal) = match index.level {
        Level::Collection => ("collection", "collection_metadata (collection)", ""),
        Level::Source => (
            "source",
            "revision_metadata (source, revision)",
            " AND ordinal = -1",
        ),
        Level::Chunk => (
            "chunk",
            "revision_metadata (source, revision, ordinal)",
            " AND ordinal >= 0",
        ),
    };
    // SQLite DDL does not accept bound parameters. quote() produces SQL string
    // literals. The public validation excludes embedded NULs from these fields.
    let (namespace, key): (String, String) = tx
        .query_row(
            "SELECT quote(?1), quote(?2)",
            params![index.namespace, index.key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(database_error)?;
    tx.execute_batch(&format!(
        "CREATE INDEX \"{}\" ON {target} WHERE namespace = {namespace} AND key = {key}{ordinal};",
        physical_name(&index.name),
    ))
    .map_err(database_error)?;
    tx.execute(
        "INSERT INTO metadata_indexes (name, level, namespace, key) VALUES (?1, ?2, ?3, ?4)",
        params![index.name.as_str(), level, index.namespace, index.key],
    )
    .map_err(database_error)?;
    Ok(())
}

fn physical_name(name: &ScopeId) -> String {
    format!("sanctum_meta_{name}")
}
