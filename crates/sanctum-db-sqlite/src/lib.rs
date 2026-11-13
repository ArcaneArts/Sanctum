use std::{fs, ops::Deref, path::Path, time::Duration};

use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Row, Transaction, TransactionBehavior, params,
};
use sanctum_core::{ByteRange, Error, ExternalSource, GitProvenance, Result, UrlProvenance};
use uuid::Uuid;

pub mod embedding;
pub mod hosted;
mod lifecycle;
pub mod metadata;
pub mod metadata_filter;
pub mod metadata_query;
pub mod queue;
pub mod snapshot;
pub mod watched_folders;
pub use lifecycle::{RevisionKey, SourceIdentity};

const CATALOG_SCHEMA: &str = include_str!("catalog.sql");
const CATALOG_MIGRATIONS: &[&str] = &[
    CATALOG_SCHEMA,
    include_str!("catalog_v2.sql"),
    include_str!("catalog_v3.sql"),
];
const COLLECTION_SCHEMA: &str = include_str!("collection.sql");
const COLLECTION_MIGRATIONS: &[&str] = &[
    COLLECTION_SCHEMA,
    include_str!("collection_v2.sql"),
    include_str!("collection_v3.sql"),
    include_str!("collection_v4.sql"),
    include_str!("collection_v5.sql"),
    include_str!("collection_v6.sql"),
    include_str!("collection_v7.sql"),
    include_str!("collection_v8.sql"),
    include_str!("collection_v9.sql"),
    include_str!("collection_v10.sql"),
];

#[derive(Debug)]
pub struct Revision {
    pub source: Uuid,
    pub revision: Uuid,
    pub name: String,
    pub external: Option<ExternalSource>,
    pub content: RevisionContent,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct RevisionContent {
    pub bytes: u64,
    pub chunks: u64,
    pub hash: String,
    pub original_hash: String,
    pub encoding: String,
    pub extractor: Option<String>,
    pub retain_original: bool,
    pub url: Option<UrlProvenance>,
    pub git: Option<GitProvenance>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ChunkRef {
    pub source: Uuid,
    pub revision: Uuid,
    pub ordinal: u64,
}

#[derive(Debug)]
pub struct Chunk {
    pub reference: ChunkRef,
    pub name: String,
    pub external: Option<ExternalSource>,
    pub range: ByteRange,
    pub hash: String,
}

pub fn create_collection(catalog_path: &Path, db_path: &Path, collection: &str) -> Result<()> {
    fs::create_dir_all(catalog_path.parent().expect("catalog has a parent"))?;
    let mut catalog = open(catalog_path, true)?;
    migrate(&mut catalog, CATALOG_MIGRATIONS)?;
    let transaction = catalog
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(database_error)?;
    if !activate_collection(&transaction, collection)? {
        return Err(Error::Conflict(format!(
            "collection {collection} already exists"
        )));
    }

    fs::create_dir_all(db_path.parent().expect("database has a parent"))?;
    migrate(&mut open(db_path, true)?, COLLECTION_MIGRATIONS)?;
    transaction.commit().map_err(database_error)
}

pub fn initialize_catalog(transaction: &Transaction<'_>) -> Result<()> {
    apply_migrations(transaction, CATALOG_MIGRATIONS)
}

pub fn initialize_collection(transaction: &Transaction<'_>) -> Result<()> {
    apply_migrations(transaction, COLLECTION_MIGRATIONS)
}

pub fn activate_collection(transaction: &Transaction<'_>, collection: &str) -> Result<bool> {
    if collection_deleted(transaction, collection)? {
        return Err(Error::Conflict("collection ID was retired".into()));
    }
    transaction
        .execute(
            "INSERT OR IGNORE INTO collections (id) VALUES (?1)",
            [collection],
        )
        .map(|count| count == 1)
        .map_err(database_error)
}

fn collection_deleted(connection: &Connection, collection: &str) -> Result<bool> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM deleted_collections WHERE id = ?1)",
            [collection],
            |row| row.get(0),
        )
        .map_err(database_error)
}

pub fn catalog_collection_deleted(path: &Path, collection: &str) -> Result<bool> {
    let connection = open_read_only(path)?;
    if schema_version(&connection)? < 3 {
        return Ok(false);
    }
    collection_deleted(&connection, collection)
}

pub fn delete_catalog_collection(transaction: &Transaction<'_>, collection: &str) -> Result<()> {
    if collection_deleted(transaction, collection)? {
        return Ok(());
    }
    let deleted = transaction
        .execute("DELETE FROM collections WHERE id = ?1", [collection])
        .map_err(database_error)?;
    if deleted == 0 {
        return Err(Error::NotFound(format!(
            "collection {collection} was not found"
        )));
    }
    transaction
        .execute(
            "INSERT INTO deleted_collections(id) VALUES(?1)",
            [collection],
        )
        .map_err(database_error)?;
    Ok(())
}

pub fn list_collections(path: &Path) -> Result<Vec<String>> {
    match path.try_exists()? {
        true => query_all(
            &open(path, false)?,
            "SELECT id FROM collections ORDER BY id",
            [],
            |row| row.get(0),
        ),
        false => Ok(Vec::new()),
    }
}

pub fn list_collections_page(path: &Path, after: Option<&str>, limit: u32) -> Result<Vec<String>> {
    if !path.try_exists()? {
        return Ok(Vec::new());
    }
    query_all(
        &open_read_only(path)?,
        "SELECT id FROM collections WHERE id > ?1 ORDER BY id LIMIT ?2",
        params![after.unwrap_or_default(), limit],
        |row| row.get(0),
    )
}

pub fn require_collection(path: &Path, collection: &str) -> Result<()> {
    if path.try_exists()? {
        let found = open_read_only(path)?
            .query_row(
                "SELECT 1 FROM collections WHERE id = ?1",
                [collection],
                |_| Ok(()),
            )
            .optional()
            .map_err(database_error)?;
        if found.is_some() {
            return Ok(());
        }
    }
    Err(Error::NotFound(format!(
        "collection {collection} was not found"
    )))
}

pub struct CollectionDb(Connection);

impl CollectionDb {
    pub fn open(path: &Path) -> Result<Self> {
        let mut connection = open(path, false)?;
        migrate(&mut connection, COLLECTION_MIGRATIONS)?;
        Ok(Self(connection))
    }

    pub fn open_read_only(path: &Path) -> Result<Self> {
        open_read_only(path).map(Self)
    }

    pub fn imported_sources(&self, namespace: &str, after: &str) -> Result<Vec<String>> {
        query_all(
            &self.0,
            "SELECT external_id FROM sources
             WHERE external_namespace = ?1 AND deleted = 0 AND external_id > ?2
             ORDER BY external_id LIMIT 128",
            params![namespace, after],
            |row| row.get(0),
        )
    }

    pub fn revision_summary(
        &self,
        source: Uuid,
        revision: Uuid,
    ) -> Result<Option<RevisionContent>> {
        self.0
            .query_row(
                "SELECT content_hash, chunk_count, bytes, COALESCE(original_hash, content_hash),
                    source_encoding, extractor, retain_original, url_provenance, git_provenance
                 FROM revisions WHERE source_id = ?1 AND id = ?2",
                params![source.to_string(), revision.to_string()],
                |row| {
                    Ok(RevisionContent {
                        hash: row.get(0)?,
                        chunks: unsigned_column(row, 1)?,
                        bytes: unsigned_column(row, 2)?,
                        original_hash: row.get(3)?,
                        encoding: row.get(4)?,
                        extractor: row.get(5)?,
                        retain_original: row.get(6)?,
                        url: read_json(row, 7)?,
                        git: read_json(row, 8)?,
                    })
                },
            )
            .optional()
            .map_err(database_error)
    }

    pub fn write(&mut self) -> Result<RevisionWriter<'_>> {
        let transaction = self
            .0
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error)?;
        Ok(RevisionWrite(transaction))
    }

    pub fn read_snapshot<T>(&self, read: impl FnOnce(&Self) -> Result<T>) -> Result<T> {
        let transaction = self.0.unchecked_transaction().map_err(database_error)?;
        let result = read(self)?;
        transaction.commit().map_err(database_error)?;
        Ok(result)
    }

    pub fn search(
        &self,
        phrase: &str,
        limit: u32,
        filter: Option<&metadata_filter::Filter>,
        collection_metadata: &[metadata::Entry],
    ) -> Result<Vec<Chunk>> {
        let mut sql = String::from(
            "SELECT c.source_id, c.revision_id, c.ordinal, s.name, s.external_namespace,
                    s.external_id, c.start_byte, c.end_byte, c.content_hash
             FROM chunks_fts
             JOIN chunks c ON c.id = chunks_fts.rowid
             JOIN sources s ON s.id = c.source_id AND s.current_revision = c.revision_id
                AND s.deleted = 0
             WHERE chunks_fts MATCH ?1",
        );
        let mut arguments = vec![phrase.to_owned().into()];
        if let Some(filter) = filter {
            metadata_filter::append_condition(
                &self.0,
                &mut sql,
                &mut arguments,
                filter,
                collection_metadata,
            )?;
        }
        sql.push_str(" ORDER BY chunks_fts.rank, c.id LIMIT ?");
        arguments.push(i64::from(limit).into());
        query_all(
            &self.0,
            &sql,
            rusqlite::params_from_iter(arguments),
            row_chunk,
        )
    }

    pub fn fetch(&self, reference: &ChunkRef) -> Result<Chunk> {
        let chunk = self
            .0
            .query_row(
                "SELECT c.source_id, c.revision_id, c.ordinal, s.name, s.external_namespace,
                        s.external_id, c.start_byte, c.end_byte, c.content_hash
                 FROM chunks c JOIN sources s ON s.id = c.source_id
                 WHERE c.source_id = ?1 AND c.revision_id = ?2 AND c.ordinal = ?3
                    AND s.deleted = 0",
                params![
                    reference.source.to_string(),
                    reference.revision.to_string(),
                    sql_integer(reference.ordinal)?
                ],
                row_chunk,
            )
            .optional()
            .map_err(database_error)?;
        if let Some(chunk) = chunk {
            return Ok(chunk);
        }
        let stale = self
            .0
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM deleted_sources WHERE id = ?1)
                 OR EXISTS(SELECT 1 FROM sources WHERE id = ?1 AND current_revision <> ?2)",
                params![reference.source.to_string(), reference.revision.to_string()],
                |row| row.get::<_, bool>(0),
            )
            .map_err(database_error)?;
        if stale {
            return Err(Error::StaleRevision(
                "source revision is no longer available".into(),
            ));
        }
        Err(Error::NotFound("chunk was not found".into()))
    }
}

pub type RevisionWriter<'db> = RevisionWrite<Transaction<'db>>;

pub struct RevisionWrite<T>(T);

impl RevisionWrite<Transaction<'_>> {
    pub fn commit(self) -> Result<()> {
        self.0.commit().map_err(database_error)
    }
}

impl<T: Deref<Target = Connection>> RevisionWrite<T> {
    pub fn add_chunk(
        &self,
        reference: &ChunkRef,
        range: ByteRange,
        hash: &str,
        text: &str,
    ) -> Result<()> {
        let id = self.0.query_row(
            "INSERT INTO chunks (source_id, revision_id, ordinal, start_byte, end_byte, content_hash)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6) RETURNING id",
            params![reference.source.to_string(), reference.revision.to_string(), sql_integer(reference.ordinal)?, sql_integer(range.start)?, sql_integer(range.end)?, hash],
            |row| row.get::<_, i64>(0),
        ).map_err(database_error)?;
        self.0
            .execute(
                "INSERT INTO chunks_fts (rowid, body) VALUES (?1, ?2)",
                params![id, text],
            )
            .map_err(database_error)?;
        Ok(())
    }

    pub fn register(
        &self,
        data: &Revision,
        replacement: bool,
        profile: Option<&str>,
    ) -> Result<()> {
        if replacement {
            self.replace_current(data)?;
        } else {
            self.register_source(data)?;
            metadata::record_change(
                &self.0,
                "source_created",
                data.source,
                data.revision,
                None,
                None,
            )?;
        }
        self.0
            .execute(
                "INSERT INTO revisions (source_id, id, content_hash, bytes, chunk_count, chunker,
                    original_hash, source_encoding, extractor, retain_original, url_provenance, git_provenance)
             VALUES (?1, ?2, ?3, ?4, ?5, 'utf8-overlap-v1', ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    data.source.to_string(),
                    data.revision.to_string(),
                    data.content.hash,
                    sql_integer(data.content.bytes)?,
                    sql_integer(data.content.chunks)?,
                    data.content.original_hash,
                    data.content.encoding,
                    data.content.extractor,
                    data.content.retain_original,
                    write_json(data.content.url.as_ref())?,
                    write_json(data.content.git.as_ref())?
                ],
            )
            .map_err(database_error)?;
        if replacement {
            self.release_retirements(profile)?;
        }
        Ok(())
    }

    fn register_source(&self, data: &Revision) -> Result<()> {
        self.reject_deleted(data.source)?;
        let namespace = data.external.as_ref().map(|external| &external.namespace);
        let id = data.external.as_ref().map(|external| &external.id);
        let inserted = self
            .0
            .execute(
                "INSERT INTO sources (id, name, external_namespace, external_id, current_revision)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT (external_namespace, external_id) DO NOTHING",
                params![
                    data.source.to_string(),
                    data.name,
                    namespace,
                    id,
                    data.revision.to_string()
                ],
            )
            .map_err(database_error)?;
        if inserted == 0 {
            return Err(Error::Conflict(
                "external source already exists in this collection".into(),
            ));
        }

        Ok(())
    }
}

fn open_read_only(path: &Path) -> Result<Connection> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(database_error)?;
    connection
        .busy_timeout(Duration::from_secs(10))
        .map_err(database_error)?;
    Ok(connection)
}

fn open(path: &Path, create: bool) -> Result<Connection> {
    let mut flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    if create {
        flags |= OpenFlags::SQLITE_OPEN_CREATE;
    }
    let connection = Connection::open_with_flags(path, flags).map_err(database_error)?;
    connection
        .busy_timeout(Duration::from_secs(10))
        .map_err(database_error)?;
    connection
        .pragma_update(None, "foreign_keys", true)
        .map_err(database_error)?;
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(database_error)?;
    Ok(connection)
}

fn migrate(connection: &mut Connection, schemas: &[&str]) -> Result<()> {
    let version = schema_version(connection)?;
    if version == schemas.len() {
        return Ok(());
    }
    if version == 0 {
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(database_error)?;
    }
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(database_error)?;
    apply_migrations(&transaction, schemas)?;
    transaction.commit().map_err(database_error)
}

fn apply_migrations(transaction: &Transaction<'_>, schemas: &[&str]) -> Result<()> {
    let version = schema_version(transaction)?;
    if version > schemas.len() {
        return Err(Error::Invalid(format!(
            "unsupported database schema {version}"
        )));
    }
    for (index, schema) in schemas.iter().enumerate().skip(version) {
        transaction.execute_batch(schema).map_err(database_error)?;
        transaction
            .pragma_update(None, "user_version", (index + 1) as u32)
            .map_err(database_error)?;
    }
    Ok(())
}

fn schema_version(connection: &Connection) -> Result<usize> {
    connection
        .pragma_query_value(None, "user_version", |row| row.get::<_, u32>(0))
        .map(|version| version as usize)
        .map_err(database_error)
}

fn query_all<T>(
    connection: &Connection,
    sql: &str,
    parameters: impl rusqlite::Params,
    mapper: impl FnMut(&Row<'_>) -> rusqlite::Result<T>,
) -> Result<Vec<T>> {
    let mut statement = connection.prepare(sql).map_err(database_error)?;
    statement
        .query_map(parameters, mapper)
        .map_err(database_error)?
        .collect::<rusqlite::Result<_>>()
        .map_err(database_error)
}

fn row_chunk(row: &Row<'_>) -> rusqlite::Result<Chunk> {
    let uuid = |column| {
        let value: String = row.get(column)?;
        Uuid::parse_str(&value).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                column,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })
    };
    let external = match (row.get(4)?, row.get(5)?) {
        (Some(namespace), Some(id)) => Some(ExternalSource { namespace, id }),
        _ => None,
    };
    Ok(Chunk {
        reference: ChunkRef {
            source: uuid(0)?,
            revision: uuid(1)?,
            ordinal: unsigned_column(row, 2)?,
        },
        name: row.get(3)?,
        external,
        range: ByteRange {
            start: unsigned_column(row, 6)?,
            end: unsigned_column(row, 7)?,
        },
        hash: row.get(8)?,
    })
}

fn sql_integer(value: u64) -> Result<i64> {
    i64::try_from(value)
        .map_err(|_| Error::Invalid("integer exceeds SQLite's supported range".into()))
}

fn read_json<T: serde::de::DeserializeOwned>(
    row: &Row<'_>,
    column: usize,
) -> rusqlite::Result<Option<T>> {
    row.get::<_, Option<String>>(column)?
        .map(|value| {
            serde_json::from_str(&value).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    column,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })
        })
        .transpose()
}

fn write_json<T: serde::Serialize>(value: Option<&T>) -> Result<Option<String>> {
    value
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| Error::Internal(Box::new(error)))
}

fn unsigned_column(row: &Row<'_>, column: usize) -> rusqlite::Result<u64> {
    let value: i64 = row.get(column)?;
    u64::try_from(value).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(column, value))
}

fn database_error(error: rusqlite::Error) -> Error {
    match error.sqlite_error_code() {
        Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked) => {
            Error::Busy("database is busy".into())
        }
        _ => Error::Internal(Box::new(error)),
    }
}
