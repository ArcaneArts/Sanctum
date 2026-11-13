use std::{fs, path::Path, time::Duration};

use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Row, Transaction, TransactionBehavior, params,
};
use sanctum_core::{ByteRange, Error, ExternalSource, Result};
use uuid::Uuid;

pub mod queue;

const CATALOG_SCHEMA: &str = include_str!("catalog.sql");
const COLLECTION_SCHEMA: &str = include_str!("collection.sql");

#[derive(Debug)]
pub struct Revision {
    pub source: Uuid,
    pub revision: Uuid,
    pub name: String,
    pub external: Option<ExternalSource>,
    pub bytes: u64,
    pub chunks: u64,
    pub hash: String,
}

#[derive(Clone, Debug)]
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
    migrate(&mut catalog, CATALOG_SCHEMA)?;
    let transaction = catalog
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(database_error)?;
    let inserted = transaction
        .execute(
            "INSERT OR IGNORE INTO collections (id) VALUES (?1)",
            [collection],
        )
        .map_err(database_error)?;
    if inserted == 0 {
        return Err(Error::Conflict(format!(
            "collection {collection} already exists"
        )));
    }

    fs::create_dir_all(db_path.parent().expect("database has a parent"))?;
    migrate(&mut open(db_path, true)?, COLLECTION_SCHEMA)?;
    transaction.commit().map_err(database_error)
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

pub fn require_collection(path: &Path, collection: &str) -> Result<()> {
    if path.try_exists()? {
        let found = open(path, false)?
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
        Ok(Self(open(path, false)?))
    }

    pub fn revision_summary(&self, source: Uuid, revision: Uuid) -> Result<Option<(String, u64)>> {
        self.0
            .query_row(
                "SELECT content_hash, chunk_count FROM revisions WHERE source_id = ?1 AND id = ?2",
                params![source.to_string(), revision.to_string()],
                |row| Ok((row.get(0)?, unsigned_column(row, 1)?)),
            )
            .optional()
            .map_err(database_error)
    }

    pub fn write(&mut self) -> Result<RevisionWriter<'_>> {
        Ok(RevisionWriter(
            self.0
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(database_error)?,
        ))
    }

    pub fn search(&self, phrase: &str, limit: u32) -> Result<Vec<Chunk>> {
        query_all(
            &self.0,
            "SELECT c.source_id, c.revision_id, c.ordinal, s.name, s.external_namespace,
                    s.external_id, c.start_byte, c.end_byte, c.content_hash
             FROM chunks_fts
             JOIN chunks c ON c.id = chunks_fts.rowid
             JOIN sources s ON s.id = c.source_id AND s.current_revision = c.revision_id
             WHERE chunks_fts MATCH ?1
             ORDER BY chunks_fts.rank, c.id LIMIT ?2",
            params![phrase, limit],
            row_chunk,
        )
    }

    pub fn fetch(&self, reference: &ChunkRef) -> Result<Chunk> {
        self.0
            .query_row(
                "SELECT c.source_id, c.revision_id, c.ordinal, s.name, s.external_namespace,
                        s.external_id, c.start_byte, c.end_byte, c.content_hash
                 FROM chunks c JOIN sources s ON s.id = c.source_id
                 WHERE c.source_id = ?1 AND c.revision_id = ?2 AND c.ordinal = ?3",
                params![
                    reference.source.to_string(),
                    reference.revision.to_string(),
                    sql_integer(reference.ordinal)?
                ],
                row_chunk,
            )
            .optional()
            .map_err(database_error)?
            .ok_or_else(|| Error::NotFound("chunk was not found".into()))
    }
}

pub struct RevisionWriter<'db>(Transaction<'db>);

impl RevisionWriter<'_> {
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

    pub fn register(&self, data: &Revision) -> Result<()> {
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

        self.0
            .execute(
                "INSERT INTO revisions (source_id, id, content_hash, bytes, chunk_count, chunker)
             VALUES (?1, ?2, ?3, ?4, ?5, 'utf8-overlap-v1')",
                params![
                    data.source.to_string(),
                    data.revision.to_string(),
                    data.hash,
                    sql_integer(data.bytes)?,
                    sql_integer(data.chunks)?
                ],
            )
            .map_err(database_error)?;
        Ok(())
    }

    pub fn commit(self) -> Result<()> {
        self.0.commit().map_err(database_error)
    }
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

fn migrate(connection: &mut Connection, schema: &str) -> Result<()> {
    connection
        .pragma_update(None, "journal_mode", "WAL")
        .map_err(database_error)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(database_error)?;
    let version: u32 = transaction
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(database_error)?;
    match version {
        0 => {
            transaction.execute_batch(schema).map_err(database_error)?;
            transaction
                .pragma_update(None, "user_version", 1)
                .map_err(database_error)?;
        }
        1 => {}
        _ => {
            return Err(Error::Invalid(format!(
                "unsupported database schema {version}"
            )));
        }
    }
    transaction.commit().map_err(database_error)
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
