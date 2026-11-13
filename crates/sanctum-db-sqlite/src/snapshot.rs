use std::{
    path::Path,
    time::{Duration, Instant},
};

use rusqlite::{
    Connection, OpenFlags,
    backup::{Backup, StepResult},
    hooks::{AuthAction, AuthContext, Authorization},
    limits::Limit,
    params_from_iter,
    types::Value,
};
use sanctum_core::{Error, Result};

use crate::{CATALOG_MIGRATIONS, database_error, migrate, open, query_all, schema_version};

const MAX_RESULT_BYTES: usize = 1024 * 1024;

pub struct Description {
    pub schema_version: usize,
    pub sqlite_version: &'static str,
    pub schema: Vec<SchemaObject>,
}

pub struct SchemaObject {
    pub kind: String,
    pub name: String,
    pub table: String,
    pub sql: String,
}

pub struct Page {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub next_offset: Option<u64>,
}

pub use rusqlite::types::Value as SqlValue;

pub fn prepare_catalog(path: &Path) -> Result<()> {
    if !path.try_exists()? {
        return Err(Error::NotFound("tenant catalog was not found".into()));
    }
    migrate(&mut open(path, false)?, CATALOG_MIGRATIONS)
}

pub fn backup(
    source: &Path,
    destination: &Path,
    canceled: impl Fn() -> bool,
) -> Result<Description> {
    let source = open_readonly(source)?;
    source.execute_batch("BEGIN;").map_err(database_error)?;
    // Read from the source transaction to pin its committed generation.
    let description = describe(&source)?;
    let mut destination = Connection::open(destination).map_err(database_error)?;
    destination
        .busy_timeout(Duration::from_millis(100))
        .map_err(database_error)?;
    {
        let backup = Backup::new(&source, &mut destination).map_err(database_error)?;
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            if canceled() {
                return Err(Error::Busy("database snapshot canceled".into()));
            }
            if Instant::now() >= deadline {
                return Err(Error::Busy(
                    "database snapshot exceeded its time budget".into(),
                ));
            }
            match backup.step(256).map_err(database_error)? {
                StepResult::Done => break,
                StepResult::Busy | StepResult::Locked => {
                    std::thread::sleep(Duration::from_millis(15))
                }
                _ => {}
            }
        }
    }
    destination
        .pragma_update(None, "journal_mode", "DELETE")
        .map_err(database_error)?;
    Ok(description)
}

fn open_readonly(path: &Path) -> Result<Connection> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(database_error)?;
    connection
        .busy_timeout(Duration::from_secs(1))
        .map_err(database_error)?;
    Ok(connection)
}

pub fn describe_file(path: &Path) -> Result<Description> {
    describe(&open_readonly(path)?)
}

fn describe(connection: &Connection) -> Result<Description> {
    Ok(Description {
        schema_version: schema_version(connection)?,
        sqlite_version: rusqlite::version(),
        schema: query_all(
            connection,
            "SELECT type, name, tbl_name, COALESCE(sql, '') FROM sqlite_schema ORDER BY type, name",
            [],
            |row| {
                Ok(SchemaObject {
                    kind: row.get(0)?,
                    name: row.get(1)?,
                    table: row.get(2)?,
                    sql: row.get(3)?,
                })
            },
        )?,
    })
}

pub fn query(
    path: &Path,
    sql: &str,
    parameters: Vec<Value>,
    offset: u64,
    limit: u32,
    canceled: impl Fn() -> bool + Send + 'static,
) -> Result<Page> {
    if sql.is_empty() || sql.len() > 16_384 || !(1..=100).contains(&limit) || parameters.len() > 64
    {
        return Err(Error::Invalid(
            "SQL requires 1 to 16384 bytes, at most 64 parameters, and a limit of 1 to 100".into(),
        ));
    }
    if canceled() {
        return Err(Error::Busy("SQL query canceled".into()));
    }
    if parameters.iter().map(value_bytes).sum::<usize>() > MAX_RESULT_BYTES {
        return Err(Error::Invalid("SQL parameters exceed 1 MiB".into()));
    }
    let mut connection = open_readonly(path)?;
    connection
        .pragma_update(None, "query_only", true)
        .map_err(database_error)?;
    connection
        .pragma_update(None, "trusted_schema", false)
        .map_err(database_error)?;
    for (limit, value) in [
        (Limit::SQLITE_LIMIT_LENGTH, MAX_RESULT_BYTES as i32),
        (Limit::SQLITE_LIMIT_COLUMN, 128),
        (Limit::SQLITE_LIMIT_EXPR_DEPTH, 64),
        (Limit::SQLITE_LIMIT_COMPOUND_SELECT, 16),
        (Limit::SQLITE_LIMIT_VARIABLE_NUMBER, 64),
    ] {
        connection.set_limit(limit, value).map_err(database_error)?;
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    connection
        .progress_handler(
            1_000,
            Some(move || Instant::now() >= deadline || canceled()),
        )
        .map_err(database_error)?;
    connection
        .authorizer(Some(authorize))
        .map_err(database_error)?;
    let mut statement = connection.prepare(sql).map_err(query_error)?;
    if !statement.readonly() || statement.column_count() == 0 || statement.is_explain() != 0 {
        return Err(Error::Invalid("SQL must be one read-only SELECT".into()));
    }
    let columns: Vec<_> = statement.column_names().map(str::to_owned).collect();
    let mut bytes = columns.iter().map(|name| name.len() + 8).sum::<usize>();
    let mut rows = statement
        .query(params_from_iter(parameters))
        .map_err(query_error)?;
    for _ in 0..offset {
        if rows.next().map_err(query_error)?.is_none() {
            return Ok(Page {
                columns,
                rows: Vec::new(),
                next_offset: None,
            });
        }
    }
    let mut result = Vec::new();
    let mut more = false;
    while let Some(row) = rows.next().map_err(query_error)? {
        if result.len() == limit as usize {
            more = true;
            break;
        }
        let row_bytes = (0..columns.len()).try_fold(16, |bytes, index| {
            let value = row.get_ref(index).map_err(query_error)?;
            Ok::<_, Error>(
                bytes
                    + 16
                    + match value {
                        rusqlite::types::ValueRef::Text(text)
                        | rusqlite::types::ValueRef::Blob(text) => text.len(),
                        _ => 0,
                    },
            )
        })?;
        bytes += row_bytes;
        if bytes > MAX_RESULT_BYTES {
            if result.is_empty() {
                return Err(Error::Invalid(
                    "SQL result row exceeds the 1 MiB page budget".into(),
                ));
            }
            more = true;
            break;
        }
        let values = (0..columns.len())
            .map(|index| row.get::<_, Value>(index))
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(query_error)?;
        result.push(values);
    }
    let next_offset = more.then(|| offset + result.len() as u64);
    Ok(Page {
        columns,
        rows: result,
        next_offset,
    })
}

fn value_bytes(value: &Value) -> usize {
    16 + match value {
        Value::Text(text) => text.len(),
        Value::Blob(bytes) => bytes.len(),
        _ => 0,
    }
}

fn authorize(context: AuthContext<'_>) -> Authorization {
    let allowed = match context.action {
        AuthAction::Select | AuthAction::Recursive => true,
        AuthAction::Read { column_name, .. } => {
            context.database_name == Some("main")
                // SQLite omits the schema for columnless reads such as count(*).
                || (context.database_name.is_none() && column_name.is_empty())
        }
        AuthAction::Function { function_name } => matches!(
            function_name.to_ascii_lowercase().as_str(),
            "abs"
                | "avg"
                | "coalesce"
                | "concat"
                | "count"
                | "date"
                | "datetime"
                | "format"
                | "glob"
                | "group_concat"
                | "hex"
                | "ifnull"
                | "iif"
                | "instr"
                | "json"
                | "json_array_length"
                | "json_extract"
                | "json_group_array"
                | "json_group_object"
                | "json_object"
                | "json_quote"
                | "json_type"
                | "json_valid"
                | "->"
                | "->>"
                | "length"
                | "like"
                | "lower"
                | "ltrim"
                | "max"
                | "min"
                | "nullif"
                | "printf"
                | "quote"
                | "replace"
                | "round"
                | "rtrim"
                | "strftime"
                | "substr"
                | "substring"
                | "sum"
                | "total"
                | "trim"
                | "typeof"
                | "unicode"
                | "unixepoch"
                | "upper"
        ),
        _ => false,
    };
    if allowed {
        Authorization::Allow
    } else {
        Authorization::Deny
    }
}

fn query_error(error: rusqlite::Error) -> Error {
    if error.sqlite_error_code() == Some(rusqlite::ErrorCode::OperationInterrupted) {
        return Error::Busy("SQL canceled or exceeded its execution budget".into());
    }
    Error::Invalid(format!("SQL query failed: {error}"))
}
