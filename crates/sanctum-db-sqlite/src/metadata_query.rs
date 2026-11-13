use std::{
    cmp::Ordering,
    path::Path,
    time::{Duration, Instant},
};

use rusqlite::{
    Connection, TransactionBehavior, functions::FunctionFlags, params_from_iter,
    types::Value as SqlValue,
};
use sanctum_core::{Error, Result};
use serde_json::Value;

use crate::{
    CATALOG_MIGRATIONS, CollectionDb, database_error,
    metadata::Entry,
    metadata_filter::{self, CollectionValues, Filter, Level, PathSegment},
    migrate, open, query_all, unsigned_column,
};

pub struct Field {
    pub level: Level,
    pub namespace: String,
    pub key: String,
    pub path: Vec<PathSegment>,
}

pub enum View {
    List {
        sort: Option<Field>,
        descending: bool,
    },
    Count {
        group_by: Option<Field>,
    },
}

pub struct Query {
    pub level: Level,
    pub filter: Option<Filter>,
    pub view: View,
    pub offset: u32,
    pub limit: u32,
}

pub struct Match {
    pub collection: String,
    pub source: Option<String>,
    pub revision: Option<String>,
    pub ordinal: Option<u64>,
    pub value_json: Option<String>,
}

pub struct Group {
    pub value_json: Option<String>,
    pub count: u64,
}

pub enum Results {
    Matches(Vec<Match>),
    Counts(Vec<Group>),
}

pub struct Page {
    pub results: Results,
    pub next_offset: Option<u32>,
}

pub fn catalog(path: &Path, query: Query) -> Result<Page> {
    if query.level != Level::Collection {
        return Err(Error::Invalid(
            "catalog metadata queries require collection targets".into(),
        ));
    }
    let field = validate(&query)?;
    if !path.try_exists()? {
        let results = match query.view {
            View::List { .. } => Results::Matches(Vec::new()),
            View::Count { group_by: Some(_) } => Results::Counts(Vec::new()),
            View::Count { group_by: None } => Results::Counts(
                (query.offset == 0)
                    .then_some(Group {
                        value_json: None,
                        count: 0,
                    })
                    .into_iter()
                    .collect(),
            ),
        };
        return Ok(Page {
            results,
            next_offset: None,
        });
    }
    let mut connection = open(path, false)?;
    migrate(&mut connection, CATALOG_MIGRATIONS)?;
    execute(connection, &query, field, CollectionValues::Catalog, None)
}

impl CollectionDb {
    pub fn query_metadata(self, collection: &str, query: Query, values: &[Entry]) -> Result<Page> {
        if query.level == Level::Collection {
            return Err(Error::Invalid(
                "collection database queries require source or chunk targets".into(),
            ));
        }
        let field = validate(&query)?;
        execute(
            self.0,
            &query,
            field,
            CollectionValues::Entries(values),
            Some(collection),
        )
    }
}

fn validate(query: &Query) -> Result<Option<&Field>> {
    let field = match &query.view {
        View::List { sort, .. } => sort.as_ref(),
        View::Count { group_by } => group_by.as_ref(),
    };
    if !(1..=32).contains(&query.limit) {
        return Err(Error::Invalid(
            "metadata query limit must be 1 to 32".into(),
        ));
    }
    if let Some(filter) = &query.filter {
        filter.validate()?;
        if !filter.allows_level(query.level) {
            return Err(Error::Invalid(
                "metadata filter cannot select a level below its targets".into(),
            ));
        }
    }
    if let Some(field) = field {
        metadata_filter::validate_field(&field.namespace, &field.key, &field.path)?;
        if field.level > query.level {
            return Err(Error::Invalid(
                "metadata field cannot select a level below its targets".into(),
            ));
        }
    }
    Ok(field)
}

fn execute(
    mut connection: Connection,
    query: &Query,
    field: Option<&Field>,
    values: CollectionValues<'_>,
    collection: Option<&str>,
) -> Result<Page> {
    let deadline = Instant::now() + Duration::from_secs(5);
    connection
        .progress_handler(2_000, Some(move || Instant::now() >= deadline))
        .map_err(database_error)?;
    let tx = connection
        .transaction_with_behavior(TransactionBehavior::Deferred)
        .map_err(database_error)?;
    let mut arguments = Vec::new();
    let mut sql = String::from("WITH candidates AS (SELECT ");
    match query.level {
        Level::Collection => {
            sql.push_str("c.collection, NULL AS source_id, NULL AS revision_id, NULL AS ordinal, ")
        }
        level => {
            sql.push_str("? AS collection, c.source_id, c.revision_id, ");
            arguments.push(
                collection
                    .expect("revision query has a collection")
                    .to_owned()
                    .into(),
            );
            if level == Level::Chunk {
                sql.push_str("c.ordinal, ");
            } else {
                sql.push_str("NULL AS ordinal, ");
            }
        }
    }
    append_value(&tx, &mut sql, &mut arguments, field, values)?;
    sql.push_str(" AS value ");
    sql.push_str(match query.level {
        Level::Collection => "FROM (SELECT id AS collection FROM collections) c WHERE 1",
        Level::Source => "FROM (SELECT id AS source_id, current_revision AS revision_id FROM sources WHERE deleted = 0) c WHERE 1",
        Level::Chunk => "FROM chunks c JOIN sources s ON s.id = c.source_id AND s.current_revision = c.revision_id WHERE s.deleted = 0",
    });
    if let Some(filter) = &query.filter {
        metadata_filter::append_for(&tx, &mut sql, &mut arguments, filter, values)?;
    }
    sql.push_str(") ");
    let mut page = query_page(&tx, sql, arguments, query).map_err(|error| {
        if Instant::now() >= deadline {
            Error::Busy("metadata query exceeded its execution budget".into())
        } else {
            error
        }
    })?;
    tx.commit().map_err(database_error)?;
    page.next_offset = match &mut page.results {
        Results::Matches(rows) => trim_page(rows, query),
        Results::Counts(rows) => trim_page(rows, query),
    };
    Ok(page)
}

fn append_value(
    connection: &Connection,
    sql: &mut String,
    arguments: &mut Vec<SqlValue>,
    field: Option<&Field>,
    values: CollectionValues<'_>,
) -> Result<()> {
    let Some(field) = field else {
        sql.push_str("NULL");
        return Ok(());
    };
    connection
        .create_collation("sanctum_metadata_order", compare_json)
        .map_err(database_error)?;
    if let (Level::Collection, CollectionValues::Entries(entries)) = (field.level, values) {
        let json = entries
            .iter()
            .find(|entry| entry.namespace == field.namespace && entry.key == field.key)
            .map(|entry| select_json(&entry.json, &field.path))
            .transpose()
            .map_err(database_error)?
            .flatten();
        sql.push('?');
        arguments.push(json.map_or(SqlValue::Null, SqlValue::Text));
        return Ok(());
    }
    let path = field.path.clone();
    connection
        .create_scalar_function(
            "sanctum_metadata_value",
            1,
            FunctionFlags::SQLITE_UTF8
                | FunctionFlags::SQLITE_DETERMINISTIC
                | FunctionFlags::SQLITE_DIRECTONLY,
            move |context| select_json(context.get::<String>(0)?.as_str(), &path),
        )
        .map_err(database_error)?;
    sql.push_str(match field.level {
        Level::Collection => "(SELECT sanctum_metadata_value(m.value) FROM collection_metadata m WHERE m.collection = c.collection",
        Level::Source => "(SELECT sanctum_metadata_value(m.value) FROM revision_metadata m WHERE m.source = c.source_id AND m.revision = c.revision_id AND m.ordinal = -1",
        Level::Chunk => "(SELECT sanctum_metadata_value(m.value) FROM revision_metadata m WHERE m.source = c.source_id AND m.revision = c.revision_id AND m.ordinal = c.ordinal",
    });
    sql.push_str(" AND m.namespace = ? AND m.key = ?)");
    arguments.push(field.namespace.clone().into());
    arguments.push(field.key.clone().into());
    Ok(())
}

fn query_page(
    connection: &Connection,
    mut sql: String,
    mut arguments: Vec<SqlValue>,
    query: &Query,
) -> Result<Page> {
    match &query.view {
        View::List { sort, descending } => {
            sql.push_str("SELECT collection, source_id, revision_id, ordinal, value FROM candidates ORDER BY ");
            if sort.is_some() {
                sql.push_str(if *descending {
                    "value COLLATE sanctum_metadata_order DESC NULLS LAST, "
                } else {
                    "value COLLATE sanctum_metadata_order ASC NULLS LAST, "
                });
            }
            sql.push_str("collection, source_id, ordinal");
        }
        View::Count { group_by: Some(_) } => sql.push_str(
            "SELECT value, count(*) FROM candidates GROUP BY value COLLATE sanctum_metadata_order
             ORDER BY count(*) DESC, value COLLATE sanctum_metadata_order NULLS LAST",
        ),
        View::Count { group_by: None } => sql.push_str("SELECT NULL, count(*) FROM candidates"),
    }
    sql.push_str(" LIMIT ? OFFSET ?");
    arguments.push(i64::from(query.limit + 1).into());
    arguments.push(i64::from(query.offset).into());
    let arguments = params_from_iter(arguments);
    let results = match query.view {
        View::List { .. } => Results::Matches(query_all(connection, &sql, arguments, |row| {
            Ok(Match {
                collection: row.get(0)?,
                source: row.get(1)?,
                revision: row.get(2)?,
                ordinal: row.get::<_, Option<i64>>(3)?.map(|value| value as u64),
                value_json: row.get(4)?,
            })
        })?),
        View::Count { .. } => Results::Counts(query_all(connection, &sql, arguments, |row| {
            Ok(Group {
                value_json: row.get(0)?,
                count: unsigned_column(row, 1)?,
            })
        })?),
    };
    Ok(Page {
        results,
        next_offset: None,
    })
}

fn trim_page<T>(rows: &mut Vec<T>, query: &Query) -> Option<u32> {
    if rows.len() > query.limit as usize {
        rows.pop();
        query.offset.checked_add(query.limit)
    } else {
        None
    }
}

fn select_json(json: &str, path: &[PathSegment]) -> rusqlite::Result<Option<String>> {
    let value = serde_json::from_str(json)
        .map_err(|error| rusqlite::Error::UserFunctionError(Box::new(error)))?;
    Ok(metadata_filter::select(Some(&value), path).map(Value::to_string))
}

fn compare_json(left: &str, right: &str) -> Ordering {
    let (Ok(left), Ok(right)) = (serde_json::from_str(left), serde_json::from_str(right)) else {
        return left.cmp(right);
    };
    compare_value(&left, &right)
}

fn compare_value(left: &Value, right: &Value) -> Ordering {
    let rank = |value: &Value| match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Number(_) => 2,
        Value::String(_) => 3,
        Value::Array(_) => 4,
        Value::Object(_) => 5,
    };
    match (left, right) {
        (Value::Bool(left), Value::Bool(right)) => left.cmp(right),
        (Value::Number(left), Value::Number(right)) => metadata_filter::numeric_order(left, right),
        (Value::String(left), Value::String(right)) => left.cmp(right),
        (Value::Array(left), Value::Array(right)) => left
            .iter()
            .zip(right)
            .map(|(left, right)| compare_value(left, right))
            .find(|order| !order.is_eq())
            .unwrap_or_else(|| left.len().cmp(&right.len())),
        (Value::Object(left), Value::Object(right)) => {
            let mut left: Vec<_> = left.iter().collect();
            let mut right: Vec<_> = right.iter().collect();
            left.sort_unstable_by_key(|(key, _)| *key);
            right.sort_unstable_by_key(|(key, _)| *key);
            left.iter()
                .zip(&right)
                .map(|((left_key, left), (right_key, right))| {
                    left_key
                        .cmp(right_key)
                        .then_with(|| compare_value(left, right))
                })
                .find(|order| !order.is_eq())
                .unwrap_or_else(|| left.len().cmp(&right.len()))
        }
        _ => rank(left).cmp(&rank(right)),
    }
}
