use std::{cmp::Ordering, fmt::Write};

use rusqlite::{Connection, functions::FunctionFlags, types::Value as SqlValue};
use sanctum_core::{Error, Result};
use serde_json::{Number, Value};

use super::{
    database_error,
    metadata::{Entry, valid_name},
    metadata_indexes::{self, Index},
};

pub(super) const MATCHES_FUNCTION: &str = "sanctum_metadata_matches";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Collection,
    Source,
    Chunk,
}

#[derive(Clone)]
pub enum PathSegment {
    Property(String),
    Index(u32),
}

#[derive(Clone, Copy)]
pub enum Operator {
    Equal,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
    Contains,
    Exists,
    Missing,
}

#[derive(Clone)]
pub struct Predicate {
    pub level: Level,
    pub namespace: String,
    pub key: String,
    pub path: Vec<PathSegment>,
    pub operator: Operator,
    pub operand: Option<Value>,
}

pub enum Filter {
    Test(Predicate),
    All(Vec<Filter>),
    Any(Vec<Filter>),
    Not(Box<Filter>),
}

impl Filter {
    pub fn visit_fields(&self, visit: &mut impl FnMut(Level, &str, &str)) {
        match self {
            Self::Test(test) => visit(test.level, &test.namespace, &test.key),
            Self::All(children) | Self::Any(children) => {
                for child in children {
                    child.visit_fields(visit);
                }
            }
            Self::Not(child) => child.visit_fields(visit),
        }
    }

    pub fn uses_collection_metadata(&self) -> bool {
        match self {
            Self::Test(test) => matches!(test.level, Level::Collection),
            Self::All(children) | Self::Any(children) => {
                children.iter().any(Self::uses_collection_metadata)
            }
            Self::Not(child) => child.uses_collection_metadata(),
        }
    }

    pub(super) fn allows_level(&self, level: Level) -> bool {
        match self {
            Self::Test(test) => test.level <= level,
            Self::All(children) | Self::Any(children) => {
                children.iter().all(|child| child.allows_level(level))
            }
            Self::Not(child) => child.allows_level(level),
        }
    }

    pub fn validate(&self) -> Result<()> {
        let mut nodes = 0;
        self.validate_node(1, &mut nodes)
    }

    fn validate_node(&self, depth: usize, nodes: &mut usize) -> Result<()> {
        *nodes += 1;
        if depth > 8 || *nodes > 64 {
            return Err(Error::Invalid(
                "metadata filter exceeds depth or node limit".into(),
            ));
        }
        match self {
            Self::Test(test) => test.validate(),
            Self::All(children) | Self::Any(children) => {
                if children.is_empty() {
                    return Err(Error::Invalid(
                        "metadata filter groups require at least one child".into(),
                    ));
                }
                for child in children {
                    child.validate_node(depth + 1, nodes)?;
                }
                Ok(())
            }
            Self::Not(child) => child.validate_node(depth + 1, nodes),
        }
    }
}

impl Predicate {
    fn validate(&self) -> Result<()> {
        validate_field(&self.namespace, &self.key, &self.path)?;
        match (self.operator, &self.operand) {
            (Operator::Exists | Operator::Missing, None)
            | (Operator::Equal | Operator::Contains, Some(_))
            | (
                Operator::Less
                | Operator::LessOrEqual
                | Operator::Greater
                | Operator::GreaterOrEqual,
                Some(Value::Number(_) | Value::String(_)),
            ) => Ok(()),
            _ => Err(Error::Invalid(
                "invalid operand for metadata filter operator".into(),
            )),
        }
    }

    fn matches(&self, root: Option<&Value>) -> bool {
        let selected = select(root, &self.path);
        match self.operator {
            Operator::Exists => return selected.is_some(),
            Operator::Missing => return selected.is_none(),
            _ => {}
        }
        let (Some(selected), Some(operand)) = (selected, self.operand.as_ref()) else {
            return false;
        };
        match self.operator {
            Operator::Equal => equal(selected, operand),
            Operator::Contains => selected
                .as_array()
                .is_some_and(|items| items.iter().any(|value| equal(value, operand))),
            operator => compare(selected, operand).is_some_and(|order| match operator {
                Operator::Less => order.is_lt(),
                Operator::LessOrEqual => order.is_le(),
                Operator::Greater => order.is_gt(),
                Operator::GreaterOrEqual => order.is_ge(),
                _ => false,
            }),
        }
    }
}

pub(super) fn validate_field(namespace: &str, key: &str, path: &[PathSegment]) -> Result<()> {
    if !valid_name(namespace) || !valid_name(key) {
        return Err(Error::Invalid("invalid metadata namespace or key".into()));
    }
    if path.len() > 16
        || path.iter().any(
            |segment| matches!(segment, PathSegment::Property(property) if property.len() > 1024),
        )
    {
        return Err(Error::Invalid(
            "metadata path exceeds its size limit".into(),
        ));
    }
    Ok(())
}

pub(super) fn select<'a>(mut value: Option<&'a Value>, path: &[PathSegment]) -> Option<&'a Value> {
    for segment in path {
        value = match segment {
            PathSegment::Property(property) => value?.get(property),
            PathSegment::Index(index) => value?.get(*index as usize),
        };
    }
    value
}

fn equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => numeric_order(left, right).is_eq(),
        (Value::Array(left), Value::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| equal(left, right))
        }
        (Value::Object(left), Value::Object(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .all(|(key, left)| right.get(key).is_some_and(|right| equal(left, right)))
        }
        _ => left == right,
    }
}

fn compare(left: &Value, right: &Value) -> Option<Ordering> {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => Some(numeric_order(left, right)),
        (Value::String(left), Value::String(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

pub(super) fn numeric_order(left: &Number, right: &Number) -> Ordering {
    let int = |n: &Number| {
        n.as_i64()
            .map(i128::from)
            .or_else(|| n.as_u64().map(i128::from))
    };
    let float = |n: &Number| n.as_f64().expect("JSON numbers are finite");
    match (int(left), int(right)) {
        (Some(left), Some(right)) => left.cmp(&right),
        (Some(left), None) => int_float_order(left, float(right)),
        (None, Some(right)) => int_float_order(right, float(left)).reverse(),
        (None, None) => float(left)
            .partial_cmp(&float(right))
            .expect("JSON numbers are finite"),
    }
}

fn int_float_order(integer: i128, float: f64) -> Ordering {
    integer.cmp(&(float as i128)).then_with(|| {
        // The integer operands are i64 or u64, so saturation cannot produce equality.
        0.0_f64
            .partial_cmp(&float.fract())
            .expect("JSON numbers are finite")
    })
}

#[derive(Clone, Copy)]
pub(super) enum CollectionValues<'a> {
    Entries(&'a [Entry]),
    Catalog,
}

/// Adds a parameterized predicate to a query where the candidate chunk is aliased as c.
pub(super) fn append_condition(
    connection: &Connection,
    sql: &mut String,
    arguments: &mut Vec<SqlValue>,
    filter: &Filter,
    collection: &[Entry],
) -> Result<()> {
    append_for(
        connection,
        sql,
        arguments,
        filter,
        CollectionValues::Entries(collection),
    )
}

pub(super) fn append_for(
    connection: &Connection,
    sql: &mut String,
    arguments: &mut Vec<SqlValue>,
    filter: &Filter,
    collection: CollectionValues<'_>,
) -> Result<()> {
    filter.validate()?;
    let indexes = if has_indexable_test(filter, collection) {
        metadata_indexes::list(connection)?
    } else {
        Vec::new()
    };
    let mut predicates = Vec::new();
    sql.push_str(" AND ");
    compile(
        sql,
        arguments,
        &mut predicates,
        filter,
        collection,
        &indexes,
    )?;
    if !predicates.is_empty() {
        connection
            .create_scalar_function(
                MATCHES_FUNCTION,
                2,
                FunctionFlags::SQLITE_UTF8
                    | FunctionFlags::SQLITE_DETERMINISTIC
                    | FunctionFlags::SQLITE_DIRECTONLY,
                move |context| {
                    let index = context.get::<u32>(0)? as usize;
                    let json = context.get::<String>(1)?;
                    let value = serde_json::from_str(&json)
                        .map_err(|error| rusqlite::Error::UserFunctionError(Box::new(error)))?;
                    let test = &predicates[index];
                    Ok(test.matches(Some(&value)))
                },
            )
            .map_err(database_error)?;
    }
    Ok(())
}

fn has_indexable_test(filter: &Filter, collection: CollectionValues<'_>) -> bool {
    match filter {
        Filter::Test(test) => {
            indexable(test)
                && !(test.level == Level::Collection
                    && matches!(collection, CollectionValues::Entries(_)))
        }
        Filter::All(children) | Filter::Any(children) => children
            .iter()
            .any(|child| has_indexable_test(child, collection)),
        Filter::Not(_) => false,
    }
}

fn indexable(test: &Predicate) -> bool {
    test.path.is_empty() && matches!(test.operator, Operator::Exists | Operator::Equal)
}

fn compile(
    sql: &mut String,
    arguments: &mut Vec<SqlValue>,
    predicates: &mut Vec<Predicate>,
    filter: &Filter,
    collection: CollectionValues<'_>,
    indexes: &[Index],
) -> Result<()> {
    match filter {
        Filter::Test(test) => {
            if let (Level::Collection, CollectionValues::Entries(collection)) =
                (test.level, collection)
            {
                let value = collection
                    .iter()
                    .find(|entry| entry.namespace == test.namespace && entry.key == test.key)
                    .map(|entry| serde_json::from_str(&entry.json))
                    .transpose()
                    .map_err(|error| Error::Internal(Box::new(error)))?;
                sql.push_str(if test.matches(value.as_ref()) {
                    "1"
                } else {
                    "0"
                });
                return Ok(());
            }
            arguments.push(test.namespace.clone().into());
            arguments.push(test.key.clone().into());
            let indexed = indexable(test)
                && indexes.iter().any(|index| {
                    index.level == test.level
                        && index.namespace == test.namespace
                        && index.key == test.key
                });
            let mut operator = test.operator;
            if matches!(operator, Operator::Missing) {
                operator = Operator::Exists;
                sql.push_str("NOT ");
            }
            sql.push_str(match (indexed, test.level) {
                (true, Level::Collection) =>
                    "c.collection IN (SELECT m.collection FROM collection_metadata m
                        WHERE m.namespace = ? AND m.key = ?",
                (true, Level::Source) =>
                    "(c.source_id, c.revision_id) IN (SELECT m.source, m.revision FROM revision_metadata m
                        WHERE m.namespace = ? AND m.key = ? AND m.ordinal = -1",
                (true, Level::Chunk) =>
                    "(c.source_id, c.revision_id, c.ordinal) IN (SELECT m.source, m.revision, m.ordinal
                        FROM revision_metadata m WHERE m.namespace = ? AND m.key = ? AND m.ordinal >= 0",
                (false, Level::Collection) => "EXISTS (SELECT 1 FROM collection_metadata m
                    WHERE m.collection = c.collection AND m.namespace = ? AND m.key = ?",
                (false, Level::Source) => "EXISTS (SELECT 1 FROM revision_metadata m
                    WHERE m.source = c.source_id AND m.revision = c.revision_id
                      AND m.ordinal = -1 AND m.namespace = ? AND m.key = ?",
                (false, Level::Chunk) => "EXISTS (SELECT 1 FROM revision_metadata m
                    WHERE m.source = c.source_id AND m.revision = c.revision_id
                      AND m.ordinal = c.ordinal AND m.namespace = ? AND m.key = ?",
            });
            if !test.path.is_empty() || !matches!(operator, Operator::Exists) {
                let index = predicates.len();
                predicates.push(Predicate {
                    operator,
                    ..test.clone()
                });
                write!(sql, " AND {MATCHES_FUNCTION}({index}, m.value)")
                    .expect("writing to a String");
            }
            sql.push(')');
        }
        Filter::All(children) | Filter::Any(children) => {
            let separator = if matches!(filter, Filter::All(_)) {
                " AND "
            } else {
                " OR "
            };
            sql.push('(');
            for (index, child) in children.iter().enumerate() {
                if index != 0 {
                    sql.push_str(separator);
                }
                compile(sql, arguments, predicates, child, collection, indexes)?;
            }
            sql.push(')');
        }
        Filter::Not(child) => {
            sql.push_str("NOT (");
            compile(sql, arguments, predicates, child, collection, &[])?;
            sql.push(')');
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rusqlite::{TransactionBehavior, params, params_from_iter};
    use serde_json::json;

    use super::*;
    use crate::{initialize_catalog, initialize_collection, metadata_indexes::Edit, query_all};

    #[test]
    fn named_partial_indexes_preserve_matches_and_narrow_key_lookups() {
        for (level, select, insert) in [
            (
                Level::Collection,
                "SELECT c.collection FROM (SELECT id AS collection FROM collections) c WHERE 1",
                "INSERT INTO collection_metadata VALUES ('with-key', ?1, ?2, '1.0')",
            ),
            (
                Level::Source,
                "SELECT c.source_id FROM (SELECT id AS source_id, current_revision AS revision_id FROM sources) c WHERE 1",
                "INSERT INTO revision_metadata VALUES ('with-key', 'r', -1, ?1, ?2, '1.0')",
            ),
            (
                Level::Chunk,
                "SELECT c.source_id FROM chunks c WHERE 1",
                "INSERT INTO revision_metadata VALUES ('with-key', 'r', 0, ?1, ?2, '1.0')",
            ),
        ] {
            let mut connection = Connection::open_in_memory().unwrap();
            let tx = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            match level {
                Level::Collection => {
                    initialize_catalog(&tx).unwrap();
                    tx.execute_batch(
                        "INSERT INTO collections (id) VALUES ('with-key'), ('absent');",
                    )
                    .unwrap();
                }
                Level::Source | Level::Chunk => {
                    initialize_collection(&tx).unwrap();
                    tx.execute_batch(
                        "INSERT INTO sources (id, name, current_revision) VALUES ('with-key', 'with key', 'r'), ('absent', 'absent', 'r');
                         INSERT INTO revisions (source_id, id, content_hash, bytes, chunk_count, chunker)
                            VALUES ('with-key', 'r', 'hash', 1, 1, 'test'), ('absent', 'r', 'hash', 1, 1, 'test');
                         INSERT INTO chunks (source_id, revision_id, ordinal, start_byte, end_byte, content_hash)
                            VALUES ('with-key', 'r', 0, 0, 1, 'hash'), ('absent', 'r', 0, 0, 1, 'hash');",
                    ).unwrap();
                }
            }
            let namespace = "integration's";
            let key = "clé 'test'";
            tx.execute(insert, params![namespace, key]).unwrap();
            let values = if level == Level::Collection {
                CollectionValues::Catalog
            } else {
                CollectionValues::Entries(&[])
            };
            let check = |connection: &Connection, indexed: bool| {
                for (operator, operand, expected) in [
                    (Operator::Exists, None, "with-key"),
                    (Operator::Equal, Some(json!(1)), "with-key"),
                    (Operator::Missing, None, "absent"),
                ] {
                    let filter = Filter::Test(Predicate {
                        level,
                        namespace: namespace.into(),
                        key: key.into(),
                        path: Vec::new(),
                        operator,
                        operand,
                    });
                    let mut sql = select.to_owned();
                    let mut arguments = Vec::new();
                    append_for(connection, &mut sql, &mut arguments, &filter, values).unwrap();
                    let plan = query_all(
                        connection,
                        &format!("EXPLAIN QUERY PLAN {sql}"),
                        params_from_iter(&arguments),
                        |row| row.get::<_, String>(3),
                    )
                    .unwrap();
                    let used_index = plan
                        .iter()
                        .any(|step| step.contains("sanctum_meta_frequent"));
                    if !indexed || !matches!(operator, Operator::Missing) {
                        assert_eq!(used_index, indexed, "{plan:?}");
                    }
                    let matches =
                        query_all(connection, &sql, params_from_iter(&arguments), |row| {
                            row.get::<_, String>(0)
                        })
                        .unwrap();
                    assert_eq!(matches, [expected]);
                }
            };
            check(&tx, false);
            let name: sanctum_core::ScopeId = "frequent".parse().unwrap();
            metadata_indexes::apply(
                &tx,
                Edit::Create(Index {
                    name: name.clone(),
                    level,
                    namespace: namespace.into(),
                    key: key.into(),
                }),
            )
            .unwrap();
            check(&tx, true);
            metadata_indexes::apply(&tx, Edit::Drop(name)).unwrap();
            check(&tx, false);
        }
    }
}
