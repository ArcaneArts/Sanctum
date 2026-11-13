use std::{cmp::Ordering, fmt::Write};

use rusqlite::{Connection, functions::FunctionFlags, types::Value as SqlValue};
use sanctum_core::{Error, Result};
use serde_json::{Number, Value};

use super::{
    database_error,
    metadata::{Entry, valid_name},
};

#[derive(Clone, Copy)]
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
    pub fn uses_collection_metadata(&self) -> bool {
        match self {
            Self::Test(test) => matches!(test.level, Level::Collection),
            Self::All(children) | Self::Any(children) => {
                children.iter().any(Self::uses_collection_metadata)
            }
            Self::Not(child) => child.uses_collection_metadata(),
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
        if !valid_name(&self.namespace) || !valid_name(&self.key) {
            return Err(Error::Invalid("invalid metadata namespace or key".into()));
        }
        if self.path.len() > 16 || self.path.iter().any(|segment| {
            matches!(segment, PathSegment::Property(property) if property.len() > 1024)
        }) {
            return Err(Error::Invalid("metadata path exceeds its size limit".into()));
        }
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

    fn select<'a>(&self, mut value: Option<&'a Value>) -> Option<&'a Value> {
        for segment in &self.path {
            value = match segment {
                PathSegment::Property(property) => value?.get(property),
                PathSegment::Index(index) => value?.get(*index as usize),
            };
        }
        value
    }

    fn matches(&self, root: Option<&Value>) -> bool {
        let selected = self.select(root);
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

fn numeric_order(left: &Number, right: &Number) -> Ordering {
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

/// Adds a parameterized predicate to a query where the candidate chunk is aliased as c.
pub(super) fn append_condition(
    connection: &Connection,
    sql: &mut String,
    arguments: &mut Vec<SqlValue>,
    filter: &Filter,
    collection: &[Entry],
) -> Result<()> {
    filter.validate()?;
    let mut predicates = Vec::new();
    sql.push_str(" AND ");
    compile(sql, arguments, &mut predicates, filter, collection)?;
    if !predicates.is_empty() {
        connection
            .create_scalar_function(
                "sanctum_metadata_matches",
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

fn compile(
    sql: &mut String,
    arguments: &mut Vec<SqlValue>,
    predicates: &mut Vec<Predicate>,
    filter: &Filter,
    collection: &[Entry],
) -> Result<()> {
    match filter {
        Filter::Test(test) => {
            if matches!(test.level, Level::Collection) {
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
            let index = predicates.len();
            let mut predicate = test.clone();
            if matches!(predicate.operator, Operator::Missing) {
                predicate.operator = Operator::Exists;
                sql.push_str("NOT ");
            }
            predicates.push(predicate);
            let ordinal = match test.level {
                Level::Source => "-1",
                Level::Chunk => "c.ordinal",
                Level::Collection => unreachable!("collection metadata was evaluated above"),
            };
            write!(
                sql,
                "EXISTS (SELECT 1 FROM revision_metadata AS m
                WHERE m.source = c.source_id AND m.revision = c.revision_id
                  AND m.ordinal = {ordinal} AND m.namespace = ? AND m.key = ?
                  AND sanctum_metadata_matches({index}, m.value))"
            )
            .expect("writing to a String");
            arguments.push(test.namespace.clone().into());
            arguments.push(test.key.clone().into());
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
                compile(sql, arguments, predicates, child, collection)?;
            }
            sql.push(')');
        }
        Filter::Not(child) => {
            sql.push_str("NOT (");
            compile(sql, arguments, predicates, child, collection)?;
            sql.push(')');
        }
    }
    Ok(())
}
