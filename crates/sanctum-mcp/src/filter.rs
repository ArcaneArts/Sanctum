use rmcp::schemars;
use sanctum_core::{Error, Result};
use sanctum_service::metadata_filter as domain;
use serde::Deserialize;

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum MetadataFilter {
    Test(Predicate),
    All { filters: Vec<MetadataFilter> },
    Any { filters: Vec<MetadataFilter> },
    Not { filter: Box<MetadataFilter> },
}

#[derive(Deserialize, schemars::JsonSchema)]
pub(crate) struct Predicate {
    level: Level,
    namespace: String,
    key: String,
    #[serde(default)]
    path: Vec<PathSegment>,
    operator: Operator,
    /// JSON-encoded operand. Use the string "null" for a JSON null comparison.
    operand_json: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
enum Level {
    Collection,
    Source,
    Chunk,
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
enum Operator {
    Equal,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
    Contains,
    Exists,
    Missing,
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(untagged)]
enum PathSegment {
    Property(String),
    Index(u32),
}

impl TryFrom<MetadataFilter> for domain::Filter {
    type Error = Error;

    fn try_from(value: MetadataFilter) -> Result<Self> {
        Ok(match value {
            MetadataFilter::Test(test) => Self::Test(test.try_into()?),
            MetadataFilter::All { filters } => Self::All(convert(filters)?),
            MetadataFilter::Any { filters } => Self::Any(convert(filters)?),
            MetadataFilter::Not { filter } => Self::Not(Box::new((*filter).try_into()?)),
        })
    }
}

fn convert(filters: Vec<MetadataFilter>) -> Result<Vec<domain::Filter>> {
    filters.into_iter().map(TryInto::try_into).collect()
}

impl TryFrom<Predicate> for domain::Predicate {
    type Error = Error;

    fn try_from(test: Predicate) -> Result<Self> {
        Ok(Self {
            level: match test.level {
                Level::Collection => domain::Level::Collection,
                Level::Source => domain::Level::Source,
                Level::Chunk => domain::Level::Chunk,
            },
            namespace: test.namespace,
            key: test.key,
            path: test
                .path
                .into_iter()
                .map(|part| match part {
                    PathSegment::Property(name) => domain::PathSegment::Property(name),
                    PathSegment::Index(index) => domain::PathSegment::Index(index),
                })
                .collect(),
            operator: match test.operator {
                Operator::Equal => domain::Operator::Equal,
                Operator::Less => domain::Operator::Less,
                Operator::LessOrEqual => domain::Operator::LessOrEqual,
                Operator::Greater => domain::Operator::Greater,
                Operator::GreaterOrEqual => domain::Operator::GreaterOrEqual,
                Operator::Contains => domain::Operator::Contains,
                Operator::Exists => domain::Operator::Exists,
                Operator::Missing => domain::Operator::Missing,
            },
            operand: test
                .operand_json
                .as_deref()
                .map(|json| {
                    if json.len() > 64 * 1024 {
                        return Err(Error::Invalid("metadata operand exceeds 64 KiB".into()));
                    }
                    serde_json::from_str(json)
                        .map_err(|_| Error::Invalid("metadata operand is invalid JSON".into()))
                })
                .transpose()?,
        })
    }
}
