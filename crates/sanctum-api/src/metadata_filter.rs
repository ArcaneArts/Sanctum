use sanctum_core::{Error, Result};
use sanctum_proto::v1::{self, metadata_filter::Expression, metadata_path_segment::Segment};
use sanctum_service::{
    metadata_filter::{Filter, Level, Operator, PathSegment, Predicate},
    metadata_query::{Field, Query, View},
};

pub(super) fn parse(filter: v1::MetadataFilter) -> Result<Filter> {
    parse_node(filter, 1, &mut 0)
}

fn parse_node(filter: v1::MetadataFilter, depth: usize, nodes: &mut usize) -> Result<Filter> {
    *nodes += 1;
    if depth > 8 || *nodes > 64 {
        return Err(Error::Invalid(
            "metadata filter exceeds depth or node limit".into(),
        ));
    }
    let mut group = |group: v1::MetadataFilterGroup| {
        group
            .filters
            .into_iter()
            .map(|child| parse_node(child, depth + 1, nodes))
            .collect::<Result<Vec<_>>>()
    };
    Ok(match filter.expression {
        Some(Expression::Test(test)) => Filter::Test(parse_predicate(test)?),
        Some(Expression::All(children)) => Filter::All(group(children)?),
        Some(Expression::Any(children)) => Filter::Any(group(children)?),
        Some(Expression::Not(child)) => {
            Filter::Not(Box::new(parse_node(*child, depth + 1, nodes)?))
        }
        None => {
            return Err(Error::Invalid(
                "metadata filter expression is required".into(),
            ));
        }
    })
}

fn parse_predicate(test: v1::MetadataPredicate) -> Result<Predicate> {
    let level = parse_level(test.level())?;
    let operator = match test.operator() {
        v1::MetadataOperator::Equal => Operator::Equal,
        v1::MetadataOperator::Less => Operator::Less,
        v1::MetadataOperator::LessOrEqual => Operator::LessOrEqual,
        v1::MetadataOperator::Greater => Operator::Greater,
        v1::MetadataOperator::GreaterOrEqual => Operator::GreaterOrEqual,
        v1::MetadataOperator::Contains => Operator::Contains,
        v1::MetadataOperator::Exists => Operator::Exists,
        v1::MetadataOperator::Missing => Operator::Missing,
        _ => {
            return Err(Error::Invalid(
                "metadata filter operator is required".into(),
            ));
        }
    };
    let path = parse_path(test.path)?;
    let operand = test
        .operand_json
        .map(|json| {
            if json.len() > 64 * 1024 {
                return Err(Error::Invalid("metadata operand exceeds 64 KiB".into()));
            }
            serde_json::from_str(&json)
                .map_err(|error| Error::Invalid(format!("invalid metadata operand JSON: {error}")))
        })
        .transpose()?;
    Ok(Predicate {
        level,
        operator,
        namespace: test.namespace,
        key: test.key,
        path,
        operand,
    })
}

pub(super) fn parse_query(request: v1::QueryMetadataRequest) -> Result<Query> {
    use v1::query_metadata_request::View as WireView;
    let level = parse_level(request.level())?;
    let view = match request.view {
        Some(WireView::List(view)) => View::List {
            sort: view.sort_by.map(parse_field).transpose()?,
            descending: view.descending,
        },
        Some(WireView::Count(view)) => View::Count {
            group_by: view.group_by.map(parse_field).transpose()?,
        },
        None => View::List {
            sort: None,
            descending: false,
        },
    };
    Ok(Query {
        level,
        view,
        filter: request.filter.map(parse).transpose()?,
        limit: request.limit.unwrap_or(16),
        offset: request.offset,
    })
}

fn parse_field(field: v1::MetadataField) -> Result<Field> {
    Ok(Field {
        level: parse_level(field.level())?,
        namespace: field.namespace,
        key: field.key,
        path: parse_path(field.path)?,
    })
}

fn parse_level(level: v1::MetadataLevel) -> Result<Level> {
    match level {
        v1::MetadataLevel::Collection => Ok(Level::Collection),
        v1::MetadataLevel::Source => Ok(Level::Source),
        v1::MetadataLevel::Chunk => Ok(Level::Chunk),
        _ => Err(Error::Invalid("metadata level is required".into())),
    }
}

fn parse_path(path: Vec<v1::MetadataPathSegment>) -> Result<Vec<PathSegment>> {
    path.into_iter()
        .map(|value| match value.segment {
            Some(Segment::Property(property)) => Ok(PathSegment::Property(property)),
            Some(Segment::Index(index)) => Ok(PathSegment::Index(index)),
            None => Err(Error::Invalid("metadata path segment is required".into())),
        })
        .collect()
}
