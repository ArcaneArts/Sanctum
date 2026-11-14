use sanctum_proto::v1;
use sanctum_service::{
    DatabaseTarget,
    metadata_filter::Level,
    metadata_indexes::{Edit, Index},
};
use tonic::{Response, Status};

use crate::{Backend, Reply, database::parse_target, metadata_filter::parse_level, status, work};

pub(super) async fn list(
    backend: &Backend,
    request: v1::DatabaseTarget,
) -> Reply<v1::ListMetadataIndexesResponse> {
    let target = parse_target(request).map_err(status)?;
    let indexes = match backend {
        Backend::Local(service) => {
            let service = service.clone();
            work(move || service.metadata_indexes(target)).await?
        }
        Backend::Hosted(catalog) => catalog.metadata_indexes(target).await.map_err(status)?,
    };
    Ok(Response::new(v1::ListMetadataIndexesResponse {
        indexes: indexes
            .into_iter()
            .map(|index| v1::MetadataIndex {
                name: index.name.to_string(),
                level: match index.level {
                    Level::Collection => v1::MetadataLevel::Collection,
                    Level::Source => v1::MetadataLevel::Source,
                    Level::Chunk => v1::MetadataLevel::Chunk,
                }
                .into(),
                namespace: index.namespace,
                key: index.key,
            })
            .collect(),
    }))
}

pub(super) async fn create(
    backend: &Backend,
    request: v1::CreateMetadataIndexRequest,
) -> Reply<v1::Empty> {
    let target = required_target(request.target).map_err(status)?;
    let index = request
        .index
        .ok_or_else(|| Status::invalid_argument("metadata index is required"))?;
    let edit = Edit::Create(Index {
        name: index.name.parse().map_err(status)?,
        level: parse_level(index.level()).map_err(status)?,
        namespace: index.namespace,
        key: index.key,
    });
    apply(backend, target, edit).await
}

pub(super) async fn drop(
    backend: &Backend,
    request: v1::DropMetadataIndexRequest,
) -> Reply<v1::Empty> {
    let target = required_target(request.target).map_err(status)?;
    let edit = Edit::Drop(request.name.parse().map_err(status)?);
    apply(backend, target, edit).await
}

fn required_target(target: Option<v1::DatabaseTarget>) -> sanctum_core::Result<DatabaseTarget> {
    parse_target(
        target.ok_or_else(|| sanctum_core::Error::Invalid("database target is required".into()))?,
    )
}

async fn apply(backend: &Backend, target: DatabaseTarget, edit: Edit) -> Reply<v1::Empty> {
    match backend {
        Backend::Local(service) => {
            let service = service.clone();
            work(move || service.edit_metadata_index(target, edit)).await?;
        }
        Backend::Hosted(catalog) => catalog
            .edit_metadata_index(target, edit)
            .await
            .map_err(status)?,
    }
    Ok(Response::new(v1::Empty {}))
}
