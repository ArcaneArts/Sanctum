use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
    http::StatusCode,
    routing::post,
};
use sanctum_core::{Error, Scope};
use sanctum_service::HostedCatalog;
use serde::Deserialize;
use uuid::Uuid;

pub(crate) fn router(service: HostedCatalog) -> Router {
    Router::new()
        .route("/internal/tasks/register-upload", post(register))
        .route("/internal/tasks/index-upload", post(index))
        .layer(DefaultBodyLimit::max(4096))
        .with_state(service)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RegisterUpload {
    tenant: String,
    collection: String,
    operation_id: Uuid,
}

async fn register(
    State(service): State<HostedCatalog>,
    Json(task): Json<RegisterUpload>,
) -> StatusCode {
    apply(service, task, false).await
}

async fn index(
    State(service): State<HostedCatalog>,
    Json(task): Json<RegisterUpload>,
) -> StatusCode {
    apply(service, task, true).await
}

async fn apply(service: HostedCatalog, task: RegisterUpload, indexing: bool) -> StatusCode {
    let (Ok(tenant), Ok(collection)) = (task.tenant.parse(), task.collection.parse()) else {
        return StatusCode::BAD_REQUEST;
    };
    if task.operation_id.is_nil() {
        return StatusCode::BAD_REQUEST;
    }
    let scope = Scope { tenant, collection };
    let result = if indexing {
        service.index_upload(&scope, task.operation_id).await
    } else {
        service.register_upload(&scope, task.operation_id).await
    };
    match result {
        Ok(_) => StatusCode::NO_CONTENT,
        Err(Error::NotFound(_)) => StatusCode::NOT_FOUND,
        Err(error) => {
            tracing::warn!(
                %error, tenant = %scope.tenant, collection = %scope.collection,
                operation = %task.operation_id, indexing, "hosted task will need a retry"
            );
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}
