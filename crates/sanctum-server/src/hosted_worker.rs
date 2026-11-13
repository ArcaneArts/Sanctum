use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
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
        .route("/internal/maintenance/dispatch/{shard}", post(reconcile))
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
        Ok(_) => {
            service.try_dispatch(&scope, task.operation_id).await;
            StatusCode::NO_CONTENT
        }
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

async fn reconcile(State(service): State<HostedCatalog>, Path(shard): Path<u8>) -> Response {
    match service.reconcile_shard(shard).await {
        Ok(page) => Json(serde_json::json!({
            "dispatched": page.dispatched,
            "failed": page.failed,
            "more": page.next_cursor.is_some(),
        }))
        .into_response(),
        Err(Error::Invalid(_)) => StatusCode::BAD_REQUEST.into_response(),
        Err(error) => {
            tracing::warn!(%error, shard, "could not advance hosted dispatch scan");
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
    }
}
