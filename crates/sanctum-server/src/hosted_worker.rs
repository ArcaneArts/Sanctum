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
        .route("/internal/tasks/embed-collection", post(embed))
        .route("/internal/tasks/embed-upload", post(embed_upload))
        .route(
            "/internal/tasks/rebuild-collection",
            post(rebuild_collection),
        )
        .route(
            "/internal/maintenance/embedding-profile",
            post(discover_profile),
        )
        .route("/internal/maintenance/dispatch/{shard}", post(reconcile))
        .route("/internal/maintenance/source-sweep", post(sweep_sources))
        .route(
            "/internal/maintenance/snapshot-sweep",
            post(sweep_snapshots),
        )
        .route("/internal/maintenance/upload-sweep", post(sweep_uploads))
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EmbedCollection {
    tenant: String,
    collection: String,
}

async fn embed(
    State(service): State<HostedCatalog>,
    Json(task): Json<EmbedCollection>,
) -> Response {
    let (Ok(tenant), Ok(collection)) = (task.tenant.parse(), task.collection.parse()) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let scope = Scope { tenant, collection };
    match service.embed_collection(&scope).await {
        Ok(processed) => Json(serde_json::json!({ "processed": processed })).into_response(),
        Err(Error::Invalid(_)) => StatusCode::BAD_REQUEST.into_response(),
        Err(Error::NotFound(_)) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => {
            tracing::warn!(%error, tenant = %scope.tenant, collection = %scope.collection, "hosted embedding task will need a retry");
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EmbedUpload {
    tenant: String,
    collection: String,
    operation_id: Uuid,
    step: u32,
    profile: Option<String>,
}

async fn embed_upload(
    State(service): State<HostedCatalog>,
    Json(task): Json<EmbedUpload>,
) -> StatusCode {
    let (Ok(tenant), Ok(collection)) = (task.tenant.parse(), task.collection.parse()) else {
        return StatusCode::BAD_REQUEST;
    };
    if task.operation_id.is_nil() {
        return StatusCode::BAD_REQUEST;
    }
    let scope = Scope { tenant, collection };
    match service
        .deliver_embedding_upload(
            &scope,
            task.operation_id,
            task.step,
            task.profile.as_deref(),
        )
        .await
    {
        Ok(()) => {
            service.try_dispatch(&scope, task.operation_id).await;
            StatusCode::NO_CONTENT
        }
        Err(Error::NotFound(_)) => StatusCode::NOT_FOUND,
        Err(error) => {
            tracing::warn!(%error, tenant = %scope.tenant, collection = %scope.collection,
                operation = %task.operation_id, step = task.step, "hosted embedding task will need a retry");
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}

async fn discover_profile(State(service): State<HostedCatalog>) -> Response {
    match service.discover_profile_rebuild().await {
        Ok(page) => Json(serde_json::json!({
            "discovered": page.discovered,
            "complete": page.complete,
        }))
        .into_response(),
        Err(error) => {
            tracing::warn!(%error, "profile rebuild discovery will need a retry");
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
    }
}

async fn rebuild_collection(
    State(service): State<HostedCatalog>,
    Json(task): Json<EmbedUpload>,
) -> StatusCode {
    let (Ok(tenant), Ok(collection)) = (task.tenant.parse(), task.collection.parse()) else {
        return StatusCode::BAD_REQUEST;
    };
    if task.operation_id.is_nil() {
        return StatusCode::BAD_REQUEST;
    }
    let scope = Scope { tenant, collection };
    match service
        .deliver_profile_rebuild(
            &scope,
            task.operation_id,
            task.step,
            task.profile.as_deref(),
        )
        .await
    {
        Ok(()) => {
            service
                .try_dispatch_rebuild(&scope, task.operation_id)
                .await;
            StatusCode::NO_CONTENT
        }
        Err(error) => {
            tracing::warn!(%error, tenant = %scope.tenant, collection = %scope.collection,
                operation = %task.operation_id, step = task.step, "profile rebuild will need a retry");
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}

async fn sweep_sources(State(service): State<HostedCatalog>) -> Response {
    match service.sweep_hosted_sources().await {
        Ok(report) => Json(serde_json::json!({
            "deleted": report.deleted,
            "collections": report.collections,
            "failed": report.failed,
            "more": report.more,
        }))
        .into_response(),
        Err(error) => {
            tracing::warn!(%error, "hosted source sweep will need a retry");
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
    }
}

async fn sweep_snapshots(State(service): State<HostedCatalog>) -> Response {
    match service.sweep_hosted_snapshots().await {
        Ok(report) => Json(serde_json::json!({
            "deleted": report.deleted,
            "units": report.units,
            "failed": report.failed,
            "more": report.more,
        }))
        .into_response(),
        Err(error) => {
            tracing::warn!(%error, "hosted snapshot sweep will need a retry");
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
    }
}

async fn sweep_uploads(State(service): State<HostedCatalog>) -> Response {
    match service.sweep_abandoned_uploads().await {
        Ok(report) => Json(serde_json::json!({
            "deleted": report.deleted,
            "expired": report.expired,
            "failed": report.failed,
            "more": report.more,
        }))
        .into_response(),
        Err(error) => {
            tracing::warn!(%error, "abandoned upload sweep will need a retry");
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
    }
}
