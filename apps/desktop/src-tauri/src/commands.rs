use sanctum_proto::v1;
use serde::Serialize;
use tauri::{AppHandle, State, ipc::Channel};
use tauri_plugin_clipboard_manager::ClipboardExt;
use tauri_plugin_dialog::DialogExt;

use crate::host::{Host, RuntimeStatus, Settings};

type Reply<T> = Result<T, String>;

#[derive(Serialize)]
pub(crate) struct Overview {
    runtime: RuntimeStatus,
    collections: Vec<String>,
    paused: bool,
    jobs: Vec<Job>,
    embedding: Option<Embedding>,
    api_error: Option<String>,
}

#[derive(Serialize)]
struct Job {
    collection: String,
    id: String,
    name: String,
    state: String,
    total_bytes: u64,
    processed_bytes: u64,
    attempts: u32,
    error: Option<String>,
    retryable: bool,
    cancelable: bool,
}

#[derive(Serialize)]
struct Embedding {
    enabled: bool,
    total: u64,
    embedded: u64,
    failed: u64,
    retrying: u64,
    error: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct Usage {
    bytes: u64,
    files: u64,
}

#[derive(Serialize)]
pub(crate) struct ImportEvent {
    path: String,
    state: &'static str,
    message: Option<String>,
}

fn error(value: impl std::fmt::Display) -> String {
    value.to_string()
}

fn scope(collection: String) -> Option<v1::Scope> {
    Some(v1::Scope {
        tenant: "default".into(),
        collection,
    })
}

#[tauri::command]
pub(crate) fn copy_mcp(app: AppHandle, value: String) -> Reply<()> {
    app.clipboard().write_text(value).map_err(error)
}

#[tauri::command]
pub(crate) async fn settings(host: State<'_, Host>) -> Reply<Settings> {
    Ok(host.settings().await)
}

#[tauri::command]
pub(crate) async fn save_settings(host: State<'_, Host>, settings: Settings) -> Reply<()> {
    host.save_settings(settings).await.map_err(error)
}

#[tauri::command]
pub(crate) fn apply_settings(host: State<'_, Host>) {
    host.apply_settings();
}

#[tauri::command]
pub(crate) async fn overview(host: State<'_, Host>, collection: Option<String>) -> Reply<Overview> {
    let mut overview = Overview {
        runtime: host.status().await,
        collections: Vec::new(),
        paused: false,
        jobs: Vec::new(),
        embedding: None,
        api_error: None,
    };
    let Ok(mut client) = host.client().await else {
        return Ok(overview);
    };
    let mut queue_client = client.clone();
    let mut jobs_client = client.clone();
    let result = tokio::try_join!(
        client.list_collections(v1::ListCollectionsRequest {
            tenant: "default".into(),
            ..Default::default()
        }),
        queue_client.get_local_queue(v1::Empty {}),
        jobs_client.list_ingestion_jobs(v1::ListIngestionJobsRequest {
            tenant: "default".into(),
            collection: collection.clone(),
            limit: Some(25),
            before_operation_id: None,
        }),
    );
    let (collections, queue, jobs) = match result {
        Ok(result) => result,
        Err(error) => {
            overview.api_error = Some(error.message().to_owned());
            return Ok(overview);
        }
    };
    overview.collections = collections
        .into_inner()
        .collections
        .into_iter()
        .filter_map(|collection| collection.scope.map(|scope| scope.collection))
        .collect();
    overview.paused = queue.into_inner().paused;
    overview.jobs = jobs
        .into_inner()
        .jobs
        .into_iter()
        .map(|job| {
            let state = job.state();
            Job {
                collection: job.scope.map_or_else(String::new, |scope| scope.collection),
                id: job.operation_id,
                name: job.name,
                state: match state {
                    v1::IngestionState::Accepted => "Queued",
                    v1::IngestionState::Registered => "Registered",
                    v1::IngestionState::Extracting => "Extracting",
                    v1::IngestionState::Indexing => "Indexing",
                    v1::IngestionState::Publishing => "Publishing",
                    v1::IngestionState::TextReady => "Text ready",
                    v1::IngestionState::Failed => "Failed",
                    v1::IngestionState::Canceled => "Canceled",
                    v1::IngestionState::Unspecified => "Unknown",
                }
                .into(),
                total_bytes: job.total_bytes,
                processed_bytes: job.processed_bytes,
                attempts: job.attempts,
                error: job.error_message,
                retryable: job.error_code.as_deref() == Some("retryable"),
                cancelable: matches!(
                    state,
                    v1::IngestionState::Accepted
                        | v1::IngestionState::Extracting
                        | v1::IngestionState::Indexing
                ),
            }
        })
        .collect();
    if let Some(collection) = collection.or_else(|| overview.collections.first().cloned()) {
        match client
            .get_embedding_status(v1::EmbeddingStatusRequest {
                scope: scope(collection),
                ..Default::default()
            })
            .await
        {
            Ok(result) => {
                let result = result.into_inner();
                let coverage = result.coverage.unwrap_or_default();
                overview.embedding = Some(Embedding {
                    enabled: coverage.profile_id.is_some(),
                    total: coverage.total_chunks,
                    embedded: coverage.embedded_chunks,
                    failed: coverage.failed_chunks,
                    retrying: result.retrying_chunks,
                    error: result.sample_error,
                });
            }
            Err(error) => overview.api_error = Some(error.message().to_owned()),
        }
    }
    Ok(overview)
}

#[tauri::command]
pub(crate) async fn create_collection(host: State<'_, Host>, collection: String) -> Reply<()> {
    host.client()
        .await
        .map_err(error)?
        .create_collection(v1::CreateCollectionRequest {
            scope: scope(collection),
            ..Default::default()
        })
        .await
        .map_err(|status| status.message().to_owned())?;
    Ok(())
}

#[tauri::command]
pub(crate) async fn pick_path(app: AppHandle, folder: bool) -> Reply<Option<String>> {
    tokio::task::spawn_blocking(move || {
        let dialog = app.dialog().file();
        let path = if folder {
            dialog.blocking_pick_folder()
        } else {
            dialog.blocking_pick_file()
        };
        path.map(|path| {
            path.into_path()
                .map_err(error)?
                .into_os_string()
                .into_string()
                .map_err(|_| "the selected path is not UTF-8".into())
        })
        .transpose()
    })
    .await
    .map_err(error)?
}

#[tauri::command]
pub(crate) async fn import_local(
    host: State<'_, Host>,
    collection: String,
    path: String,
    events: Channel<ImportEvent>,
) -> Reply<()> {
    let mut stream = host
        .client()
        .await
        .map_err(error)?
        .import_local(v1::ImportLocalRequest {
            scope: scope(collection),
            path,
            include: Vec::new(),
            exclude: Vec::new(),
            prune_missing: false,
            retain_original: false,
        })
        .await
        .map_err(|status| status.message().to_owned())?
        .into_inner();
    while let Some(event) = stream
        .message()
        .await
        .map_err(|status| status.message().to_owned())?
    {
        let state = match event.state() {
            v1::ImportState::Queued => "queued",
            v1::ImportState::Pending => "pending",
            v1::ImportState::Unchanged => "unchanged",
            v1::ImportState::Missing => "missing",
            v1::ImportState::Deleted => "deleted",
            v1::ImportState::Failed => "failed",
            v1::ImportState::Unspecified => "unknown",
        };
        events
            .send(ImportEvent {
                path: event.path,
                state,
                message: event.message,
            })
            .map_err(error)?;
    }
    Ok(())
}

#[tauri::command]
pub(crate) async fn pause_queue(host: State<'_, Host>, paused: bool) -> Reply<()> {
    host.client()
        .await
        .map_err(error)?
        .set_local_queue(v1::SetLocalQueueRequest { paused })
        .await
        .map_err(|status| status.message().to_owned())?;
    Ok(())
}

#[tauri::command]
pub(crate) async fn retry_job(
    host: State<'_, Host>,
    collection: String,
    operation_id: String,
) -> Reply<()> {
    host.client()
        .await
        .map_err(error)?
        .retry_ingestion_job(v1::IngestionJobRequest {
            scope: scope(collection),
            operation_id,
        })
        .await
        .map_err(|status| status.message().to_owned())?;
    Ok(())
}

#[tauri::command]
pub(crate) async fn cancel_job(
    host: State<'_, Host>,
    collection: String,
    operation_id: String,
) -> Reply<()> {
    host.client()
        .await
        .map_err(error)?
        .cancel_ingestion_job(v1::IngestionJobRequest {
            scope: scope(collection),
            operation_id,
        })
        .await
        .map_err(|status| status.message().to_owned())?;
    Ok(())
}

#[tauri::command]
pub(crate) async fn storage_usage(
    host: State<'_, Host>,
    collection: Option<String>,
) -> Reply<Usage> {
    use v1::storage_usage_request::Scope;
    let scope = match collection {
        Some(collection) => Scope::Collection(v1::Scope {
            tenant: "default".into(),
            collection,
        }),
        None => Scope::Cluster(v1::Empty {}),
    };
    let usage = host
        .client()
        .await
        .map_err(error)?
        .get_storage_usage(v1::StorageUsageRequest { scope: Some(scope) })
        .await
        .map_err(|status| status.message().to_owned())?
        .into_inner();
    Ok(Usage {
        bytes: usage.bytes,
        files: usage.files,
    })
}
