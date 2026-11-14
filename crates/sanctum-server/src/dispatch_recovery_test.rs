use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
};
use sanctum_proto::v1::{self, sanctum_client::SanctumClient, upload_text_frame::Frame};
use sanctum_service::HostedCatalog;
use sanctum_storage::{Key, Storage};
use sanctum_storage_s3::{S3Config, S3Storage};
use tokio::net::TcpListener;
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
use tonic::transport::{Channel, Endpoint};
use uuid::Uuid;

use super::{CloudTasks, CloudTasksConfig, Dispatch, Result, STANDARD, Task, Value, json};
use base64::Engine;

const TEXT: &str = "Café observation recorded during rhodonite recovery.\n";
const TASK_PATH: &str = "/v2/projects/project/locations/region/queues/repair/tasks";

#[tokio::test]
async fn filesystem_dispatch_recovers_missing_task_after_restart() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Arc::new(sanctum_storage_fs::FileStorage::open(directory.path()).unwrap());
    verify_recovery(storage, None).await;
}

#[tokio::test]
#[ignore = "requires a live MinIO bucket and S3 credentials"]
async fn minio_dispatch_recovers_missing_task_after_restart() {
    let storage = S3Storage::new(S3Config {
        bucket: std::env::var("SANCTUM_MINIO_BUCKET").expect("SANCTUM_MINIO_BUCKET"),
        region: "us-east-1".into(),
        endpoint: Some(std::env::var("SANCTUM_MINIO_ENDPOINT").expect("SANCTUM_MINIO_ENDPOINT")),
        allow_http: true,
        virtual_hosted_style: false,
    })
    .unwrap();
    let storage = Arc::new(storage);
    let prefix = Key::parse(format!("dispatch-recovery-{}", Uuid::new_v4())).unwrap();
    eprintln!("MinIO test prefix {prefix}");
    verify_recovery(storage.clone(), Some(prefix.clone())).await;
    loop {
        let objects = storage
            .list(Some(&prefix), None, 100)
            .await
            .unwrap()
            .objects;
        if objects.is_empty() {
            break;
        }
        for object in objects {
            storage.delete(&object.key).await.unwrap();
        }
    }
}

struct FixtureDispatch(CloudTasks);

#[async_trait::async_trait]
impl Dispatch for FixtureDispatch {
    async fn send(&self, task: Task<'_>) -> Result<()> {
        self.0.send_with_token(task, "fixture").await
    }
}

#[derive(Default)]
struct TaskQueue {
    enabled: AtomicBool,
    refused: Mutex<Vec<Value>>,
    accepted: Mutex<Vec<Value>>,
}

async fn queue_task(
    State(queue): State<Arc<TaskQueue>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> StatusCode {
    assert_eq!(headers["authorization"], "Bearer fixture");
    if queue.enabled.load(Ordering::SeqCst) {
        let mut accepted = queue.accepted.lock().unwrap();
        if accepted
            .iter()
            .any(|entry| entry["task"]["name"] == body["task"]["name"])
        {
            return StatusCode::CONFLICT;
        }
        accepted.push(body);
        StatusCode::OK
    } else {
        queue.refused.lock().unwrap().push(body);
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn serve_http(router: Router) -> (String, AbortOnDropHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let task = AbortOnDropHandle::new(tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    }));
    (origin, task)
}

async fn serve_api(catalog: HostedCatalog) -> (SanctumClient<Channel>, AbortOnDropHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = Endpoint::from_shared(format!("http://{}", listener.local_addr().unwrap()))
        .unwrap()
        .timeout(Duration::from_secs(15));
    let task = AbortOnDropHandle::new(tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(sanctum_api::hosted_grpc(catalog, CancellationToken::new()))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    }));
    (SanctumClient::new(endpoint.connect().await.unwrap()), task)
}

async fn verify_recovery(storage: Arc<dyn Storage>, prefix: Option<Key>) {
    let queue = Arc::new(TaskQueue::default());
    let (tasks_origin, _task_server) = serve_http(
        Router::new()
            .route(TASK_PATH, post(queue_task))
            .with_state(queue.clone()),
    )
    .await;
    let make_catalog = || {
        let dispatcher = CloudTasks::with_endpoint(
            CloudTasksConfig {
                queue: "projects/project/locations/region/queues/repair".into(),
                worker_url: "https://sanctum-worker.example".into(),
                service_account: "tasks@project.iam.gserviceaccount.com".into(),
            },
            &format!("{tasks_origin}/v2/"),
        )
        .unwrap();
        HostedCatalog::new(storage.clone(), prefix.clone(), 1024)
            .unwrap()
            .with_dispatch(Arc::new(FixtureDispatch(dispatcher)))
    };
    let scope = v1::Scope {
        tenant: "default".into(),
        collection: "repair".into(),
    };
    let operation = Uuid::new_v4();
    let payload = json!({
        "tenant": &scope.tenant,
        "collection": &scope.collection,
        "operation_id": operation,
    });
    let job = v1::IngestionJobRequest {
        scope: Some(scope.clone()),
        operation_id: operation.to_string(),
    };
    let (mut api, api_server) = serve_api(make_catalog()).await;
    api.create_collection(v1::CreateCollectionRequest {
        scope: Some(scope.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    let frames = [
        Frame::Header(v1::UploadTextHeader {
            scope: Some(scope.clone()),
            request_id: Some(operation.to_string()),
            name: "repair.txt".into(),
            ..Default::default()
        }),
        Frame::Data(TEXT.as_bytes().to_vec()),
        Frame::End(v1::UploadTextEnd {}),
    ]
    .map(|frame| v1::UploadTextFrame { frame: Some(frame) });
    let accepted = api
        .submit_text(tokio_stream::iter(frames))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(accepted.operation_id, operation.to_string());
    assert_eq!(accepted.total_bytes, TEXT.len() as u64);
    assert_eq!(accepted.state(), v1::IngestionState::Accepted);
    assert_eq!(queue.refused.lock().unwrap().len(), 1);
    assert!(queue.accepted.lock().unwrap().is_empty());
    drop(api);
    api_server.abort();
    assert!(api_server.await.unwrap_err().is_cancelled());

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    let (worker, first_worker) = serve_http(crate::hosted_worker::router(make_catalog())).await;
    let shard = operation.as_bytes()[0] % sanctum_service::DISPATCH_SHARDS;
    let early = http
        .post(format!("{worker}/internal/tasks/index-upload"))
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(early.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        reconcile(&http, &worker, shard).await,
        json!({"dispatched": 0, "failed": 1, "more": false}),
    );
    assert!(queue.accepted.lock().unwrap().is_empty());
    first_worker.abort();
    assert!(first_worker.await.unwrap_err().is_cancelled());

    queue.enabled.store(true, Ordering::SeqCst);
    let (worker, _worker_server) = serve_http(crate::hosted_worker::router(make_catalog())).await;
    assert_eq!(
        reconcile(&http, &worker, shard).await,
        json!({"dispatched": 1, "failed": 0, "more": false}),
    );
    assert_eq!(
        queue.accepted.lock().unwrap()[0]["task"]["httpRequest"],
        queue.refused.lock().unwrap()[0]["task"]["httpRequest"],
        "repair must enqueue the original registration task",
    );
    assert_eq!(
        reconcile(&http, &worker, shard).await,
        json!({"dispatched": 0, "failed": 0, "more": false}),
    );
    let stages = ["register-upload", "prepare-upload", "index-upload"];
    for (index, stage) in stages.iter().enumerate() {
        let task = queue.accepted.lock().unwrap()[index].clone();
        let request = &task["task"]["httpRequest"];
        let path = format!("/internal/tasks/{stage}");
        assert_eq!(
            request["url"],
            format!("https://sanctum-worker.example{path}")
        );
        assert_eq!(request["httpMethod"], "POST");
        let body = STANDARD.decode(request["body"].as_str().unwrap()).unwrap();
        assert_eq!(serde_json::from_slice::<Value>(&body).unwrap(), payload);
        let response = http
            .post(format!("{worker}{path}"))
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }
    assert_eq!(queue.accepted.lock().unwrap().len(), stages.len());

    let (mut api, _api_server) = serve_api(make_catalog()).await;
    let published = api
        .get_ingestion_job(job.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(published.state(), v1::IngestionState::TextReady);
    let search = v1::SearchTextRequest {
        scope: Some(scope),
        query: "rhodonite".into(),
        mode: v1::SearchMode::Lexical.into(),
        ..Default::default()
    };
    let before = api.search_text(search.clone()).await.unwrap().into_inner();
    assert_eq!(before.hits.len(), 1);
    let hit = &before.hits[0];
    assert!(!hit.database_generation.as_deref().unwrap().is_empty());
    assert_eq!(hit.chunk.as_ref().unwrap().source_id, published.source_id);
    assert_eq!(
        hit.chunk.as_ref().unwrap().revision_id,
        published.revision_id
    );
    let fetched = api
        .fetch_chunk(v1::FetchChunkRequest {
            chunk: hit.chunk.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        (fetched.start_byte, fetched.end_byte),
        (0, TEXT.len() as u64)
    );
    assert_eq!(fetched.excerpt, TEXT);

    for stage in stages {
        let replay = http
            .post(format!("{worker}/internal/tasks/{stage}"))
            .json(&payload)
            .send()
            .await
            .unwrap();
        assert_eq!(replay.status(), StatusCode::NO_CONTENT);
    }
    assert_eq!(queue.accepted.lock().unwrap().len(), stages.len());
    let after = api.search_text(search).await.unwrap().into_inner();
    assert_eq!(
        before.hits[0].database_generation,
        after.hits[0].database_generation
    );
    assert_eq!(before.hits[0].chunk, after.hits[0].chunk);
}

async fn reconcile(http: &reqwest::Client, worker: &str, shard: u8) -> Value {
    http.post(format!("{worker}/internal/maintenance/dispatch/{shard}"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}
