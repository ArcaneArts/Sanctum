use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime},
};

use axum::{
    Json, Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
};
use sanctum_core::{ByteRange, Error, Scope};
use sanctum_proto::v1::{self, sanctum_client::SanctumClient, upload_text_frame::Frame};
use sanctum_service::HostedCatalog;
use sanctum_storage::{Condition, Download, Key, ListPage, Object, Reader, Storage, Version};
use sanctum_storage_s3::{S3Config, S3Storage};
use tokio::{io::AsyncReadExt, net::TcpListener, sync::oneshot};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
use tonic::transport::{Channel, Endpoint};
use uuid::Uuid;

use super::{CloudTasks, CloudTasksConfig, Dispatch, Result, STANDARD, Task, Value, json};
use base64::Engine;

const TEXT: &str = "Café observation recorded during rhodonite recovery.\n";
const RTF: &[u8] = br"{\rtf1\ansi Caf\'e9 observation recorded during rhodonite recovery.\par}";
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

struct StalledCheckpoint {
    storage: Arc<dyn Storage>,
    checkpoint: Mutex<Option<oneshot::Sender<oneshot::Receiver<()>>>>,
}

#[async_trait::async_trait]
impl Storage for StalledCheckpoint {
    async fn head(&self, key: &Key) -> Result<Object> {
        self.storage.head(key).await
    }

    async fn get(&self, key: &Key) -> Result<Download> {
        self.storage.get(key).await
    }

    async fn read_range(&self, key: &Key, range: ByteRange) -> Result<Bytes> {
        self.storage.read_range(key, range).await
    }

    async fn put_stream(&self, key: &Key, reader: Reader, length: u64) -> Result<Version> {
        self.storage.put_stream(key, reader, length).await
    }

    async fn put_control(&self, key: &Key, bytes: Bytes, condition: Condition) -> Result<Version> {
        if key.as_str().ends_with("/operation.json") && matches!(condition, Condition::Match(_)) {
            let checkpoint = self.checkpoint.lock().unwrap().take();
            if let Some(checkpoint) = checkpoint {
                let payload: Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(payload["stage"], "pending_indexing");
                let (in_flight, canceled) = oneshot::channel();
                checkpoint.send(canceled).unwrap();
                std::future::pending::<()>().await;
                drop(in_flight);
            }
        }
        self.storage.put_control(key, bytes, condition).await
    }

    async fn list(
        &self,
        prefix: Option<&Key>,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<ListPage> {
        self.storage.list(prefix, cursor, limit).await
    }

    async fn delete(&self, key: &Key) -> Result<()> {
        self.storage.delete(key).await
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
    let catalog_with_storage = |storage| {
        let dispatcher = CloudTasks::with_endpoint(
            CloudTasksConfig {
                queue: "projects/project/locations/region/queues/repair".into(),
                worker_url: "https://sanctum-worker.example".into(),
                service_account: "tasks@project.iam.gserviceaccount.com".into(),
            },
            &format!("{tasks_origin}/v2/"),
        )
        .unwrap();
        HostedCatalog::new(storage, prefix.clone(), 1024)
            .unwrap()
            .with_dispatch(Arc::new(FixtureDispatch(dispatcher)))
    };
    let make_catalog = || catalog_with_storage(storage.clone());
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
    let accepted = submit_source(&mut api, &scope, operation, "repair.rtf", RTF).await;
    assert_eq!(accepted.operation_id, operation.to_string());
    assert_eq!(accepted.total_bytes, RTF.len() as u64);
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
    let (worker, repair_server) = serve_http(crate::hosted_worker::router(make_catalog())).await;
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
    repair_server.abort();
    assert!(repair_server.await.unwrap_err().is_cancelled());

    let before_registration = collection_generation(storage.as_ref(), prefix.as_ref()).await;
    let (checkpoint, mut reached) = oneshot::channel();
    let stalled = catalog_with_storage(Arc::new(StalledCheckpoint {
        storage: storage.clone(),
        checkpoint: Mutex::new(Some(checkpoint)),
    }));
    let (deadline_worker, deadline_server) = serve_http(
        crate::hosted_worker::router_with_task_deadline(stalled, Duration::from_secs(2)),
    )
    .await;
    let timeout = http
        .post(format!("{deadline_worker}/internal/tasks/register-upload"))
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(timeout.status(), StatusCode::SERVICE_UNAVAILABLE);
    let mut canceled = reached
        .try_recv()
        .expect("registration reached envelope write");
    assert!(
        matches!(
            canceled.try_recv(),
            Err(oneshot::error::TryRecvError::Closed)
        ),
        "worker dropped the stalled write",
    );
    let committed_registration = collection_generation(storage.as_ref(), prefix.as_ref()).await;
    assert_ne!(before_registration, committed_registration);
    assert_eq!(queue.accepted.lock().unwrap().len(), 1);
    deadline_server.abort();
    assert!(deadline_server.await.unwrap_err().is_cancelled());

    let (mut api, _api_server) = serve_api(make_catalog()).await;
    let after_timeout = api
        .get_ingestion_job(job.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(after_timeout.state(), v1::IngestionState::Accepted);
    let (worker, _worker_server) = serve_http(crate::hosted_worker::router(make_catalog())).await;
    let stages = ["register-upload", "prepare-upload", "index-upload"];
    for (index, stage) in stages.iter().enumerate() {
        let task = queue.accepted.lock().unwrap()[index].clone();
        assert_eq!(task["task"]["dispatchDeadline"], "600s");
        let request = &task["task"]["httpRequest"];
        let path = format!("/internal/tasks/{stage}");
        assert_eq!(
            request["url"],
            format!("https://sanctum-worker.example{path}")
        );
        assert_eq!(request["httpMethod"], "POST");
        let body = STANDARD.decode(request["body"].as_str().unwrap()).unwrap();
        assert_eq!(serde_json::from_slice::<Value>(&body).unwrap(), payload);
        if index == 1 {
            let (checkpoint, mut reached) = oneshot::channel();
            let stalled = catalog_with_storage(Arc::new(StalledCheckpoint {
                storage: storage.clone(),
                checkpoint: Mutex::new(Some(checkpoint)),
            }));
            let (slow_worker, _slow_server) = serve_http(
                crate::hosted_worker::router_with_task_deadline(stalled, Duration::from_secs(2)),
            )
            .await;
            let response = http
                .post(format!("{slow_worker}{path}"))
                .header("content-type", "application/json")
                .body(body.clone())
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            let mut canceled = reached
                .try_recv()
                .expect("extraction reached its checkpoint write");
            assert!(
                matches!(
                    canceled.try_recv(),
                    Err(oneshot::error::TryRecvError::Closed)
                ),
                "worker dropped the stalled write",
            );
            let pending = api
                .get_ingestion_job(job.clone())
                .await
                .unwrap()
                .into_inner();
            assert_eq!(pending.state(), v1::IngestionState::Registered);
            assert!(pending.preparation.is_none());
            assert_eq!(queue.accepted.lock().unwrap().len(), 2);
        }
        let response = http
            .post(format!("{worker}{path}"))
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        if index == 0 {
            assert_eq!(
                collection_generation(storage.as_ref(), prefix.as_ref()).await,
                committed_registration,
                "retry reused the registered revision instead of publishing again",
            );
            let registered = api
                .get_ingestion_job(job.clone())
                .await
                .unwrap()
                .into_inner();
            assert_eq!(registered.state(), v1::IngestionState::Registered);
        }
        if index == 1 {
            let prepared = api
                .get_ingestion_job(job.clone())
                .await
                .unwrap()
                .into_inner();
            let Some(v1::hosted_preparation::Outcome::Ready(preparation)) =
                prepared.preparation.and_then(|stage| stage.outcome)
            else {
                panic!("retry did not save the RTF preparation");
            };
            assert_eq!(preparation.canonical_bytes, TEXT.len() as u64);
        }
    }
    assert_eq!(queue.accepted.lock().unwrap().len(), stages.len());

    let published = api
        .get_ingestion_job(job.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(published.state(), v1::IngestionState::TextReady);
    let search = v1::SearchTextRequest {
        scope: Some(scope.clone()),
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

    let key = scoped_key(
        prefix.as_ref(),
        &format!("default/collections/repair/operations/{operation}/operation.json"),
    );
    let (_, version) = read_control(storage.as_ref(), &key).await;
    let expired_at = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    // Simulate the upload sweep's durable compact record while the fixture
    // still has all three original Cloud Tasks deliveries.
    storage
        .put_control(
            &key,
            serde_json::to_vec(&json!({ "operation": operation, "expired_at": expired_at }))
                .unwrap()
                .into(),
            Condition::Match(version),
        )
        .await
        .unwrap();
    assert_eq!(
        api.get_ingestion_job(job.clone()).await.unwrap_err().code(),
        tonic::Code::NotFound,
    );
    let unknown = Uuid::new_v4();
    let fenced = Uuid::new_v4();
    let fence = scoped_key(
        prefix.as_ref(),
        &format!("default/collections/repair/operations/{fenced}/operation.json"),
    );
    storage
        .put_control(
            &fence,
            serde_json::to_vec(&json!({ "operation": fenced, "fence_id": Uuid::new_v4() }))
                .unwrap()
                .into(),
            Condition::Create,
        )
        .await
        .unwrap();
    let committed = collection_generation(storage.as_ref(), prefix.as_ref()).await;
    let (fresh_worker, _fresh_server) =
        serve_http(crate::hosted_worker::router(make_catalog())).await;
    for stage in stages.into_iter().chain(["embed-upload"]) {
        for (id, expected) in [
            (operation, StatusCode::NO_CONTENT),
            (unknown, StatusCode::NOT_FOUND),
            (fenced, StatusCode::NOT_FOUND),
        ] {
            let mut body = payload.clone();
            body["operation_id"] = json!(id);
            if stage == "embed-upload" {
                body["step"] = json!(0);
            }
            let response = http
                .post(format!("{fresh_worker}/internal/tasks/{stage}"))
                .json(&body)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), expected, "{stage} for {id}");
        }
    }
    assert_eq!(queue.accepted.lock().unwrap().len(), stages.len());
    assert_eq!(
        collection_generation(storage.as_ref(), prefix.as_ref()).await,
        committed,
    );
    let retained = api
        .fetch_chunk(v1::FetchChunkRequest {
            chunk: hit.chunk.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(retained.excerpt, TEXT);

    verify_retirement(
        storage.clone(),
        prefix.as_ref(),
        &http,
        queue.as_ref(),
        make_catalog,
    )
    .await;
}

async fn verify_retirement(
    storage: Arc<dyn Storage>,
    prefix: Option<&Key>,
    http: &reqwest::Client,
    queue: &TaskQueue,
    make_catalog: impl Fn() -> HostedCatalog,
) {
    let (mut api, _api_server) = serve_api(make_catalog()).await;
    let scope = v1::Scope {
        tenant: "default".into(),
        collection: "repair".into(),
    };
    let accepted_before = queue.accepted.lock().unwrap().len();
    let retired = v1::Scope {
        collection: "retired".into(),
        ..scope.clone()
    };
    api.create_collection(v1::CreateCollectionRequest {
        scope: Some(retired.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    // Place all three pending hints in the same repair shard. The operation
    // UUID can recur in another collection, so retirement must be scoped.
    queue.enabled.store(false, Ordering::SeqCst);
    let operation = Uuid::from_u128(1);
    let second = Uuid::from_u128(2);
    for (scope, operation) in [
        (&scope, operation),
        (&retired, operation),
        (&retired, second),
    ] {
        assert_eq!(
            submit_source(&mut api, scope, operation, "repair.txt", TEXT.as_bytes())
                .await
                .state(),
            v1::IngestionState::Accepted,
        );
    }
    let retired_scope = Scope {
        tenant: retired.tenant.parse().unwrap(),
        collection: retired.collection.parse().unwrap(),
    };
    make_catalog()
        .register_upload(&retired_scope, second)
        .await
        .unwrap();
    api.delete_collection(v1::DeleteCollectionRequest {
        scope: Some(retired),
        ..Default::default()
    })
    .await
    .unwrap();
    let hint = |collection, operation| {
        let storage = storage.clone();
        async move {
            let envelope = scoped_key(
                prefix,
                &format!("default/collections/{collection}/operations/{operation}/operation.json"),
            );
            let (record, _) = read_control(storage.as_ref(), &envelope).await;
            let attempt = record["attempt"].as_str().unwrap();
            scoped_key(
                prefix,
                &format!("_dispatch/00/pending/default/{collection}/{operation}.{attempt}"),
            )
        }
    };
    let first_hint = hint("retired", operation).await;
    let second_hint = hint("retired", second).await;
    let live_hint = hint("repair", operation).await;
    let active_uncommitted = scoped_key(
        prefix,
        &format!("_dispatch/00/pending/default/repair/{}", Uuid::from_u128(3)),
    );
    storage
        .put_control(
            &active_uncommitted,
            Bytes::from_static(b"{}"),
            Condition::Create,
        )
        .await
        .unwrap();
    let abandoned_attempt = Uuid::from_u128(5);
    let losing_hint = scoped_key(
        prefix,
        &format!("_dispatch/00/pending/default/repair/{operation}.{abandoned_attempt}"),
    );
    storage
        .put_control(&losing_hint, Bytes::from_static(b"{}"), Condition::Create)
        .await
        .unwrap();
    let head = scoped_key(prefix, "default/collections/retired/HEAD.json");
    let operation_key = scoped_key(
        prefix,
        &format!("default/collections/retired/operations/{second}/operation.json"),
    );
    storage.head(&first_hint).await.unwrap();
    storage.head(&second_hint).await.unwrap();

    let (early_worker, _early_server) =
        serve_http(crate::hosted_worker::router(make_catalog())).await;
    let response = http
        .post(format!("{early_worker}/internal/tasks/prepare-upload"))
        .json(&json!({ "tenant": "default", "collection": "retired", "operation_id": second }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let (record, _) = read_control(storage.as_ref(), &operation_key).await;
    assert_eq!(record["stage"], "pending_indexing");
    assert!(record["preparation"].is_null());

    let standalone = HostedCatalog::new(storage.clone(), prefix.cloned(), 1024)
        .unwrap()
        .with_standalone_worker(Arc::new(tokio::sync::Notify::new()));
    let retired_page = standalone
        .work_standalone_shard(0, None, || false)
        .await
        .unwrap();
    assert_eq!((retired_page.dispatched, retired_page.failed), (1, 0));
    assert!(matches!(
        storage.head(&first_hint).await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        storage.head(&second_hint).await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        storage.head(&losing_hint).await,
        Err(Error::NotFound(_))
    ));
    storage.head(&operation_key).await.unwrap();
    storage.head(&live_hint).await.unwrap();

    // Simulate an operation-only hint left by an older server.
    let legacy_hint = scoped_key(
        prefix,
        &format!("_dispatch/00/pending/default/retired/{second}"),
    );
    storage
        .put_control(&legacy_hint, Bytes::from_static(b"{}"), Condition::Create)
        .await
        .unwrap();
    let (mut manifest, version) = read_control(storage.as_ref(), &head).await;
    manifest["deleted_at_ms"] = json!(
        (SystemTime::now() - Duration::from_secs(901))
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    );
    storage
        .put_control(
            &head,
            serde_json::to_vec(&manifest).unwrap().into(),
            Condition::Match(version),
        )
        .await
        .unwrap();
    let sweeper = make_catalog();
    loop {
        let report = sweeper.sweep_hosted_snapshots().await.unwrap();
        assert_eq!(report.failed, 0);
        if !report.more {
            break;
        }
    }
    assert!(matches!(
        storage.head(&operation_key).await,
        Err(Error::NotFound(_))
    ));
    storage.head(&legacy_hint).await.unwrap();

    queue.enabled.store(true, Ordering::SeqCst);
    let (resumed, _resumed_server) = serve_http(crate::hosted_worker::router(make_catalog())).await;
    let replay_hint = scoped_key(
        prefix,
        &format!("_dispatch/00/pending/default/retired/rebuild-{second}"),
    );
    storage
        .put_control(&replay_hint, Bytes::from_static(b"{}"), Condition::Create)
        .await
        .unwrap();
    assert_eq!(
        reconcile(http, &resumed, 0).await,
        json!({"dispatched": 1, "failed": 0, "more": false}),
    );
    assert!(matches!(
        storage.head(&legacy_hint).await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        storage.head(&replay_hint).await,
        Err(Error::NotFound(_))
    ));
    storage.head(&live_hint).await.unwrap();
    storage.head(&active_uncommitted).await.unwrap();
    assert_eq!(queue.accepted.lock().unwrap().len(), accepted_before + 1);
    for stage in [
        "register-upload",
        "prepare-upload",
        "index-upload",
        "embed-upload",
        "rebuild-collection",
    ] {
        let mut payload = json!({
            "tenant": "default", "collection": "retired", "operation_id": second,
        });
        if matches!(stage, "embed-upload" | "rebuild-collection") {
            payload["step"] = json!(0);
        }
        let response = http
            .post(format!("{resumed}/internal/tasks/{stage}"))
            .json(&payload)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT, "{stage}");
    }
    assert_eq!(queue.accepted.lock().unwrap().len(), accepted_before + 1);
}

async fn submit_source(
    client: &mut SanctumClient<Channel>,
    scope: &v1::Scope,
    operation: Uuid,
    name: &str,
    bytes: &[u8],
) -> v1::IngestionJob {
    let frames = [
        Frame::Header(v1::UploadTextHeader {
            scope: Some(scope.clone()),
            request_id: Some(operation.to_string()),
            name: name.into(),
            ..Default::default()
        }),
        Frame::Data(bytes.to_vec()),
        Frame::End(v1::UploadTextEnd {}),
    ]
    .map(|frame| v1::UploadTextFrame { frame: Some(frame) });
    client
        .submit_text(tokio_stream::iter(frames))
        .await
        .unwrap()
        .into_inner()
}

async fn collection_generation(storage: &dyn Storage, prefix: Option<&Key>) -> Uuid {
    let key = scoped_key(prefix, "default/collections/repair/HEAD.json");
    let (head, _) = read_control(storage, &key).await;
    serde_json::from_value(head["generation"]["id"].clone()).unwrap()
}

fn scoped_key(prefix: Option<&Key>, path: &str) -> Key {
    Key::parse(prefix.map_or_else(|| path.to_owned(), |prefix| format!("{prefix}/{path}"))).unwrap()
}

async fn read_control(storage: &dyn Storage, key: &Key) -> (Value, Version) {
    let mut download = storage.get(key).await.unwrap();
    let mut bytes = Vec::new();
    download.reader.read_to_end(&mut bytes).await.unwrap();
    (
        serde_json::from_slice(&bytes).unwrap(),
        download.object.version,
    )
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
