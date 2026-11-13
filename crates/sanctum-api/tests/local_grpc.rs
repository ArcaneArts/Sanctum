use sanctum_proto::v1::{self, sanctum_client::SanctumClient, upload_text_frame::Frame};
use sanctum_service::{Service, run_local_worker};
use tempfile::TempDir;
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{
    Code,
    transport::{Channel, Server},
};

struct Running {
    service: Service,
    client: SanctumClient<Channel>,
    stop: oneshot::Sender<()>,
    server: JoinHandle<Result<(), tonic::transport::Error>>,
}

impl Running {
    async fn start(dir: &TempDir) -> Self {
        let service = Service::open(dir.path(), 32_768).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (stop, shutdown) = oneshot::channel();
        let server_service = service.clone();
        let server = tokio::spawn(async move {
            let (stop_worker, worker_stop) = tokio::sync::watch::channel(false);
            let worker = tokio::spawn(run_local_worker(server_service.clone(), worker_stop));
            let result = Server::builder()
                .add_service(sanctum_api::grpc(server_service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown.await;
                })
                .await;
            stop_worker.send(true).unwrap();
            worker.await.unwrap();
            result
        });
        let client = SanctumClient::connect(endpoint).await.unwrap();
        Self {
            service,
            client,
            stop,
            server,
        }
    }

    async fn stop(self) {
        drop(self.client);
        self.stop.send(()).unwrap();
        self.server.await.unwrap().unwrap();
    }
}

fn scope(tenant: &str) -> Option<v1::Scope> {
    Some(v1::Scope {
        tenant: tenant.into(),
        collection: "notes".into(),
    })
}

fn header(tenant: &str) -> v1::UploadTextFrame {
    v1::UploadTextFrame {
        frame: Some(Frame::Header(v1::UploadTextHeader {
            scope: scope(tenant),
            name: "field notes.md".into(),
            external: Some(v1::ExternalSource {
                namespace: "notes-app".into(),
                id: "entry:42".into(),
            }),
            replacement: None,
        })),
    }
}

fn data(bytes: &[u8]) -> v1::UploadTextFrame {
    v1::UploadTextFrame {
        frame: Some(Frame::Data(bytes.to_vec())),
    }
}

fn end() -> v1::UploadTextFrame {
    v1::UploadTextFrame {
        frame: Some(Frame::End(v1::UploadTextEnd {})),
    }
}

async fn create(client: &mut SanctumClient<Channel>, tenant: &str) {
    client
        .create_collection(v1::CreateCollectionRequest {
            scope: scope(tenant),
        })
        .await
        .unwrap();
}

async fn search(
    client: &mut SanctumClient<Channel>,
    tenant: &str,
    query: &str,
) -> Vec<v1::TextHit> {
    client
        .search_text(v1::SearchTextRequest {
            scope: scope(tenant),
            query: query.into(),
            limit: None,
        })
        .await
        .unwrap()
        .into_inner()
        .hits
}

#[tokio::test]
async fn upload_search_fetch_survives_restart_and_stays_in_tenant() {
    let dir = TempDir::new().unwrap();
    let mut first = Running::start(&dir).await;
    assert!(matches!(
        Service::open(dir.path(), 32_768),
        Err(sanctum_core::Error::Busy(_))
    ));
    create(&mut first.client, "default").await;
    create(&mut first.client, "other").await;

    let text = format!(
        "{}\n{}🪨 quartz specimen\n",
        "a".repeat(4000),
        "β".repeat(600)
    );
    let bytes = text.as_bytes();
    let split = text.find('🪨').unwrap() + 1;
    let revision = first
        .client
        .upload_text(tokio_stream::iter([
            header("default"),
            data(&bytes[..split]),
            data(&bytes[split..]),
            end(),
        ]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(revision.bytes, bytes.len() as u64);
    assert!(revision.chunks > 1);
    first.stop().await;

    let mut second = Running::start(&dir).await;
    let response = second
        .client
        .list_collections(v1::ListCollectionsRequest {
            tenant: "default".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.collections[0].scope, scope("default"));
    assert!(
        search(&mut second.client, "other", "quartz")
            .await
            .is_empty()
    );
    let hits = search(&mut second.client, "default", "quartz specimen").await;
    assert!(!hits.is_empty());
    for hit in hits {
        assert_eq!(hit.external.as_ref().unwrap().id, "entry:42");
        assert_eq!(
            hit.excerpt,
            text[hit.start_byte as usize..hit.end_byte as usize]
        );
        let mut chunk = hit.chunk.clone().unwrap();
        assert_eq!(chunk.revision_id, revision.revision_id);
        let fetched = second
            .client
            .fetch_chunk(v1::FetchChunkRequest {
                chunk: Some(chunk.clone()),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(fetched, hit);
        chunk.scope = scope("other");
        let failure = second
            .client
            .fetch_chunk(v1::FetchChunkRequest { chunk: Some(chunk) })
            .await
            .unwrap_err();
        assert_eq!(failure.code(), Code::NotFound);
    }
    second.stop().await;
}

#[tokio::test]
async fn unfinished_oversized_and_invalid_uploads_leave_no_searchable_chunks() {
    let dir = TempDir::new().unwrap();
    let mut running = Running::start(&dir).await;
    create(&mut running.client, "default").await;

    let failure = running
        .client
        .upload_text(tokio_stream::iter([header("default"), data(b"unfinished")]))
        .await
        .unwrap_err();
    assert_eq!(failure.code(), Code::InvalidArgument);

    let failure = running
        .client
        .upload_text(tokio_stream::iter([
            header("default"),
            data(&vec![b'a'; 32_769]),
            end(),
        ]))
        .await
        .unwrap_err();
    assert_eq!(failure.code(), Code::ResourceExhausted);

    let mut bytes = b"unpublished ".repeat(600);
    bytes.push(0xff);
    let failure = running
        .client
        .upload_text(tokio_stream::iter([header("default"), data(&bytes), end()]))
        .await
        .unwrap_err();
    assert_eq!(failure.code(), Code::InvalidArgument);

    assert!(
        search(&mut running.client, "default", "unfinished")
            .await
            .is_empty()
    );
    assert!(
        search(&mut running.client, "default", "unpublished")
            .await
            .is_empty()
    );
    let staging = dir.path().join("default/notes/staging");
    assert_eq!(staging.read_dir().unwrap().count(), 0);

    running
        .client
        .upload_text(tokio_stream::iter([
            header("default"),
            data(b"published"),
            end(),
        ]))
        .await
        .unwrap();
    assert_eq!(
        search(&mut running.client, "default", "published")
            .await
            .len(),
        1
    );
    running.stop().await;
}

#[tokio::test]
async fn accepted_jobs_and_pause_survive_restart_and_cancellation_is_scoped() {
    let dir = TempDir::new().unwrap();
    let mut first = Running::start(&dir).await;
    create(&mut first.client, "default").await;
    first
        .client
        .set_local_queue(v1::SetLocalQueueRequest { paused: true })
        .await
        .unwrap();

    let queued = first
        .client
        .submit_text(tokio_stream::iter([
            header("default"),
            data(b"queued zeolite"),
            end(),
        ]))
        .await
        .unwrap()
        .into_inner();
    let canceled = first
        .client
        .submit_text(tokio_stream::iter([
            header("default"),
            data(b"canceled celestine"),
            end(),
        ]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(queued.state(), v1::IngestionState::Accepted);
    assert!(
        search(&mut first.client, "default", "zeolite")
            .await
            .is_empty()
    );
    first.stop().await;

    let mut second = Running::start(&dir).await;
    assert!(
        second
            .client
            .get_local_queue(v1::Empty {})
            .await
            .unwrap()
            .into_inner()
            .paused
    );
    let id = v1::IngestionJobRequest {
        scope: scope("default"),
        operation_id: queued.operation_id,
    };
    assert_eq!(
        second
            .client
            .get_ingestion_job(id.clone())
            .await
            .unwrap()
            .into_inner()
            .state(),
        v1::IngestionState::Accepted
    );

    let unauthorized_scope = v1::IngestionJobRequest {
        scope: scope("other"),
        operation_id: canceled.operation_id.clone(),
    };
    assert_eq!(
        second
            .client
            .cancel_ingestion_job(unauthorized_scope)
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    let canceled_id = v1::IngestionJobRequest {
        scope: scope("default"),
        operation_id: canceled.operation_id,
    };
    let job = second
        .client
        .cancel_ingestion_job(canceled_id)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(job.state(), v1::IngestionState::Canceled);

    second
        .client
        .set_local_queue(v1::SetLocalQueueRequest { paused: false })
        .await
        .unwrap();
    let done = await_ready(&mut second.client, id).await;
    assert_eq!(done.attempts, 1);
    assert_eq!(done.processed_bytes, done.total_bytes);
    assert_eq!(done.text_revision.unwrap().revision_id, queued.revision_id);
    assert_eq!(
        search(&mut second.client, "default", "zeolite").await.len(),
        1
    );
    assert!(
        search(&mut second.client, "default", "celestine")
            .await
            .is_empty()
    );
    second.stop().await;
}

#[tokio::test]
async fn recovery_reuses_a_collection_commit_with_a_missing_queue_receipt() {
    let dir = TempDir::new().unwrap();
    let mut first = Running::start(&dir).await;
    create(&mut first.client, "default").await;
    let job = first
        .client
        .submit_text(tokio_stream::iter([
            header("default"),
            data(b"receipt recovery zircon"),
            end(),
        ]))
        .await
        .unwrap()
        .into_inner();
    let id = v1::IngestionJobRequest {
        scope: scope("default"),
        operation_id: job.operation_id,
    };
    let complete = await_ready(&mut first.client, id.clone()).await;
    first.stop().await;

    let connection = rusqlite::Connection::open(dir.path().join("runtime.db")).unwrap();
    connection
        .execute(
            "UPDATE ingestion_jobs SET status = 'publishing', content_hash = NULL, chunks = NULL
         WHERE operation_id = ?1",
            [&id.operation_id],
        )
        .unwrap();
    drop(connection);

    let mut second = Running::start(&dir).await;
    let recovered = await_ready(&mut second.client, id).await;
    assert_eq!(recovered.attempts, 2);
    assert_eq!(recovered.text_revision, complete.text_revision);
    assert_eq!(
        search(&mut second.client, "default", "zircon").await.len(),
        1
    );
    second.stop().await;
}

async fn await_ready(
    client: &mut SanctumClient<Channel>,
    request: v1::IngestionJobRequest,
) -> v1::IngestionJob {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let job = client
                .get_ingestion_job(request.clone())
                .await
                .unwrap()
                .into_inner();
            match job.state() {
                v1::IngestionState::TextReady => return job,
                v1::IngestionState::Failed | v1::IngestionState::Canceled => panic!("{job:?}"),
                _ => tokio::time::sleep(std::time::Duration::from_millis(20)).await,
            }
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn retry_preserves_the_operation_and_revision_after_a_storage_failure() {
    let dir = TempDir::new().unwrap();
    let mut running = Running::start(&dir).await;
    create(&mut running.client, "default").await;
    running
        .client
        .set_local_queue(v1::SetLocalQueueRequest { paused: true })
        .await
        .unwrap();
    let job = running
        .client
        .submit_text(tokio_stream::iter([
            header("default"),
            data(b"recoverable labradorite"),
            end(),
        ]))
        .await
        .unwrap()
        .into_inner();
    let path = dir
        .path()
        .join("default/notes/sources")
        .join(&job.source_id)
        .join(&job.revision_id)
        .join("text.utf8");
    let backup = path.with_extension("backup");
    std::fs::rename(&path, &backup).unwrap();
    running
        .client
        .set_local_queue(v1::SetLocalQueueRequest { paused: false })
        .await
        .unwrap();
    let id = v1::IngestionJobRequest {
        scope: scope("default"),
        operation_id: job.operation_id,
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let state = running
                .client
                .get_ingestion_job(id.clone())
                .await
                .unwrap()
                .into_inner();
            if state.state() == v1::IngestionState::Failed {
                assert_eq!(state.error_code.as_deref(), Some("retryable"));
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    std::fs::rename(&backup, &path).unwrap();
    running
        .client
        .retry_ingestion_job(id.clone())
        .await
        .unwrap();
    let ready = await_ready(&mut running.client, id).await;
    assert_eq!(ready.attempts, 2);
    assert_eq!(ready.revision_id, job.revision_id);
    assert_eq!(
        search(&mut running.client, "default", "labradorite")
            .await
            .len(),
        1
    );
    running.stop().await;
}

#[tokio::test]
async fn replacement_preserves_identity_and_the_latest_intent_wins() {
    let dir = TempDir::new().unwrap();
    let mut running = Running::start(&dir).await;
    create(&mut running.client, "default").await;
    let original = upload(&mut running.client, header("default"), b"original agate").await;
    let old_chunk = search(&mut running.client, "default", "agate")
        .await
        .remove(0)
        .chunk
        .unwrap();

    running
        .client
        .set_local_queue(v1::SetLocalQueueRequest { paused: true })
        .await
        .unwrap();
    let old_candidate = running
        .client
        .submit_text(tokio_stream::iter([
            replacement(Some(&original.source_id), Some(&original.revision_id)),
            data(b"superseded pyrite"),
            end(),
        ]))
        .await
        .unwrap()
        .into_inner();
    let latest = running
        .client
        .submit_text(tokio_stream::iter([
            replacement(None, Some(&original.revision_id)),
            data(b"latest beryl"),
            end(),
        ]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(old_candidate.source_id, original.source_id);
    assert_eq!(latest.source_id, original.source_id);
    assert!(latest.update_sequence > old_candidate.update_sequence);
    assert_eq!(
        search(&mut running.client, "default", "agate").await.len(),
        1
    );
    assert!(
        search(&mut running.client, "default", "beryl")
            .await
            .is_empty()
    );

    running
        .client
        .set_local_queue(v1::SetLocalQueueRequest { paused: false })
        .await
        .unwrap();
    let latest = await_ready(&mut running.client, job_request(latest.operation_id)).await;
    let superseded =
        await_failure(&mut running.client, job_request(old_candidate.operation_id)).await;
    assert_eq!(superseded.error_code.as_deref(), Some("conflict"));
    assert!(
        search(&mut running.client, "default", "pyrite")
            .await
            .is_empty()
    );
    assert!(
        search(&mut running.client, "default", "agate")
            .await
            .is_empty()
    );
    let hits = search(&mut running.client, "default", "beryl").await;
    assert_eq!(
        hits[0].chunk.as_ref().unwrap().revision_id,
        latest.revision_id
    );
    assert_eq!(hits[0].external.as_ref().unwrap().id, "entry:42");
    assert_eq!(
        running
            .client
            .fetch_chunk(v1::FetchChunkRequest {
                chunk: Some(old_chunk.clone())
            })
            .await
            .unwrap()
            .into_inner()
            .excerpt,
        "original agate"
    );

    let stale = running
        .client
        .submit_text(tokio_stream::iter([
            replacement(Some(&original.source_id), Some(&original.revision_id)),
            data(b"stale"),
            end(),
        ]))
        .await
        .unwrap_err();
    assert_eq!(stale.code(), Code::FailedPrecondition);

    expire_and_reclaim(&dir, &running.service);
    assert!(!text_path(&dir, &original.source_id, &original.revision_id).exists());
    assert_eq!(
        running
            .client
            .fetch_chunk(v1::FetchChunkRequest {
                chunk: Some(old_chunk)
            })
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        search(&mut running.client, "default", "beryl").await.len(),
        1
    );
    running.stop().await;
}

#[tokio::test]
async fn deletion_blocks_queued_replacement_and_reclaims_all_revisions() {
    let dir = TempDir::new().unwrap();
    let mut running = Running::start(&dir).await;
    create(&mut running.client, "default").await;
    let first = upload(&mut running.client, header("default"), b"first opal").await;
    let current = upload(
        &mut running.client,
        replacement(Some(&first.source_id), None),
        b"second topaz",
    )
    .await;
    running
        .client
        .set_local_queue(v1::SetLocalQueueRequest { paused: true })
        .await
        .unwrap();
    let late = running
        .client
        .submit_text(tokio_stream::iter([
            replacement(Some(&first.source_id), None),
            data(b"late chalcedony"),
            end(),
        ]))
        .await
        .unwrap()
        .into_inner();
    let mut delete = v1::DeleteSourceRequest {
        scope: scope("default"),
        source_id: current.source_id.clone(),
        expected_revision_id: Some(first.revision_id.clone()),
    };
    assert_eq!(
        running
            .client
            .delete_source(delete.clone())
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    delete.expected_revision_id = Some(current.revision_id.clone());
    running.client.delete_source(delete).await.unwrap();
    assert!(
        search(&mut running.client, "default", "topaz")
            .await
            .is_empty()
    );
    running
        .client
        .set_local_queue(v1::SetLocalQueueRequest { paused: false })
        .await
        .unwrap();
    let failed = await_failure(&mut running.client, job_request(late.operation_id)).await;
    assert_eq!(failed.error_code.as_deref(), Some("conflict"));
    expire_and_reclaim(&dir, &running.service);
    assert!(!text_path(&dir, &first.source_id, &first.revision_id).exists());
    assert!(!text_path(&dir, &current.source_id, &current.revision_id).exists());
    assert!(!text_path(&dir, &late.source_id, &late.revision_id).exists());
    let imported = upload(
        &mut running.client,
        header("default"),
        b"new source malachite",
    )
    .await;
    assert_ne!(imported.source_id, first.source_id);
    running.stop().await;
}

#[tokio::test]
async fn startup_migrates_v1_and_reclaims_files_left_before_acceptance() {
    let dir = TempDir::new().unwrap();
    let store = dir.path().join("default/notes");
    std::fs::create_dir_all(store.join("staging")).unwrap();
    std::fs::write(store.join("staging/.tmp-leftover"), b"truncated upload").unwrap();
    let catalog = rusqlite::Connection::open(dir.path().join("default/root.db")).unwrap();
    catalog
        .execute_batch(
            "PRAGMA user_version = 1;
         CREATE TABLE collections (id TEXT PRIMARY KEY NOT NULL);
         INSERT INTO collections VALUES ('notes');",
        )
        .unwrap();
    drop(catalog);
    let collection = rusqlite::Connection::open(store.join("meta.db")).unwrap();
    collection
        .execute_batch(include_str!("../../sanctum-db-sqlite/src/collection.sql"))
        .unwrap();
    collection.pragma_update(None, "user_version", 1).unwrap();
    drop(collection);
    let runtime = rusqlite::Connection::open(dir.path().join("runtime.db")).unwrap();
    runtime
        .execute_batch(include_str!("../../sanctum-db-sqlite/src/runtime.sql"))
        .unwrap();
    runtime.pragma_update(None, "user_version", 1).unwrap();
    drop(runtime);

    let orphan = text_path(
        &dir,
        &uuid::Uuid::new_v4().to_string(),
        &uuid::Uuid::new_v4().to_string(),
    );
    std::fs::create_dir_all(orphan.parent().unwrap()).unwrap();
    std::fs::write(&orphan, b"persisted before the queue commit").unwrap();
    let mut running = Running::start(&dir).await;
    assert!(!orphan.exists());
    assert!(!store.join("staging/.tmp-leftover").exists());
    let revision = upload(&mut running.client, header("default"), b"migrated obsidian").await;
    assert_eq!(
        search(&mut running.client, "default", "obsidian")
            .await
            .len(),
        1
    );
    assert!(text_path(&dir, &revision.source_id, &revision.revision_id).exists());
    running.stop().await;
}

fn replacement(source: Option<&str>, expected: Option<&str>) -> v1::UploadTextFrame {
    let mut request = header("default");
    if let Some(Frame::Header(header)) = &mut request.frame {
        header.replacement = Some(v1::Replacement {
            source_id: source.map(str::to_string),
            expected_revision_id: expected.map(str::to_string),
        });
    }
    request
}

fn job_request(operation_id: String) -> v1::IngestionJobRequest {
    v1::IngestionJobRequest {
        scope: scope("default"),
        operation_id,
    }
}

async fn upload(
    client: &mut SanctumClient<Channel>,
    header: v1::UploadTextFrame,
    bytes: &[u8],
) -> v1::TextRevision {
    client
        .upload_text(tokio_stream::iter([header, data(bytes), end()]))
        .await
        .unwrap()
        .into_inner()
}

async fn await_failure(
    client: &mut SanctumClient<Channel>,
    request: v1::IngestionJobRequest,
) -> v1::IngestionJob {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let job = client
                .get_ingestion_job(request.clone())
                .await
                .unwrap()
                .into_inner();
            match job.state() {
                v1::IngestionState::Failed => return job,
                v1::IngestionState::TextReady | v1::IngestionState::Canceled => panic!("{job:?}"),
                _ => tokio::time::sleep(std::time::Duration::from_millis(20)).await,
            }
        }
    })
    .await
    .unwrap()
}

fn text_path(dir: &TempDir, source: &str, revision: &str) -> std::path::PathBuf {
    dir.path()
        .join("default/notes/sources")
        .join(source)
        .join(revision)
        .join("text.utf8")
}

fn expire_and_reclaim(dir: &TempDir, service: &Service) {
    let db = rusqlite::Connection::open(dir.path().join("default/notes/meta.db")).unwrap();
    db.execute(
        "UPDATE revisions SET retire_at = 0 WHERE retire_at IS NOT NULL",
        [],
    )
    .unwrap();
    drop(db);
    service
        .reclaim_retired(&sanctum_core::Scope {
            tenant: "default".parse().unwrap(),
            collection: "notes".parse().unwrap(),
        })
        .unwrap();
}
