use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use prost::Message;
use sanctum_embedding::{EmbedError, Provider, Purpose};
use sanctum_proto::v1::{self, sanctum_client::SanctumClient, upload_text_frame::Frame};
use sanctum_service::{
    Service, run_embedding_worker, run_local_maintenance, run_local_worker, run_watched_folders,
};
use tempfile::TempDir;
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
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
        Self::with_embedding(dir, None).await
    }

    async fn with_embedding(dir: &TempDir, provider: Option<Arc<dyn Provider>>) -> Self {
        Self::start_with_workers(dir, provider, true).await
    }

    async fn start_with_workers(
        dir: &TempDir,
        provider: Option<Arc<dyn Provider>>,
        workers: bool,
    ) -> Self {
        let service = Service::open_with_embedding(dir.path(), 32_768, provider).unwrap();
        Self::with_service(service, workers).await
    }

    async fn with_service(service: Service, workers: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (stop, shutdown) = oneshot::channel();
        let server_service = service.clone();
        let server = tokio::spawn(async move {
            let (stop_worker, worker_stop) = tokio::sync::watch::channel(false);
            let workers = workers.then(|| {
                let worker = tokio::spawn(run_local_worker(
                    server_service.clone(),
                    worker_stop.clone(),
                ));
                let embedding = tokio::spawn(run_embedding_worker(
                    server_service.clone(),
                    worker_stop.clone(),
                ));
                let watcher =
                    tokio::spawn(run_watched_folders(server_service.clone(), worker_stop));
                (worker, embedding, watcher)
            });
            let cancellation = CancellationToken::new();
            let result = Server::builder()
                .add_service(sanctum_api::grpc(server_service, cancellation.clone()))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown.await;
                    cancellation.cancel();
                })
                .await;
            let _ = stop_worker.send(true);
            if let Some((worker, embedding, watcher)) = workers {
                worker.await.unwrap();
                embedding.await.unwrap();
                watcher.await.unwrap();
            }
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

#[tokio::test]
async fn local_upload_request_ids_replay_across_concurrency_restart_and_expiry() {
    let dir = TempDir::new().unwrap();
    let mut running = Running::start_with_workers(&dir, None, false).await;
    create(&mut running.client, "default").await;
    create(&mut running.client, "other").await;
    let uuid = uuid::Uuid::new_v4();
    let with_id = |mut frame: v1::UploadTextFrame| {
        if let Some(Frame::Header(header)) = &mut frame.frame {
            header.request_id = Some(uuid.to_string());
        }
        frame
    };
    let frames = |frame, bytes| tokio_stream::iter([frame, data(bytes), end()]);
    let input = with_id(header("default"));
    let text = b"local idempotency quartz".as_slice();
    let mut second = running.client.clone();
    let (left, right) = tokio::join!(
        running.client.submit_text(frames(input.clone(), text)),
        second.submit_text(frames(input.clone(), text)),
    );
    let first = left.unwrap().into_inner();
    assert_eq!(first, right.unwrap().into_inner());
    assert_eq!(
        running
            .client
            .submit_text(frames(input.clone(), b"different"))
            .await
            .unwrap_err()
            .code(),
        Code::AlreadyExists
    );
    let other = running
        .client
        .submit_text(frames(with_id(header("other")), text))
        .await
        .unwrap()
        .into_inner();
    assert_ne!(first.operation_id, other.operation_id);
    drop(second);
    running.stop().await;

    let mut running = Running::start_with_workers(&dir, None, false).await;
    assert_eq!(
        running
            .client
            .submit_text(frames(input.clone(), text))
            .await
            .unwrap()
            .into_inner(),
        first
    );
    assert!(running.service.process_next().unwrap());
    if running
        .client
        .get_ingestion_job(job_request(first.operation_id.clone()))
        .await
        .unwrap()
        .into_inner()
        .state()
        != v1::IngestionState::TextReady
    {
        assert!(running.service.process_next().unwrap());
    }
    let replay = running
        .client
        .upload_text(frames(input.clone(), text))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(replay.revision_id, first.revision_id);
    let db = rusqlite::Connection::open(dir.path().join("runtime.db")).unwrap();
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM ingestion_jobs WHERE tenant = 'default'",
            [],
            |row| row.get::<_, u32>(0)
        )
        .unwrap(),
        1
    );
    db.execute(
        "UPDATE ingestion_jobs SET finished_at = 0 WHERE operation_id = ?1",
        [&first.operation_id],
    )
    .unwrap();
    drop(db);
    let (stop, shutdown) = tokio::sync::watch::channel(false);
    let maintenance = tokio::spawn(run_local_maintenance(running.service.clone(), shutdown));
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if running
                .client
                .get_ingestion_job(job_request(first.operation_id.clone()))
                .await
                .is_err_and(|error| error.code() == Code::NotFound)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    stop.send(true).unwrap();
    maintenance.await.unwrap();
    let expired = running
        .client
        .submit_text(frames(input, text))
        .await
        .unwrap_err();
    assert_eq!(expired.code(), Code::AlreadyExists);
    assert!(expired.message().contains("expired"));
    assert_eq!(
        search(&mut running.client, "default", "quartz").await.len(),
        1
    );
    running.stop().await;
}

#[tokio::test]
async fn local_pending_literal_scan_does_not_require_lexical_publication() {
    use v1::pending_text_scan_event::Event;
    let dir = TempDir::new().unwrap();
    let mut api = Running::start_with_workers(&dir, None, false).await;
    create(&mut api.client, "default").await;
    let input = "config: key=🦀café";
    let queued = api
        .client
        .submit_text(tokio_stream::iter([
            header("default"),
            data(input.as_bytes()),
            end(),
        ]))
        .await
        .unwrap()
        .into_inner();
    assert!(search(&mut api.client, "default", "café").await.is_empty());
    let mut scan = api
        .client
        .scan_pending_text(v1::ScanPendingTextRequest {
            job: Some(job_request(queued.operation_id)),
            literal: "=🦀café".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(
        matches!(scan.message().await.unwrap().unwrap().event, Some(Event::Source(source)) if source.source_id.as_deref() == Some(&queued.source_id))
    );
    let Some(Event::Progress(progress)) = scan.message().await.unwrap().unwrap().event else {
        panic!("missing scan progress")
    };
    assert_eq!(progress.stop(), v1::PendingTextScanStop::Complete);
    let [hit] = progress.matches.as_slice() else {
        panic!("expected one match")
    };
    assert_eq!(
        &input[hit.start_byte as usize..hit.end_byte as usize],
        "=🦀café"
    );
    assert!(scan.message().await.unwrap().is_none());
    api.stop().await;
}

#[tokio::test]
async fn local_upload_keeps_accepted_chunk_settings_across_restart() {
    let dir = TempDir::new().unwrap();
    let configured = Service::open(dir.path(), 32_768)
        .unwrap()
        .with_chunking(sanctum_core::ChunkSettings {
            window_bytes: 256,
            overlap_bytes: 32,
        })
        .unwrap();
    let mut api = Running::with_service(configured, false).await;
    create(&mut api.client, "default").await;
    let accepted = api
        .client
        .submit_text(tokio_stream::iter(vec![
            header("default"),
            v1::UploadTextFrame {
                frame: Some(Frame::Data(vec![b'x'; 1024])),
            },
            v1::UploadTextFrame {
                frame: Some(Frame::End(v1::UploadTextEnd {})),
            },
        ]))
        .await
        .unwrap()
        .into_inner();
    api.stop().await;

    let mut api = Running::start(&dir).await;
    let result = await_ready(&mut api.client, job_request(accepted.operation_id)).await;
    let revision = result.text_revision.unwrap();
    assert_eq!(revision.chunker, "utf8-overlap-v1/256/32");
    assert!(revision.chunks > 1);
    api.stop().await;
}

#[tokio::test]
async fn local_collection_deletion_purges_jobs_and_recovers_from_a_tombstone() {
    let dir = TempDir::new().unwrap();
    let mut api = Running::start(&dir).await;
    let info = api
        .client
        .get_runtime_info(v1::Empty {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        (info.status.as_str(), info.storage.as_str()),
        ("serving", "filesystem")
    );
    assert!(info.capabilities.contains(&"local_import".into()));
    create(&mut api.client, "default").await;
    create(&mut api.client, "other").await;
    let renamed = api
        .client
        .update_collection(v1::UpdateCollectionRequest {
            scope: scope("default"),
            display_name: "Local notes".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(renamed.scope, scope("default"));
    assert_eq!(renamed.display_name.as_deref(), Some("Local notes"));

    let uploaded = upload(&mut api.client, header("default"), b"indexed quartz").await;
    let session = api
        .client
        .open_database_session(v1::DatabaseTarget {
            tenant: "default".into(),
            collection: Some("notes".into()),
        })
        .await
        .unwrap()
        .into_inner()
        .session
        .unwrap();
    api.service.pause_queue(true).unwrap();
    let queued = api
        .client
        .submit_text(tokio_stream::iter(vec![
            header("default"),
            v1::UploadTextFrame {
                frame: Some(Frame::Data(b"pending text".to_vec())),
            },
            v1::UploadTextFrame {
                frame: Some(Frame::End(v1::UploadTextEnd {})),
            },
        ]))
        .await
        .unwrap()
        .into_inner();
    let request = v1::DeleteCollectionRequest {
        scope: scope("default"),
        request_id: None,
    };
    api.client.delete_collection(request.clone()).await.unwrap();
    api.client.delete_collection(request).await.unwrap();
    assert!(!dir.path().join("default/notes").exists());
    assert!(dir.path().join("other/notes/meta.db").exists());
    assert_eq!(
        api.client
            .get_ingestion_job(v1::IngestionJobRequest {
                scope: scope("default"),
                operation_id: queued.operation_id,
            })
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    let page = api
        .client
        .query_database(v1::QueryDatabaseRequest {
            session: Some(session),
            sql: "SELECT current_revision FROM sources".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        page.rows[0].values[0].value,
        Some(v1::sql_value::Value::Text(uploaded.revision_id))
    );
    api.stop().await;

    // Simulate a stop after catalog publication and before removal of the files.
    sanctum_db_sqlite::retire_local_collection(&dir.path().join("other/root.db"), "notes").unwrap();
    assert!(dir.path().join("other/notes/meta.db").exists());
    let mut api = Running::start(&dir).await;
    assert!(!dir.path().join("other/notes").exists());
    assert!(
        sanctum_db_sqlite::deleted_catalog_collections(&dir.path().join("other/root.db"))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        api.client
            .create_collection(v1::CreateCollectionRequest {
                scope: scope("default"),
                request_id: None,
            })
            .await
            .unwrap_err()
            .code(),
        Code::AlreadyExists
    );
    api.stop().await;
}

#[tokio::test]
async fn database_sessions_pin_and_download_a_scoped_read_only_generation() {
    use std::io::Write;
    use v1::sql_value::Value;

    let dir = TempDir::new().unwrap();
    let mut running = Running::start(&dir).await;
    let client = &mut running.client;
    create(client, "default").await;
    create(client, "other").await;
    let first = upload(client, header("default"), b"original copper note").await;

    let target = v1::DatabaseTarget {
        tenant: "default".into(),
        collection: Some("notes".into()),
    };
    let opened = client
        .open_database_session(target.clone())
        .await
        .unwrap()
        .into_inner();
    assert!(opened.schema_version > 0);
    assert!(opened.schema.iter().any(|entry| entry.name == "chunks_fts"));
    assert_eq!(opened.sqlite_features, ["FTS5"]);
    assert!(opened.bytes > 0);
    let handle = opened.session.unwrap();
    let newer = upload(
        client,
        replacement(Some(&first.source_id), Some(&first.revision_id)),
        b"updated copper note",
    )
    .await;
    assert_ne!(first.revision_id, newer.revision_id);

    let mut query = v1::QueryDatabaseRequest {
        session: Some(handle.clone()),
        sql: "SELECT current_revision, ?2, ?3, ?4 FROM sources WHERE id = ?1".into(),
        parameters: vec![
            v1::SqlValue {
                value: Some(Value::Text(first.source_id.clone())),
            },
            v1::SqlValue {
                value: Some(Value::Integer(i64::MAX)),
            },
            v1::SqlValue {
                value: Some(Value::Blob(vec![0, 255, 32])),
            },
            v1::SqlValue {
                value: Some(Value::Null(v1::Empty {})),
            },
        ],
        ..Default::default()
    };
    let page = client
        .query_database(query.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        page.rows[0].values[0].value,
        Some(Value::Text(first.revision_id.clone()))
    );
    assert_eq!(&page.rows[0].values[1..], &query.parameters[1..]);

    query.sql = "SELECT name FROM sqlite_schema ORDER BY name".into();
    query.parameters.clear();
    query.limit = Some(1);
    let page = client
        .query_database(query.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(page.rows.len(), 1);
    query.offset = page.next_offset.unwrap();
    let next = client
        .query_database(query.clone())
        .await
        .unwrap()
        .into_inner();
    assert_ne!(page.rows, next.rows);

    let mut wrong_scope = handle.clone();
    wrong_scope.target.as_mut().unwrap().tenant = "other".into();
    query.session = Some(wrong_scope.clone());
    assert_eq!(
        client
            .query_database(query.clone())
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    assert_eq!(
        client
            .close_database_session(wrong_scope.clone())
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    assert_eq!(
        client
            .download_database(wrong_scope)
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );

    query.session = Some(handle.clone());
    query.offset = 0;
    for sql in [
        "UPDATE sources SET name = 'forbidden'",
        "SELECT 1; SELECT 2",
        "SELECT load_extension('forbidden')",
        "ATTACH DATABASE ':memory:' AS other",
        "PRAGMA query_only = OFF",
    ] {
        query.sql = sql.into();
        assert_eq!(
            client
                .query_database(query.clone())
                .await
                .unwrap_err()
                .code(),
            Code::InvalidArgument,
            "{sql}"
        );
    }

    let session = running
        .service
        .database_session(
            &sanctum_service::DatabaseTarget {
                tenant: "default".parse().unwrap(),
                collection: Some("notes".parse().unwrap()),
            },
            handle.generation.parse().unwrap(),
        )
        .unwrap();
    let checks = AtomicUsize::new(0);
    let interrupted = session.query(
        "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<10000000)
         SELECT sum(x) FROM n",
        Vec::new(),
        0,
        1,
        move || checks.fetch_add(1, Ordering::Relaxed) > 3,
    );
    assert!(matches!(interrupted, Err(sanctum_core::Error::Busy(_))));
    drop(session);

    let mut stream = client
        .download_database(handle.clone())
        .await
        .unwrap()
        .into_inner();
    let mut downloaded = tempfile::NamedTempFile::new().unwrap();
    let mut bytes = 0;
    while let Some(frame) = stream.message().await.unwrap() {
        assert!(frame.data.len() <= sanctum_core::UPLOAD_FRAME_BYTES);
        downloaded.write_all(&frame.data).unwrap();
        bytes += frame.data.len() as u64;
    }
    downloaded.flush().unwrap();
    assert_eq!(bytes, opened.bytes);
    let db = rusqlite::Connection::open_with_flags(
        downloaded.path(),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let result: (String, String) = db.query_row(
        "SELECT current_revision, (SELECT integrity_check FROM pragma_integrity_check LIMIT 1) FROM sources",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    ).unwrap();
    assert_eq!(result, (first.revision_id, "ok".into()));
    let mode: String = db
        .pragma_query_value(None, "journal_mode", |row| row.get(0))
        .unwrap();
    assert_eq!(mode, "delete");
    drop(db);

    client.close_database_session(handle.clone()).await.unwrap();
    assert_eq!(
        client.download_database(handle).await.unwrap_err().code(),
        Code::NotFound
    );
    let catalog = client
        .open_database_session(v1::DatabaseTarget {
            tenant: "default".into(),
            collection: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(catalog.sqlite_features.is_empty());
    assert!(
        catalog
            .schema
            .iter()
            .any(|entry| entry.name == "collection_metadata")
    );
    let catalog_handle = catalog.session.unwrap();
    running.stop().await;
    let mut running = Running::start(&dir).await;
    assert_eq!(
        running
            .client
            .download_database(catalog_handle)
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    assert_eq!(
        std::fs::read_dir(dir.path().join(".database-sessions")).map_or(0, Iterator::count),
        0
    );
    running.stop().await;
}

#[tokio::test]
async fn change_stream_replays_and_resumes_in_scope() {
    let dir = TempDir::new().unwrap();
    let mut running = Running::start(&dir).await;
    create(&mut running.client, "default").await;
    create(&mut running.client, "other").await;
    let first = upload(
        &mut running.client,
        header("default"),
        b"before the subscription",
    )
    .await;
    let cursor = running
        .client
        .get_change_cursor(v1::ChangeCursorRequest {
            scope: scope("default"),
        })
        .await
        .unwrap()
        .into_inner()
        .cursor;
    assert!(cursor > 0);
    let mut request = v1::ListChangesRequest {
        scope: scope("default"),
        after_cursor: cursor,
        limit: Some(1),
    };
    let mut stream = running
        .client
        .watch_changes(request.clone())
        .await
        .unwrap()
        .into_inner();
    let initial = next_changes(&mut stream).await;
    assert!(initial.changes.is_empty());
    assert_eq!(initial.next_cursor, cursor);
    upload(&mut running.client, header("other"), b"from another tenant").await;
    let revision = upload(
        &mut running.client,
        replacement(Some(&first.source_id), Some(&first.revision_id)),
        b"replacement content",
    )
    .await;
    let replaced = next_changes(&mut stream).await;
    assert_eq!(replaced.changes.len(), 1);
    assert_eq!(replaced.changes[0].kind, "source_replaced");
    assert_eq!(replaced.changes[0].revision_id, revision.revision_id);
    drop(stream);

    request.after_cursor = replaced.next_cursor;
    let mut resumed = running
        .client
        .watch_changes(request.clone())
        .await
        .unwrap()
        .into_inner();
    let reset = next_changes(&mut resumed).await;
    assert_eq!(reset.changes.len(), 1);
    assert_eq!(reset.changes[0].kind, "metadata_reset");
    assert_eq!(reset.changes[0].source_id, first.source_id);
    assert!(reset.next_cursor > replaced.next_cursor);
    let current = running
        .client
        .get_change_cursor(v1::ChangeCursorRequest {
            scope: scope("default"),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(current.cursor, reset.next_cursor);
    request.after_cursor = current.cursor + 1;
    assert_eq!(
        running
            .client
            .watch_changes(request)
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );

    tokio::time::timeout(Duration::from_secs(5), running.stop())
        .await
        .unwrap();
    assert!(resumed.message().await.unwrap().is_none());
}

#[tokio::test]
async fn change_stream_reports_an_expired_cursor_and_can_restart_at_head() {
    let dir = TempDir::new().unwrap();
    let mut running = Running::start(&dir).await;
    create(&mut running.client, "default").await;
    let revision = upload(&mut running.client, header("default"), b"old event").await;
    let db = rusqlite::Connection::open(dir.path().join("default/notes/meta.db")).unwrap();
    db.execute(
        "WITH RECURSIVE events(n) AS (
            VALUES (1) UNION ALL SELECT n + 1 FROM events WHERE n < 10000
        ) INSERT INTO source_changes (kind, source, revision)
          SELECT 'metadata_changed', ?1, ?2 FROM events",
        rusqlite::params![revision.source_id, revision.revision_id],
    )
    .unwrap();
    let newest = upload(
        &mut running.client,
        v1::UploadTextFrame {
            frame: Some(Frame::Header(v1::UploadTextHeader {
                scope: scope("default"),
                name: "retention.md".into(),
                ..Default::default()
            })),
        },
        b"trigger retention",
    )
    .await;
    let (oldest, retained): (i64, i64) = db
        .query_row(
            "SELECT min(cursor), count(*) FROM source_changes",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!((oldest, retained), (3, 10000));
    assert_ne!(newest.revision_id, revision.revision_id);
    drop(db);

    let mut request = v1::ListChangesRequest {
        scope: scope("default"),
        after_cursor: 0,
        limit: None,
    };
    let error = running
        .client
        .watch_changes(request.clone())
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("rescan"));
    assert_eq!(
        running
            .client
            .list_changes(request.clone())
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    request.after_cursor = running
        .client
        .get_change_cursor(v1::ChangeCursorRequest {
            scope: scope("default"),
        })
        .await
        .unwrap()
        .into_inner()
        .cursor;
    assert_eq!(request.after_cursor, 10002);
    let mut stream = running
        .client
        .watch_changes(request.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        next_changes(&mut stream).await.next_cursor,
        request.after_cursor
    );
    drop(stream);
    running.stop().await;
}

async fn next_changes(
    stream: &mut tonic::Streaming<v1::ListChangesResponse>,
) -> v1::ListChangesResponse {
    tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn git_import_reads_committed_text_and_reconciles_each_ref() {
    use v1::{ImportState, import_git_request::Repository};
    let repo = TempDir::new().unwrap();
    let root = repo.path();
    git_fixture(root, &["init", "-b", "main"]);
    std::fs::write(root.join("keep.md"), "Committed rhodonite café.").unwrap();
    std::fs::write(root.join("gone.md"), "An old peridot note.").unwrap();
    std::fs::write(root.join("ignored.yml"), "unselected: serpentine").unwrap();
    git_fixture(root, &["add", "."]);
    git_fixture(root, &["commit", "-m", "Initial"]);
    let initial = git_fixture(root, &["rev-parse", "HEAD"]);
    let initial_blob = git_fixture(root, &["rev-parse", "HEAD:keep.md"]);
    git_fixture(root, &["tag", "-a", "first", "-m", "First"]);
    std::fs::write(root.join("keep.md"), "Uncommitted kunzite.").unwrap();

    let dir = TempDir::new().unwrap();
    let mut running = Running::start(&dir).await;
    create(&mut running.client, "default").await;
    create(&mut running.client, "other").await;
    let mut request = v1::ImportGitRequest {
        scope: scope("default"),
        repository: Some(Repository::LocalPath(root.to_str().unwrap().into())),
        reference: "refs/heads/main".into(),
        include: vec!["*.md".into()],
        ..Default::default()
    };
    let events = git_events(&mut running.client, request.clone()).await;
    assert_eq!(events.len(), 2);
    let original = events
        .iter()
        .find(|event| event.file.as_ref().unwrap().path == "keep.md")
        .unwrap();
    let original_file = original.file.as_ref().unwrap();
    let original_git = original.git.as_ref().unwrap();
    assert_eq!(original_git.commit, initial);
    assert_eq!(original_git.blob, initial_blob);
    let source = original_file.source_id.clone();
    let revision = original_file.revision_id.clone();
    for event in &events {
        let file = event.file.as_ref().unwrap();
        assert_eq!(file.state(), ImportState::Queued);
        let job = await_ready(
            &mut running.client,
            job_request(file.operation_id.clone().unwrap()),
        )
        .await;
        assert_eq!(job.git, event.git);
        assert_eq!(job.text_revision.unwrap().git, event.git);
    }
    assert_eq!(
        search(&mut running.client, "default", "rhodonite").await[0].excerpt,
        "Committed rhodonite café."
    );
    assert!(
        search(&mut running.client, "default", "kunzite")
            .await
            .is_empty()
    );
    assert!(
        search(&mut running.client, "default", "serpentine")
            .await
            .is_empty()
    );
    assert!(
        search(&mut running.client, "other", "rhodonite")
            .await
            .is_empty()
    );

    let repeated = git_events(&mut running.client, request.clone()).await;
    assert!(
        repeated
            .iter()
            .all(|event| event.file.as_ref().unwrap().state() == ImportState::Unchanged)
    );

    std::fs::write(root.join("keep.md"), "Updated citrine café.").unwrap();
    std::fs::remove_file(root.join("gone.md")).unwrap();
    git_fixture(root, &["add", "-A"]);
    git_fixture(root, &["commit", "-m", "Update"]);
    let second = git_fixture(root, &["rev-parse", "HEAD"]);
    let events = git_events(&mut running.client, request.clone()).await;
    assert_eq!(events.len(), 2);
    let updated = events
        .iter()
        .find(|event| event.file.as_ref().unwrap().state() == ImportState::Queued)
        .unwrap();
    assert_eq!(updated.git.as_ref().unwrap().commit, second);
    let file = updated.file.as_ref().unwrap();
    assert_eq!(file.source_id, source);
    assert_ne!(file.revision_id, revision);
    await_ready(
        &mut running.client,
        job_request(file.operation_id.clone().unwrap()),
    )
    .await;
    assert!(
        events
            .iter()
            .any(|event| event.file.as_ref().unwrap().state() == ImportState::Missing)
    );
    assert_eq!(
        search(&mut running.client, "default", "peridot")
            .await
            .len(),
        1
    );

    // A too-large committed blob leaves existing data intact even with pruning enabled.
    std::fs::write(root.join("oversize.md"), vec![b'a'; 32_769]).unwrap();
    git_fixture(root, &["add", "oversize.md"]);
    git_fixture(root, &["commit", "-m", "Oversized file"]);
    request.prune_missing = true;
    let bad = git_events(&mut running.client, request.clone()).await;
    assert!(
        bad.iter()
            .any(|event| event.file.as_ref().unwrap().state() == ImportState::Failed)
    );
    assert!(
        bad.iter()
            .all(|event| event.file.as_ref().unwrap().state() != ImportState::Deleted)
    );
    assert_eq!(
        search(&mut running.client, "default", "peridot")
            .await
            .len(),
        1
    );

    git_fixture(root, &["rm", "oversize.md"]);
    git_fixture(root, &["commit", "-m", "Remove oversized file"]);
    let events = git_events(&mut running.client, request.clone()).await;
    assert!(
        events
            .iter()
            .any(|event| event.file.as_ref().unwrap().state() == ImportState::Deleted)
    );
    assert!(
        search(&mut running.client, "default", "peridot")
            .await
            .is_empty()
    );

    request.reference = "refs/tags/first".into();
    let tagged = git_events(&mut running.client, request).await;
    assert_eq!(tagged.len(), 2);
    assert!(
        tagged
            .iter()
            .all(|event| event.git.as_ref().unwrap().commit == initial)
    );
    assert!(
        tagged
            .iter()
            .all(|event| event.file.as_ref().unwrap().source_id != source)
    );
    running.stop().await;
}

async fn git_events(
    client: &mut SanctumClient<Channel>,
    request: v1::ImportGitRequest,
) -> Vec<v1::ImportGitEvent> {
    let mut stream = client.import_git(request).await.unwrap().into_inner();
    let mut events = Vec::new();
    while let Some(event) = stream.message().await.unwrap() {
        events.push(event);
    }
    events
}

fn git_fixture(path: &std::path::Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .current_dir(path)
        .args([
            "-c",
            "user.name=Sanctum Test",
            "-c",
            "user.email=test@example.org",
            "-c",
            "commit.gpgsign=false",
            "-c",
            "tag.gpgSign=false",
            "-c",
            "core.hooksPath=",
        ])
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "Git fixture: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().into()
}

#[tokio::test]
async fn url_import_tracks_provenance_and_reconciles_content() {
    use axum::{Router, http::header, response::Redirect, routing::get};

    let variant = Arc::new(AtomicUsize::new(0));
    let document = {
        let variant = variant.clone();
        move || {
            let variant = variant.clone();
            async move {
                let (text, etag) = match variant.load(Ordering::Relaxed) {
                    0 => ("A café keeps chrysanthemum notes.", r#""v1""#),
                    1 => ("A café keeps chrysanthemum notes.", r#""v2""#),
                    _ => ("A café keeps magnolia notes.", r#""v3""#),
                };
                (
                    [
                        (header::CONTENT_TYPE, "text/plain; charset=UTF-8"),
                        (header::ETAG, etag),
                        (header::LAST_MODIFIED, "Sun, 06 Nov 1994 08:49:37 GMT"),
                    ],
                    text,
                )
            }
        }
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let http = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/notes", get(|| async { Redirect::temporary("/raw.txt") }))
                .route("/raw.txt", get(document))
                .route(
                    "/report",
                    get(|| async {
                        (
                            [(header::CONTENT_TYPE, "application/rtf")],
                            r"{\rtf1\ansi A beryllium note.}",
                        )
                    }),
                ),
        )
        .await
        .unwrap();
    });
    let dir = TempDir::new().unwrap();
    let mut running = Running::start(&dir).await;
    create(&mut running.client, "default").await;
    create(&mut running.client, "other").await;
    let mut request = v1::ImportUrlRequest {
        scope: scope("default"),
        url: format!("{origin}/notes#ignored"),
        retain_original: false,
    };
    running
        .client
        .set_local_queue(v1::SetLocalQueueRequest { paused: true })
        .await
        .unwrap();
    let first = running
        .client
        .import_url(request.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first.state(), v1::ImportState::Queued);
    let provenance = first.url.clone().unwrap();
    assert_eq!(provenance.requested_url, format!("{origin}/notes"));
    assert_eq!(provenance.final_url, format!("{origin}/raw.txt"));
    assert_eq!(provenance.etag.as_deref(), Some(r#""v1""#));
    assert_eq!(
        provenance.last_modified.as_deref(),
        Some("Sun, 06 Nov 1994 08:49:37 GMT")
    );
    let pending = running
        .client
        .import_url(request.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(pending.state(), v1::ImportState::Pending);
    assert_eq!(pending.operation_id, first.operation_id);
    let job = job_request(first.operation_id.unwrap());
    running.stop().await;

    let mut running = Running::start(&dir).await;
    let accepted = running
        .client
        .get_ingestion_job(job.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(accepted.state(), v1::IngestionState::Accepted);
    assert_eq!(accepted.url, Some(provenance.clone()));
    let mut other = job.clone();
    other.scope = scope("other");
    assert_eq!(
        running
            .client
            .get_ingestion_job(other)
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    running
        .client
        .set_local_queue(v1::SetLocalQueueRequest { paused: false })
        .await
        .unwrap();
    let revision = await_ready(&mut running.client, job)
        .await
        .text_revision
        .unwrap();
    assert_eq!(revision.url, Some(provenance.clone()));
    assert_eq!(
        search(&mut running.client, "default", "chrysanthemum").await[0].excerpt,
        "A café keeps chrysanthemum notes."
    );
    assert!(
        search(&mut running.client, "other", "chrysanthemum")
            .await
            .is_empty()
    );

    let db = rusqlite::Connection::open(dir.path().join("default/notes/meta.db")).unwrap();
    let saved: String = db
        .query_row(
            "SELECT url_provenance FROM revisions WHERE id = ?1",
            [&first.revision_id],
            |row| row.get(0),
        )
        .unwrap();
    let saved: sanctum_core::UrlProvenance = serde_json::from_str(&saved).unwrap();
    assert_eq!(saved.final_url, provenance.final_url);
    assert_eq!(saved.etag, provenance.etag);

    variant.store(1, Ordering::Relaxed);
    let same = running
        .client
        .import_url(request.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(same.state(), v1::ImportState::Unchanged);
    assert_eq!(same.revision_id, first.revision_id);
    assert_eq!(same.url.unwrap().etag.as_deref(), Some(r#""v2""#));

    variant.store(2, Ordering::Relaxed);
    let updated = running
        .client
        .import_url(request.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(updated.state(), v1::ImportState::Queued);
    assert_eq!(updated.source_id, first.source_id);
    assert_ne!(updated.revision_id, first.revision_id);
    await_ready(
        &mut running.client,
        job_request(updated.operation_id.unwrap()),
    )
    .await;
    assert!(
        search(&mut running.client, "default", "chrysanthemum")
            .await
            .is_empty()
    );
    assert_eq!(
        search(&mut running.client, "default", "magnolia")
            .await
            .len(),
        1
    );

    request.url = format!("{origin}/report");
    request.retain_original = true;
    let rtf = running
        .client
        .import_url(request)
        .await
        .unwrap()
        .into_inner();
    let revision = await_ready(&mut running.client, job_request(rtf.operation_id.unwrap()))
        .await
        .text_revision
        .unwrap();
    assert!(revision.extractor.is_some());
    assert!(revision.retain_original);
    assert_eq!(
        search(&mut running.client, "default", "beryllium").await[0].excerpt,
        "A beryllium note."
    );
    running.stop().await;
    http.abort();
}

#[tokio::test]
async fn url_import_exposes_http_failures_and_enforces_size_and_origin_policy() {
    use axum::{
        Router,
        body::{Body, Bytes},
        http::{StatusCode, header},
        response::Redirect,
        routing::get,
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let origin = format!("http://{address}");
    // The hostname differs, but the address would reach this same server.
    let denied = format!("http://localhost:{}/target", address.port());
    let count = Arc::new(AtomicUsize::new(0));
    let hits = count.clone();
    let http = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route(
                    "/server-error",
                    get(|| async { StatusCode::SERVICE_UNAVAILABLE }),
                )
                .route(
                    "/binary",
                    get(|| async { ([(header::CONTENT_TYPE, "image/png")], "binary") }),
                )
                .route(
                    "/oversize",
                    get(|| async {
                        let chunks = tokio_stream::iter(
                            (0..3)
                                .map(|_| Ok::<_, std::io::Error>(Bytes::from(vec![b'a'; 16_384]))),
                        );
                        (
                            [(header::CONTENT_TYPE, "text/plain")],
                            Body::from_stream(chunks),
                        )
                    }),
                )
                .route(
                    "/redirect",
                    get(move || async move { Redirect::temporary(&denied) }),
                )
                .route(
                    "/target",
                    get(move || async move {
                        hits.fetch_add(1, Ordering::Relaxed);
                        "private"
                    }),
                ),
        )
        .await
        .unwrap();
    });
    let dir = TempDir::new().unwrap();
    let service = Service::open(dir.path(), 32_768)
        .unwrap()
        .with_url_origins(Some(HashMap::from([(origin.clone(), None)])))
        .unwrap();
    let mut running = Running::with_service(service, true).await;
    create(&mut running.client, "default").await;
    for (path, code, message) in [
        ("/missing", Code::InvalidArgument, "HTTP 404"),
        ("/server-error", Code::Unavailable, "HTTP 503"),
        ("/binary", Code::InvalidArgument, "content type"),
        ("/oversize", Code::InvalidArgument, "upload limit"),
        ("/redirect", Code::InvalidArgument, "origin is not allowed"),
    ] {
        let error = running
            .client
            .import_url(v1::ImportUrlRequest {
                scope: scope("default"),
                url: format!("{origin}{path}"),
                retain_original: false,
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), code, "{error}");
        assert!(error.message().contains(message), "{error}");
    }
    assert_eq!(count.load(Ordering::Relaxed), 0);
    assert!(
        running
            .client
            .list_ingestion_jobs(v1::ListIngestionJobsRequest {
                tenant: "default".into(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner()
            .jobs
            .is_empty()
    );
    running.stop().await;
    http.abort();
}

struct EmbeddingFixture {
    profile: &'static str,
    fail_queries: AtomicBool,
    fail_documents: AtomicBool,
    reject_input: bool,
    document_inputs: AtomicUsize,
    query_inputs: AtomicUsize,
}

impl EmbeddingFixture {
    fn new(profile: &'static str, reject_input: bool) -> Arc<Self> {
        Arc::new(Self {
            profile,
            fail_queries: AtomicBool::new(false),
            fail_documents: AtomicBool::new(false),
            reject_input,
            document_inputs: AtomicUsize::new(0),
            query_inputs: AtomicUsize::new(0),
        })
    }
}

#[async_trait::async_trait]
impl Provider for EmbeddingFixture {
    fn profile_id(&self) -> &str {
        self.profile
    }

    fn dimensions(&self) -> usize {
        2
    }

    async fn embed(&self, purpose: Purpose, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
        if matches!(purpose, Purpose::Query) {
            self.query_inputs.fetch_add(texts.len(), Ordering::SeqCst);
        }
        if matches!(purpose, Purpose::Query) && self.fail_queries.load(Ordering::SeqCst) {
            return Err(EmbedError::Transient("simulated provider outage".into()));
        }
        if matches!(purpose, Purpose::Document) {
            if self.fail_documents.load(Ordering::SeqCst) {
                return Err(EmbedError::Transient("simulated document outage".into()));
            }
            if self.reject_input && texts.iter().any(|text| text.contains("reject input")) {
                return Err(EmbedError::Input("simulated context limit".into()));
            }
            self.document_inputs
                .fetch_add(texts.len(), Ordering::SeqCst);
        }
        Ok(texts
            .iter()
            .map(|text| {
                if text.contains("quartz") || text.contains("minerals") {
                    vec![2.0, 0.0]
                } else {
                    vec![0.0, 2.0]
                }
            })
            .collect())
    }
}

fn vector_search(query: &str, mode: v1::SearchMode) -> v1::SearchTextRequest {
    v1::SearchTextRequest {
        scope: scope("default"),
        query: query.into(),
        limit: Some(1),
        metadata_filter: None,
        mode: mode.into(),
    }
}

async fn await_coverage(client: &mut SanctumClient<Channel>, embedded: u64, failed: u64) {
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let coverage = client
                .get_embedding_status(v1::EmbeddingStatusRequest {
                    scope: scope("default"),
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_inner()
                .coverage
                .unwrap();
            if coverage.embedded_chunks == embedded && coverage.failed_chunks == failed {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn search_collections_ranks_scoped_matches_with_one_query_embedding() {
    let dir = TempDir::new().unwrap();
    let provider = EmbeddingFixture::new("multi-profile", true);
    let mut running = Running::with_embedding(&dir, Some(provider.clone())).await;
    create(&mut running.client, "default").await;
    create(&mut running.client, "other").await;
    let reference_scope = Some(v1::Scope {
        tenant: "default".into(),
        collection: "reference".into(),
    });
    running
        .client
        .create_collection(v1::CreateCollectionRequest {
            scope: reference_scope.clone(),
            ..Default::default()
        })
        .await
        .unwrap();
    let a = upload(&mut running.client, header("default"), b"quartz notes").await;
    let mut second = header("default");
    if let Some(Frame::Header(header)) = &mut second.frame {
        header.scope = reference_scope.clone();
    }
    let b = upload(&mut running.client, second, b"quartz reject input").await;
    upload(&mut running.client, header("other"), b"quartz private").await;
    await_coverage(&mut running.client, 1, 0).await;
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let status = running
                .client
                .get_embedding_status(v1::EmbeddingStatusRequest {
                    scope: reference_scope.clone(),
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_inner();
            if status.coverage.unwrap().failed_chunks == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();

    let mut request = v1::SearchCollectionsRequest {
        tenant: "default".into(),
        collections: vec!["reference".into(), "notes".into()],
        query: "quartz".into(),
        limit: Some(2),
        mode: v1::SearchMode::Hybrid.into(),
        metadata_filter: None,
    };
    let result = running
        .client
        .search_collections(request.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(provider.query_inputs.load(Ordering::SeqCst), 1);
    assert_eq!(result.mode(), v1::SearchMode::Hybrid);
    assert_eq!(
        result.degraded_reason.as_deref(),
        Some("embedding_coverage_partial")
    );
    let coverage = result.embedding_coverage.unwrap();
    assert_eq!(
        (
            coverage.total_chunks,
            coverage.embedded_chunks,
            coverage.failed_chunks
        ),
        (2, 1, 1)
    );
    let ids: Vec<_> = result
        .hits
        .iter()
        .map(|hit| hit.chunk.as_ref().unwrap().source_id.as_str())
        .collect();
    assert_eq!(ids, [&a.source_id, &b.source_id]);
    assert_eq!(
        result.hits[0].embedding_state(),
        v1::ChunkEmbeddingState::Ready
    );
    assert_eq!(
        result.hits[1].embedding_state(),
        v1::ChunkEmbeddingState::Failed
    );
    request.collections.reverse();
    request.mode = v1::SearchMode::Lexical.into();
    let lexical = running
        .client
        .search_collections(request.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(lexical.mode(), v1::SearchMode::Lexical);
    assert_eq!(lexical.hits, result.hits);
    assert_eq!(provider.query_inputs.load(Ordering::SeqCst), 1);
    request.mode = v1::SearchMode::Semantic.into();
    let semantic = running
        .client
        .search_collections(request.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(semantic.mode(), v1::SearchMode::Semantic);
    assert_eq!(semantic.hits.len(), 1);
    assert_eq!(
        semantic.hits[0].chunk.as_ref().unwrap().source_id,
        a.source_id
    );
    assert_eq!(provider.query_inputs.load(Ordering::SeqCst), 2);

    write_metadata(
        &mut running.client,
        v1::MetadataTarget {
            scope: reference_scope,
            ..Default::default()
        },
        vec![metadata_edit(
            "visible",
            v1::metadata_mutation::Edit::SetJson("true".into()),
        )],
    )
    .await
    .unwrap();
    request.mode = v1::SearchMode::Hybrid.into();
    request.limit = Some(1);
    request.metadata_filter = Some(v1::MetadataFilter {
        expression: Some(v1::metadata_filter::Expression::Test(
            v1::MetadataPredicate {
                level: v1::MetadataLevel::Collection.into(),
                namespace: "demo.integration".into(),
                key: "visible".into(),
                operator: v1::MetadataOperator::Exists.into(),
                ..Default::default()
            },
        )),
    });
    let filtered = running
        .client
        .search_collections(request.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        filtered.hits[0].chunk.as_ref().unwrap().source_id,
        b.source_id
    );

    request.tenant = "other".into();
    let missing = running
        .client
        .search_collections(request.clone())
        .await
        .unwrap_err();
    assert_eq!(missing.code(), Code::NotFound);
    request.collections = vec!["notes".into(), "notes".into()];
    let duplicate = running
        .client
        .search_collections(request)
        .await
        .unwrap_err();
    assert_eq!(duplicate.code(), Code::InvalidArgument);
    running.stop().await;
}

#[tokio::test]
async fn embedding_status_reports_a_scheduled_retry() {
    let dir = TempDir::new().unwrap();
    let provider = EmbeddingFixture::new("retry-profile", false);
    provider.fail_documents.store(true, Ordering::SeqCst);
    let mut running = Running::with_embedding(&dir, Some(provider)).await;
    create(&mut running.client, "default").await;
    let revision = upload(&mut running.client, header("default"), b"quartz specimen").await;
    await_coverage(&mut running.client, 0, 1).await;
    let status = running
        .client
        .get_embedding_status(v1::EmbeddingStatusRequest {
            scope: scope("default"),
            source_id: Some(revision.source_id),
            revision_id: Some(revision.revision_id),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(status.retrying_chunks, 1);
    assert!(status.next_retry_at_unix_seconds.is_some());
    assert_eq!(
        status.sample_error.as_deref(),
        Some("simulated document outage")
    );
    let hits = search(&mut running.client, "default", "quartz").await;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].embedding_state(), v1::ChunkEmbeddingState::Retrying);
    running.stop().await;
}

#[tokio::test]
async fn embedding_search_reuses_vectors_and_isolates_profiles() {
    let dir = TempDir::new().unwrap();
    let provider = EmbeddingFixture::new("profile-a", true);
    let mut running = Running::with_embedding(&dir, Some(provider.clone())).await;
    create(&mut running.client, "default").await;
    let quartz = upload(&mut running.client, header("default"), b"quartz specimens").await;
    let mut alternate = header("default");
    if let Some(Frame::Header(header)) = &mut alternate.frame {
        header.external = None;
    }
    let forecast = upload(&mut running.client, alternate, b"tomorrow's forecast").await;
    await_coverage(&mut running.client, 2, 0).await;
    assert_eq!(provider.document_inputs.load(Ordering::SeqCst), 2);
    let request = vector_search("minerals", v1::SearchMode::Semantic);
    let response = running
        .client
        .search_text(request.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.mode(), v1::SearchMode::Semantic);
    assert_eq!(
        response.hits[0].chunk.as_ref().unwrap().source_id,
        quartz.source_id
    );
    assert_eq!(response.hits[0].score, Some(1.0));

    write_metadata(
        &mut running.client,
        v1::MetadataTarget {
            scope: scope("default"),
            source_id: Some(forecast.source_id.clone()),
            revision_id: Some(forecast.revision_id),
            chunk_ordinal: None,
        },
        vec![metadata_edit(
            "eligible",
            v1::metadata_mutation::Edit::SetJson("true".into()),
        )],
    )
    .await
    .unwrap();
    let mut filtered = request.clone();
    filtered.metadata_filter = Some(v1::MetadataFilter {
        expression: Some(v1::metadata_filter::Expression::Test(
            v1::MetadataPredicate {
                level: v1::MetadataLevel::Source.into(),
                namespace: "demo.integration".into(),
                key: "eligible".into(),
                operator: v1::MetadataOperator::Exists.into(),
                ..Default::default()
            },
        )),
    });
    let filtered = running
        .client
        .search_text(filtered)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        filtered.hits[0].chunk.as_ref().unwrap().source_id,
        forecast.source_id
    );

    let mut query = vector_search("quartz nonexistent", v1::SearchMode::Hybrid);
    query.limit = Some(2);
    let ranked = running
        .client
        .search_text(query)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ranked.mode(), v1::SearchMode::Hybrid);
    assert_eq!(ranked.hits.len(), 2);
    // A partial lexical match must not add a second rank contribution when
    // semantic ranking found eligible chunks.
    assert!(ranked.hits[0].score.unwrap() <= 1.0 / 61.0);

    provider.fail_queries.store(true, Ordering::SeqCst);
    let response = running
        .client
        .search_text(vector_search("quartz", v1::SearchMode::Hybrid))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.mode(), v1::SearchMode::Lexical);
    assert_eq!(
        response.degraded_reason.as_deref(),
        Some("embedding_provider_unavailable")
    );
    assert_eq!(response.hits.len(), 1);
    assert_eq!(
        running
            .client
            .search_text(request.clone())
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    provider.fail_queries.store(false, Ordering::SeqCst);
    let same = upload(
        &mut running.client,
        replacement(Some(&quartz.source_id), Some(&quartz.revision_id)),
        b"quartz specimens",
    )
    .await;
    await_coverage(&mut running.client, 2, 0).await;
    assert_eq!(provider.document_inputs.load(Ordering::SeqCst), 2);
    expire_and_reclaim(&dir, &running.service);
    assert!(!text_path(&dir, &quartz.source_id, &quartz.revision_id).exists());

    let changed = upload(
        &mut running.client,
        replacement(Some(&quartz.source_id), Some(&same.revision_id)),
        b"quartz reject input",
    )
    .await;
    await_coverage(&mut running.client, 1, 1).await;
    let status_request = |revision_id: &str| v1::EmbeddingStatusRequest {
        scope: scope("default"),
        source_id: Some(changed.source_id.clone()),
        revision_id: Some(revision_id.into()),
    };
    let embedding_status = running
        .client
        .get_embedding_status(status_request(&changed.revision_id))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(embedding_status.retrying_chunks, 0);
    assert_eq!(
        embedding_status.sample_error.as_deref(),
        Some("simulated context limit")
    );
    assert_eq!(embedding_status.coverage.unwrap().failed_chunks, 1);
    let stale = running
        .client
        .get_embedding_status(status_request(&same.revision_id))
        .await
        .unwrap_err();
    assert_eq!(stale.code(), Code::FailedPrecondition);
    expire_and_reclaim(&dir, &running.service);
    assert!(text_path(&dir, &same.source_id, &same.revision_id).exists());
    assert_eq!(
        search(&mut running.client, "default", "quartz").await[0].excerpt,
        "quartz reject input"
    );
    let partial = running
        .client
        .search_text(vector_search("quartz", v1::SearchMode::Hybrid))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(partial.mode(), v1::SearchMode::Hybrid);
    assert_eq!(
        partial.degraded_reason.as_deref(),
        Some("embedding_coverage_partial")
    );
    running
        .client
        .set_local_queue(v1::SetLocalQueueRequest { paused: true })
        .await
        .unwrap();
    running.stop().await;

    let corrected = EmbeddingFixture::new("profile-b", false);
    let mut restarted = Running::with_embedding(&dir, Some(corrected.clone())).await;
    assert_eq!(
        restarted
            .client
            .search_text(request.clone())
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    let fallback = restarted
        .client
        .search_text(vector_search("quartz", v1::SearchMode::Hybrid))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fallback.mode(), v1::SearchMode::Lexical);
    assert_eq!(
        fallback.degraded_reason.as_deref(),
        Some("embeddings_unavailable")
    );
    let coverage = fallback.embedding_coverage.unwrap();
    assert_eq!(coverage.profile_id.as_deref(), Some("profile-b"));
    assert_eq!((coverage.embedded_chunks, coverage.total_chunks), (0, 2));
    restarted
        .client
        .set_local_queue(v1::SetLocalQueueRequest { paused: false })
        .await
        .unwrap();
    await_coverage(&mut restarted.client, 2, 0).await;
    assert_eq!(corrected.document_inputs.load(Ordering::SeqCst), 2);
    let response = restarted
        .client
        .search_text(request)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response.hits[0].chunk.as_ref().unwrap().revision_id,
        changed.revision_id
    );
    expire_and_reclaim(&dir, &restarted.service);
    assert!(!text_path(&dir, &same.source_id, &same.revision_id).exists());
    restarted.stop().await;
}

fn scope(tenant: &str) -> Option<v1::Scope> {
    Some(v1::Scope {
        tenant: tenant.into(),
        collection: "notes".into(),
    })
}

#[tokio::test]
async fn local_job_retention_preserves_published_and_retryable_inputs() {
    let directory = TempDir::new().unwrap();
    let mut server = Running::start_with_workers(&directory, None, false).await;
    create(&mut server.client, "default").await;
    let mut jobs = Vec::new();
    for (index, bytes) in [
        b"published quartz".as_slice(),
        &[0xff],
        b"canceled",
        b"retryable",
        b"pending",
    ]
    .into_iter()
    .enumerate()
    {
        let mut upload = header("default");
        if let Some(Frame::Header(header)) = &mut upload.frame {
            header.external.as_mut().unwrap().id = index.to_string();
        }
        jobs.push(
            server
                .client
                .submit_text(tokio_stream::iter([upload, data(bytes), end()]))
                .await
                .unwrap()
                .into_inner(),
        );
    }
    let [published, failed, canceled, retryable, pending] = jobs.as_slice() else {
        unreachable!();
    };
    let text_path_for =
        |job: &v1::IngestionJob| text_path(&directory, &job.source_id, &job.revision_id);
    let db_path = directory.path().join("runtime.db");
    let db = rusqlite::Connection::open(&db_path).unwrap();
    db.execute(
        "UPDATE ingestion_jobs SET created_at = unixepoch() - 60 * 86400",
        [],
    )
    .unwrap();
    server
        .client
        .cancel_ingestion_job(job_request(canceled.operation_id.clone()))
        .await
        .unwrap();
    // Make each claim deterministic without running a concurrent ingestion worker.
    for (index, job) in [published, failed, retryable, pending]
        .into_iter()
        .enumerate()
    {
        db.execute(
            "UPDATE ingestion_jobs SET created_at = ?1 WHERE operation_id = ?2",
            rusqlite::params![index as i64, job.operation_id],
        )
        .unwrap();
    }
    assert!(server.service.process_next().unwrap());
    assert!(server.service.process_next().unwrap());
    let path = text_path_for(retryable);
    let backup = path.with_extension("backup");
    std::fs::rename(&path, &backup).unwrap();
    assert!(server.service.process_next().is_err());
    std::fs::rename(&backup, &path).unwrap();
    for job in [published, failed, canceled, retryable] {
        let finished: i64 = db
            .query_row(
                "SELECT finished_at FROM ingestion_jobs WHERE operation_id = ?1",
                [&job.operation_id],
                |row| row.get(0),
            )
            .unwrap();
        assert!(finished > 0);
        db.execute(
            "UPDATE ingestion_jobs SET finished_at = 0 WHERE operation_id = ?1",
            [&job.operation_id],
        )
        .unwrap();
    }
    drop(db);
    let (stop, shutdown) = tokio::sync::watch::channel(false);
    let maintenance = tokio::spawn(run_local_maintenance(server.service.clone(), shutdown));
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if server
                .client
                .get_ingestion_job(job_request(published.operation_id.clone()))
                .await
                .is_err_and(|error| error.code() == Code::NotFound)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    stop.send(true).unwrap();
    maintenance.await.unwrap();
    server.stop().await;

    let mut server = Running::start_with_workers(&directory, None, false).await;
    for job in [published, failed, canceled] {
        assert_eq!(
            server
                .client
                .get_ingestion_job(job_request(job.operation_id.clone()))
                .await
                .unwrap_err()
                .code(),
            Code::NotFound
        );
    }
    for (job, state) in [
        (retryable, v1::IngestionState::Failed),
        (pending, v1::IngestionState::Accepted),
    ] {
        assert_eq!(
            server
                .client
                .get_ingestion_job(job_request(job.operation_id.clone()))
                .await
                .unwrap()
                .into_inner()
                .state(),
            state
        );
        assert!(text_path_for(job).exists());
    }
    assert_eq!(
        search(&mut server.client, "default", "quartz").await[0].excerpt,
        "published quartz"
    );
    assert!(text_path_for(published).exists());
    assert!(!text_path_for(failed).exists());
    assert!(!text_path_for(canceled).exists());
    server.stop().await;
}

#[tokio::test]
async fn queue_activity_pages_persist_and_stay_in_scope() {
    let dir = TempDir::new().unwrap();
    let mut running = Running::start(&dir).await;
    create(&mut running.client, "default").await;
    create(&mut running.client, "other").await;
    running
        .client
        .create_collection(v1::CreateCollectionRequest {
            scope: Some(v1::Scope {
                tenant: "default".into(),
                collection: "reference".into(),
            }),
            request_id: None,
        })
        .await
        .unwrap();
    running
        .client
        .set_local_queue(v1::SetLocalQueueRequest { paused: true })
        .await
        .unwrap();
    let mut accepted = Vec::new();
    for (tenant, collection) in [
        ("default", "notes"),
        ("default", "reference"),
        ("other", "notes"),
    ] {
        let mut request = header(tenant);
        if let Some(Frame::Header(header)) = &mut request.frame {
            header.scope.as_mut().unwrap().collection = collection.into();
        }
        let job = running
            .client
            .submit_text(tokio_stream::iter([request, data(b"queued text"), end()]))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(job.state(), v1::IngestionState::Accepted);
        assert!(job.created_at_unix_seconds > 0);
        accepted.push(job);
    }
    running.stop().await;

    let mut restarted = Running::start(&dir).await;
    let mut request = v1::ListIngestionJobsRequest {
        tenant: "default".into(),
        limit: Some(1),
        ..Default::default()
    };
    let first = restarted
        .client
        .list_ingestion_jobs(request.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first.jobs.len(), 1);
    request.before_operation_id = first.next_before_operation_id;
    assert!(request.before_operation_id.is_some());
    let second = restarted
        .client
        .list_ingestion_jobs(request.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(second.jobs.len(), 1);
    assert!(second.next_before_operation_id.is_none());
    let mut ids = [
        first.jobs[0].operation_id.as_str(),
        second.jobs[0].operation_id.as_str(),
    ];
    ids.sort_unstable();
    let mut expected = [
        accepted[0].operation_id.as_str(),
        accepted[1].operation_id.as_str(),
    ];
    expected.sort_unstable();
    assert_eq!(ids, expected);

    request.tenant = "other".into();
    assert_eq!(
        restarted
            .client
            .list_ingestion_jobs(request.clone())
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    request.tenant = "default".into();
    request.collection = Some("notes".into());
    request.before_operation_id = None;
    let notes = restarted
        .client
        .list_ingestion_jobs(request)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(notes.jobs, accepted[..1]);
    restarted.stop().await;
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
            retain_original: false,
            request_id: None,
            source_metadata: Vec::new(),
            chunk_metadata: Vec::new(),
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
            ..Default::default()
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
            metadata_filter: None,
            mode: v1::SearchMode::Lexical.into(),
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
            ..Default::default()
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
    let detail = rpc_error_detail(&failure);
    assert_eq!(detail.reason, "invalid_argument");
    assert_eq!(detail.field.as_deref(), Some("frame"));
    assert!(!detail.retryable);
    assert!(detail.operation_id.is_none());

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
    let detail = rpc_error_detail(&failure);
    assert_eq!(detail.reason, "source_too_large");
    assert_eq!(detail.field.as_deref(), Some("data"));

    let mut bytes = b"unpublished ".repeat(600);
    bytes.push(0xff);
    let failure = running
        .client
        .upload_text(tokio_stream::iter([header("default"), data(&bytes), end()]))
        .await
        .unwrap_err();
    assert_eq!(failure.code(), Code::InvalidArgument);
    let detail = rpc_error_detail(&failure);
    assert_eq!(detail.reason, "invalid_source");
    assert!(!detail.retryable);
    let operation_id = detail.operation_id.unwrap();
    let failed_job = running
        .client
        .get_ingestion_job(v1::IngestionJobRequest {
            scope: Some(v1::Scope {
                tenant: "default".into(),
                collection: "notes".into(),
            }),
            operation_id: operation_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(failed_job.operation_id, operation_id);
    assert_eq!(failed_job.state(), v1::IngestionState::Failed);

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

fn rpc_error_detail(status: &tonic::Status) -> v1::RpcError {
    let envelope = tonic_types::Status::decode(status.details()).unwrap();
    assert_eq!(envelope.code, status.code() as i32);
    assert_eq!(envelope.message, status.message());
    let detail = &envelope.details[0];
    assert_eq!(detail.type_url, "type.googleapis.com/sanctum.v1.RpcError");
    v1::RpcError::decode(detail.value.as_slice()).unwrap()
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
        request_id: None,
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

#[tokio::test]
async fn folder_watch_reconciles_offline_edits_and_prunes_explicitly() {
    let dir = TempDir::new().unwrap();
    let files = TempDir::new().unwrap();
    let first = files.path().join("first.md");
    let removed = files.path().join("removed.md");
    std::fs::write(&first, "zircon").unwrap();
    std::fs::write(&removed, "spinel").unwrap();
    std::fs::write(files.path().join("skip.txt"), "invisible").unwrap();
    let mut running = Running::start(&dir).await;
    create(&mut running.client, "default").await;
    create(&mut running.client, "other").await;
    running
        .client
        .set_local_queue(v1::SetLocalQueueRequest { paused: true })
        .await
        .unwrap();
    let request = v1::WatchFolderRequest {
        scope: scope("default"),
        path: files.path().to_str().unwrap().into(),
        include: vec!["**/*.md".into()],
        prune_missing: true,
        interval_seconds: Some(3600),
        ..Default::default()
    };
    let watch = running
        .client
        .watch_folder(request.clone())
        .await
        .unwrap()
        .into_inner();
    assert!(watch.last_scan_at_unix_seconds.is_none());
    assert_eq!(
        running
            .client
            .watch_folder(request)
            .await
            .unwrap_err()
            .code(),
        Code::AlreadyExists
    );
    let list = |tenant| v1::ListWatchedFoldersRequest {
        scope: scope(tenant),
        ..Default::default()
    };
    assert!(
        running
            .client
            .list_watched_folders(list("other"))
            .await
            .unwrap()
            .into_inner()
            .folders
            .is_empty()
    );
    running
        .client
        .set_local_queue(v1::SetLocalQueueRequest { paused: false })
        .await
        .unwrap();
    let initial = await_watch_hit(&mut running.client, "zircon").await;
    await_watch_hit(&mut running.client, "spinel").await;
    running.stop().await;

    std::fs::write(&first, "andalusite").unwrap();
    std::fs::remove_file(&removed).unwrap();
    let mut running = Running::start(&dir).await;
    let updated = await_watch_hit(&mut running.client, "andalusite").await;
    assert_eq!(initial.source_id, updated.source_id);
    assert_ne!(initial.revision_id, updated.revision_id);
    // The edit can be searchable while the same scan is still pruning removals.
    let folder = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let response = running
                .client
                .list_watched_folders(list("default"))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(response.folders.len(), 1);
            let folder = response.folders.into_iter().next().unwrap();
            if folder.last_deleted == 1 {
                break folder;
            }
            assert_eq!(folder.last_failed, 0, "{folder:?}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(folder.id, watch.id);
    assert!(
        search(&mut running.client, "default", "spinel")
            .await
            .is_empty()
    );
    assert!(
        search(&mut running.client, "default", "invisible")
            .await
            .is_empty()
    );
    let request = v1::UnwatchFolderRequest {
        scope: scope("other"),
        id: watch.id,
    };
    assert_eq!(
        running
            .client
            .unwatch_folder(request.clone())
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    running
        .client
        .unwatch_folder(v1::UnwatchFolderRequest {
            scope: scope("default"),
            ..request
        })
        .await
        .unwrap();
    assert!(
        running
            .client
            .list_watched_folders(list("default"))
            .await
            .unwrap()
            .into_inner()
            .folders
            .is_empty()
    );
    assert_eq!(
        search(&mut running.client, "default", "andalusite")
            .await
            .len(),
        1
    );
    running.stop().await;
}

async fn await_watch_hit(client: &mut SanctumClient<Channel>, query: &str) -> v1::ChunkRef {
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if let Some(hit) = search(client, "default", query).await.pop() {
                return hit.chunk.unwrap();
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn directory_import_tracks_edits_duplicates_and_removed_paths() {
    let dir = TempDir::new().unwrap();
    let input = TempDir::new().unwrap();
    let nested = input.path().join("nested");
    std::fs::create_dir(&nested).unwrap();
    let notes = nested.join("sample.md");
    let removed = input.path().join("remove.md");
    std::fs::write(&notes, "first lazurite").unwrap();
    std::fs::write(&removed, "removed citrine").unwrap();
    std::fs::write(input.path().join("private.md"), "excluded diamond").unwrap();
    std::fs::write(input.path().join("ignored.rs"), "excluded rust").unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(input.path(), input.path().join("cycle")).unwrap();

    let mut running = Running::start(&dir).await;
    create(&mut running.client, "default").await;
    let request = v1::ImportLocalRequest {
        scope: scope("default"),
        path: input.path().to_str().unwrap().into(),
        include: vec!["**/*.md".into()],
        exclude: vec!["private.md".into()],
        prune_missing: false,
        retain_original: false,
    };
    running
        .client
        .set_local_queue(v1::SetLocalQueueRequest { paused: true })
        .await
        .unwrap();
    let initial = import(&mut running.client, request.clone()).await;
    assert_eq!(initial.len(), 2);
    let first = initial
        .iter()
        .find(|item| item.path == "nested/sample.md")
        .unwrap();
    assert_eq!(first.state(), v1::ImportState::Queued);
    let pending = import(&mut running.client, request.clone()).await;
    assert_eq!(pending.len(), 2);
    let same = pending.iter().find(|item| item.path == first.path).unwrap();
    assert_eq!(same.state(), v1::ImportState::Pending);
    assert_eq!(same.operation_id, first.operation_id);

    std::fs::write(&notes, "second tourmaline").unwrap();
    let edited = import(&mut running.client, request.clone()).await;
    let replacement = edited.iter().find(|item| item.path == first.path).unwrap();
    assert_eq!(replacement.state(), v1::ImportState::Queued);
    assert_eq!(replacement.source_id, first.source_id);
    assert_ne!(replacement.operation_id, first.operation_id);

    running
        .client
        .set_local_queue(v1::SetLocalQueueRequest { paused: false })
        .await
        .unwrap();
    await_ready(
        &mut running.client,
        job_request(replacement.operation_id.clone().unwrap()),
    )
    .await;
    let removed_job = initial
        .iter()
        .find(|item| item.path == "remove.md")
        .unwrap();
    await_ready(
        &mut running.client,
        job_request(removed_job.operation_id.clone().unwrap()),
    )
    .await;
    await_failure(
        &mut running.client,
        job_request(first.operation_id.clone().unwrap()),
    )
    .await;
    assert!(
        search(&mut running.client, "default", "lazurite")
            .await
            .is_empty()
    );
    assert_eq!(
        search(&mut running.client, "default", "tourmaline")
            .await
            .len(),
        1
    );
    assert!(
        search(&mut running.client, "default", "diamond")
            .await
            .is_empty()
    );

    std::fs::write(&notes, "third fluorite").unwrap();
    let edited = import(&mut running.client, request.clone()).await;
    let replacement = edited.iter().find(|item| item.path == first.path).unwrap();
    assert_eq!(replacement.state(), v1::ImportState::Queued);
    assert_eq!(replacement.source_id, first.source_id);
    await_ready(
        &mut running.client,
        job_request(replacement.operation_id.clone().unwrap()),
    )
    .await;
    assert_eq!(
        search(&mut running.client, "default", "fluorite")
            .await
            .len(),
        1
    );
    assert!(
        search(&mut running.client, "default", "tourmaline")
            .await
            .is_empty()
    );

    std::fs::remove_file(removed).unwrap();
    let check = import(&mut running.client, request.clone()).await;
    assert!(
        check
            .iter()
            .any(|item| item.path == "nested/sample.md"
                && item.state() == v1::ImportState::Unchanged)
    );
    assert!(
        check
            .iter()
            .any(|item| item.path == "remove.md" && item.state() == v1::ImportState::Missing)
    );
    assert_eq!(
        search(&mut running.client, "default", "citrine")
            .await
            .len(),
        1
    );

    let pruned = import(
        &mut running.client,
        v1::ImportLocalRequest {
            prune_missing: true,
            ..request
        },
    )
    .await;
    assert!(
        pruned
            .iter()
            .any(|item| item.path == "remove.md" && item.state() == v1::ImportState::Deleted)
    );
    assert!(
        search(&mut running.client, "default", "citrine")
            .await
            .is_empty()
    );
    running.stop().await;
}

async fn import(
    client: &mut SanctumClient<Channel>,
    request: v1::ImportLocalRequest,
) -> Vec<v1::ImportLocalEvent> {
    let mut stream = client.import_local(request).await.unwrap().into_inner();
    let mut events = Vec::new();
    while let Some(event) = stream.message().await.unwrap() {
        assert_ne!(event.state(), v1::ImportState::Failed, "{event:?}");
        events.push(event);
    }
    events
}

#[tokio::test]
async fn rtf_upload_indexes_canonical_bytes_and_honors_original_retention() {
    let dir = TempDir::new().unwrap();
    let mut running = Running::start(&dir).await;
    create(&mut running.client, "default").await;
    let rtf = br"{\rtf1\ansi\ansicpg1252{\fonttbl HiddenGarnet;}Caf\'e9\par Beryl \u28450?}";
    let canonical = "Café\nBeryl 漢";
    running
        .client
        .set_local_queue(v1::SetLocalQueueRequest { paused: true })
        .await
        .unwrap();
    let job = running
        .client
        .submit_text(tokio_stream::iter([
            rtf_header(false, "rtf-discard"),
            data(rtf),
            end(),
        ]))
        .await
        .unwrap()
        .into_inner();
    let text = text_path(&dir, &job.source_id, &job.revision_id);
    let original = text.with_file_name("original");
    assert_eq!(std::fs::read(&original).unwrap(), rtf);
    assert!(!text.exists());
    running.stop().await;

    let mut running = Running::start(&dir).await;
    running
        .client
        .set_local_queue(v1::SetLocalQueueRequest { paused: false })
        .await
        .unwrap();
    let request = job_request(job.operation_id.clone());
    let ready = await_ready(&mut running.client, request.clone()).await;
    let info = ready.text_revision.unwrap();
    assert_eq!(info.bytes, canonical.len() as u64);
    assert_ne!(info.content_blake3, info.original_blake3);
    assert_eq!(info.source_encoding, "windows-1252");
    assert_eq!(info.extractor.as_deref(), Some("rtf-text-v1"));
    assert_eq!(std::fs::read_to_string(&text).unwrap(), canonical);
    assert!(!original.exists());
    assert!(
        search(&mut running.client, "default", "HiddenGarnet")
            .await
            .is_empty()
    );
    let hit = search(&mut running.client, "default", "beryl")
        .await
        .remove(0);
    assert_eq!(
        hit.excerpt,
        canonical[hit.start_byte as usize..hit.end_byte as usize]
    );

    let retained = upload(&mut running.client, rtf_header(true, "rtf-retain"), rtf).await;
    assert!(retained.retain_original);
    assert_eq!(
        std::fs::read(
            text_path(&dir, &retained.source_id, &retained.revision_id).with_file_name("original")
        )
        .unwrap(),
        rtf
    );
    running.stop().await;

    // Recover a collection commit after original cleanup and before its queue receipt.
    let runtime = rusqlite::Connection::open(dir.path().join("runtime.db")).unwrap();
    runtime
        .execute(
            "UPDATE ingestion_jobs SET status = 'publishing', chunks = NULL, content_hash = NULL,
            canonical_bytes = NULL, original_hash = NULL, source_encoding = NULL
         WHERE operation_id = ?1",
            [&job.operation_id],
        )
        .unwrap();
    drop(runtime);
    let mut running = Running::start(&dir).await;
    assert_eq!(
        await_ready(&mut running.client, request)
            .await
            .text_revision
            .unwrap(),
        info
    );
    let malformed = running
        .client
        .upload_text(tokio_stream::iter([
            rtf_header(false, "rtf-malformed"),
            data(br"{\rtf1 partial spinel"),
            end(),
        ]))
        .await
        .unwrap_err();
    assert_eq!(malformed.code(), Code::InvalidArgument);
    assert!(
        search(&mut running.client, "default", "spinel")
            .await
            .is_empty()
    );
    running.stop().await;
}

#[tokio::test]
async fn rtf_directory_import_compares_original_digest_and_retention_policy() {
    let dir = TempDir::new().unwrap();
    let input = TempDir::new().unwrap();
    std::fs::write(
        input.path().join("specimen.RTF"),
        br"{\rtf1 zircon specimen}",
    )
    .unwrap();
    let mut running = Running::start(&dir).await;
    create(&mut running.client, "default").await;
    let mut request = v1::ImportLocalRequest {
        scope: scope("default"),
        path: input.path().to_str().unwrap().into(),
        ..Default::default()
    };
    let imported = import(&mut running.client, request.clone()).await.remove(0);
    await_ready(
        &mut running.client,
        job_request(imported.operation_id.unwrap()),
    )
    .await;
    let repeated = import(&mut running.client, request.clone()).await.remove(0);
    assert_eq!(repeated.state(), v1::ImportState::Unchanged);

    request.retain_original = true;
    let changed = import(&mut running.client, request).await.remove(0);
    assert_eq!(changed.state(), v1::ImportState::Queued);
    assert_eq!(changed.source_id, imported.source_id);
    let revision = await_ready(
        &mut running.client,
        job_request(changed.operation_id.unwrap()),
    )
    .await
    .text_revision
    .unwrap();
    assert!(revision.retain_original);
    assert!(
        text_path(&dir, &revision.source_id, &revision.revision_id)
            .with_file_name("original")
            .is_file()
    );
    assert_eq!(
        search(&mut running.client, "default", "zircon").await.len(),
        1
    );
    running.stop().await;
}

fn rtf_header(retain_original: bool, external_id: &str) -> v1::UploadTextFrame {
    let mut request = header("default");
    if let Some(Frame::Header(header)) = &mut request.frame {
        header.name = "sample.rtf".into();
        header.external.as_mut().unwrap().id = external_id.into();
        header.retain_original = retain_original;
    }
    request
}

#[tokio::test]
async fn local_upload_metadata_publishes_with_replacement_after_restart() {
    let dir = TempDir::new().unwrap();
    let entry = |key: &str, value: &str| v1::MetadataEntry {
        namespace: "integration".into(),
        key: key.into(),
        value_json: value.into(),
    };
    let with_metadata = |mut frame: v1::UploadTextFrame, metadata| {
        if let Some(Frame::Header(header)) = &mut frame.frame {
            header.source_metadata = metadata;
            header.chunk_metadata = vec![v1::UploadChunkMetadata {
                ordinal: 0,
                entries: vec![entry("chunk", "true")],
            }];
        }
        frame
    };
    let mut running = Running::start(&dir).await;
    create(&mut running.client, "default").await;
    let first = upload(
        &mut running.client,
        with_metadata(header("default"), vec![entry("old", "true")]),
        b"old quartz",
    )
    .await;
    running
        .client
        .set_local_queue(v1::SetLocalQueueRequest { paused: true })
        .await
        .unwrap();
    let candidate = with_metadata(
        replacement(Some(&first.source_id), Some(&first.revision_id)),
        vec![entry("new", "[1,null]")],
    );
    let accepted = running
        .client
        .submit_text(tokio_stream::iter([candidate, data(b"new quartz"), end()]))
        .await
        .unwrap()
        .into_inner();
    let target = v1::MetadataTarget {
        scope: scope("default"),
        source_id: Some(first.source_id.clone()),
        revision_id: Some(first.revision_id.clone()),
        ..Default::default()
    };
    assert_eq!(
        running
            .client
            .get_metadata(target.clone())
            .await
            .unwrap()
            .into_inner()
            .entries,
        vec![entry("old", "true")]
    );
    running.stop().await;
    let mut running = Running::start(&dir).await;
    running
        .client
        .set_local_queue(v1::SetLocalQueueRequest { paused: false })
        .await
        .unwrap();
    await_ready(&mut running.client, job_request(accepted.operation_id)).await;
    let current = v1::MetadataTarget {
        revision_id: Some(accepted.revision_id.clone()),
        ..target.clone()
    };
    assert_eq!(
        running
            .client
            .get_metadata(current.clone())
            .await
            .unwrap()
            .into_inner()
            .entries,
        vec![entry("new", "[1,null]")]
    );
    assert_eq!(
        running
            .client
            .get_metadata(target.clone())
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    let chunk = v1::MetadataTarget {
        chunk_ordinal: Some(0),
        ..current.clone()
    };
    assert_eq!(
        running
            .client
            .get_metadata(chunk.clone())
            .await
            .unwrap()
            .into_inner()
            .entries,
        vec![entry("chunk", "true")]
    );
    let mut invalid = replacement(Some(&first.source_id), Some(&accepted.revision_id));
    if let Some(Frame::Header(header)) = &mut invalid.frame {
        header.chunk_metadata = vec![v1::UploadChunkMetadata {
            ordinal: 1,
            entries: vec![entry("chunk", "false")],
        }];
    }
    let bad = running
        .client
        .submit_text(tokio_stream::iter([invalid, data(b"failed quartz"), end()]))
        .await
        .unwrap()
        .into_inner();
    let failed = await_failure(&mut running.client, job_request(bad.operation_id)).await;
    assert_eq!(failed.error_code.as_deref(), Some("invalid_source"));
    assert_eq!(
        running
            .client
            .get_metadata(chunk)
            .await
            .unwrap()
            .into_inner()
            .entries,
        vec![entry("chunk", "true")]
    );
    running.stop().await;
}

#[tokio::test]
async fn metadata_is_atomic_and_scoped_to_the_current_revision() {
    use v1::metadata_mutation::Edit;
    let dir = TempDir::new().unwrap();
    let mut running = Running::start(&dir).await;
    create(&mut running.client, "default").await;
    create(&mut running.client, "other").await;
    let original = upload(&mut running.client, header("default"), b"metadata apatite").await;

    let collection = v1::MetadataTarget {
        scope: scope("default"),
        ..Default::default()
    };
    let source = v1::MetadataTarget {
        scope: scope("default"),
        source_id: Some(original.source_id.clone()),
        revision_id: Some(original.revision_id.clone()),
        chunk_ordinal: None,
    };
    let chunk = v1::MetadataTarget {
        chunk_ordinal: Some(0),
        ..source.clone()
    };

    write_metadata(
        &mut running.client,
        collection.clone(),
        vec![metadata_edit(
            "category",
            Edit::SetJson(r#""reference""#.into()),
        )],
    )
    .await
    .unwrap();
    write_metadata(
        &mut running.client,
        source.clone(),
        vec![metadata_edit(
            "details",
            Edit::SetJson(r#"{"tag":["green"],"drop":4,"nested":{"kept":true}}"#.into()),
        )],
    )
    .await
    .unwrap();
    let result = write_metadata(
        &mut running.client,
        source.clone(),
        vec![
            metadata_edit(
                "details",
                Edit::MergePatchJson(r#"{"drop":null,"nested":{"added":3}}"#.into()),
            ),
            metadata_edit("nullable", Edit::SetJson("null".into())),
        ],
    )
    .await
    .unwrap();
    let merged: serde_json::Value = serde_json::from_str(&result.entries[0].value_json).unwrap();
    assert_eq!(
        merged,
        serde_json::json!({"tag":["green"], "nested":{"kept":true,"added":3}})
    );
    assert_eq!(result.entries[1].value_json, "null");
    write_metadata(
        &mut running.client,
        chunk.clone(),
        vec![metadata_edit(
            "confidence-from-client",
            Edit::SetJson("0.75".into()),
        )],
    )
    .await
    .unwrap();

    let failure = write_metadata(
        &mut running.client,
        source.clone(),
        vec![
            metadata_edit("nullable", Edit::Remove(v1::Empty {})),
            metadata_edit("", Edit::SetJson("true".into())),
        ],
    )
    .await
    .unwrap_err();
    assert_eq!(failure.code(), Code::InvalidArgument);
    assert_eq!(
        running
            .client
            .get_metadata(source.clone())
            .await
            .unwrap()
            .into_inner()
            .entries
            .len(),
        2
    );
    let removed = write_metadata(
        &mut running.client,
        source.clone(),
        vec![metadata_edit("nullable", Edit::Remove(v1::Empty {}))],
    )
    .await
    .unwrap();
    assert_eq!(removed.entries.len(), 1);
    assert!(
        running
            .client
            .get_metadata(v1::MetadataTarget {
                scope: scope("other"),
                ..collection.clone()
            })
            .await
            .unwrap()
            .into_inner()
            .entries
            .is_empty()
    );
    assert_eq!(
        running
            .client
            .get_metadata(v1::MetadataTarget {
                scope: scope("other"),
                ..source.clone()
            })
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );

    let page = running
        .client
        .list_changes(v1::ListChangesRequest {
            scope: scope("default"),
            after_cursor: 0,
            limit: Some(2),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(page.changes.len(), 2);
    assert_eq!(page.changes[0].kind, "source_created");
    assert_eq!(page.changes[1].kind, "metadata_changed");
    let second = running
        .client
        .list_changes(v1::ListChangesRequest {
            scope: scope("default"),
            after_cursor: page.next_cursor,
            limit: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(second.changes.len(), 3);
    assert!(
        second
            .changes
            .iter()
            .any(|change| change.chunk_ordinal == Some(0))
    );

    let updated = upload(
        &mut running.client,
        replacement(Some(&original.source_id), None),
        b"metadata fluorite",
    )
    .await;
    let stale = write_metadata(
        &mut running.client,
        chunk,
        vec![metadata_edit("later", Edit::SetJson("true".into()))],
    )
    .await
    .unwrap_err();
    assert_eq!(stale.code(), Code::FailedPrecondition);
    let current = v1::MetadataTarget {
        revision_id: Some(updated.revision_id.clone()),
        ..source
    };
    assert!(
        running
            .client
            .get_metadata(current)
            .await
            .unwrap()
            .into_inner()
            .entries
            .is_empty()
    );
    assert_eq!(
        running
            .client
            .get_metadata(collection)
            .await
            .unwrap()
            .into_inner()
            .entries
            .len(),
        1
    );

    let changes = running
        .client
        .list_changes(v1::ListChangesRequest {
            scope: scope("default"),
            after_cursor: second.next_cursor,
            limit: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        changes
            .changes
            .iter()
            .map(|change| change.kind.as_str())
            .collect::<Vec<_>>(),
        ["source_replaced", "metadata_reset"]
    );
    assert_eq!(changes.changes[1].revision_id, updated.revision_id);
    assert_eq!(
        changes.changes[1].previous_revision_id.as_deref(),
        Some(original.revision_id.as_str())
    );
    running.stop().await;
}

fn metadata_edit(key: &str, edit: v1::metadata_mutation::Edit) -> v1::MetadataMutation {
    v1::MetadataMutation {
        namespace: "demo.integration".into(),
        key: key.into(),
        edit: Some(edit),
    }
}

async fn write_metadata(
    client: &mut SanctumClient<Channel>,
    target: v1::MetadataTarget,
    edits: Vec<v1::MetadataMutation>,
) -> Result<v1::MetadataResponse, Box<tonic::Status>> {
    Ok(client
        .mutate_metadata(v1::MutateMetadataRequest {
            target: Some(target),
            edits,
        })
        .await?
        .into_inner())
}

#[tokio::test]
async fn named_metadata_indexes_are_scoped_persisted_and_idempotent() {
    let dir = TempDir::new().unwrap();
    let mut running = Running::start(&dir).await;
    create(&mut running.client, "default").await;
    create(&mut running.client, "other").await;
    let target = v1::DatabaseTarget {
        tenant: "default".into(),
        collection: Some("notes".into()),
    };
    let index = v1::MetadataIndex {
        name: "integration-status".into(),
        level: v1::MetadataLevel::Source.into(),
        namespace: "integration's".into(),
        key: "status".into(),
    };
    let request = v1::CreateMetadataIndexRequest {
        target: Some(target.clone()),
        index: Some(index.clone()),
    };
    running
        .client
        .create_metadata_index(request.clone())
        .await
        .unwrap();
    running
        .client
        .create_metadata_index(request.clone())
        .await
        .unwrap();
    let mut duplicate = request.clone();
    duplicate.index.as_mut().unwrap().name = "same-field".into();
    assert_eq!(
        running
            .client
            .create_metadata_index(duplicate)
            .await
            .unwrap_err()
            .code(),
        Code::AlreadyExists
    );
    let mut wrong_level = request;
    wrong_level.index.as_mut().unwrap().level = v1::MetadataLevel::Collection.into();
    assert_eq!(
        running
            .client
            .create_metadata_index(wrong_level)
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    running.stop().await;

    let mut running = Running::start(&dir).await;
    let client = &mut running.client;
    assert_eq!(
        client
            .list_metadata_indexes(target.clone())
            .await
            .unwrap()
            .into_inner()
            .indexes,
        std::slice::from_ref(&index)
    );
    let other = v1::DatabaseTarget {
        tenant: "other".into(),
        ..target.clone()
    };
    assert!(
        client
            .list_metadata_indexes(other.clone())
            .await
            .unwrap()
            .into_inner()
            .indexes
            .is_empty()
    );
    let mut drop = v1::DropMetadataIndexRequest {
        target: Some(other),
        name: index.name,
    };
    assert_eq!(
        client
            .drop_metadata_index(drop.clone())
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    drop.target = Some(target.clone());
    client.drop_metadata_index(drop).await.unwrap();
    assert!(
        client
            .list_metadata_indexes(target)
            .await
            .unwrap()
            .into_inner()
            .indexes
            .is_empty()
    );
    running.stop().await;
}

#[tokio::test]
async fn metadata_queries_filter_sort_and_count_without_coercing_json() {
    use v1::{
        MetadataLevel as Level, MetadataOperator as Operator, metadata_filter::Expression,
        metadata_mutation::Edit, metadata_path_segment::Segment, query_metadata_request::View,
    };
    let dir = TempDir::new().unwrap();
    let mut running = Running::start(&dir).await;
    let client = &mut running.client;
    create(client, "default").await;
    create(client, "other").await;
    let edit = |target, key: &str, value: &str| v1::MutateMetadataRequest {
        target: Some(target),
        edits: vec![v1::MetadataMutation {
            namespace: "integration".into(),
            key: key.into(),
            edit: Some(Edit::SetJson(value.into())),
        }],
    };
    for (tenant, enabled) in [("default", "true"), ("other", "false")] {
        client
            .mutate_metadata(edit(
                v1::MetadataTarget {
                    scope: scope(tenant),
                    ..Default::default()
                },
                "enabled",
                enabled,
            ))
            .await
            .unwrap();
    }
    let mut revisions = Vec::new();
    for (index, value) in [
        Some(r#"{"a.b":9007199254740993}"#),
        Some(r#"{"a.b":9007199254740992}"#),
        Some(r#"{"a.b":null}"#),
        Some(r#"{"a.b":1}"#),
        Some(r#"{"a.b":1.0}"#),
        None,
    ]
    .into_iter()
    .enumerate()
    {
        let header = v1::UploadTextFrame {
            frame: Some(Frame::Header(v1::UploadTextHeader {
                scope: scope("default"),
                name: format!("{index}.md"),
                ..Default::default()
            })),
        };
        let revision = upload(client, header, b"searchable example").await;
        if let Some(value) = value {
            client
                .mutate_metadata(edit(
                    v1::MetadataTarget {
                        scope: scope("default"),
                        source_id: Some(revision.source_id.clone()),
                        revision_id: Some(revision.revision_id.clone()),
                        chunk_ordinal: None,
                    },
                    "priority",
                    value,
                ))
                .await
                .unwrap();
        }
        revisions.push(revision);
    }

    let field = v1::MetadataField {
        level: Level::Source.into(),
        namespace: "integration".into(),
        key: "priority".into(),
        path: vec![v1::MetadataPathSegment {
            segment: Some(Segment::Property("a.b".into())),
        }],
    };
    let filter = v1::MetadataFilter {
        expression: Some(Expression::Test(v1::MetadataPredicate {
            level: Level::Collection.into(),
            namespace: "integration".into(),
            key: "enabled".into(),
            operator: Operator::Equal.into(),
            operand_json: Some("true".into()),
            ..Default::default()
        })),
    };
    let mut request = v1::QueryMetadataRequest {
        tenant: "default".into(),
        collection: Some("notes".into()),
        level: Level::Source.into(),
        filter: Some(filter.clone()),
        view: Some(View::List(v1::MetadataList {
            sort_by: Some(field.clone()),
            descending: true,
        })),
        limit: Some(2),
        ..Default::default()
    };
    let first = client
        .query_metadata(request.clone())
        .await
        .unwrap()
        .into_inner();
    let values: Vec<_> = first
        .matches
        .iter()
        .map(|row| row.value_json.as_deref())
        .collect();
    assert_eq!(
        values,
        vec![Some("9007199254740993"), Some("9007199254740992")]
    );
    assert_eq!(
        first.matches[0]
            .target
            .as_ref()
            .unwrap()
            .source_id
            .as_deref(),
        Some(revisions[0].source_id.as_str())
    );
    request.offset = first.next_offset.unwrap();
    let second = client
        .query_metadata(request.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(second.matches.len(), 2);
    assert_eq!(second.next_offset, Some(4));
    request.offset = second.next_offset.unwrap();
    let third = client
        .query_metadata(request.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(third.matches[0].value_json.as_deref(), Some("null"));
    assert_eq!(third.matches[1].value_json, None);
    assert!(third.next_offset.is_none());

    request.offset = 0;
    request.limit = None;
    request.level = Level::Chunk.into();
    request.view = Some(View::Count(v1::MetadataCount {
        group_by: Some(field.clone()),
    }));
    let groups = client
        .query_metadata(request.clone())
        .await
        .unwrap()
        .into_inner()
        .groups;
    assert_eq!(groups.len(), 5);
    assert_eq!(groups[0].count, 2);
    assert_eq!(
        groups[0]
            .value_json
            .as_deref()
            .unwrap()
            .parse::<f64>()
            .unwrap(),
        1.0
    );
    assert!(
        groups
            .iter()
            .any(|group| group.value_json.as_deref() == Some("null") && group.count == 1)
    );
    assert!(
        groups
            .iter()
            .any(|group| group.value_json.is_none() && group.count == 1)
    );

    request.view = Some(View::Count(v1::MetadataCount::default()));
    let count = client
        .query_metadata(request.clone())
        .await
        .unwrap()
        .into_inner()
        .groups[0]
        .count;
    assert_eq!(count, 6);
    request.tenant = "other".into();
    assert_eq!(
        client
            .query_metadata(request.clone())
            .await
            .unwrap()
            .into_inner()
            .groups[0]
            .count,
        0
    );

    let mut catalog = v1::QueryMetadataRequest {
        tenant: "default".into(),
        level: Level::Collection.into(),
        filter: Some(filter),
        ..Default::default()
    };
    let listed = client
        .query_metadata(catalog.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        listed.matches[0].target.as_ref().unwrap().scope,
        scope("default")
    );
    catalog.tenant = "other".into();
    assert!(
        client
            .query_metadata(catalog.clone())
            .await
            .unwrap()
            .into_inner()
            .matches
            .is_empty()
    );
    catalog.tenant = "missing".into();
    catalog.view = Some(View::Count(v1::MetadataCount::default()));
    assert_eq!(
        client
            .query_metadata(catalog.clone())
            .await
            .unwrap()
            .into_inner()
            .groups[0]
            .count,
        0
    );
    catalog.view = Some(View::List(v1::MetadataList {
        sort_by: Some(field),
        descending: false,
    }));
    assert_eq!(
        client.query_metadata(catalog).await.unwrap_err().code(),
        Code::InvalidArgument
    );
    running.stop().await;
}

#[tokio::test]
async fn metadata_filters_apply_to_candidates_before_limit_and_preserve_json_types() {
    use v1::{
        MetadataLevel as Level, MetadataOperator as Op, metadata_filter::Expression,
        metadata_mutation::Edit, metadata_path_segment::Segment,
    };

    let dir = TempDir::new().unwrap();
    let mut running = Running::start(&dir).await;
    create(&mut running.client, "default").await;
    create(&mut running.client, "other").await;
    let mut alternate = header("default");
    if let Some(Frame::Header(header)) = &mut alternate.frame {
        header.external = None;
    }
    let rejected = upload(&mut running.client, alternate, b"quartz quartz quartz").await;
    let accepted = upload(
        &mut running.client,
        header("default"),
        b"quartz mixed sample",
    )
    .await;
    let initial = search(&mut running.client, "default", "quartz").await;
    assert_eq!(
        initial[0].chunk.as_ref().unwrap().source_id,
        rejected.source_id
    );

    let source = v1::MetadataTarget {
        scope: scope("default"),
        source_id: Some(accepted.source_id.clone()),
        revision_id: Some(accepted.revision_id),
        chunk_ordinal: None,
    };
    write_metadata(
        &mut running.client,
        source.clone(),
        vec![metadata_edit(
            "record",
            Edit::SetJson(
                r#"{"item.with\"quote":[{"size":9007199254740993,"tags":["mineral",null]}]}"#
                    .into(),
            ),
        )],
    )
    .await
    .unwrap();
    write_metadata(
        &mut running.client,
        v1::MetadataTarget {
            chunk_ordinal: Some(0),
            ..source
        },
        vec![metadata_edit("review", Edit::SetJson("null".into()))],
    )
    .await
    .unwrap();
    write_metadata(
        &mut running.client,
        v1::MetadataTarget {
            scope: scope("default"),
            ..Default::default()
        },
        vec![metadata_edit(
            "catalog",
            Edit::SetJson(r#""reference""#.into()),
        )],
    )
    .await
    .unwrap();

    let segment = |segment| v1::MetadataPathSegment {
        segment: Some(segment),
    };
    let nested = |key: &str| {
        vec![
            segment(Segment::Property("item.with\"quote".into())),
            segment(Segment::Index(0)),
            segment(Segment::Property(key.into())),
        ]
    };
    let filter =
        |level: Level, key: &str, operator: Op, value: Option<&str>, path| v1::MetadataFilter {
            expression: Some(Expression::Test(v1::MetadataPredicate {
                level: level.into(),
                namespace: "demo.integration".into(),
                key: key.into(),
                operator: operator.into(),
                operand_json: value.map(str::to_owned),
                path,
            })),
        };
    let group = |filters| v1::MetadataFilter {
        expression: Some(Expression::All(v1::MetadataFilterGroup { filters })),
    };
    let eligible = group(vec![
        filter(
            Level::Collection,
            "catalog",
            Op::Equal,
            Some(r#""reference""#),
            vec![],
        ),
        filter(
            Level::Source,
            "record",
            Op::Greater,
            Some("9007199254740992.0"),
            nested("size"),
        ),
        filter(
            Level::Source,
            "record",
            Op::Contains,
            Some("null"),
            nested("tags"),
        ),
        filter(Level::Chunk, "review", Op::Equal, Some("null"), vec![]),
        v1::MetadataFilter {
            expression: Some(Expression::Not(Box::new(filter(
                Level::Chunk,
                "review",
                Op::Missing,
                None,
                vec![],
            )))),
        },
    ]);
    let request = v1::SearchTextRequest {
        scope: scope("default"),
        query: "quartz".into(),
        limit: Some(1),
        metadata_filter: Some(eligible),
        mode: v1::SearchMode::Lexical.into(),
    };
    let hits = running
        .client
        .search_text(request.clone())
        .await
        .unwrap()
        .into_inner()
        .hits;
    assert_eq!(hits.len(), 1);
    assert_eq!(
        hits[0].chunk.as_ref().unwrap().source_id,
        accepted.source_id
    );
    assert!(
        running
            .client
            .search_text(v1::SearchTextRequest {
                scope: scope("other"),
                ..request.clone()
            })
            .await
            .unwrap()
            .into_inner()
            .hits
            .is_empty()
    );

    let missing = filter(Level::Chunk, "review", Op::Missing, None, vec![]);
    let hits = running
        .client
        .search_text(v1::SearchTextRequest {
            metadata_filter: Some(missing),
            ..request.clone()
        })
        .await
        .unwrap()
        .into_inner()
        .hits;
    assert_eq!(
        hits[0].chunk.as_ref().unwrap().source_id,
        rejected.source_id
    );
    let string_number = filter(
        Level::Source,
        "record",
        Op::Equal,
        Some(r#""9007199254740993""#),
        nested("size"),
    );
    let matches = running
        .client
        .search_text(v1::SearchTextRequest {
            metadata_filter: Some(string_number),
            ..request.clone()
        })
        .await
        .unwrap()
        .into_inner()
        .hits;
    assert!(matches.is_empty());
    let malformed = filter(Level::Source, "record", Op::Less, Some("true"), vec![]);
    assert_eq!(
        running
            .client
            .search_text(v1::SearchTextRequest {
                metadata_filter: Some(malformed),
                ..request
            })
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    running.stop().await;
}

#[tokio::test]
async fn storage_usage_counts_retained_and_staged_files_at_each_scope() {
    use v1::storage_usage_request::Scope as UsageScope;
    let dir = TempDir::new().unwrap();
    let outside = TempDir::new().unwrap();
    let mut running = Running::start(&dir).await;
    create(&mut running.client, "default").await;
    create(&mut running.client, "other").await;
    let revision = upload(
        &mut running.client,
        rtf_header(true, "usage-rtf"),
        br"{\rtf1 Peridot}",
    )
    .await;
    let canonical = text_path(&dir, &revision.source_id, &revision.revision_id);
    let collection = dir.path().join("default/notes");
    // Hold the fixture still while comparing two independent walks.
    running.stop().await;
    let mut running = Running::start_with_workers(&dir, None, false).await;
    let staged = collection.join("staging/working-file");
    std::fs::write(&staged, b"pending bytes").unwrap();
    std::fs::write(outside.path().join("unmanaged"), vec![0; 64 * 1024]).unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(outside.path(), collection.join("external")).unwrap();

    let expected = |path: &std::path::Path| {
        let mut totals = (0u64, 0u64);
        fn collect(path: &std::path::Path, totals: &mut (u64, u64)) {
            for entry in std::fs::read_dir(path).unwrap() {
                let entry = entry.unwrap();
                let kind = entry.file_type().unwrap();
                if kind.is_dir() {
                    collect(&entry.path(), totals);
                } else if kind.is_file() {
                    totals.0 += entry.metadata().unwrap().len();
                    totals.1 += 1;
                }
            }
        }
        collect(path, &mut totals);
        totals
    };
    let collection_request = v1::StorageUsageRequest {
        scope: Some(UsageScope::Collection(scope("default").unwrap())),
    };
    let first = running
        .client
        .get_storage_usage(collection_request.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!((first.bytes, first.files), expected(&collection));
    assert_eq!(first.scope.as_ref(), Some(&collection_request));
    assert!(first.started_at_unix_ms > 0 && first.finished_at_unix_ms >= first.started_at_unix_ms);
    assert!(canonical.with_file_name("original").is_file());

    let tenant_request = v1::StorageUsageRequest {
        scope: Some(UsageScope::Tenant("default".into())),
    };
    let tenant = running
        .client
        .get_storage_usage(tenant_request)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        (tenant.bytes, tenant.files),
        expected(&dir.path().join("default"))
    );
    assert!(tenant.bytes > first.bytes);
    let cluster_request = v1::StorageUsageRequest {
        scope: Some(UsageScope::Cluster(v1::Empty {})),
    };
    let cluster = running
        .client
        .get_storage_usage(cluster_request)
        .await
        .unwrap()
        .into_inner();
    assert_eq!((cluster.bytes, cluster.files), expected(dir.path()));
    assert!(cluster.bytes > tenant.bytes);

    std::fs::remove_file(staged).unwrap();
    let refreshed = running
        .client
        .get_storage_usage(collection_request)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(refreshed.bytes + b"pending bytes".len() as u64, first.bytes);
    assert_eq!(refreshed.files + 1, first.files);
    let missing = running
        .client
        .get_storage_usage(v1::StorageUsageRequest {
            scope: Some(UsageScope::Tenant("missing".into())),
        })
        .await
        .unwrap_err();
    assert_eq!(missing.code(), Code::NotFound);
    running.stop().await;
}

#[tokio::test]
async fn hybrid_falls_back_to_partial_terms_without_embeddings() {
    let directory = TempDir::new().unwrap();
    let mut running = Running::with_embedding(&directory, None).await;
    create(&mut running.client, "default").await;
    let target = upload(&mut running.client, header("default"), b"quartz specimens").await;
    let mut second = header("default");
    if let Some(Frame::Header(header)) = &mut second.frame {
        header.external = None;
    }
    upload(&mut running.client, second, b"quartz sediment").await;

    let strict = vector_search("quartz nonexistent", v1::SearchMode::Lexical);
    assert!(
        running
            .client
            .search_text(strict.clone())
            .await
            .unwrap()
            .into_inner()
            .hits
            .is_empty()
    );
    let mut hybrid = strict;
    hybrid.mode = v1::SearchMode::Hybrid.into();
    hybrid.limit = Some(10);
    let partial = running
        .client
        .search_text(hybrid.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(partial.mode(), v1::SearchMode::Lexical);
    assert_eq!(
        partial.degraded_reason.as_deref(),
        Some("embedding_disabled")
    );
    assert_eq!(partial.hits.len(), 2);

    hybrid.query = "quartz specimens".into();
    let exact = running
        .client
        .search_text(hybrid)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(exact.hits.len(), 1);
    assert_eq!(
        exact.hits[0].chunk.as_ref().unwrap().source_id,
        target.source_id
    );
    running.stop().await;
}

#[tokio::test]
async fn new_upload_is_accepted_while_its_collection_database_has_a_writer() {
    let directory = TempDir::new().unwrap();
    let mut running = Running::start_with_workers(&directory, None, false).await;
    create(&mut running.client, "default").await;
    let path = directory.path().join("default/notes/meta.db");
    let lock = rusqlite::Connection::open(path).unwrap();
    lock.execute_batch("BEGIN IMMEDIATE").unwrap();
    let response = tokio::time::timeout(
        Duration::from_secs(2),
        running.client.submit_text(tokio_stream::iter([
            header("default"),
            v1::UploadTextFrame {
                frame: Some(Frame::Data(b"independent upload".to_vec())),
            },
            v1::UploadTextFrame {
                frame: Some(Frame::End(v1::UploadTextEnd {})),
            },
        ])),
    )
    .await
    .expect("new upload acceptance must not wait for the collection writer")
    .unwrap()
    .into_inner();
    assert_eq!(response.state(), v1::IngestionState::Accepted);
    lock.execute_batch("ROLLBACK").unwrap();
    let service = running.service.clone();
    assert!(
        tokio::task::spawn_blocking(move || service.process_next())
            .await
            .unwrap()
            .unwrap()
    );
    assert_eq!(
        search(&mut running.client, "default", "independent")
            .await
            .len(),
        1
    );
    running.stop().await;
}
