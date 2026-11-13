use std::sync::{
    Arc,
    atomic::{AtomicU8, AtomicUsize, Ordering},
};

use sanctum_core::{ByteRange, Error, Result as StorageResult};
use sanctum_proto::v1::{self, sanctum_client::SanctumClient, upload_text_frame::Frame};
use sanctum_service::{HostedCatalog, TaskStage};
use sanctum_storage::{Condition, Download, Key, ListPage, Object, Reader, Storage, Version};
use tokio::{io::AsyncReadExt, net::TcpListener};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::{Code, transport::Server};

#[tokio::test]
async fn hosted_catalog_recovers_across_server_restart_and_stays_in_scope() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(sanctum_storage_fs::FileStorage::open(directory.path()).unwrap());
    let mut state = start(HostedCatalog::new(store.clone(), None, 1024 * 1024).unwrap()).await;
    let info = state
        .client
        .get_runtime_info(v1::Empty {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        (info.status.as_str(), info.storage.as_str()),
        ("serving", "object")
    );
    assert!(info.capabilities.contains(&"retrieval".into()));
    assert!(!info.capabilities.contains(&"local_import".into()));
    let request_id = uuid::Uuid::new_v4().to_string();
    let create =
        |tenant: &str, collection: &str, request_id: Option<String>| v1::CreateCollectionRequest {
            scope: Some(v1::Scope {
                tenant: tenant.into(),
                collection: collection.into(),
            }),
            request_id,
        };
    // Simulate a crash after initial collection publication and before activation.
    let interrupted = sanctum_core::Scope {
        tenant: "default".parse().unwrap(),
        collection: "catalog".parse().unwrap(),
    };
    let executor =
        sanctum_db::Publication::new(store.clone(), std::time::Duration::from_secs(15)).unwrap();
    let unit = sanctum_db::Unit::collection(None, &interrupted).unwrap();
    let mut writer = executor.acquire(&unit).await.unwrap();
    let (initial, _) = writer
        .apply(
            vec![sanctum_db::Command {
                operation: uuid::Uuid::nil(),
                payload: (),
            }],
            |tx, ()| {
                sanctum_db_sqlite::initialize_collection(tx)?;
                Ok("initialized".into())
            },
        )
        .await
        .unwrap();
    writer.release().await.unwrap();
    let empty = state
        .client
        .list_collections(v1::ListCollectionsRequest {
            tenant: "default".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(empty.collections.is_empty());

    let first = create("default", "catalog", Some(request_id.clone()));
    state.client.create_collection(first.clone()).await.unwrap();
    assert_eq!(executor.generation(&unit).await.unwrap(), Some(initial));
    state
        .client
        .create_collection(create("default", "notes", None))
        .await
        .unwrap();
    state
        .client
        .create_collection(create("other", "private", None))
        .await
        .unwrap();
    state.stop().await;

    let mut state = start(HostedCatalog::new(store.clone(), None, 1024 * 1024).unwrap()).await;
    state.client.create_collection(first).await.unwrap();
    assert_eq!(
        state
            .client
            .create_collection(create("default", "catalog", None))
            .await
            .unwrap_err()
            .code(),
        Code::AlreadyExists
    );
    assert_eq!(
        state
            .client
            .create_collection(create("default", "conflict", Some(request_id)))
            .await
            .unwrap_err()
            .code(),
        Code::AlreadyExists
    );
    let mut request = v1::ListCollectionsRequest {
        tenant: "default".into(),
        limit: Some(1),
        ..Default::default()
    };
    let page = state
        .client
        .list_collections(request.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        page.collections[0].scope.as_ref().unwrap().collection,
        "catalog"
    );
    request.after = page.next_after;
    let page = state
        .client
        .list_collections(request)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        page.collections[0].scope.as_ref().unwrap().collection,
        "notes"
    );
    assert!(page.next_after.is_none());
    let initial = &page.collections[0];
    assert_eq!(initial.display_name.as_deref(), Some("notes"));
    let renamed = state
        .client
        .update_collection(v1::UpdateCollectionRequest {
            scope: initial.scope.clone(),
            display_name: "Field notes 🦀".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(renamed.scope, initial.scope);
    assert_eq!(
        renamed.created_at_unix_seconds,
        initial.created_at_unix_seconds
    );
    let listed = state
        .client
        .list_collections(v1::ListCollectionsRequest {
            tenant: "default".into(),
            after: Some("catalog".into()),
            limit: Some(1),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.collections, vec![renamed]);

    use v1::storage_usage_request::Scope;
    let tenant = state
        .client
        .get_storage_usage(v1::StorageUsageRequest {
            scope: Some(Scope::Tenant("default".into())),
        })
        .await
        .unwrap()
        .into_inner();
    let collection = state
        .client
        .get_storage_usage(v1::StorageUsageRequest {
            scope: Some(Scope::Collection(v1::Scope {
                tenant: "default".into(),
                collection: "catalog".into(),
            })),
        })
        .await
        .unwrap()
        .into_inner();
    let expected = sanctum_storage::usage(
        store.as_ref(),
        Some(&sanctum_storage::Key::parse("default").unwrap()),
    )
    .await
    .unwrap();
    assert_eq!(
        (tenant.bytes, tenant.files),
        (expected.bytes, expected.files)
    );
    assert_eq!(collection.files, 2);
    assert!(collection.bytes > 0 && collection.bytes < tenant.bytes);
    assert_eq!(
        state
            .client
            .get_local_queue(v1::Empty {})
            .await
            .unwrap_err()
            .code(),
        Code::Unimplemented
    );
    state.stop().await;
}

#[tokio::test]
async fn hosted_collection_deletion_recovers_between_catalog_and_collection_commits() {
    use std::time::Duration;

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(sanctum_storage_fs::FileStorage::open(dir.path()).unwrap());
    let catalog = || HostedCatalog::new(store.clone(), None, 1024).unwrap();
    let mut api = start(catalog()).await;
    let scope = v1::Scope {
        tenant: "default".into(),
        collection: "notes".into(),
    };
    api.client
        .create_collection(v1::CreateCollectionRequest {
            scope: Some(scope.clone()),
            request_id: None,
        })
        .await
        .unwrap();
    let core_scope = sanctum_core::Scope {
        tenant: "default".parse().unwrap(),
        collection: "notes".parse().unwrap(),
    };
    let unit = sanctum_db::Unit::collection(None, &core_scope).unwrap();
    let executor = sanctum_db::Publication::new(store.clone(), Duration::from_secs(30)).unwrap();
    let lease = executor.acquire(&unit).await.unwrap();

    let request = v1::DeleteCollectionRequest {
        scope: Some(scope.clone()),
        request_id: Some(uuid::Uuid::new_v4().to_string()),
    };
    assert_eq!(
        api.client
            .delete_collection(request.clone())
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    let listed = api
        .client
        .list_collections(v1::ListCollectionsRequest {
            tenant: "default".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(listed.collections.is_empty());
    lease.release().await.unwrap();
    api.stop().await;

    let service = catalog();
    assert_eq!(service.sweep_hosted_snapshots().await.unwrap().failed, 0);
    assert!(matches!(
        executor.acquire(&unit).await,
        Err(Error::NotFound(_))
    ));
    let mut api = start(service).await;
    api.client.delete_collection(request.clone()).await.unwrap();
    let other = v1::DeleteCollectionRequest {
        scope: Some(v1::Scope {
            collection: "other".into(),
            ..scope.clone()
        }),
        ..request
    };
    assert_eq!(
        api.client
            .delete_collection(other)
            .await
            .unwrap_err()
            .code(),
        Code::AlreadyExists
    );
    assert_eq!(
        api.client
            .create_collection(v1::CreateCollectionRequest {
                scope: Some(scope),
                request_id: None
            })
            .await
            .unwrap_err()
            .code(),
        Code::AlreadyExists
    );
    api.stop().await;
}

#[tokio::test]
async fn hosted_collection_reclamation_keeps_open_snapshots_and_resumes() {
    use std::time::{Duration, SystemTime};

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(sanctum_storage_fs::FileStorage::open(dir.path()).unwrap());
    let catalog = || HostedCatalog::new(store.clone(), None, 1024).unwrap();
    let service = catalog();
    let scope = sanctum_core::Scope {
        tenant: "default".parse().unwrap(),
        collection: "notes".parse().unwrap(),
    };
    service.create_collection(&scope, None).await.unwrap();
    let upload = service
        .stage_upload(
            scope.clone(),
            sanctum_service::TextUpload {
                name: "notes.txt".into(),
                external: None,
                replacement: None,
                retain_original: false,
            },
            None,
        )
        .await
        .unwrap();
    std::io::Write::write_all(&mut upload.reopen().unwrap(), b"published text").unwrap();
    let accepted = upload.enqueue().await.unwrap();
    service
        .register_upload(&scope, accepted.operation)
        .await
        .unwrap();
    service
        .index_upload(&scope, accepted.operation)
        .await
        .unwrap();
    let target = sanctum_service::DatabaseTarget {
        tenant: scope.tenant.clone(),
        collection: Some(scope.collection.clone()),
    };
    let session = service.open_database_session(&target).await.unwrap();
    service.delete_collection(&scope, None).await.unwrap();
    assert!(matches!(
        service.ingestion_job(&scope, accepted.operation).await,
        Err(Error::NotFound(_))
    ));
    let downloaded = catalog()
        .database_session(&target, session.id)
        .await
        .unwrap();
    assert_eq!(downloaded.generation.id, session.snapshot_generation);
    assert_eq!(service.sweep_hosted_snapshots().await.unwrap().deleted, 0);

    let prefix = Key::parse("default/collections/notes").unwrap();
    let head = Key::parse(format!("{prefix}/HEAD.json")).unwrap();
    for index in 0..130 {
        let key = Key::parse(format!("{prefix}/sources/late-{index:03}")).unwrap();
        store
            .put_stream(&key, Box::pin(std::io::Cursor::new(b"x")), 1)
            .await
            .unwrap();
    }
    let mut record = store.get(&head).await.unwrap();
    let mut bytes = Vec::new();
    record.reader.read_to_end(&mut bytes).await.unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    value["deleted_at_ms"] = serde_json::json!(
        (SystemTime::now() - Duration::from_secs(901))
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    );
    store
        .put_control(
            &head,
            serde_json::to_vec(&value).unwrap().into(),
            Condition::Match(record.object.version),
        )
        .await
        .unwrap();
    let first = service.sweep_hosted_snapshots().await.unwrap();
    assert!(first.more && first.deleted > 0);
    let restarted = catalog();
    for _ in 0..8 {
        let report = restarted.sweep_hosted_snapshots().await.unwrap();
        assert_eq!(report.failed, 0);
        if !report.more {
            break;
        }
    }
    let remaining = store.list(Some(&prefix), None, 8).await.unwrap().objects;
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].key, head);
    let result = sanctum_service::database_snapshot::query(
        downloaded.path(),
        "SELECT count(*) FROM chunks",
        Vec::new(),
        0,
        1,
        || false,
    )
    .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![sanctum_service::database_snapshot::SqlValue::Integer(
            1
        )]]
    );
}

#[tokio::test]
async fn hosted_context_trims_utf8_boundaries_and_bounds_the_response() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(sanctum_storage_fs::FileStorage::open(dir.path()).unwrap());
    let catalog = HostedCatalog::new(store, None, 32_768).unwrap();
    let mut api = start(catalog.clone()).await;
    let scope = sanctum_core::Scope {
        tenant: "default".parse().unwrap(),
        collection: "notes".parse().unwrap(),
    };
    let wire_scope = v1::Scope {
        tenant: "default".into(),
        collection: "notes".into(),
    };
    catalog.create_collection(&scope, None).await.unwrap();
    let text = "🦀".repeat(3000);
    let header = v1::UploadTextHeader {
        scope: Some(wire_scope.clone()),
        name: "notes.txt".into(),
        ..Default::default()
    };
    let job = api
        .client
        .submit_text(upload_frames(&header, [text.as_bytes()]))
        .await
        .unwrap()
        .into_inner();
    let operation = job.operation_id.parse().unwrap();
    catalog.register_upload(&scope, operation).await.unwrap();
    catalog.index_upload(&scope, operation).await.unwrap();
    let request = v1::FetchContextRequest {
        chunk: Some(v1::ChunkRef {
            scope: Some(wire_scope),
            source_id: job.source_id,
            revision_id: job.revision_id,
            ordinal: 1,
        }),
        before_bytes: 5,
        after_bytes: 5,
    };
    let context = api
        .client
        .fetch_context(request.clone())
        .await
        .unwrap()
        .into_inner();
    let hit = context.hit.unwrap();
    assert_eq!(context.start_byte + 4, hit.start_byte);
    assert_eq!(context.end_byte, hit.end_byte + 4);
    assert!(hit.database_generation.is_some());
    assert_eq!(
        context.text,
        text[context.start_byte as usize..context.end_byte as usize]
    );
    let clipped = api
        .client
        .fetch_context(v1::FetchContextRequest {
            before_bytes: 16_384,
            after_bytes: 16_384,
            ..request.clone()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        (clipped.start_byte, clipped.end_byte),
        (0, text.len() as u64)
    );
    let too_large = v1::FetchContextRequest {
        before_bytes: 16_385,
        ..request
    };
    assert_eq!(
        api.client
            .fetch_context(too_large)
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    api.stop().await;
}

#[tokio::test]
async fn hosted_upload_keeps_accepted_chunk_settings_across_worker_restart() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(sanctum_storage_fs::FileStorage::open(dir.path()).unwrap());
    let configured = HostedCatalog::new(store.clone(), None, 32_768)
        .unwrap()
        .with_chunking(sanctum_core::ChunkSettings {
            window_bytes: 257,
            overlap_bytes: 33,
        })
        .unwrap();
    let scope = sanctum_core::Scope {
        tenant: "default".parse().unwrap(),
        collection: "notes".parse().unwrap(),
    };
    configured.create_collection(&scope, None).await.unwrap();
    let upload = configured
        .stage_upload(
            scope.clone(),
            sanctum_service::TextUpload {
                name: "notes.txt".into(),
                external: None,
                replacement: None,
                retain_original: false,
            },
            None,
        )
        .await
        .unwrap();
    let text = "🦀".repeat(300);
    std::io::Write::write_all(&mut upload.reopen().unwrap(), text.as_bytes()).unwrap();
    let accepted = upload.enqueue().await.unwrap();

    let worker = HostedCatalog::new(store, None, 32_768).unwrap();
    worker
        .register_upload(&scope, accepted.operation)
        .await
        .unwrap();
    let result = worker
        .index_upload(&scope, accepted.operation)
        .await
        .unwrap();
    let content = result.content.unwrap();
    assert_eq!(content.chunker, "utf8-overlap-v1/257/33");
    assert!(content.chunks > 1);
    let reference = sanctum_service::ChunkRef {
        source: result.source.unwrap(),
        revision: result.revision,
        ordinal: 1,
    };
    let hit = worker.fetch(&scope, reference).await.unwrap();
    assert_eq!(
        hit.excerpt,
        text[hit.chunk.range.start as usize..hit.chunk.range.end as usize]
    );
    assert!(hit.excerpt.len() <= 257);
    let (stored, _) = worker
        .source(&scope, result.source.unwrap(), None)
        .await
        .unwrap();
    assert_eq!(stored.content.chunker, content.chunker);
}

#[tokio::test]
async fn hosted_sources_page_current_revisions_and_keep_tenant_scope() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(sanctum_storage_fs::FileStorage::open(dir.path()).unwrap());
    let catalog = HostedCatalog::new(store, None, 1024).unwrap();
    let mut api = start(catalog.clone()).await;
    let scope = sanctum_core::Scope {
        tenant: "default".parse().unwrap(),
        collection: "notes".parse().unwrap(),
    };
    let wire_scope = v1::Scope {
        tenant: "default".into(),
        collection: "notes".into(),
    };
    catalog.create_collection(&scope, None).await.unwrap();
    catalog
        .create_collection(
            &sanctum_core::Scope {
                tenant: "other".parse().unwrap(),
                collection: scope.collection.clone(),
            },
            None,
        )
        .await
        .unwrap();

    let header = v1::UploadTextHeader {
        scope: Some(wire_scope.clone()),
        name: "notes.rtf".into(),
        external: Some(v1::ExternalSource {
            namespace: "integration".into(),
            id: "first".into(),
        }),
        retain_original: true,
        ..Default::default()
    };
    let pending = api
        .client
        .submit_text(upload_frames(&header, [br"{\rtf1\ansi quartz}".as_slice()]))
        .await
        .unwrap()
        .into_inner();
    let mut list = v1::ListSourcesRequest {
        scope: Some(wire_scope.clone()),
        limit: Some(1),
        ..Default::default()
    };
    assert!(
        api.client
            .list_sources(list.clone())
            .await
            .unwrap()
            .into_inner()
            .sources
            .is_empty()
    );
    let operation = pending.operation_id.parse().unwrap();
    catalog.register_upload(&scope, operation).await.unwrap();
    catalog.index_upload(&scope, operation).await.unwrap();
    let other = v1::UploadTextHeader {
        name: "second.txt".into(),
        external: None,
        ..header.clone()
    };
    let job = api
        .client
        .submit_text(upload_frames(&other, [b"second".as_slice()]))
        .await
        .unwrap()
        .into_inner();
    let operation = job.operation_id.parse().unwrap();
    catalog.register_upload(&scope, operation).await.unwrap();
    catalog.index_upload(&scope, operation).await.unwrap();

    let first_page = api
        .client
        .list_sources(list.clone())
        .await
        .unwrap()
        .into_inner();
    list.after_source_id = first_page.next_after_source_id.clone();
    assert!(list.after_source_id.is_some());
    let second_page = api.client.list_sources(list).await.unwrap().into_inner();
    assert_eq!(
        first_page.database_generation,
        second_page.database_generation
    );
    assert!(first_page.database_generation.is_some());
    assert_ne!(
        first_page.sources[0].source_id,
        second_page.sources[0].source_id
    );
    assert!(second_page.next_after_source_id.is_none());

    let mut inspect = v1::GetSourceRequest {
        scope: Some(wire_scope.clone()),
        source_id: pending.source_id.clone(),
        expected_revision_id: Some(pending.revision_id.clone()),
    };
    let found = api
        .client
        .get_source(inspect.clone())
        .await
        .unwrap()
        .into_inner();
    let current = found.current_revision.unwrap();
    assert_eq!(current.bytes, 6);
    assert_eq!(current.external, header.external);
    assert_ne!(current.content_blake3, current.original_blake3);
    assert!(current.retain_original && current.extractor.is_some());
    inspect.expected_revision_id = Some(uuid::Uuid::new_v4().to_string());
    assert_eq!(
        api.client
            .get_source(inspect.clone())
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    inspect.scope = Some(v1::Scope {
        tenant: "other".into(),
        ..wire_scope.clone()
    });
    assert_eq!(
        api.client
            .get_source(inspect.clone())
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    api.client
        .delete_source(v1::DeleteSourceRequest {
            scope: Some(wire_scope.clone()),
            source_id: pending.source_id,
            expected_revision_id: Some(pending.revision_id),
            request_id: None,
        })
        .await
        .unwrap();
    inspect.scope = Some(wire_scope);
    inspect.expected_revision_id = None;
    assert_eq!(
        api.client.get_source(inspect).await.unwrap_err().code(),
        Code::NotFound
    );
    api.stop().await;
}

#[tokio::test]
async fn hosted_synchronous_upload_waits_for_worker_and_replays() {
    use std::time::Duration;

    let directory = tempfile::tempdir().unwrap();
    let storage = Arc::new(sanctum_storage_fs::FileStorage::open(directory.path()).unwrap());
    let service = HostedCatalog::new(storage, None, 1024).unwrap();
    let mut server = start(service.clone()).await;
    let scope = sanctum_core::Scope {
        tenant: "default".parse().unwrap(),
        collection: "notes".parse().unwrap(),
    };
    service.create_collection(&scope, None).await.unwrap();

    for (name, content) in [
        ("notes.txt", b"quartz note".as_slice()),
        ("invalid.txt", &[0xff][..]),
    ] {
        let operation = uuid::Uuid::new_v4();
        let header = v1::UploadTextHeader {
            scope: Some(v1::Scope {
                tenant: "default".into(),
                collection: "notes".into(),
            }),
            name: name.into(),
            request_id: Some(operation.to_string()),
            ..Default::default()
        };
        let mut upload_client = server.client.clone();
        let upload_header = header.clone();
        let upload = tokio::spawn(async move {
            upload_client
                .upload_text(upload_frames(&upload_header, [content]))
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match service.ingestion_job(&scope, operation).await {
                    Ok(job) => {
                        assert!(matches!(
                            job.state,
                            sanctum_service::HostedJobState::Accepted
                        ));
                        break;
                    }
                    Err(Error::NotFound(_)) => tokio::time::sleep(Duration::from_millis(20)).await,
                    Err(error) => panic!("{error}"),
                }
            }
        })
        .await
        .unwrap();
        assert!(!upload.is_finished());
        service.register_upload(&scope, operation).await.unwrap();
        assert!(!upload.is_finished());
        service.index_upload(&scope, operation).await.unwrap();
        let response = tokio::time::timeout(Duration::from_secs(5), upload)
            .await
            .unwrap()
            .unwrap();
        let replay = server
            .client
            .upload_text(upload_frames(&header, [content]))
            .await;
        if name == "notes.txt" {
            let revision = response.unwrap().into_inner();
            assert_eq!(revision.bytes, content.len() as u64);
            assert_eq!(revision, replay.unwrap().into_inner());
        } else {
            for result in [response, replay] {
                let failure = result.unwrap_err();
                assert_eq!(failure.code(), Code::InvalidArgument);
                let stored = failure
                    .metadata()
                    .get_bin("sanctum-operation-id-bin")
                    .unwrap()
                    .to_bytes()
                    .unwrap();
                assert_eq!(stored.as_ref(), operation.as_bytes());
            }
        }
    }
    server.stop().await;
}

#[tokio::test]
async fn hosted_submissions_store_exact_bytes_and_replay_after_restart() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(sanctum_storage_fs::FileStorage::open(directory.path()).unwrap());
    let prefix = Key::parse("workspace").unwrap();
    let catalog = HostedCatalog::new(store.clone(), Some(prefix), 1024 * 1024).unwrap();
    let mut state = start(catalog.clone()).await;
    let scope = v1::Scope {
        tenant: "default".into(),
        collection: "notes".into(),
    };
    let operation = uuid::Uuid::new_v4().to_string();
    let header = v1::UploadTextHeader {
        scope: Some(scope.clone()),
        name: "field-notes.md".into(),
        external: Some(v1::ExternalSource {
            namespace: "editor".into(),
            id: "note:42".into(),
        }),
        request_id: Some(operation.clone()),
        ..Default::default()
    };
    let bytes = "café 🦀\n".as_bytes();
    assert_eq!(
        state
            .client
            .submit_text(upload_frames(&header, [bytes]))
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    state
        .client
        .create_collection(v1::CreateCollectionRequest {
            scope: Some(scope.clone()),
            ..Default::default()
        })
        .await
        .unwrap();

    let mut no_end = frames(&header, [bytes]);
    no_end.pop();
    assert_eq!(
        state
            .client
            .submit_text(tokio_stream::iter(no_end))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    let mut extra = frames(&header, [bytes]);
    extra.push(v1::UploadTextFrame {
        frame: Some(Frame::Data(vec![0])),
    });
    assert_eq!(
        state
            .client
            .submit_text(tokio_stream::iter(extra))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    let max_frame = vec![b'a'; sanctum_core::UPLOAD_FRAME_BYTES];
    assert_eq!(
        state
            .client
            .submit_text(upload_frames(
                &header,
                std::iter::repeat_n(max_frame.as_slice(), 5)
            ))
            .await
            .unwrap_err()
            .code(),
        Code::ResourceExhausted
    );
    let oversized_frame = vec![0; max_frame.len() + 1];
    assert_eq!(
        state
            .client
            .submit_text(upload_frames(&header, [oversized_frame.as_slice()]))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    let operation_prefix = Key::parse(format!(
        "workspace/default/collections/notes/operations/{operation}"
    ))
    .unwrap();
    assert!(
        store
            .list(Some(&operation_prefix), None, 10)
            .await
            .unwrap()
            .objects
            .is_empty()
    );

    let mut duplicate = state.client.clone();
    let (left, right) = tokio::join!(
        state
            .client
            .submit_text(upload_frames(&header, bytes.chunks(2))),
        duplicate.submit_text(upload_frames(&header, [bytes]))
    );
    let accepted = left.unwrap().into_inner();
    assert_eq!(accepted, right.unwrap().into_inner());
    assert_eq!(accepted.operation_id, operation);
    assert_eq!(accepted.state(), v1::IngestionState::Accepted);
    assert_eq!(accepted.total_bytes, bytes.len() as u64);
    assert_eq!(accepted.update_sequence, 0);
    assert_eq!(accepted.processed_bytes, 0);
    assert!(accepted.text_revision.is_none());
    assert!(uuid::Uuid::parse_str(&accepted.source_id).is_ok());
    assert!(uuid::Uuid::parse_str(&accepted.revision_id).is_ok());
    assert_eq!(accepted.external, header.external);

    let page = store.list(Some(&operation_prefix), None, 10).await.unwrap();
    assert_eq!(page.objects.len(), 2);
    let envelope = Key::parse(format!("{operation_prefix}/operation.json")).unwrap();
    let mut envelope_body = Vec::new();
    store
        .get(&envelope)
        .await
        .unwrap()
        .reader
        .read_to_end(&mut envelope_body)
        .await
        .unwrap();
    let envelope: serde_json::Value = serde_json::from_slice(&envelope_body).unwrap();
    assert_eq!(envelope["stage"], "pending_registration");
    let input_key = Key::parse(format!(
        "{operation_prefix}/inputs/{}",
        envelope["attempt"].as_str().unwrap()
    ))
    .unwrap();
    let mut stored = Vec::new();
    store
        .get(&input_key)
        .await
        .unwrap()
        .reader
        .read_to_end(&mut stored)
        .await
        .unwrap();
    assert_eq!(stored, bytes);
    state.stop().await;

    let mut state = start(catalog).await;
    let job = |scope| v1::IngestionJobRequest {
        scope: Some(scope),
        operation_id: operation.clone(),
    };
    assert_eq!(
        state
            .client
            .get_ingestion_job(job(scope.clone()))
            .await
            .unwrap()
            .into_inner(),
        accepted
    );
    let other = v1::Scope {
        tenant: "other".into(),
        ..scope.clone()
    };
    assert_eq!(
        state
            .client
            .get_ingestion_job(job(other))
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    assert_eq!(
        state
            .client
            .submit_text(upload_frames(&header, [bytes]))
            .await
            .unwrap()
            .into_inner(),
        accepted
    );
    assert_eq!(
        state
            .client
            .submit_text(upload_frames(&header, [b"different!".as_slice()]))
            .await
            .unwrap_err()
            .code(),
        Code::AlreadyExists
    );
    let mut changed = header.clone();
    changed.retain_original = true;
    assert_eq!(
        state
            .client
            .submit_text(upload_frames(&changed, [bytes]))
            .await
            .unwrap_err()
            .code(),
        Code::AlreadyExists
    );

    let replacement = v1::UploadTextHeader {
        request_id: None,
        replacement: Some(v1::Replacement {
            source_id: None,
            expected_revision_id: Some(accepted.revision_id),
        }),
        ..header
    };
    let job = state
        .client
        .submit_text(upload_frames(&replacement, [bytes]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(job.state(), v1::IngestionState::Accepted);
    assert!(job.source_id.is_empty());
    assert_ne!(job.operation_id, operation);
    state.stop().await;
}

#[tokio::test]
async fn hosted_cancellation_recovers_from_lost_checkpoints_without_publishing() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Arc::new(InterruptAcceptance {
        inner: sanctum_storage_fs::FileStorage::open(directory.path()).unwrap(),
        interrupt: AtomicU8::new(0),
    });
    let catalog = HostedCatalog::new(storage.clone(), None, 1024).unwrap();
    let scope = sanctum_core::Scope {
        tenant: "default".parse().unwrap(),
        collection: "notes".parse().unwrap(),
    };
    let wire_scope = v1::Scope {
        tenant: "default".into(),
        collection: "notes".into(),
    };
    for tenant in ["default", "other"] {
        catalog
            .create_collection(
                &sanctum_core::Scope {
                    tenant: tenant.parse().unwrap(),
                    ..scope.clone()
                },
                None,
            )
            .await
            .unwrap();
    }
    let mut server = start(catalog.clone()).await;
    let header = v1::UploadTextHeader {
        scope: Some(wire_scope.clone()),
        name: "notes.txt".into(),
        external: Some(v1::ExternalSource {
            namespace: "fixture".into(),
            id: "document".into(),
        }),
        ..Default::default()
    };
    let payload = [b"quartz".as_slice()];
    for registered in [false, true] {
        let accepted = server
            .client
            .submit_text(upload_frames(&header, payload))
            .await
            .unwrap()
            .into_inner();
        let id = accepted.operation_id.parse().unwrap();
        let request = v1::IngestionJobRequest {
            scope: Some(wire_scope.clone()),
            operation_id: accepted.operation_id,
        };
        let wrong_tenant = v1::IngestionJobRequest {
            scope: Some(v1::Scope {
                tenant: "other".into(),
                ..wire_scope.clone()
            }),
            ..request.clone()
        };
        assert_eq!(
            server
                .client
                .cancel_ingestion_job(wrong_tenant)
                .await
                .unwrap_err()
                .code(),
            Code::NotFound
        );
        if registered {
            assert_eq!(
                catalog
                    .register_upload(&scope, id)
                    .await
                    .unwrap()
                    .update_sequence,
                1
            );
        }
        storage.interrupt.store(1, Ordering::Relaxed);
        assert_eq!(
            server
                .client
                .cancel_ingestion_job(request.clone())
                .await
                .unwrap_err()
                .code(),
            Code::Unavailable
        );
        server.stop().await;
        let recovered = HostedCatalog::new(storage.clone(), None, 1024).unwrap();
        if registered {
            assert!(matches!(
                recovered.index_upload(&scope, id).await.unwrap().state,
                sanctum_service::HostedJobState::Canceled
            ));
        } else {
            assert!(matches!(
                recovered.register_upload(&scope, id).await.unwrap().state,
                sanctum_service::HostedJobState::Canceled
            ));
        }
        server = start(recovered.clone()).await;
        let canceled = server
            .client
            .cancel_ingestion_job(request)
            .await
            .unwrap()
            .into_inner();
        assert_eq!(canceled.state(), v1::IngestionState::Canceled);
        assert_eq!(canceled.update_sequence, u64::from(registered));
        assert!(matches!(
            recovered.index_upload(&scope, id).await.unwrap().state,
            sanctum_service::HostedJobState::Canceled
        ));
        let input_prefix =
            Key::parse(format!("default/collections/notes/operations/{id}/inputs")).unwrap();
        assert!(
            storage
                .list(Some(&input_prefix), None, 10)
                .await
                .unwrap()
                .objects
                .is_empty()
        );
        assert!(
            server
                .client
                .search_text(v1::SearchTextRequest {
                    scope: Some(wire_scope.clone()),
                    query: "quartz".into(),
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_inner()
                .hits
                .is_empty()
        );
    }

    // The external ID was released. Once the next upload's text is committed,
    // cancellation must refuse it even if the envelope still says registered.
    let published = server
        .client
        .submit_text(upload_frames(&header, payload))
        .await
        .unwrap()
        .into_inner();
    let id = published.operation_id.parse().unwrap();
    catalog.register_upload(&scope, id).await.unwrap();
    storage.interrupt.store(1, Ordering::Relaxed);
    assert!(matches!(
        catalog.index_upload(&scope, id).await,
        Err(Error::Busy(_))
    ));
    let request = v1::IngestionJobRequest {
        scope: Some(wire_scope),
        operation_id: published.operation_id,
    };
    assert_eq!(
        server
            .client
            .cancel_ingestion_job(request)
            .await
            .unwrap_err()
            .code(),
        Code::AlreadyExists
    );
    assert!(matches!(
        catalog.index_upload(&scope, id).await.unwrap().state,
        sanctum_service::HostedJobState::TextReady
    ));
    server.stop().await;
}

#[tokio::test]
async fn hosted_registration_replays_after_checkpoint_failure_and_orders_replacements() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Arc::new(InterruptAcceptance {
        inner: sanctum_storage_fs::FileStorage::open(directory.path()).unwrap(),
        interrupt: AtomicU8::new(0),
    });
    let catalog = HostedCatalog::new(storage.clone(), None, 1024).unwrap();
    let mut server = start(catalog.clone()).await;
    let scope = sanctum_core::Scope {
        tenant: "default".parse().unwrap(),
        collection: "notes".parse().unwrap(),
    };
    let wire_scope = v1::Scope {
        tenant: scope.tenant.to_string(),
        collection: scope.collection.to_string(),
    };
    server
        .client
        .create_collection(v1::CreateCollectionRequest {
            scope: Some(wire_scope.clone()),
            request_id: None,
        })
        .await
        .unwrap();
    let source = uuid::Uuid::new_v4();
    let revision = uuid::Uuid::new_v4();
    let external = v1::ExternalSource {
        namespace: "editor".into(),
        id: "known".into(),
    };

    // A published baseline revision lets both competing intents target the same current revision.
    let executor =
        sanctum_db::Publication::new(storage.clone(), std::time::Duration::from_secs(15)).unwrap();
    let unit = sanctum_db::Unit::collection(None, &scope).unwrap();
    let mut writer = executor.acquire(&unit).await.unwrap();
    writer
        .apply(
            vec![sanctum_db::Command {
                operation: uuid::Uuid::new_v4(),
                payload: (source, revision),
            }],
            |tx, (source, revision)| {
                tx.execute(
            "INSERT INTO sources (id, name, external_namespace, external_id, current_revision)
             VALUES (?1, 'baseline.txt', 'editor', 'known', ?2)",
            [source.to_string(), revision.to_string()],
        ).map_err(|error| Error::Internal(Box::new(error)))?;
                tx.execute(
            "INSERT INTO revisions (source_id, id, content_hash, bytes, chunk_count, chunker)
             VALUES (?1, ?2, 'baseline', 0, 0, 'utf8-overlap-v1')",
            [source.to_string(), revision.to_string()],
        ).map_err(|error| Error::Internal(Box::new(error)))?;
                Ok("baseline".into())
            },
        )
        .await
        .unwrap();
    writer.release().await.unwrap();

    let header = v1::UploadTextHeader {
        scope: Some(wire_scope.clone()),
        name: "replacement.txt".into(),
        external: Some(external.clone()),
        replacement: Some(v1::Replacement {
            source_id: None,
            expected_revision_id: Some(revision.to_string()),
        }),
        ..Default::default()
    };
    let data = [b"replacement".as_slice()];
    let first = server
        .client
        .submit_text(upload_frames(&header, data))
        .await
        .unwrap()
        .into_inner();
    assert!(first.source_id.is_empty());
    let first_id = first.operation_id.parse().unwrap();
    storage.interrupt.store(1, Ordering::Relaxed);
    assert!(matches!(
        catalog.register_upload(&scope, first_id).await,
        Err(Error::Busy(_))
    ));
    assert!(executor.receipt(&unit, first_id).await.unwrap().is_some());
    let first_snapshot = executor.generation(&unit).await.unwrap();
    server.stop().await;

    let mut server = start(catalog.clone()).await;
    let request = v1::IngestionJobRequest {
        scope: Some(wire_scope.clone()),
        operation_id: first.operation_id,
    };
    assert_eq!(
        server
            .client
            .get_ingestion_job(request.clone())
            .await
            .unwrap()
            .into_inner()
            .state(),
        v1::IngestionState::Accepted
    );
    assert_eq!(
        catalog
            .register_upload(&scope, first_id)
            .await
            .unwrap()
            .update_sequence,
        1
    );
    assert_eq!(executor.generation(&unit).await.unwrap(), first_snapshot);
    let registered = server
        .client
        .get_ingestion_job(request)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(registered.state(), v1::IngestionState::Registered);
    assert_eq!(registered.source_id, source.to_string());
    assert_eq!(registered.external, Some(external));
    assert_eq!(registered.update_sequence, 1);
    assert!(registered.text_revision.is_none());

    let second = server
        .client
        .submit_text(upload_frames(&header, data))
        .await
        .unwrap()
        .into_inner();
    let second_id = second.operation_id.parse().unwrap();
    assert_eq!(
        catalog
            .register_upload(&scope, second_id)
            .await
            .unwrap()
            .update_sequence,
        2
    );
    assert_eq!(
        catalog
            .register_upload(&scope, first_id)
            .await
            .unwrap()
            .update_sequence,
        1
    );
    let stale = v1::UploadTextHeader {
        replacement: Some(v1::Replacement {
            source_id: Some(source.to_string()),
            expected_revision_id: Some(uuid::Uuid::new_v4().to_string()),
        }),
        ..header.clone()
    };
    let stale = server
        .client
        .submit_text(upload_frames(&stale, data))
        .await
        .unwrap()
        .into_inner();
    catalog
        .register_upload(&scope, stale.operation_id.parse().unwrap())
        .await
        .unwrap();
    let stale = server
        .client
        .get_ingestion_job(v1::IngestionJobRequest {
            scope: Some(wire_scope),
            operation_id: stale.operation_id,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(stale.state(), v1::IngestionState::Failed);
    assert_eq!(stale.error_code.as_deref(), Some("stale_revision"));
    assert_eq!(stale.update_sequence, 0);

    let scratch = tempfile::tempdir().unwrap();
    let generation = executor.read(&unit, scratch.path()).await.unwrap().unwrap();
    let database = rusqlite::Connection::open(generation.path()).unwrap();
    let latest: (String, String, i64) = database.query_row(
        "SELECT operation_id, revision_id, update_sequence FROM hosted_revision_intents WHERE source_id = ?1",
        [source.to_string()],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    ).unwrap();
    assert_eq!(latest, (second.operation_id, second.revision_id, 2));
    let current: String = database
        .query_row(
            "SELECT current_revision FROM sources WHERE id = ?1",
            [source.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(current, revision.to_string());
    server.stop().await;
}

#[tokio::test]
async fn hosted_indexing_publishes_searchable_text_and_fences_old_replacements() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Arc::new(InterruptAcceptance {
        inner: sanctum_storage_fs::FileStorage::open(directory.path()).unwrap(),
        interrupt: AtomicU8::new(0),
    });
    let catalog = HostedCatalog::new(storage.clone(), None, 1024 * 1024).unwrap();
    let mut server = start(catalog.clone()).await;
    let scope = sanctum_core::Scope {
        tenant: "default".parse().unwrap(),
        collection: "notes".parse().unwrap(),
    };
    let wire_scope = v1::Scope {
        tenant: scope.tenant.to_string(),
        collection: scope.collection.to_string(),
    };
    server
        .client
        .create_collection(v1::CreateCollectionRequest {
            scope: Some(wire_scope.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
    let header = v1::UploadTextHeader {
        scope: Some(wire_scope.clone()),
        name: "field-notes.txt".into(),
        ..Default::default()
    };
    let body = "unicorn café 🦀\n".repeat(400);
    let job = server
        .client
        .submit_text(upload_frames(&header, body.as_bytes().chunks(97)))
        .await
        .unwrap()
        .into_inner();
    let operation = job.operation_id.parse().unwrap();
    assert!(matches!(
        catalog.index_upload(&scope, operation).await,
        Err(Error::Busy(_))
    ));
    catalog.register_upload(&scope, operation).await.unwrap();
    storage.interrupt.store(1, Ordering::Relaxed);
    assert!(matches!(
        catalog.index_upload(&scope, operation).await,
        Err(Error::Busy(_))
    ));
    let executor =
        sanctum_db::Publication::new(storage.clone(), std::time::Duration::from_secs(15)).unwrap();
    let unit = sanctum_db::Unit::collection(None, &scope).unwrap();
    let generation = executor.generation(&unit).await.unwrap();
    server.stop().await;

    let mut server = start(catalog.clone()).await;
    catalog.index_upload(&scope, operation).await.unwrap();
    assert_eq!(executor.generation(&unit).await.unwrap(), generation);
    let job = server
        .client
        .get_ingestion_job(v1::IngestionJobRequest {
            scope: Some(wire_scope.clone()),
            operation_id: job.operation_id,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(job.state(), v1::IngestionState::TextReady);
    assert_eq!(job.processed_bytes, body.len() as u64);
    assert!(job.text_revision.as_ref().unwrap().chunks > 1);
    let query = |term: &str| v1::SearchTextRequest {
        scope: Some(wire_scope.clone()),
        query: term.into(),
        mode: v1::SearchMode::Hybrid.into(),
        ..Default::default()
    };
    let hits = server
        .client
        .search_text(query("unicorn"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(hits.mode(), v1::SearchMode::Lexical);
    assert_eq!(hits.degraded_reason.as_deref(), Some("embedding_disabled"));
    assert!(hits.hits.len() > 1);
    let hit = hits.hits.iter().find(|hit| hit.start_byte > 0).unwrap();
    let first_generation = generation.unwrap().id.to_string();
    assert_eq!(
        hit.database_generation.as_deref(),
        Some(first_generation.as_str())
    );
    assert_eq!(
        hit.excerpt,
        body[hit.start_byte as usize..hit.end_byte as usize]
    );
    assert_eq!(
        server
            .client
            .fetch_chunk(v1::FetchChunkRequest {
                chunk: hit.chunk.clone()
            })
            .await
            .unwrap()
            .into_inner(),
        *hit
    );

    let replacement = v1::UploadTextHeader {
        name: "field-notes.rtf".into(),
        retain_original: true,
        replacement: Some(v1::Replacement {
            source_id: Some(job.source_id),
            expected_revision_id: Some(job.revision_id),
        }),
        ..header
    };
    let old = server
        .client
        .submit_text(upload_frames(
            &replacement,
            [br"{\rtf1\ansi obsolete}".as_slice()],
        ))
        .await
        .unwrap()
        .into_inner();
    let old_operation = old.operation_id.parse().unwrap();
    catalog
        .register_upload(&scope, old_operation)
        .await
        .unwrap();
    let rtf = br"{\rtf1\ansi replacement caf\'e9\par next}";
    let new = server
        .client
        .submit_text(upload_frames(&replacement, [rtf.as_slice()]))
        .await
        .unwrap()
        .into_inner();
    let new_operation = new.operation_id.parse().unwrap();
    catalog
        .register_upload(&scope, new_operation)
        .await
        .unwrap();
    let stale = catalog.index_upload(&scope, old_operation).await.unwrap();
    assert!(
        matches!(stale.state, sanctum_service::HostedJobState::Failed { code, .. } if code == "stale_revision")
    );
    catalog.index_upload(&scope, new_operation).await.unwrap();
    let hits = server
        .client
        .search_text(query("replacement"))
        .await
        .unwrap()
        .into_inner()
        .hits;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].excerpt, "replacement café\nnext");
    let current = executor.generation(&unit).await.unwrap().unwrap();
    assert_ne!(current.id.to_string(), first_generation);
    assert_eq!(
        hits[0].database_generation.as_deref(),
        Some(current.id.to_string().as_str())
    );
    assert!(
        server
            .client
            .search_text(query("unicorn"))
            .await
            .unwrap()
            .into_inner()
            .hits
            .is_empty()
    );
    let new = server
        .client
        .get_ingestion_job(v1::IngestionJobRequest {
            scope: Some(wire_scope),
            operation_id: new.operation_id,
        })
        .await
        .unwrap()
        .into_inner();
    let content = new.text_revision.unwrap();
    assert_eq!(content.extractor.as_deref(), Some("rtf-text-v1"));
    assert!(content.retain_original);
    assert_ne!(content.content_blake3, content.original_blake3);

    let scratch = tempfile::tempdir().unwrap();
    let snapshot = executor.read(&unit, scratch.path()).await.unwrap().unwrap();
    let connection = rusqlite::Connection::open(snapshot.path()).unwrap();
    let original: String = connection
        .query_row(
            "SELECT original_key FROM hosted_revision_objects WHERE revision_id = ?1",
            [content.revision_id],
            |row| row.get(0),
        )
        .unwrap();
    let mut original_body = Vec::new();
    storage
        .get(&Key::parse(original).unwrap())
        .await
        .unwrap()
        .reader
        .read_to_end(&mut original_body)
        .await
        .unwrap();
    assert_eq!(original_body, rtf);
    let malformed = v1::UploadTextHeader {
        scope: Some(v1::Scope {
            tenant: "default".into(),
            collection: "notes".into(),
        }),
        name: "malformed.rtf".into(),
        external: Some(v1::ExternalSource {
            namespace: "editor".into(),
            id: "retryable-external".into(),
        }),
        ..Default::default()
    };
    let invalid = server
        .client
        .submit_text(upload_frames(&malformed, [b"invalid".as_slice()]))
        .await
        .unwrap()
        .into_inner();
    let invalid_id = invalid.operation_id.parse().unwrap();
    catalog.register_upload(&scope, invalid_id).await.unwrap();
    let failure = catalog.index_upload(&scope, invalid_id).await.unwrap();
    assert!(
        matches!(failure.state, sanctum_service::HostedJobState::Failed { code, .. } if code == "invalid_source")
    );
    let valid = server
        .client
        .submit_text(upload_frames(&malformed, [br"{\rtf1 valid}".as_slice()]))
        .await
        .unwrap()
        .into_inner();
    let valid_id = valid.operation_id.parse().unwrap();
    assert!(matches!(
        catalog
            .register_upload(&scope, valid_id)
            .await
            .unwrap()
            .state,
        sanctum_service::HostedJobState::Registered
    ));
    assert!(matches!(
        catalog.index_upload(&scope, valid_id).await.unwrap().state,
        sanctum_service::HostedJobState::TextReady
    ));
    server.stop().await;
}

#[tokio::test]
async fn hosted_embeddings_reuse_current_inputs_and_isolate_profiles() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Arc::new(sanctum_storage_fs::FileStorage::open(directory.path()).unwrap());
    let provider = Arc::new(HostedEmbeddingFixture {
        profile: "fixture-v1",
        fail: AtomicU8::new(0),
        queries: AtomicUsize::new(0),
    });
    let catalog = HostedCatalog::new(storage.clone(), None, 1024)
        .unwrap()
        .with_embedding(Some(provider.clone()));
    let scope = sanctum_core::Scope {
        tenant: "default".parse().unwrap(),
        collection: "notes".parse().unwrap(),
    };
    catalog.create_collection(&scope, None).await.unwrap();
    let wire_scope = v1::Scope {
        tenant: "default".into(),
        collection: "notes".into(),
    };
    let header = v1::UploadTextHeader {
        scope: Some(wire_scope.clone()),
        name: "notes.txt".into(),
        ..Default::default()
    };
    let mut server = start(catalog.clone()).await;
    let body = [b"unicorn field notes".as_slice()];
    let first = server
        .client
        .submit_text(upload_frames(&header, body))
        .await
        .unwrap()
        .into_inner();
    let operation = first.operation_id.parse().unwrap();
    catalog.register_upload(&scope, operation).await.unwrap();
    catalog.index_upload(&scope, operation).await.unwrap();
    let hybrid = v1::SearchTextRequest {
        scope: Some(wire_scope.clone()),
        query: "unicorn".into(),
        mode: v1::SearchMode::Hybrid.into(),
        ..Default::default()
    };
    let pending = server
        .client
        .search_text(hybrid.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(pending.mode(), v1::SearchMode::Lexical);
    assert_eq!(
        pending.hits[0].embedding_state(),
        v1::ChunkEmbeddingState::Pending
    );
    assert_eq!(
        pending.degraded_reason.as_deref(),
        Some("embeddings_unavailable")
    );
    assert_eq!(catalog.embed_collection(&scope).await.unwrap(), 1);
    assert_eq!(catalog.embed_collection(&scope).await.unwrap(), 0);

    let ready = server
        .client
        .get_embedding_status(v1::EmbeddingStatusRequest {
            scope: Some(wire_scope.clone()),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .coverage
        .unwrap();
    assert_eq!((ready.total_chunks, ready.embedded_chunks), (1, 1));
    assert_eq!(ready.profile_id.as_deref(), Some("fixture-v1"));
    let semantic = v1::SearchTextRequest {
        query: "imaginary horse".into(),
        mode: v1::SearchMode::Semantic.into(),
        ..hybrid.clone()
    };
    let result = server
        .client
        .search_text(semantic.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(result.mode(), v1::SearchMode::Semantic);
    assert_eq!(result.hits[0].excerpt, "unicorn field notes");
    provider.fail.store(1, Ordering::Relaxed);
    let fallback = server
        .client
        .search_text(hybrid.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fallback.mode(), v1::SearchMode::Lexical);
    assert_eq!(
        fallback.degraded_reason.as_deref(),
        Some("embedding_provider_unavailable")
    );
    assert_eq!(fallback.hits.len(), 1);
    assert_eq!(
        fallback.hits[0].embedding_state(),
        v1::ChunkEmbeddingState::Ready
    );

    let replacement = v1::UploadTextHeader {
        replacement: Some(v1::Replacement {
            source_id: Some(first.source_id),
            expected_revision_id: Some(first.revision_id),
        }),
        ..header
    };
    let second = server
        .client
        .submit_text(upload_frames(&replacement, body))
        .await
        .unwrap()
        .into_inner();
    let operation = second.operation_id.parse().unwrap();
    catalog.register_upload(&scope, operation).await.unwrap();
    catalog.index_upload(&scope, operation).await.unwrap();
    assert_eq!(catalog.embed_collection(&scope).await.unwrap(), 0);
    server.stop().await;

    let catalog = HostedCatalog::new(storage, None, 1024)
        .unwrap()
        .with_embedding(Some(Arc::new(HostedEmbeddingFixture {
            profile: "fixture-v2",
            fail: AtomicU8::new(0),
            queries: AtomicUsize::new(0),
        })));
    let mut server = start(catalog.clone()).await;
    let changed = server
        .client
        .search_text(hybrid)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(changed.mode(), v1::SearchMode::Lexical);
    assert_eq!(
        changed.hits[0].embedding_state(),
        v1::ChunkEmbeddingState::Pending
    );
    let coverage = changed.embedding_coverage.unwrap();
    assert_eq!(coverage.profile_id.as_deref(), Some("fixture-v2"));
    assert_eq!(coverage.embedded_chunks, 0);
    assert_eq!(catalog.embed_collection(&scope).await.unwrap(), 1);
    let rebuilt = server
        .client
        .search_text(semantic)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(rebuilt.mode(), v1::SearchMode::Semantic);
    assert_eq!(
        rebuilt.hits[0].chunk.as_ref().unwrap().revision_id,
        second.revision_id
    );
    server.stop().await;
}

#[tokio::test]
async fn hosted_collection_search_uses_one_query_vector_and_keeps_hits_scoped() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Arc::new(sanctum_storage_fs::FileStorage::open(directory.path()).unwrap());
    let provider = Arc::new(HostedEmbeddingFixture {
        profile: "fixture-v1",
        fail: AtomicU8::new(0),
        queries: AtomicUsize::new(0),
    });
    let service = HostedCatalog::new(storage, None, 1024)
        .unwrap()
        .with_embedding(Some(provider.clone()));
    let mut server = start(service.clone()).await;
    for (tenant, collection, text) in [
        ("default", "first", "shared first 🦀"),
        ("default", "second", "shared second café"),
        ("other", "private", "shared private"),
    ] {
        let scope = sanctum_core::Scope {
            tenant: tenant.parse().unwrap(),
            collection: collection.parse().unwrap(),
        };
        service.create_collection(&scope, None).await.unwrap();
        let header = v1::UploadTextHeader {
            scope: Some(v1::Scope {
                tenant: tenant.into(),
                collection: collection.into(),
            }),
            name: "notes.txt".into(),
            ..Default::default()
        };
        let accepted = server
            .client
            .submit_text(upload_frames(&header, [text.as_bytes()]))
            .await
            .unwrap()
            .into_inner();
        let operation = accepted.operation_id.parse().unwrap();
        service.register_upload(&scope, operation).await.unwrap();
        service.index_upload(&scope, operation).await.unwrap();
        if collection == "first" {
            assert_eq!(service.embed_collection(&scope).await.unwrap(), 1);
        }
    }
    let query = v1::SearchCollectionsRequest {
        tenant: "default".into(),
        collections: vec!["second".into(), "first".into()],
        query: "shared".into(),
        mode: v1::SearchMode::Hybrid.into(),
        ..Default::default()
    };
    let result = server
        .client
        .search_collections(query.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(provider.queries.load(Ordering::Relaxed), 1);
    assert_eq!(result.mode(), v1::SearchMode::Hybrid);
    assert_eq!(
        result.degraded_reason.as_deref(),
        Some("embedding_coverage_partial")
    );
    let coverage = result.embedding_coverage.unwrap();
    assert_eq!((coverage.total_chunks, coverage.embedded_chunks), (2, 1));
    let hits: Vec<_> = result.hits.iter().map(|hit| hit.excerpt.as_str()).collect();
    assert_eq!(hits, ["shared first 🦀", "shared second café"]);
    assert_eq!(
        result.hits[0].embedding_state(),
        v1::ChunkEmbeddingState::Ready
    );
    assert_eq!(
        result.hits[1].embedding_state(),
        v1::ChunkEmbeddingState::Pending
    );
    for hit in result.hits {
        let reference = hit.chunk.unwrap();
        assert_eq!(reference.scope.as_ref().unwrap().tenant, "default");
        let fetched = server
            .client
            .fetch_chunk(v1::FetchChunkRequest {
                chunk: Some(reference.clone()),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(fetched.excerpt, hit.excerpt);
        assert_eq!(fetched.database_generation, hit.database_generation);
        assert_eq!(fetched.embedding_state, hit.embedding_state);
        let context = server
            .client
            .fetch_context(v1::FetchContextRequest {
                chunk: Some(reference),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(context.hit.unwrap().embedding_state, hit.embedding_state);
    }
    let wrong_tenant = v1::SearchCollectionsRequest {
        collections: vec!["private".into()],
        ..query.clone()
    };
    assert_eq!(
        server
            .client
            .search_collections(wrong_tenant)
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    let duplicate = v1::SearchCollectionsRequest {
        collections: vec!["first".into(), "first".into()],
        ..query
    };
    assert_eq!(
        server
            .client
            .search_collections(duplicate)
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    server.stop().await;
}

struct HostedEmbeddingFixture {
    profile: &'static str,
    fail: AtomicU8,
    queries: AtomicUsize,
}

#[async_trait::async_trait]
impl sanctum_embedding::Provider for HostedEmbeddingFixture {
    fn profile_id(&self) -> &str {
        self.profile
    }

    fn dimensions(&self) -> usize {
        2
    }

    async fn embed(
        &self,
        purpose: sanctum_embedding::Purpose,
        texts: &[String],
    ) -> Result<Vec<Vec<f32>>, sanctum_embedding::EmbedError> {
        if matches!(purpose, sanctum_embedding::Purpose::Query) {
            self.queries.fetch_add(1, Ordering::Relaxed);
        }
        if self.fail.load(Ordering::Relaxed) != 0 {
            return Err(sanctum_embedding::EmbedError::Transient(
                "fixture outage".into(),
            ));
        }
        Ok(texts.iter().map(|_| vec![1.0, 0.0]).collect())
    }
}

#[tokio::test]
async fn hosted_manual_retry_restores_a_dispatch_after_restart() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Arc::new(InterruptAcceptance {
        inner: sanctum_storage_fs::FileStorage::open(directory.path()).unwrap(),
        interrupt: AtomicU8::new(0),
    });
    let dispatch = Arc::new(RetryDispatch::default());
    let catalog = || {
        HostedCatalog::new(storage.clone(), None, 1024)
            .unwrap()
            .with_dispatch(dispatch.clone())
    };
    let scope = sanctum_core::Scope {
        tenant: "default".parse().unwrap(),
        collection: "notes".parse().unwrap(),
    };
    let wire_scope = v1::Scope {
        tenant: "default".into(),
        collection: "notes".into(),
    };
    let service = catalog();
    service.create_collection(&scope, None).await.unwrap();
    let mut server = start(service.clone()).await;
    let operation = uuid::Uuid::from_u128(123);
    let header = v1::UploadTextHeader {
        scope: Some(wire_scope.clone()),
        name: "retry.txt".into(),
        request_id: Some(operation.to_string()),
        ..Default::default()
    };
    let accepted = server
        .client
        .submit_text(upload_frames(&header, [b"quartz".as_slice()]))
        .await
        .unwrap()
        .into_inner();
    let request = v1::IngestionJobRequest {
        scope: Some(wire_scope),
        operation_id: operation.to_string(),
    };
    assert_eq!(
        &*dispatch.delivered.lock().unwrap(),
        &[(TaskStage::Register, 0)]
    );

    storage.interrupt.store(2, Ordering::Relaxed);
    assert_eq!(
        server
            .client
            .retry_ingestion_job(request.clone())
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    server.stop().await;
    let restarted = catalog();
    let report = restarted.reconcile_shard(0).await.unwrap();
    assert_eq!((report.dispatched, report.failed), (1, 0));
    assert_eq!(
        &*dispatch.delivered.lock().unwrap(),
        &[(TaskStage::Register, 0), (TaskStage::Register, 1)]
    );
    let mut server = start(restarted.clone()).await;
    restarted.register_upload(&scope, operation).await.unwrap();
    restarted.try_dispatch(&scope, operation).await;
    let retried = server
        .client
        .retry_ingestion_job(request.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(retried.state(), v1::IngestionState::Registered);
    assert_eq!(retried.operation_id, accepted.operation_id);
    assert_eq!(retried.source_id, accepted.source_id);
    assert_eq!(retried.revision_id, accepted.revision_id);
    assert_eq!(
        &*dispatch.delivered.lock().unwrap(),
        &[
            (TaskStage::Register, 0),
            (TaskStage::Register, 1),
            (TaskStage::Index, 1),
            (TaskStage::Index, 2),
        ]
    );
    restarted.index_upload(&scope, operation).await.unwrap();
    assert_eq!(
        server
            .client
            .retry_ingestion_job(request)
            .await
            .unwrap_err()
            .code(),
        Code::AlreadyExists
    );
    server.stop().await;
}

#[derive(Default)]
struct RetryDispatch {
    delivered: std::sync::Mutex<Vec<(TaskStage, u32)>>,
}

#[async_trait::async_trait]
impl sanctum_service::Dispatch for RetryDispatch {
    async fn send(&self, task: sanctum_service::Task<'_>) -> StorageResult<()> {
        self.delivered
            .lock()
            .unwrap()
            .push((task.stage, task.retry));
        Ok(())
    }
}

#[tokio::test]
async fn dispatch_reconciliation_recovers_a_failed_handoff_and_advances_its_cursor() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Arc::new(sanctum_storage_fs::FileStorage::open(directory.path()).unwrap());
    let dispatch = Arc::new(TestDispatch {
        fail: AtomicU8::new(1),
        delivered: std::sync::Mutex::new(Vec::new()),
    });
    let service = HostedCatalog::new(storage.clone(), None, 1024)
        .unwrap()
        .with_dispatch(dispatch.clone());
    let scope = sanctum_core::Scope {
        tenant: "default".parse().unwrap(),
        collection: "notes".parse().unwrap(),
    };
    service.create_collection(&scope, None).await.unwrap();
    let mut server = start(service.clone()).await;
    let operation = uuid::Uuid::from_u128(100);
    let header = v1::UploadTextHeader {
        scope: Some(v1::Scope {
            tenant: scope.tenant.to_string(),
            collection: scope.collection.to_string(),
        }),
        name: "recover.txt".into(),
        request_id: Some(operation.to_string()),
        ..Default::default()
    };
    let accepted = server
        .client
        .submit_text(upload_frames(&header, [b"recovered".as_slice()]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(accepted.state(), v1::IngestionState::Accepted);
    assert!(dispatch.delivered.lock().unwrap().is_empty());
    server.stop().await;

    // The first page consists of hints left before the corresponding envelopes existed.
    for i in 1..=16 {
        let key = Key::parse(format!(
            "_dispatch/00/pending/default/notes/{}",
            uuid::Uuid::from_u128(i)
        ))
        .unwrap();
        storage
            .put_control(&key, "{}".as_bytes().into(), Condition::Create)
            .await
            .unwrap();
    }
    let first = service.reconcile_shard(0).await.unwrap();
    assert_eq!((first.dispatched, first.failed), (0, 0));
    assert!(first.next_cursor.is_some());
    let restarted = HostedCatalog::new(storage.clone(), None, 1024)
        .unwrap()
        .with_dispatch(dispatch.clone());
    let second = restarted.reconcile_shard(0).await.unwrap();
    assert_eq!((second.dispatched, second.failed), (1, 0));
    assert!(second.next_cursor.is_none());
    assert_eq!(
        &*dispatch.delivered.lock().unwrap(),
        &[(operation, TaskStage::Register)]
    );
    restarted.try_dispatch(&scope, operation).await;
    assert_eq!(dispatch.delivered.lock().unwrap().len(), 1);

    restarted.register_upload(&scope, operation).await.unwrap();
    restarted.try_dispatch(&scope, operation).await;
    assert_eq!(
        &*dispatch.delivered.lock().unwrap(),
        &[
            (operation, TaskStage::Register),
            (operation, TaskStage::Index)
        ]
    );
    restarted.index_upload(&scope, operation).await.unwrap();
    restarted.try_dispatch(&scope, operation).await;
    let key = Key::parse(format!("_dispatch/00/pending/default/notes/{operation}")).unwrap();
    assert!(matches!(storage.head(&key).await, Err(Error::NotFound(_))));
}

#[tokio::test]
async fn embedding_task_checkpoint_replays_a_committed_batch_without_republishing() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Arc::new(InterruptAcceptance {
        inner: sanctum_storage_fs::FileStorage::open(directory.path()).unwrap(),
        interrupt: AtomicU8::new(0),
    });
    let dispatch = Arc::new(TestDispatch {
        fail: AtomicU8::new(0),
        delivered: std::sync::Mutex::new(Vec::new()),
    });
    let provider = Arc::new(HostedEmbeddingFixture {
        profile: "fixture-v1",
        fail: AtomicU8::new(0),
        queries: AtomicUsize::new(0),
    });
    let service = HostedCatalog::new(storage.clone(), None, 1024)
        .unwrap()
        .with_dispatch(dispatch.clone())
        .with_embedding(Some(provider.clone()));
    let scope = sanctum_core::Scope {
        tenant: "default".parse().unwrap(),
        collection: "notes".parse().unwrap(),
    };
    service.create_collection(&scope, None).await.unwrap();
    let mut server = start(service.clone()).await;
    let operation = uuid::Uuid::from_u128(101);
    let header = v1::UploadTextHeader {
        scope: Some(v1::Scope {
            tenant: scope.tenant.to_string(),
            collection: scope.collection.to_string(),
        }),
        name: "embed.txt".into(),
        request_id: Some(operation.to_string()),
        ..Default::default()
    };
    server
        .client
        .submit_text(upload_frames(&header, [b"embed me".as_slice()]))
        .await
        .unwrap();
    service.register_upload(&scope, operation).await.unwrap();
    service.index_upload(&scope, operation).await.unwrap();
    service.try_dispatch(&scope, operation).await;
    assert_eq!(
        dispatch.delivered.lock().unwrap().last(),
        Some(&(operation, TaskStage::Embed { step: 0 }))
    );
    assert!(matches!(
        service.embed_upload(&scope, operation, 1).await,
        Err(Error::Busy(_))
    ));

    storage.interrupt.store(1, Ordering::Relaxed);
    assert!(matches!(
        service.embed_upload(&scope, operation, 0).await,
        Err(Error::Busy(_))
    ));
    assert_eq!(
        service
            .embedding_status(&scope, None)
            .await
            .unwrap()
            .coverage
            .embedded,
        1
    );
    let executor =
        sanctum_db::Publication::new(storage.clone(), std::time::Duration::from_secs(15)).unwrap();
    let unit = sanctum_db::Unit::collection(None, &scope).unwrap();
    let snapshot = executor.generation(&unit).await.unwrap();
    server.stop().await;

    provider.fail.store(1, Ordering::Relaxed);
    let restarted = HostedCatalog::new(storage.clone(), None, 1024)
        .unwrap()
        .with_dispatch(dispatch.clone())
        .with_embedding(Some(provider));
    restarted.embed_upload(&scope, operation, 0).await.unwrap();
    assert_eq!(executor.generation(&unit).await.unwrap(), snapshot);
    restarted.reconcile_shard(0).await.unwrap();
    assert_eq!(
        dispatch.delivered.lock().unwrap().last(),
        Some(&(operation, TaskStage::Embed { step: 1 }))
    );
    restarted.embed_upload(&scope, operation, 0).await.unwrap();
    restarted.embed_upload(&scope, operation, 1).await.unwrap();
    restarted.try_dispatch(&scope, operation).await;
    let key = Key::parse(format!("_dispatch/00/pending/default/notes/{operation}")).unwrap();
    assert!(matches!(storage.head(&key).await, Err(Error::NotFound(_))));
}

#[tokio::test]
async fn idle_collections_rebuild_after_a_profile_change() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Arc::new(sanctum_storage_fs::FileStorage::open(directory.path()).unwrap());
    let dispatch = Arc::new(TestDispatch {
        fail: AtomicU8::new(0),
        delivered: std::sync::Mutex::new(Vec::new()),
    });
    let catalog = |profile| {
        HostedCatalog::new(storage.clone(), None, 1024)
            .unwrap()
            .with_dispatch(dispatch.clone())
            .with_embedding(Some(Arc::new(HostedEmbeddingFixture {
                profile,
                fail: AtomicU8::new(0),
                queries: AtomicUsize::new(0),
            })))
    };
    let first = catalog("fixture-v1");
    let scope = sanctum_core::Scope {
        tenant: "default".parse().unwrap(),
        collection: "notes".parse().unwrap(),
    };
    first.create_collection(&scope, None).await.unwrap();
    let mut server = start(first.clone()).await;
    let header = v1::UploadTextHeader {
        scope: Some(v1::Scope {
            tenant: "default".into(),
            collection: "notes".into(),
        }),
        name: "idle.txt".into(),
        ..Default::default()
    };
    let accepted = server
        .client
        .submit_text(upload_frames(&header, [b"idle unicorn".as_slice()]))
        .await
        .unwrap()
        .into_inner();
    let upload = accepted.operation_id.parse().unwrap();
    first.register_upload(&scope, upload).await.unwrap();
    first.index_upload(&scope, upload).await.unwrap();
    first.embed_upload(&scope, upload, 0).await.unwrap();
    first.embed_upload(&scope, upload, 1).await.unwrap();
    first.try_dispatch(&scope, upload).await;
    assert_eq!(
        first
            .embedding_status(&scope, None)
            .await
            .unwrap()
            .coverage
            .embedded,
        1
    );
    assert!(first.discover_profile_rebuild().await.unwrap().complete);
    server.stop().await;

    let second = catalog("fixture-v2");
    assert_eq!(
        second
            .embedding_status(&scope, None)
            .await
            .unwrap()
            .coverage
            .embedded,
        0
    );
    // Force a second discovery page and verify that a new instance resumes it.
    for i in 0..256 {
        let key = Key::parse(format!("_fixture/{i:03}")).unwrap();
        storage
            .put_control(&key, b"{}".as_slice().into(), Condition::Create)
            .await
            .unwrap();
    }
    assert!(!second.discover_profile_rebuild().await.unwrap().complete);
    drop(second);
    let second = catalog("fixture-v2");
    assert!(second.discover_profile_rebuild().await.unwrap().complete);
    for shard in 0..16 {
        second.reconcile_shard(shard).await.unwrap();
    }
    let operation = dispatch
        .delivered
        .lock()
        .unwrap()
        .iter()
        .find_map(|&(operation, stage)| {
            matches!(stage, TaskStage::Rebuild { step: 0 }).then_some(operation)
        })
        .unwrap();
    assert!(matches!(
        first.run_profile_rebuild(&scope, operation, 0).await,
        Err(Error::Busy(_))
    ));
    assert!(matches!(
        second.run_profile_rebuild(&scope, operation, 1).await,
        Err(Error::Busy(_))
    ));
    second
        .run_profile_rebuild(&scope, operation, 0)
        .await
        .unwrap();
    let coverage = second.embedding_status(&scope, None).await.unwrap();
    assert_eq!(coverage.profile.as_deref(), Some("fixture-v2"));
    assert_eq!(coverage.coverage.embedded, 1);
    second.try_dispatch_rebuild(&scope, operation).await;
    assert!(
        dispatch
            .delivered
            .lock()
            .unwrap()
            .contains(&(operation, TaskStage::Rebuild { step: 1 }))
    );
    second
        .run_profile_rebuild(&scope, operation, 0)
        .await
        .unwrap();
    second
        .run_profile_rebuild(&scope, operation, 1)
        .await
        .unwrap();
    second.try_dispatch_rebuild(&scope, operation).await;
    assert_eq!(
        second.discover_profile_rebuild().await.unwrap().discovered,
        0
    );
    let pending = Key::parse("_dispatch").unwrap();
    assert!(
        !storage
            .list(Some(&pending), None, 128)
            .await
            .unwrap()
            .objects
            .iter()
            .any(|item| item.key.as_str().ends_with(&operation.to_string()))
    );
}

struct TestDispatch {
    fail: AtomicU8,
    delivered: std::sync::Mutex<Vec<(uuid::Uuid, TaskStage)>>,
}

#[async_trait::async_trait]
impl sanctum_service::Dispatch for TestDispatch {
    async fn send(&self, task: sanctum_service::Task<'_>) -> StorageResult<()> {
        if self.fail.swap(0, Ordering::Relaxed) != 0 {
            return Err(Error::Busy("dispatch interrupted".into()));
        }
        self.delivered
            .lock()
            .unwrap()
            .push((task.operation, task.stage));
        Ok(())
    }
}

#[tokio::test]
async fn hosted_change_stream_resumes_committed_events_across_instances() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Arc::new(sanctum_storage_fs::FileStorage::open(directory.path()).unwrap());
    let catalog = || HostedCatalog::new(storage.clone(), None, 1024).unwrap();
    let writer = catalog();
    let scope = sanctum_core::Scope {
        tenant: "default".parse().unwrap(),
        collection: "notes".parse().unwrap(),
    };
    writer.create_collection(&scope, None).await.unwrap();
    writer
        .create_collection(
            &sanctum_core::Scope {
                tenant: "other".parse().unwrap(),
                collection: scope.collection.clone(),
            },
            None,
        )
        .await
        .unwrap();
    let wire_scope = v1::Scope {
        tenant: "default".into(),
        collection: "notes".into(),
    };
    let cursor_request = v1::ChangeCursorRequest {
        scope: Some(wire_scope.clone()),
    };
    let mut api = start(catalog()).await;
    assert_eq!(
        api.client
            .get_change_cursor(cursor_request.clone())
            .await
            .unwrap()
            .into_inner()
            .cursor,
        0
    );

    let mut page = v1::ListChangesRequest {
        scope: Some(wire_scope.clone()),
        after_cursor: 0,
        limit: Some(1),
    };
    let mut stream = api
        .client
        .watch_changes(page.clone())
        .await
        .unwrap()
        .into_inner();
    assert!(stream.message().await.unwrap().unwrap().changes.is_empty());

    let header = v1::UploadTextHeader {
        scope: Some(wire_scope.clone()),
        name: "notes.txt".into(),
        ..Default::default()
    };
    let accepted = api
        .client
        .submit_text(upload_frames(&header, [b"first note".as_slice()]))
        .await
        .unwrap()
        .into_inner();
    let operation = accepted.operation_id.parse().unwrap();
    writer.register_upload(&scope, operation).await.unwrap();
    let pending = api
        .client
        .list_changes(page.clone())
        .await
        .unwrap()
        .into_inner();
    assert!(pending.changes.is_empty());
    writer.index_upload(&scope, operation).await.unwrap();
    let created = tokio::time::timeout(std::time::Duration::from_secs(8), stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(created.changes[0].kind, "source_created");
    page.after_cursor = created.next_cursor;
    drop(stream);
    api.stop().await;

    let mut api = start(catalog()).await;
    let replacement = v1::UploadTextHeader {
        replacement: Some(v1::Replacement {
            source_id: Some(accepted.source_id.clone()),
            expected_revision_id: Some(accepted.revision_id.clone()),
        }),
        ..header
    };
    let accepted = api
        .client
        .submit_text(upload_frames(&replacement, [b"new note".as_slice()]))
        .await
        .unwrap()
        .into_inner();
    let operation = accepted.operation_id.parse().unwrap();
    writer.register_upload(&scope, operation).await.unwrap();
    writer.index_upload(&scope, operation).await.unwrap();
    let mut resumed = api
        .client
        .watch_changes(page.clone())
        .await
        .unwrap()
        .into_inner();
    let replaced = resumed.message().await.unwrap().unwrap();
    let reset = resumed.message().await.unwrap().unwrap();
    assert_eq!(replaced.changes[0].kind, "source_replaced");
    assert_eq!(reset.changes[0].kind, "metadata_reset");
    assert_eq!(reset.changes[0].revision_id, accepted.revision_id);
    assert_eq!(reset.next_cursor, replaced.next_cursor + 1);
    assert_eq!(
        api.client
            .get_change_cursor(cursor_request)
            .await
            .unwrap()
            .into_inner()
            .cursor,
        reset.next_cursor
    );
    let other = v1::ListChangesRequest {
        scope: Some(v1::Scope {
            tenant: "other".into(),
            ..wire_scope
        }),
        ..page.clone()
    };
    assert_eq!(
        api.client.list_changes(other).await.unwrap_err().code(),
        Code::InvalidArgument
    );
    page.after_cursor = reset.next_cursor;
    assert!(
        api.client
            .list_changes(page)
            .await
            .unwrap()
            .into_inner()
            .changes
            .is_empty()
    );
    drop(resumed);
    api.stop().await;
}

#[tokio::test]
async fn hosted_metadata_is_atomic_and_filters_published_revisions() {
    use v1::{
        MetadataLevel, MetadataOperator, metadata_filter::Expression, metadata_mutation::Edit,
        query_metadata_request::View,
    };

    let directory = tempfile::tempdir().unwrap();
    let storage = Arc::new(sanctum_storage_fs::FileStorage::open(directory.path()).unwrap());
    let service = HostedCatalog::new(storage.clone(), None, 1024).unwrap();
    let mut server = start(service.clone()).await;
    let scope = sanctum_core::Scope {
        tenant: "default".parse().unwrap(),
        collection: "notes".parse().unwrap(),
    };
    service.create_collection(&scope, None).await.unwrap();
    let collection = v1::MetadataTarget {
        scope: Some(v1::Scope {
            tenant: "default".into(),
            collection: "notes".into(),
        }),
        ..Default::default()
    };
    let edit = |key: &str, value: Edit| v1::MetadataMutation {
        namespace: "integration".into(),
        key: key.into(),
        edit: Some(value),
    };
    let change = |target: &v1::MetadataTarget, edits| v1::MutateMetadataRequest {
        target: Some(target.clone()),
        edits,
    };
    let header = v1::UploadTextHeader {
        scope: collection.scope.clone(),
        name: "notes.txt".into(),
        ..Default::default()
    };
    let accepted = server
        .client
        .submit_text(upload_frames(&header, [b"quartz notes".as_slice()]))
        .await
        .unwrap()
        .into_inner();
    let operation = accepted.operation_id.parse().unwrap();
    service.register_upload(&scope, operation).await.unwrap();
    service.index_upload(&scope, operation).await.unwrap();
    let source = v1::MetadataTarget {
        source_id: Some(accepted.source_id.clone()),
        revision_id: Some(accepted.revision_id.clone()),
        ..collection.clone()
    };
    let chunk = v1::MetadataTarget {
        chunk_ordinal: Some(0),
        ..source.clone()
    };
    for (target, value) in [
        (&collection, "true"),
        (&source, r#"{"score":7}"#),
        (&chunk, "null"),
    ] {
        server
            .client
            .mutate_metadata(change(
                target,
                vec![edit("status", Edit::SetJson(value.into()))],
            ))
            .await
            .unwrap();
    }
    let invalid = change(
        &source,
        vec![
            edit("status", Edit::SetJson("false".into())),
            edit("", Edit::SetJson("true".into())),
        ],
    );
    assert_eq!(
        server
            .client
            .mutate_metadata(invalid)
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );

    server.stop().await;
    let mut server = start(HostedCatalog::new(storage, None, 1024).unwrap()).await;
    let entries = server
        .client
        .get_metadata(source.clone())
        .await
        .unwrap()
        .into_inner()
        .entries;
    assert_eq!(entries[0].value_json, r#"{"score":7}"#);
    let patched = server
        .client
        .mutate_metadata(change(
            &source,
            vec![edit(
                "status",
                Edit::MergePatchJson(r#"{"score":8,"extra":true}"#.into()),
            )],
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(patched.entries[0].value_json, r#"{"extra":true,"score":8}"#);

    let filter = v1::MetadataFilter {
        expression: Some(Expression::Test(v1::MetadataPredicate {
            level: MetadataLevel::Collection.into(),
            namespace: "integration".into(),
            key: "status".into(),
            operator: MetadataOperator::Equal.into(),
            operand_json: Some("true".into()),
            ..Default::default()
        })),
    };
    let hit = server
        .client
        .search_text(v1::SearchTextRequest {
            scope: collection.scope.clone(),
            query: "quartz".into(),
            metadata_filter: Some(filter.clone()),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .hits
        .remove(0);
    assert_eq!(hit.representation, "canonical_utf8");
    assert_eq!(hit.metadata.len(), 1);
    assert_eq!(hit.metadata[0].level, MetadataLevel::Collection as i32);
    assert_eq!(hit.metadata[0].entry.as_ref().unwrap().value_json, "true");
    assert!(!hit.metadata_omitted);
    let large = serde_json::to_string(&"x".repeat(4096)).unwrap();
    server
        .client
        .mutate_metadata(change(&source, vec![edit("large", Edit::SetJson(large))]))
        .await
        .unwrap();
    let hit = server
        .client
        .search_text(v1::SearchTextRequest {
            scope: collection.scope.clone(),
            query: "quartz".into(),
            metadata_filter: Some(v1::MetadataFilter {
                expression: Some(Expression::Test(v1::MetadataPredicate {
                    level: MetadataLevel::Source.into(),
                    namespace: "integration".into(),
                    key: "large".into(),
                    operator: MetadataOperator::Exists.into(),
                    ..Default::default()
                })),
            }),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .hits
        .remove(0);
    assert!(hit.metadata_omitted && hit.metadata.is_empty());

    let grouped = v1::QueryMetadataRequest {
        tenant: "default".into(),
        collection: Some("notes".into()),
        level: MetadataLevel::Chunk.into(),
        filter: Some(filter.clone()),
        view: Some(View::Count(v1::MetadataCount {
            group_by: Some(v1::MetadataField {
                level: MetadataLevel::Chunk.into(),
                namespace: "integration".into(),
                key: "status".into(),
                ..Default::default()
            }),
        })),
        ..Default::default()
    };
    let groups = server
        .client
        .query_metadata(grouped)
        .await
        .unwrap()
        .into_inner()
        .groups;
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].count, 1);
    assert_eq!(groups[0].value_json.as_deref(), Some("null"));

    for (tenant, expected) in [("default", 1), ("other", 0)] {
        let result = server
            .client
            .query_metadata(v1::QueryMetadataRequest {
                tenant: tenant.into(),
                level: MetadataLevel::Collection.into(),
                filter: Some(filter.clone()),
                view: Some(View::Count(v1::MetadataCount { group_by: None })),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(result.groups[0].count, expected);
    }

    let replacement = v1::UploadTextHeader {
        replacement: Some(v1::Replacement {
            source_id: Some(accepted.source_id.clone()),
            expected_revision_id: Some(accepted.revision_id.clone()),
        }),
        ..header
    };
    let updated = server
        .client
        .submit_text(upload_frames(&replacement, [b"new quartz".as_slice()]))
        .await
        .unwrap()
        .into_inner();
    let operation = updated.operation_id.parse().unwrap();
    service.register_upload(&scope, operation).await.unwrap();
    service.index_upload(&scope, operation).await.unwrap();
    assert_eq!(
        server
            .client
            .get_metadata(source.clone())
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        server
            .client
            .mutate_metadata(change(
                &chunk,
                vec![edit("status", Edit::Remove(v1::Empty {}))]
            ))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    let new_source = v1::MetadataTarget {
        revision_id: Some(updated.revision_id),
        ..source
    };
    assert!(
        server
            .client
            .get_metadata(new_source)
            .await
            .unwrap()
            .into_inner()
            .entries
            .is_empty()
    );
    assert_eq!(
        server
            .client
            .get_metadata(collection)
            .await
            .unwrap()
            .into_inner()
            .entries[0]
            .value_json,
        "true"
    );
    server.stop().await;
}

#[tokio::test]
async fn hosted_deletion_replays_and_defers_object_reclamation() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Arc::new(sanctum_storage_fs::FileStorage::open(directory.path()).unwrap());
    let service = HostedCatalog::new(storage.clone(), None, 1024).unwrap();
    let mut server = start(service.clone()).await;
    let scope = sanctum_core::Scope {
        tenant: "default".parse().unwrap(),
        collection: "notes".parse().unwrap(),
    };
    service.create_collection(&scope, None).await.unwrap();
    let wire_scope = v1::Scope {
        tenant: "default".into(),
        collection: "notes".into(),
    };
    let header = v1::UploadTextHeader {
        scope: Some(wire_scope.clone()),
        name: "notes.txt".into(),
        ..Default::default()
    };
    let accepted = server
        .client
        .submit_text(upload_frames(&header, [b"obsolete note".as_slice()]))
        .await
        .unwrap()
        .into_inner();
    let operation = accepted.operation_id.parse().unwrap();
    service.register_upload(&scope, operation).await.unwrap();
    service.index_upload(&scope, operation).await.unwrap();
    let query = v1::SearchTextRequest {
        scope: Some(wire_scope.clone()),
        query: "obsolete".into(),
        ..Default::default()
    };
    let found = server
        .client
        .search_text(query.clone())
        .await
        .unwrap()
        .into_inner();
    let hit = found.hits[0].chunk.clone().unwrap();

    let replacement = v1::UploadTextHeader {
        replacement: Some(v1::Replacement {
            source_id: Some(accepted.source_id.clone()),
            expected_revision_id: Some(accepted.revision_id.clone()),
        }),
        ..header
    };
    let pending = server
        .client
        .submit_text(upload_frames(&replacement, [b"late note".as_slice()]))
        .await
        .unwrap()
        .into_inner();
    let pending = pending.operation_id.parse().unwrap();
    service.register_upload(&scope, pending).await.unwrap();

    let delete = v1::DeleteSourceRequest {
        scope: Some(wire_scope),
        source_id: accepted.source_id.clone(),
        expected_revision_id: Some(accepted.revision_id.clone()),
        request_id: Some(uuid::Uuid::new_v4().to_string()),
    };
    let stale = v1::DeleteSourceRequest {
        expected_revision_id: Some(uuid::Uuid::new_v4().to_string()),
        ..delete.clone()
    };
    assert_eq!(
        server.client.delete_source(stale).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    let removed = server
        .client
        .delete_source(delete.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(removed.revision_id, accepted.revision_id);
    assert_eq!(
        server
            .client
            .delete_source(delete.clone())
            .await
            .unwrap()
            .into_inner(),
        removed
    );
    let reused = v1::DeleteSourceRequest {
        expected_revision_id: None,
        ..delete
    };
    assert_eq!(
        server
            .client
            .delete_source(reused)
            .await
            .unwrap_err()
            .code(),
        Code::AlreadyExists
    );
    assert!(
        server
            .client
            .search_text(query)
            .await
            .unwrap()
            .into_inner()
            .hits
            .is_empty()
    );
    assert_eq!(
        server
            .client
            .fetch_chunk(v1::FetchChunkRequest { chunk: Some(hit) })
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    assert!(
        matches!(service.index_upload(&scope, pending).await.unwrap().state,
        sanctum_service::HostedJobState::Failed { code, .. } if code == "stale_revision")
    );

    let executor =
        sanctum_db::Publication::new(storage.clone(), std::time::Duration::from_secs(15)).unwrap();
    let unit = sanctum_db::Unit::collection(None, &scope).unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let snapshot = executor.read(&unit, scratch.path()).await.unwrap().unwrap();
    let db = rusqlite::Connection::open(snapshot.path()).unwrap();
    let key: String = db
        .query_row("SELECT text_key FROM hosted_revision_objects", [], |row| {
            row.get(0)
        })
        .unwrap();
    let key = Key::parse(key).unwrap();
    assert!(storage.head(&key).await.is_ok());
    assert_eq!(service.reclaim_hosted_sources(&scope).await.unwrap(), 0);

    let age = async |sql: &'static str| {
        let mut writer = executor.acquire(&unit).await.unwrap();
        writer
            .apply(
                vec![sanctum_db::Command {
                    operation: uuid::Uuid::new_v4(),
                    payload: sql,
                }],
                |tx, sql| {
                    tx.execute(sql, [])
                        .map_err(|error| Error::Internal(Box::new(error)))?;
                    Ok("aged".into())
                },
            )
            .await
            .unwrap();
        writer.release().await.unwrap();
    };
    age("UPDATE revisions SET retire_at = unixepoch() - 1").await;
    assert_eq!(service.reclaim_hosted_sources(&scope).await.unwrap(), 0);
    assert!(storage.head(&key).await.is_ok());
    age("UPDATE hosted_object_garbage SET delete_after = unixepoch() - 1").await;
    assert_eq!(service.sweep_hosted_sources().await.unwrap().deleted, 1);
    assert!(matches!(storage.head(&key).await, Err(Error::NotFound(_))));
    assert_eq!(service.reclaim_hosted_sources(&scope).await.unwrap(), 0);
    server.stop().await;
}

#[tokio::test]
async fn hosted_database_sessions_pin_a_scope_and_generation_across_instances() {
    use std::time::{Duration, SystemTime};
    use v1::sql_value::Value;

    let directory = tempfile::tempdir().unwrap();
    let storage = Arc::new(sanctum_storage_fs::FileStorage::open(directory.path()).unwrap());
    let catalog = || HostedCatalog::new(storage.clone(), None, 1024).unwrap();
    let writer = catalog();
    let scope = |collection: &str| sanctum_core::Scope {
        tenant: "default".parse().unwrap(),
        collection: collection.parse().unwrap(),
    };
    writer
        .create_collection(&scope("notes"), None)
        .await
        .unwrap();
    let target = v1::DatabaseTarget {
        tenant: "default".into(),
        collection: None,
    };
    let mut first = start(catalog()).await;
    let opened = first
        .client
        .open_database_session(target.clone())
        .await
        .unwrap()
        .into_inner();
    let generation: uuid::Uuid = opened.snapshot_generation.unwrap().parse().unwrap();
    let handle = opened.session.unwrap();
    assert!(
        opened
            .schema
            .iter()
            .any(|object| object.name == "collections")
    );
    first.stop().await;

    writer
        .create_collection(&scope("reference"), None)
        .await
        .unwrap();
    writer.sweep_hosted_snapshots().await.unwrap();
    let mut second = start(catalog()).await;
    let query = v1::QueryDatabaseRequest {
        session: Some(handle.clone()),
        sql: "SELECT id FROM collections WHERE id >= ?1 ORDER BY id".into(),
        parameters: vec![v1::SqlValue {
            value: Some(Value::Text("notes".into())),
        }],
        ..Default::default()
    };
    let page = second
        .client
        .query_database(query.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(page.rows.len(), 1);
    assert_eq!(
        page.rows[0].values[0].value,
        Some(Value::Text("notes".into()))
    );
    let count = v1::QueryDatabaseRequest {
        sql: "SELECT count(*) FROM collections".into(),
        parameters: Vec::new(),
        ..query.clone()
    };
    let page = second
        .client
        .query_database(count)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(page.rows[0].values[0].value, Some(Value::Integer(1)));
    let forbidden = v1::QueryDatabaseRequest {
        sql: "DELETE FROM collections".into(),
        parameters: Vec::new(),
        ..query.clone()
    };
    assert_eq!(
        second
            .client
            .query_database(forbidden)
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );

    let mut wrong_scope = handle.clone();
    wrong_scope.target.as_mut().unwrap().tenant = "other".into();
    assert_eq!(
        second
            .client
            .query_database(v1::QueryDatabaseRequest {
                session: Some(wrong_scope),
                ..query.clone()
            })
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );

    let mut stream = second
        .client
        .download_database(handle.clone())
        .await
        .unwrap()
        .into_inner();
    let mut bytes = Vec::new();
    while let Some(frame) = stream.message().await.unwrap() {
        bytes.extend(frame.data);
    }
    assert_eq!(bytes.len() as u64, opened.bytes);
    let key = Key::parse(format!("default/catalog/snapshots/{generation}/root.db")).unwrap();
    let mut original = Vec::new();
    storage
        .get(&key)
        .await
        .unwrap()
        .reader
        .read_to_end(&mut original)
        .await
        .unwrap();
    assert_eq!(bytes, original);

    second
        .client
        .close_database_session(handle.clone())
        .await
        .unwrap();
    assert_eq!(
        second
            .client
            .query_database(query.clone())
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );

    let next = second
        .client
        .open_database_session(target)
        .await
        .unwrap()
        .into_inner();
    assert_ne!(
        next.snapshot_generation.as_deref(),
        Some(generation.to_string().as_str())
    );
    let expired = next.session.unwrap();
    let unit = sanctum_db::Unit::catalog(None, &scope("notes").tenant).unwrap();
    let record_key = unit
        .snapshot_session(expired.generation.parse().unwrap())
        .unwrap();
    let mut record = storage.get(&record_key).await.unwrap();
    let mut bytes = Vec::new();
    record.reader.read_to_end(&mut bytes).await.unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    value["expires_at"] = serde_json::to_value(SystemTime::now() - Duration::from_secs(1)).unwrap();
    storage
        .put_control(
            &record_key,
            serde_json::to_vec(&value).unwrap().into(),
            Condition::Match(record.object.version),
        )
        .await
        .unwrap();
    assert_eq!(
        second
            .client
            .query_database(v1::QueryDatabaseRequest {
                session: Some(expired),
                ..query
            })
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    std::fs::OpenOptions::new()
        .write(true)
        .open(directory.path().join(record_key.as_str()))
        .unwrap()
        .set_modified(SystemTime::now() - Duration::from_secs(901))
        .unwrap();
    writer.sweep_hosted_snapshots().await.unwrap();
    assert!(matches!(
        storage.head(&record_key).await,
        Err(Error::NotFound(_))
    ));
    second.stop().await;
}

#[tokio::test]
async fn snapshot_sweep_resumes_units_and_protects_published_generations() {
    use std::time::{Duration, SystemTime};

    let directory = tempfile::tempdir().unwrap();
    let storage = Arc::new(sanctum_storage_fs::FileStorage::open(directory.path()).unwrap());
    let service = HostedCatalog::new(storage.clone(), None, 1024).unwrap();
    let tenant = "default".parse().unwrap();
    let catalog = sanctum_db::Unit::catalog(None, &tenant).unwrap();
    let executor = sanctum_db::Publication::new(storage.clone(), Duration::from_secs(15)).unwrap();
    let mut first = None;
    for index in 0..10 {
        let scope = sanctum_core::Scope {
            tenant: "default".parse().unwrap(),
            collection: format!("notes-{index}").parse().unwrap(),
        };
        service.create_collection(&scope, None).await.unwrap();
        if index == 0 {
            first = executor.generation(&catalog).await.unwrap();
        }
    }
    let first = first.unwrap();
    let current = executor.generation(&catalog).await.unwrap().unwrap();
    let held = executor.acquire(&catalog).await.unwrap();
    let report = service.sweep_hosted_snapshots().await.unwrap();
    assert_eq!((report.units, report.deleted, report.failed), (7, 0, 1));
    assert!(report.more);
    held.release().await.unwrap();
    let service = HostedCatalog::new(storage.clone(), None, 1024).unwrap();
    let report = service.sweep_hosted_snapshots().await.unwrap();
    assert_eq!((report.units, report.deleted, report.failed), (3, 0, 0));
    assert!(!report.more);
    let report = service.sweep_hosted_snapshots().await.unwrap();
    assert_eq!((report.units, report.deleted, report.failed), (8, 0, 0));

    let marker = format!("default/catalog/snapshots/{}/retired.json", first.id);
    std::fs::OpenOptions::new()
        .write(true)
        .open(directory.path().join(&marker))
        .unwrap()
        .set_modified(SystemTime::now() - Duration::from_secs(901))
        .unwrap();
    assert!(!service.sweep_hosted_snapshots().await.unwrap().more);
    let report = service.sweep_hosted_snapshots().await.unwrap();
    assert_eq!(report.deleted, 1);
    let retired = Key::parse(format!("default/catalog/snapshots/{}/root.db", first.id)).unwrap();
    assert!(matches!(
        storage.head(&retired).await,
        Err(Error::NotFound(_))
    ));
    let current_key =
        Key::parse(format!("default/catalog/snapshots/{}/root.db", current.id)).unwrap();
    assert!(storage.head(&current_key).await.is_ok());
    assert_eq!(
        executor.generation(&catalog).await.unwrap().unwrap(),
        current
    );
}

#[tokio::test]
async fn upload_sweep_reclaims_only_unreferenced_candidates_and_resumes() {
    use std::time::{Duration, SystemTime};

    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(sanctum_storage_fs::FileStorage::open(dir.path()).unwrap());
    let catalog = || HostedCatalog::new(storage.clone(), None, 1024).unwrap();
    let service = catalog();
    let scope = sanctum_core::Scope {
        tenant: "default".parse().unwrap(),
        collection: "notes".parse().unwrap(),
    };
    service.create_collection(&scope, None).await.unwrap();
    let operation = uuid::Uuid::new_v4();
    let upload = service
        .stage_upload(
            scope.clone(),
            sanctum_service::TextUpload {
                name: "notes.rtf".into(),
                external: None,
                replacement: None,
                retain_original: true,
            },
            Some(operation),
        )
        .await
        .unwrap();
    std::io::Write::write_all(&mut upload.reopen().unwrap(), br"{\rtf1\ansi quartz note}").unwrap();
    let job = upload.enqueue().await.unwrap();
    service.register_upload(&scope, operation).await.unwrap();
    service.index_upload(&scope, operation).await.unwrap();
    let base = "default/collections/notes";
    let inputs = Key::parse(format!("{base}/operations/{operation}/inputs")).unwrap();
    let winner = storage
        .list(Some(&inputs), None, 10)
        .await
        .unwrap()
        .objects
        .remove(0)
        .key;
    let extracts = Key::parse(format!(
        "{base}/sources/{}/{}/objects",
        job.source.unwrap(),
        job.revision
    ))
    .unwrap();
    let canonical = storage
        .list(Some(&extracts), None, 10)
        .await
        .unwrap()
        .objects
        .remove(0)
        .key;
    let age = |key: &Key| {
        std::fs::OpenOptions::new()
            .write(true)
            .open(dir.path().join(key.as_str()))
            .unwrap()
            .set_modified(SystemTime::now() - Duration::from_secs(31 * 86_400))
            .unwrap();
    };
    let make = |key: Key, old: bool| {
        let storage = storage.clone();
        async move {
            storage
                .put_stream(&key, Box::pin(std::io::Cursor::new(b"candidate")), 9)
                .await
                .unwrap();
            if old {
                age(&key);
            }
            key
        }
    };
    age(&winner);
    age(&canonical);
    let young = make(
        Key::parse(format!("{inputs}/{}", uuid::Uuid::new_v4())).unwrap(),
        false,
    )
    .await;
    let uncertain = make(
        Key::parse(format!(
            "{base}/operations/{}/inputs/{}",
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4()
        ))
        .unwrap(),
        true,
    )
    .await;
    let extract = make(
        Key::parse(format!("{extracts}/{}/text.utf8", uuid::Uuid::new_v4())).unwrap(),
        true,
    )
    .await;
    let mut losers = Vec::new();
    for _ in 0..10 {
        losers.push(
            make(
                Key::parse(format!("{inputs}/{}", uuid::Uuid::new_v4())).unwrap(),
                true,
            )
            .await,
        );
    }

    let first = service.sweep_abandoned_uploads().await.unwrap();
    assert!(first.more);
    assert!(first.deleted <= 8);
    let restarted = catalog();
    let mut deleted = first.deleted;
    for _ in 0..8 {
        let report = restarted.sweep_abandoned_uploads().await.unwrap();
        assert_eq!(report.failed, 0);
        deleted += report.deleted;
        if !report.more {
            break;
        }
    }
    assert_eq!(deleted, 12);
    for key in losers.into_iter().chain([extract, uncertain]) {
        assert!(matches!(storage.head(&key).await, Err(Error::NotFound(_))));
    }
    for key in [winner, canonical, young] {
        storage.head(&key).await.unwrap();
    }
    let hit = restarted
        .retrieve(
            scope,
            "quartz".into(),
            5,
            None,
            sanctum_service::SearchMode::Lexical,
        )
        .await
        .unwrap()
        .hits
        .remove(0);
    assert_eq!(hit.excerpt, "quartz note");
}

#[tokio::test]
async fn hosted_upload_retention_rejects_old_work_and_preserves_published_bytes() {
    use std::time::{Duration, SystemTime};

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(sanctum_storage_fs::FileStorage::open(dir.path()).unwrap());
    let service = HostedCatalog::new(store.clone(), None, 1024).unwrap();
    let scope = sanctum_core::Scope {
        tenant: "default".parse().unwrap(),
        collection: "notes".parse().unwrap(),
    };
    service.create_collection(&scope, None).await.unwrap();
    let upload = |name: &str| sanctum_service::TextUpload {
        name: name.into(),
        external: None,
        replacement: None,
        retain_original: false,
    };
    let age = |path: &std::path::Path| {
        std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(SystemTime::now() - Duration::from_secs(46 * 86_400))
            .unwrap();
    };
    let mut published = None;
    for (name, data) in [
        ("notes.txt", b"retained quartz".as_slice()),
        ("invalid.txt", &[0xff][..]),
    ] {
        let staged = service
            .stage_upload(scope.clone(), upload(name), None)
            .await
            .unwrap();
        std::io::Write::write_all(&mut staged.reopen().unwrap(), data).unwrap();
        let accepted = staged.enqueue().await.unwrap();
        service
            .register_upload(&scope, accepted.operation)
            .await
            .unwrap();
        let result = service
            .index_upload(&scope, accepted.operation)
            .await
            .unwrap();
        let operation = dir.path().join(format!(
            "default/collections/notes/operations/{}",
            accepted.operation
        ));
        age(&operation.join("operation.json"));
        if name == "notes.txt" {
            for input in std::fs::read_dir(operation.join("inputs")).unwrap() {
                age(&input.unwrap().path());
            }
            published = Some(result);
        }
    }
    let pending = service
        .stage_upload(scope.clone(), upload("pending.txt"), None)
        .await
        .unwrap()
        .enqueue()
        .await
        .unwrap();
    let report = service.sweep_abandoned_uploads().await.unwrap();
    assert_eq!(report.expired, 2);
    assert_eq!(service.sweep_abandoned_uploads().await.unwrap().expired, 0);
    let published = published.unwrap();
    assert!(matches!(
        service.ingestion_job(&scope, published.operation).await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        service.register_upload(&scope, published.operation).await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        service.index_upload(&scope, published.operation).await,
        Err(Error::NotFound(_))
    ));
    assert!(
        service
            .ingestion_job(&scope, pending.operation)
            .await
            .is_ok()
    );
    let retry = service
        .stage_upload(
            scope.clone(),
            upload("notes.txt"),
            Some(published.operation),
        )
        .await
        .unwrap();
    std::io::Write::write_all(&mut retry.reopen().unwrap(), b"retained quartz").unwrap();
    assert!(matches!(retry.enqueue().await, Err(Error::Conflict(_))));
    let hit = service
        .fetch(
            &scope,
            sanctum_service::ChunkRef {
                source: published.source.unwrap(),
                revision: published.revision,
                ordinal: 0,
            },
        )
        .await
        .unwrap();
    assert_eq!(hit.excerpt, "retained quartz");
}

#[tokio::test]
async fn upload_sweep_fences_an_in_flight_acceptance_before_reclaiming() {
    use std::time::{Duration, SystemTime};

    let directory = tempfile::tempdir().unwrap();
    let storage = Arc::new(InterruptAcceptance {
        inner: sanctum_storage_fs::FileStorage::open(directory.path()).unwrap(),
        interrupt: AtomicU8::new(3),
    });
    let catalog = HostedCatalog::new(storage.clone(), None, 1024).unwrap();
    let scope = sanctum_core::Scope {
        tenant: "default".parse().unwrap(),
        collection: "notes".parse().unwrap(),
    };
    catalog.create_collection(&scope, None).await.unwrap();
    let operation = uuid::Uuid::new_v4();
    let input = || sanctum_service::TextUpload {
        name: "notes.txt".into(),
        external: None,
        replacement: None,
        retain_original: false,
    };
    let upload = catalog
        .stage_upload(scope.clone(), input(), Some(operation))
        .await
        .unwrap();
    std::io::Write::write_all(&mut upload.reopen().unwrap(), b"quartz").unwrap();
    let original = tokio::spawn(async move { upload.enqueue().await });
    let prefix = Key::parse(format!(
        "default/collections/notes/operations/{operation}/inputs"
    ))
    .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while storage.interrupt.load(Ordering::Relaxed) != 4 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let abandoned = storage
        .list(Some(&prefix), None, 8)
        .await
        .unwrap()
        .objects
        .remove(0)
        .key;
    std::fs::OpenOptions::new()
        .write(true)
        .open(directory.path().join(abandoned.as_str()))
        .unwrap()
        .set_modified(SystemTime::now() - Duration::from_secs(901))
        .unwrap();

    let sweep = catalog.sweep_abandoned_uploads().await.unwrap();
    assert_eq!(sweep.deleted, 1);
    assert!(matches!(
        catalog.ingestion_job(&scope, operation).await,
        Err(Error::NotFound(_))
    ));
    storage.interrupt.store(0, Ordering::Relaxed);
    assert!(matches!(original.await.unwrap(), Err(Error::Busy(_))));
    assert!(matches!(
        storage.head(&abandoned).await,
        Err(Error::NotFound(_))
    ));

    let retry = catalog
        .stage_upload(scope.clone(), input(), Some(operation))
        .await
        .unwrap();
    std::io::Write::write_all(&mut retry.reopen().unwrap(), b"quartz").unwrap();
    let accepted = retry.enqueue().await.unwrap();
    assert_eq!(accepted.operation, operation);
    catalog.register_upload(&scope, operation).await.unwrap();
    catalog.index_upload(&scope, operation).await.unwrap();
    let hits = catalog
        .retrieve(
            scope,
            "quartz".into(),
            5,
            None,
            sanctum_service::SearchMode::Lexical,
        )
        .await
        .unwrap()
        .hits;
    assert_eq!(hits[0].excerpt, "quartz");
}

#[tokio::test]
async fn hosted_submissions_recover_from_interrupted_acceptance() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Arc::new(InterruptAcceptance {
        inner: sanctum_storage_fs::FileStorage::open(directory.path()).unwrap(),
        interrupt: AtomicU8::new(0),
    });
    let catalog = HostedCatalog::new(storage.clone(), None, 1024).unwrap();
    let mut server = start(catalog.clone()).await;
    let scope = v1::Scope {
        tenant: "default".into(),
        collection: "notes".into(),
    };
    server
        .client
        .create_collection(v1::CreateCollectionRequest {
            scope: Some(scope.clone()),
            request_id: None,
        })
        .await
        .unwrap();
    for mode in [1, 2] {
        let operation = uuid::Uuid::new_v4().to_string();
        let header = v1::UploadTextHeader {
            scope: Some(scope.clone()),
            name: "notes.txt".into(),
            request_id: Some(operation.clone()),
            ..Default::default()
        };
        let request = v1::IngestionJobRequest {
            scope: Some(scope.clone()),
            operation_id: operation.clone(),
        };
        let input = [b"durable".as_slice()];
        storage.interrupt.store(mode, Ordering::Relaxed);
        assert_eq!(
            server
                .client
                .submit_text(upload_frames(&header, input))
                .await
                .unwrap_err()
                .code(),
            Code::Unavailable
        );
        server.stop().await;
        server = start(catalog.clone()).await;
        let initial = server.client.get_ingestion_job(request.clone()).await;
        let prefix =
            Key::parse(format!("default/collections/notes/operations/{operation}")).unwrap();
        let before = storage.list(Some(&prefix), None, 10).await.unwrap().objects;
        assert_eq!(before.len(), usize::from(mode));
        let accepted = server
            .client
            .submit_text(upload_frames(&header, input))
            .await
            .unwrap()
            .into_inner();
        if mode == 1 {
            assert_eq!(initial.unwrap_err().code(), Code::NotFound);
        } else {
            assert_eq!(initial.unwrap().into_inner(), accepted);
        }
        assert_eq!(
            server
                .client
                .get_ingestion_job(request)
                .await
                .unwrap()
                .into_inner(),
            accepted
        );
        let after = storage.list(Some(&prefix), None, 10).await.unwrap().objects;
        assert_eq!(after.len(), if mode == 1 { 3 } else { 2 });
        assert!(
            before
                .iter()
                .all(|item| after.iter().any(|saved| saved.key == item.key))
        );
    }
    server.stop().await;
}

struct InterruptAcceptance {
    inner: sanctum_storage_fs::FileStorage,
    interrupt: AtomicU8,
}

#[async_trait::async_trait]
impl Storage for InterruptAcceptance {
    async fn head(&self, key: &Key) -> StorageResult<Object> {
        self.inner.head(key).await
    }

    async fn get(&self, key: &Key) -> StorageResult<Download> {
        self.inner.get(key).await
    }

    async fn read_range(&self, key: &Key, range: ByteRange) -> StorageResult<axum::body::Bytes> {
        self.inner.read_range(key, range).await
    }

    async fn put_stream(&self, key: &Key, reader: Reader, length: u64) -> StorageResult<Version> {
        self.inner.put_stream(key, reader, length).await
    }

    async fn put_control(
        &self,
        key: &Key,
        bytes: axum::body::Bytes,
        condition: Condition,
    ) -> StorageResult<Version> {
        let interrupt = if key.as_str().ends_with("/operation.json") {
            match self.interrupt.load(Ordering::Relaxed) {
                3 => {
                    self.interrupt.store(4, Ordering::Relaxed);
                    while self.interrupt.load(Ordering::Relaxed) == 4 {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                    0
                }
                1 | 2 => self.interrupt.swap(0, Ordering::Relaxed),
                _ => 0,
            }
        } else {
            0
        };
        if interrupt == 1 {
            return Err(Error::Busy("interrupted before envelope write".into()));
        }
        let version = self.inner.put_control(key, bytes, condition).await?;
        if interrupt == 2 {
            return Err(Error::Busy("envelope write response lost".into()));
        }
        Ok(version)
    }

    async fn list(
        &self,
        prefix: Option<&Key>,
        cursor: Option<&str>,
        limit: u32,
    ) -> StorageResult<ListPage> {
        self.inner.list(prefix, cursor, limit).await
    }

    async fn delete(&self, key: &Key) -> StorageResult<()> {
        self.inner.delete(key).await
    }
}

fn upload_frames<'a>(
    header: &v1::UploadTextHeader,
    parts: impl IntoIterator<Item = &'a [u8]>,
) -> tokio_stream::Iter<std::vec::IntoIter<v1::UploadTextFrame>> {
    tokio_stream::iter(frames(header, parts))
}

fn frames<'a>(
    header: &v1::UploadTextHeader,
    parts: impl IntoIterator<Item = &'a [u8]>,
) -> Vec<v1::UploadTextFrame> {
    let header = Frame::Header(header.clone());
    let data = parts.into_iter().map(|part| Frame::Data(part.to_vec()));
    let end = Frame::End(v1::UploadTextEnd {});
    std::iter::once(header)
        .chain(data)
        .chain(std::iter::once(end))
        .map(|frame| v1::UploadTextFrame { frame: Some(frame) })
        .collect()
}

struct Running {
    client: SanctumClient<tonic::transport::Channel>,
    stop: CancellationToken,
    server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl Running {
    async fn stop(self) {
        drop(self.client);
        self.stop.cancel();
        self.server.await.unwrap().unwrap();
    }
}

async fn start(catalog: HostedCatalog) -> Running {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let stop = CancellationToken::new();
    let cancellation = stop.clone();
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(sanctum_api::hosted_grpc(
                catalog,
                cancellation.child_token(),
            ))
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                cancellation.cancelled_owned(),
            )
            .await
    });
    let client = SanctumClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    Running {
        client,
        stop,
        server,
    }
}
