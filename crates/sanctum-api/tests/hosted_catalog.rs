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
    for hit in result.hits {
        let reference = hit.chunk.unwrap();
        assert_eq!(reference.scope.as_ref().unwrap().tenant, "default");
        let fetched = server
            .client
            .fetch_chunk(v1::FetchChunkRequest {
                chunk: Some(reference),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(fetched.excerpt, hit.excerpt);
        assert_eq!(fetched.database_generation, hit.database_generation);
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
            self.interrupt.swap(0, Ordering::Relaxed)
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
