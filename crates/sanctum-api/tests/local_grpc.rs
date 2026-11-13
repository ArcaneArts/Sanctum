use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use sanctum_embedding::{EmbedError, Provider, Purpose};
use sanctum_proto::v1::{self, sanctum_client::SanctumClient, upload_text_frame::Frame};
use sanctum_service::{Service, run_embedding_worker, run_local_worker};
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
                let embedding =
                    tokio::spawn(run_embedding_worker(server_service.clone(), worker_stop));
                (worker, embedding)
            });
            let result = Server::builder()
                .add_service(sanctum_api::grpc(server_service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown.await;
                })
                .await;
            let _ = stop_worker.send(true);
            if let Some((worker, embedding)) = workers {
                worker.await.unwrap();
                embedding.await.unwrap();
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
    assert_eq!(
        search(&mut running.client, "default", "quartz").await.len(),
        1
    );
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
