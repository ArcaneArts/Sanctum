use sanctum_core::{Error, ExternalSource, Result, Scope, UPLOAD_FRAME_BYTES};
use sanctum_proto::v1::{
    self,
    sanctum_server::{Sanctum, SanctumServer},
    upload_text_frame::Frame,
};
use sanctum_service::{
    ChunkRef, Hit, ImportEvent, ImportOptions, ImportState, Job, JobState, Replacement, Service,
    TextUpload,
};
use tokio::io::AsyncWriteExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

pub fn grpc(service: Service) -> SanctumServer<Grpc> {
    SanctumServer::new(Grpc(service)).max_decoding_message_size(UPLOAD_FRAME_BYTES + 8192)
}

pub struct Grpc(Service);

#[tonic::async_trait]
impl Sanctum for Grpc {
    type ImportLocalStream = ReceiverStream<std::result::Result<v1::ImportLocalEvent, Status>>;

    async fn import_local(
        &self,
        request: Request<v1::ImportLocalRequest>,
    ) -> Reply<Self::ImportLocalStream> {
        let request = request.into_inner();
        let scope = parse_scope(request.scope)?;
        if request.path.is_empty() {
            return Err(Status::invalid_argument("import path is required"));
        }
        let (sender, receiver) = tokio::sync::mpsc::channel(8);
        let service = self.0.clone();
        tokio::spawn(async move {
            let events = sender.clone();
            let result = work(move || {
                let options = ImportOptions {
                    include: request.include,
                    exclude: request.exclude,
                    prune_missing: request.prune_missing,
                };
                service.import_local(
                    &scope,
                    std::path::Path::new(&request.path),
                    &options,
                    |event| events.blocking_send(Ok(wire_import(event))).is_ok(),
                )
            })
            .await;
            if let Err(error) = result {
                let _ = sender.send(Err(error)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    async fn create_collection(
        &self,
        request: Request<v1::CreateCollectionRequest>,
    ) -> Reply<v1::Collection> {
        let scope = parse_scope(request.into_inner().scope)?;
        let service = self.0.clone();
        work(move || {
            service.create_collection(&scope)?;
            Ok(v1::Collection {
                scope: Some(wire_scope(&scope)),
            })
        })
        .await
        .map(Response::new)
    }

    async fn list_collections(
        &self,
        request: Request<v1::ListCollectionsRequest>,
    ) -> Reply<v1::ListCollectionsResponse> {
        let tenant = request.into_inner().tenant.parse().map_err(status)?;
        let service = self.0.clone();
        work(move || {
            let collections = service
                .list_collections(&tenant)?
                .into_iter()
                .map(|collection| v1::Collection {
                    scope: Some(v1::Scope {
                        tenant: tenant.to_string(),
                        collection,
                    }),
                })
                .collect();
            Ok(v1::ListCollectionsResponse { collections })
        })
        .await
        .map(Response::new)
    }

    async fn upload_text(
        &self,
        request: Request<Streaming<v1::UploadTextFrame>>,
    ) -> Reply<v1::TextRevision> {
        let mut job = submit(&self.0, request.into_inner()).await?;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            match job.state {
                JobState::TextReady => {
                    return text_revision(&job)
                        .map(Response::new)
                        .ok_or_else(|| Status::internal("completed revision receipt is missing"));
                }
                JobState::Failed => {
                    let failure = job
                        .failure
                        .ok_or_else(|| Status::internal("ingestion failed"))?;
                    return Err(match failure.code.as_str() {
                        "invalid_source" => Status::invalid_argument(failure.message),
                        "conflict" => Status::already_exists(failure.message),
                        _ => Status::unavailable(failure.message),
                    });
                }
                JobState::Canceled => return Err(Status::cancelled("ingestion canceled")),
                _ => {}
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Status::deadline_exceeded(format!(
                    "upload is still processing; query operation {}",
                    job.id
                )));
            }
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            let service = self.0.clone();
            job = work(move || service.job(&job.scope, job.id)).await?;
        }
    }

    async fn submit_text(
        &self,
        request: Request<Streaming<v1::UploadTextFrame>>,
    ) -> Reply<v1::IngestionJob> {
        submit(&self.0, request.into_inner())
            .await
            .map(wire_job)
            .map(Response::new)
    }

    async fn get_ingestion_job(
        &self,
        request: Request<v1::IngestionJobRequest>,
    ) -> Reply<v1::IngestionJob> {
        job_action(&self.0, request.into_inner(), Service::job).await
    }

    async fn cancel_ingestion_job(
        &self,
        request: Request<v1::IngestionJobRequest>,
    ) -> Reply<v1::IngestionJob> {
        job_action(&self.0, request.into_inner(), Service::cancel_job).await
    }

    async fn retry_ingestion_job(
        &self,
        request: Request<v1::IngestionJobRequest>,
    ) -> Reply<v1::IngestionJob> {
        job_action(&self.0, request.into_inner(), Service::retry_job).await
    }

    async fn get_local_queue(&self, _request: Request<v1::Empty>) -> Reply<v1::LocalQueueStatus> {
        let service = self.0.clone();
        work(move || {
            Ok(v1::LocalQueueStatus {
                paused: service.queue_paused()?,
            })
        })
        .await
        .map(Response::new)
    }

    async fn set_local_queue(
        &self,
        request: Request<v1::SetLocalQueueRequest>,
    ) -> Reply<v1::LocalQueueStatus> {
        let paused = request.into_inner().paused;
        let service = self.0.clone();
        work(move || {
            service.pause_queue(paused)?;
            Ok(v1::LocalQueueStatus { paused })
        })
        .await
        .map(Response::new)
    }

    async fn search_text(
        &self,
        request: Request<v1::SearchTextRequest>,
    ) -> Reply<v1::SearchTextResponse> {
        let request = request.into_inner();
        let scope = parse_scope(request.scope)?;
        let service = self.0.clone();
        work(move || {
            let hits = service
                .search(&scope, &request.query, request.limit.unwrap_or(10))?
                .into_iter()
                .map(|hit| wire_hit(&scope, hit))
                .collect();
            Ok(v1::SearchTextResponse { hits })
        })
        .await
        .map(Response::new)
    }

    async fn fetch_chunk(&self, request: Request<v1::FetchChunkRequest>) -> Reply<v1::TextHit> {
        let reference = request
            .into_inner()
            .chunk
            .ok_or_else(|| Status::invalid_argument("chunk is required"))?;
        let scope = parse_scope(reference.scope)?;
        if reference.ordinal > i64::MAX as u64 {
            return Err(Status::invalid_argument(
                "chunk ordinal exceeds the supported range",
            ));
        }
        let reference = ChunkRef {
            source: reference
                .source_id
                .parse()
                .map_err(|_| Status::invalid_argument("invalid source ID"))?,
            revision: reference
                .revision_id
                .parse()
                .map_err(|_| Status::invalid_argument("invalid revision ID"))?,
            ordinal: reference.ordinal,
        };
        let service = self.0.clone();
        work(move || Ok(wire_hit(&scope, service.fetch(&scope, &reference)?)))
            .await
            .map(Response::new)
    }

    async fn delete_source(
        &self,
        request: Request<v1::DeleteSourceRequest>,
    ) -> Reply<v1::DeletedSource> {
        let request = request.into_inner();
        let scope = parse_scope(request.scope)?;
        let source = request
            .source_id
            .parse()
            .map_err(|_| Status::invalid_argument("invalid source ID"))?;
        let expected =
            parse_optional_id(request.expected_revision_id, "invalid expected revision ID")?;
        let service = self.0.clone();
        work(move || {
            let revision = service.delete_source(&scope, source, expected)?;
            Ok(v1::DeletedSource {
                source_id: source.to_string(),
                revision_id: revision.to_string(),
            })
        })
        .await
        .map(Response::new)
    }
}

async fn submit(
    service: &Service,
    mut stream: Streaming<v1::UploadTextFrame>,
) -> std::result::Result<Job, Status> {
    let header = match stream.message().await?.and_then(|message| message.frame) {
        Some(Frame::Header(header)) => header,
        _ => {
            return Err(Status::invalid_argument(
                "first upload frame must be a header",
            ));
        }
    };
    let scope = parse_scope(header.scope)?;
    let replacement = if let Some(replacement) = header.replacement {
        Some(Replacement {
            source: parse_optional_id(replacement.source_id, "invalid source ID")?,
            expected_revision: parse_optional_id(
                replacement.expected_revision_id,
                "invalid expected revision ID",
            )?,
        })
    } else {
        None
    };
    let service = service.clone();
    let max_bytes = service.max_source_bytes();
    let (staged, file) = work(move || {
        let staged = service.stage_upload(
            scope,
            TextUpload {
                name: header.name,
                external: header.external.map(|value| ExternalSource {
                    namespace: value.namespace,
                    id: value.id,
                }),
                replacement,
            },
        )?;
        let file = staged.reopen()?;
        Ok((staged, file))
    })
    .await?;

    let mut file = tokio::fs::File::from_std(file);
    let mut written = 0;
    loop {
        let data = match stream.message().await?.and_then(|message| message.frame) {
            Some(Frame::Data(data)) => data,
            Some(Frame::End(_)) => break,
            _ => {
                return Err(Status::invalid_argument(
                    "expected data or explicit upload end",
                ));
            }
        };
        if data.len() > UPLOAD_FRAME_BYTES {
            return Err(Status::invalid_argument("upload frame exceeds 256 KiB"));
        }
        written += data.len() as u64;
        if written > max_bytes {
            return Err(Status::resource_exhausted(
                "source exceeds the configured upload limit",
            ));
        }
        file.write_all(&data)
            .await
            .map_err(|error| status(error.into()))?;
    }
    if stream.message().await?.is_some() {
        return Err(Status::invalid_argument(
            "unexpected frame after upload end",
        ));
    }
    file.flush().await.map_err(|error| status(error.into()))?;
    drop(file);

    work(move || staged.enqueue()).await
}

async fn job_action(
    service: &Service,
    request: v1::IngestionJobRequest,
    action: fn(&Service, &Scope, uuid::Uuid) -> Result<Job>,
) -> Reply<v1::IngestionJob> {
    let scope = parse_scope(request.scope)?;
    let id = request
        .operation_id
        .parse()
        .map_err(|_| Status::invalid_argument("invalid operation ID"))?;
    let service = service.clone();
    work(move || action(&service, &scope, id))
        .await
        .map(wire_job)
        .map(Response::new)
}

fn text_revision(job: &Job) -> Option<v1::TextRevision> {
    Some(v1::TextRevision {
        scope: Some(wire_scope(&job.scope)),
        source_id: job.source.to_string(),
        revision_id: job.revision.to_string(),
        name: job.name.clone(),
        external: job.external.clone().map(wire_external),
        bytes: job.bytes,
        chunks: job.chunks?,
        content_blake3: job.hash.clone()?,
    })
}

fn wire_job(job: Job) -> v1::IngestionJob {
    let state = match job.state {
        JobState::Accepted => v1::IngestionState::Accepted,
        JobState::Indexing => v1::IngestionState::Indexing,
        JobState::Publishing => v1::IngestionState::Publishing,
        JobState::TextReady => v1::IngestionState::TextReady,
        JobState::Failed => v1::IngestionState::Failed,
        JobState::Canceled => v1::IngestionState::Canceled,
    };
    let text_revision = text_revision(&job);
    let (error_code, error_message) = match job.failure {
        Some(failure) => (Some(failure.code), Some(failure.message)),
        None => (None, None),
    };
    v1::IngestionJob {
        scope: Some(wire_scope(&job.scope)),
        operation_id: job.id.to_string(),
        source_id: job.source.to_string(),
        revision_id: job.revision.to_string(),
        name: job.name,
        external: job.external.map(wire_external),
        total_bytes: job.bytes,
        processed_bytes: job.processed_bytes,
        attempts: job.attempts,
        state: state.into(),
        error_code,
        error_message,
        text_revision,
        update_sequence: job.update_sequence,
    }
}

type Reply<T> = std::result::Result<Response<T>, Status>;

fn wire_import(event: ImportEvent) -> v1::ImportLocalEvent {
    let state = match event.state {
        ImportState::Queued => v1::ImportState::Queued,
        ImportState::Pending => v1::ImportState::Pending,
        ImportState::Unchanged => v1::ImportState::Unchanged,
        ImportState::Missing => v1::ImportState::Missing,
        ImportState::Deleted => v1::ImportState::Deleted,
        ImportState::Failed => v1::ImportState::Failed,
    };
    v1::ImportLocalEvent {
        path: event.path,
        state: state.into(),
        source_id: event.source.map(|id| id.to_string()),
        revision_id: event.revision.map(|id| id.to_string()),
        operation_id: event.operation.map(|id| id.to_string()),
        message: event.message,
    }
}

async fn work<T: Send + 'static>(
    operation: impl FnOnce() -> Result<T> + Send + 'static,
) -> std::result::Result<T, Status> {
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| {
            tracing::error!(%error, "blocking operation failed");
            Status::internal("server operation failed")
        })?
        .map_err(status)
}

#[allow(clippy::result_large_err)]
fn parse_scope(scope: Option<v1::Scope>) -> std::result::Result<Scope, Status> {
    let scope = scope.ok_or_else(|| Status::invalid_argument("scope is required"))?;
    Ok(Scope {
        tenant: scope.tenant.parse().map_err(status)?,
        collection: scope.collection.parse().map_err(status)?,
    })
}

fn wire_scope(scope: &Scope) -> v1::Scope {
    v1::Scope {
        tenant: scope.tenant.to_string(),
        collection: scope.collection.to_string(),
    }
}

fn wire_external(value: ExternalSource) -> v1::ExternalSource {
    v1::ExternalSource {
        namespace: value.namespace,
        id: value.id,
    }
}

fn wire_hit(scope: &Scope, hit: Hit) -> v1::TextHit {
    let chunk = hit.chunk;
    v1::TextHit {
        chunk: Some(v1::ChunkRef {
            scope: Some(wire_scope(scope)),
            source_id: chunk.reference.source.to_string(),
            revision_id: chunk.reference.revision.to_string(),
            ordinal: chunk.reference.ordinal,
        }),
        name: chunk.name,
        external: chunk.external.map(wire_external),
        start_byte: chunk.range.start,
        end_byte: chunk.range.end,
        excerpt: hit.excerpt,
        content_blake3: chunk.hash,
    }
}

fn status(error: Error) -> Status {
    match error {
        Error::Invalid(message) => Status::invalid_argument(message),
        Error::NotFound(message) => Status::not_found(message),
        Error::Conflict(message) => Status::already_exists(message),
        Error::StaleRevision(message) => Status::failed_precondition(message),
        Error::Busy(message) => Status::unavailable(message),
        Error::Io(_) | Error::Internal(_) => {
            tracing::error!(%error, "application operation failed");
            Status::internal("storage or database operation failed")
        }
    }
}

#[allow(clippy::result_large_err)]
fn parse_optional_id(
    value: Option<String>,
    error: &'static str,
) -> std::result::Result<Option<uuid::Uuid>, Status> {
    value
        .map(|id| id.parse().map_err(|_| Status::invalid_argument(error)))
        .transpose()
}
