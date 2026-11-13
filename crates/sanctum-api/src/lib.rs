use sanctum_core::{
    Error, ExternalSource, GitProvenance, Result, Scope, UPLOAD_FRAME_BYTES, UrlProvenance,
    UsageScope,
};
use sanctum_proto::v1::{
    self,
    sanctum_server::{Sanctum, SanctumServer},
    upload_text_frame::Frame,
};
use sanctum_service::{
    ChunkRef, GitRepository, Hit, HostedCatalog, HostedJob, HostedJobState, ImportEvent,
    ImportOptions, ImportState, Job, JobState, Replacement, Service, TextUpload, WatchedFolder,
};
use tokio::io::AsyncWriteExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, Streaming};

mod database;
mod metadata_filter;

pub fn grpc(service: Service, shutdown: CancellationToken) -> SanctumServer<Grpc> {
    // Metadata batches can contain 32 values of up to 64 KiB each.
    SanctumServer::new(Grpc(Backend::Local(service), shutdown))
        .max_decoding_message_size(3 * 1024 * 1024)
}

pub fn hosted_grpc(catalog: HostedCatalog, shutdown: CancellationToken) -> SanctumServer<Grpc> {
    SanctumServer::new(Grpc(Backend::Hosted(catalog), shutdown))
        .max_decoding_message_size(3 * 1024 * 1024)
}

enum Backend {
    Local(Service),
    Hosted(HostedCatalog),
}

pub struct Grpc(Backend, CancellationToken);

impl Grpc {
    #[allow(clippy::result_large_err)]
    fn local(&self) -> std::result::Result<&Service, Status> {
        match &self.0 {
            Backend::Local(service) => Ok(service),
            Backend::Hosted(_) => Err(Status::unimplemented(
                "this operation requires the local filesystem service",
            )),
        }
    }
}

#[tonic::async_trait]
impl Sanctum for Grpc {
    async fn open_database_session(
        &self,
        request: Request<v1::DatabaseTarget>,
    ) -> Reply<v1::DatabaseSession> {
        database::open(self.local()?, self.1.child_token(), request.into_inner()).await
    }

    async fn query_database(
        &self,
        request: Request<v1::QueryDatabaseRequest>,
    ) -> Reply<v1::QueryDatabaseResponse> {
        database::query(self.local()?, self.1.child_token(), request.into_inner()).await
    }

    type DownloadDatabaseStream = ReceiverStream<std::result::Result<v1::DatabaseFrame, Status>>;

    async fn download_database(
        &self,
        request: Request<v1::DatabaseSessionRef>,
    ) -> Reply<Self::DownloadDatabaseStream> {
        database::download(self.local()?, self.1.clone(), request.into_inner()).await
    }

    async fn close_database_session(
        &self,
        request: Request<v1::DatabaseSessionRef>,
    ) -> Reply<v1::Empty> {
        database::close(self.local()?, request.into_inner()).await
    }

    async fn get_storage_usage(
        &self,
        request: Request<v1::StorageUsageRequest>,
    ) -> Reply<v1::StorageUsage> {
        use v1::storage_usage_request::Scope as WireScope;
        let request = request.into_inner();
        let scope = match &request.scope {
            Some(WireScope::Collection(collection)) => {
                UsageScope::Collection(parse_scope(Some(collection.clone()))?)
            }
            Some(WireScope::Tenant(tenant)) => UsageScope::Tenant(tenant.parse().map_err(status)?),
            Some(WireScope::Cluster(_)) => UsageScope::Cluster,
            None => return Err(Status::invalid_argument("storage usage scope is required")),
        };
        let measured = if let Backend::Hosted(catalog) = &self.0 {
            catalog.storage_usage(&scope).await.map_err(status)?
        } else {
            let service = self.local()?.clone();
            work(move || service.storage_usage(&scope)).await?
        };
        Ok(Response::new(v1::StorageUsage {
            scope: Some(request),
            bytes: measured.bytes,
            files: measured.files,
            started_at_unix_ms: unix_ms(measured.started_at).map_err(status)?,
            finished_at_unix_ms: unix_ms(measured.finished_at).map_err(status)?,
        }))
    }
    async fn get_metadata(
        &self,
        request: Request<v1::MetadataTarget>,
    ) -> Reply<v1::MetadataResponse> {
        let (scope, target) = metadata_target(request.into_inner())?;
        let service = self.local()?.clone();
        work(move || service.metadata(&scope, target, None))
            .await
            .map(metadata_response)
            .map(Response::new)
    }

    async fn mutate_metadata(
        &self,
        request: Request<v1::MutateMetadataRequest>,
    ) -> Reply<v1::MetadataResponse> {
        let request = request.into_inner();
        let (scope, target) = metadata_target(
            request
                .target
                .ok_or_else(|| Status::invalid_argument("metadata target is required"))?,
        )?;
        let edits = metadata_edits(request.edits)?;
        let service = self.local()?.clone();
        work(move || service.metadata(&scope, target, Some(&edits)))
            .await
            .map(metadata_response)
            .map(Response::new)
    }

    async fn query_metadata(
        &self,
        request: Request<v1::QueryMetadataRequest>,
    ) -> Reply<v1::QueryMetadataResponse> {
        use sanctum_service::metadata_query::Results;
        let request = request.into_inner();
        let tenant = request.tenant.parse().map_err(status)?;
        let collection = request
            .collection
            .as_ref()
            .map(|id| id.parse())
            .transpose()
            .map_err(status)?;
        let query = metadata_filter::parse_query(request).map_err(status)?;
        let service = self.local()?.clone();
        work(move || {
            let page = service.query_metadata(&tenant, collection, query)?;
            let mut reply = v1::QueryMetadataResponse {
                next_offset: page.next_offset,
                ..Default::default()
            };
            match page.results {
                Results::Matches(matches) => {
                    reply.matches = matches
                        .into_iter()
                        .map(|row| v1::MetadataMatch {
                            target: Some(v1::MetadataTarget {
                                scope: Some(v1::Scope {
                                    tenant: tenant.to_string(),
                                    collection: row.collection,
                                }),
                                source_id: row.source,
                                revision_id: row.revision,
                                chunk_ordinal: row.ordinal,
                            }),
                            value_json: row.value_json,
                        })
                        .collect();
                }
                Results::Counts(groups) => {
                    reply.groups = groups
                        .into_iter()
                        .map(|group| v1::MetadataGroup {
                            count: group.count,
                            value_json: group.value_json,
                        })
                        .collect();
                }
            }
            Ok(reply)
        })
        .await
        .map(Response::new)
    }

    async fn list_changes(
        &self,
        request: Request<v1::ListChangesRequest>,
    ) -> Reply<v1::ListChangesResponse> {
        let request = request.into_inner();
        let scope = parse_scope(request.scope)?;
        changes_page(
            self.local()?.clone(),
            scope,
            request.after_cursor,
            request.limit.unwrap_or(100),
        )
        .await
        .map(Response::new)
    }

    type WatchChangesStream = ReceiverStream<std::result::Result<v1::ListChangesResponse, Status>>;

    async fn watch_changes(
        &self,
        request: Request<v1::ListChangesRequest>,
    ) -> Reply<Self::WatchChangesStream> {
        let request = request.into_inner();
        let scope = parse_scope(request.scope)?;
        let limit = request.limit.unwrap_or(100);
        let service = self.local()?.clone();
        let first =
            changes_page(service.clone(), scope.clone(), request.after_cursor, limit).await?;
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let shutdown = self.1.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown.cancelled() => {}
                _ = stream_changes(service, scope, limit, first, sender) => {}
            }
        });
        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    async fn get_change_cursor(
        &self,
        request: Request<v1::ChangeCursorRequest>,
    ) -> Reply<v1::ChangeCursor> {
        let scope = parse_scope(request.into_inner().scope)?;
        let service = self.local()?.clone();
        work(move || {
            Ok(v1::ChangeCursor {
                cursor: service.change_cursor(&scope)?,
            })
        })
        .await
        .map(Response::new)
    }

    async fn import_url(
        &self,
        request: Request<v1::ImportUrlRequest>,
    ) -> Reply<v1::ImportUrlResponse> {
        let request = request.into_inner();
        let scope = parse_scope(request.scope)?;
        let (event, url) = self
            .local()?
            .import_url(scope, &request.url, request.retain_original)
            .await
            .map_err(status)?;
        Ok(Response::new(v1::ImportUrlResponse {
            state: wire_import_state(event.state).into(),
            source_id: event
                .source
                .ok_or_else(|| Status::internal("URL import omitted its source ID"))?
                .to_string(),
            revision_id: event
                .revision
                .ok_or_else(|| Status::internal("URL import omitted its revision ID"))?
                .to_string(),
            operation_id: event.operation.map(|id| id.to_string()),
            url: Some(wire_url(url)),
        }))
    }

    type ImportGitStream = ReceiverStream<std::result::Result<v1::ImportGitEvent, Status>>;

    async fn import_git(
        &self,
        request: Request<v1::ImportGitRequest>,
    ) -> Reply<Self::ImportGitStream> {
        use v1::import_git_request::Repository;
        let request = request.into_inner();
        let scope = parse_scope(request.scope)?;
        let repository = match request.repository {
            Some(Repository::LocalPath(path)) if !path.is_empty() => {
                GitRepository::Local(path.into())
            }
            Some(Repository::HttpsUrl(url)) if !url.is_empty() => GitRepository::Remote(url),
            _ => return Err(Status::invalid_argument("Git repository is required")),
        };
        let options = ImportOptions {
            include: request.include,
            exclude: request.exclude,
            prune_missing: request.prune_missing,
            retain_original: request.retain_original,
        };
        let (sender, receiver) = tokio::sync::mpsc::channel(8);
        let service = self.local()?.clone();
        tokio::spawn(async move {
            let events = sender.clone();
            let result = work(move || {
                service.import_git(
                    &scope,
                    repository,
                    &request.reference,
                    &options,
                    || events.is_closed(),
                    |event| {
                        events
                            .blocking_send(Ok(v1::ImportGitEvent {
                                file: Some(wire_import(event.import)),
                                git: event.git.map(wire_git),
                            }))
                            .is_ok()
                    },
                )
            })
            .await;
            if let Err(error) = result {
                let _ = sender.send(Err(error)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(receiver)))
    }

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
        let service = self.local()?.clone();
        tokio::spawn(async move {
            let events = sender.clone();
            let result = work(move || {
                let options = ImportOptions {
                    include: request.include,
                    exclude: request.exclude,
                    prune_missing: request.prune_missing,
                    retain_original: request.retain_original,
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

    async fn watch_folder(
        &self,
        request: Request<v1::WatchFolderRequest>,
    ) -> Reply<v1::WatchedFolder> {
        let request = request.into_inner();
        let scope = parse_scope(request.scope)?;
        let service = self.local()?.clone();
        work(move || {
            let options = ImportOptions {
                include: request.include,
                exclude: request.exclude,
                prune_missing: request.prune_missing,
                retain_original: request.retain_original,
            };
            service
                .watch_folder(
                    &scope,
                    std::path::Path::new(&request.path),
                    &options,
                    request.interval_seconds.unwrap_or(60),
                )
                .map(wire_folder)
        })
        .await
        .map(Response::new)
    }

    async fn list_watched_folders(
        &self,
        request: Request<v1::ListWatchedFoldersRequest>,
    ) -> Reply<v1::ListWatchedFoldersResponse> {
        let request = request.into_inner();
        let scope = parse_scope(request.scope)?;
        let after = parse_optional_id(request.after_id, "invalid watched folder cursor")?;
        let service = self.local()?.clone();
        work(move || {
            let (folders, next) =
                service.watched_folders(&scope, after, request.limit.unwrap_or(10))?;
            Ok(v1::ListWatchedFoldersResponse {
                folders: folders.into_iter().map(wire_folder).collect(),
                next_after_id: next.map(|id| id.to_string()),
            })
        })
        .await
        .map(Response::new)
    }

    async fn unwatch_folder(&self, request: Request<v1::UnwatchFolderRequest>) -> Reply<v1::Empty> {
        let request = request.into_inner();
        let scope = parse_scope(request.scope)?;
        let id = request
            .id
            .parse()
            .map_err(|_| Status::invalid_argument("invalid watched folder ID"))?;
        let service = self.local()?.clone();
        work(move || {
            service.unwatch_folder(&scope, id)?;
            Ok(v1::Empty {})
        })
        .await
        .map(Response::new)
    }

    async fn create_collection(
        &self,
        request: Request<v1::CreateCollectionRequest>,
    ) -> Reply<v1::Collection> {
        let request = request.into_inner();
        let scope = parse_scope(request.scope)?;
        let operation = parse_optional_id(request.request_id, "request ID must be a UUID")?;
        if let Backend::Hosted(catalog) = &self.0 {
            catalog
                .create_collection(&scope, operation)
                .await
                .map_err(status)?;
        } else {
            let service = self.local()?.clone();
            let target = scope.clone();
            work(move || service.create_collection(&target)).await?;
        }
        Ok(Response::new(v1::Collection {
            scope: Some(wire_scope(&scope)),
        }))
    }

    async fn list_collections(
        &self,
        request: Request<v1::ListCollectionsRequest>,
    ) -> Reply<v1::ListCollectionsResponse> {
        let request = request.into_inner();
        let tenant: sanctum_core::ScopeId = request.tenant.parse().map_err(status)?;
        let after = request
            .after
            .as_deref()
            .map(str::parse)
            .transpose()
            .map_err(status)?;
        let paginated = matches!(self.0, Backend::Hosted(_))
            || request.after.is_some()
            || request.limit.is_some();
        let limit = request.limit.unwrap_or(50);
        if !(1..=99).contains(&limit) {
            return Err(Status::invalid_argument(
                "collection page limit must be between 1 and 99",
            ));
        }
        let mut ids = if let Backend::Hosted(catalog) = &self.0 {
            catalog
                .list_collections(&tenant, after.as_ref(), limit + 1)
                .await
                .map_err(status)?
        } else {
            let service = self.local()?.clone();
            let tenant = tenant.clone();
            work(move || {
                if paginated {
                    service.list_collections_page(&tenant, after.as_ref(), limit + 1)
                } else {
                    service.list_collections(&tenant)
                }
            })
            .await?
        };
        let next_after = if paginated && ids.len() > limit as usize {
            ids.pop();
            ids.last().cloned()
        } else {
            None
        };
        let collections = ids
            .into_iter()
            .map(|collection| v1::Collection {
                scope: Some(v1::Scope {
                    tenant: tenant.to_string(),
                    collection,
                }),
            })
            .collect();
        Ok(Response::new(v1::ListCollectionsResponse {
            collections,
            next_after,
        }))
    }

    async fn upload_text(
        &self,
        request: Request<Streaming<v1::UploadTextFrame>>,
    ) -> Reply<v1::TextRevision> {
        let mut job = submit_local(self.local()?, request.into_inner()).await?;
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
            let service = self.local()?.clone();
            job = work(move || service.job(&job.scope, job.id)).await?;
        }
    }

    async fn submit_text(
        &self,
        request: Request<Streaming<v1::UploadTextFrame>>,
    ) -> Reply<v1::IngestionJob> {
        let stream = request.into_inner();
        let job = match &self.0 {
            Backend::Local(service) => wire_job(submit_local(service, stream).await?),
            Backend::Hosted(catalog) => submit_hosted(catalog, stream).await?,
        };
        Ok(Response::new(job))
    }

    async fn get_ingestion_job(
        &self,
        request: Request<v1::IngestionJobRequest>,
    ) -> Reply<v1::IngestionJob> {
        let request = request.into_inner();
        if let Backend::Hosted(catalog) = &self.0 {
            let scope = parse_scope(request.scope)?;
            let id = request
                .operation_id
                .parse()
                .map_err(|_| Status::invalid_argument("invalid operation ID"))?;
            let accepted = catalog.ingestion_job(&scope, id).await.map_err(status)?;
            return Ok(Response::new(wire_hosted_job(&scope, accepted)));
        }
        job_action(self.local()?, request, Service::job).await
    }

    async fn list_ingestion_jobs(
        &self,
        request: Request<v1::ListIngestionJobsRequest>,
    ) -> Reply<v1::ListIngestionJobsResponse> {
        let request = request.into_inner();
        let tenant = request.tenant.parse().map_err(status)?;
        let collection = request
            .collection
            .as_deref()
            .map(str::parse)
            .transpose()
            .map_err(status)?;
        let before = parse_optional_id(request.before_operation_id, "invalid activity cursor")?;
        let service = self.local()?.clone();
        work(move || {
            let (jobs, next) = service.activity(
                &tenant,
                collection.as_ref(),
                before,
                request.limit.unwrap_or(25),
            )?;
            Ok(v1::ListIngestionJobsResponse {
                jobs: jobs.into_iter().map(wire_job).collect(),
                next_before_operation_id: next.map(|id| id.to_string()),
            })
        })
        .await
        .map(Response::new)
    }

    async fn get_embedding_status(
        &self,
        request: Request<v1::EmbeddingStatusRequest>,
    ) -> Reply<v1::EmbeddingStatus> {
        let request = request.into_inner();
        let scope = parse_scope(request.scope)?;
        let source = parse_optional_id(request.source_id, "invalid source ID")?;
        let revision = parse_optional_id(request.revision_id, "invalid revision ID")?;
        let target = match (source, revision) {
            (None, None) => None,
            (Some(source), Some(revision)) => Some((source, revision)),
            _ => {
                return Err(Status::invalid_argument(
                    "embedding status requires both source and revision IDs",
                ));
            }
        };
        let status = match &self.0 {
            Backend::Local(service) => {
                let service = service.clone();
                work(move || service.embedding_status(&scope, target)).await?
            }
            Backend::Hosted(service) => service
                .embedding_status(&scope, target)
                .await
                .map_err(status)?,
        };
        Ok(Response::new(v1::EmbeddingStatus {
            coverage: Some(v1::EmbeddingCoverage {
                profile_id: status.profile,
                total_chunks: status.coverage.total,
                embedded_chunks: status.coverage.embedded,
                failed_chunks: status.coverage.failed,
            }),
            retrying_chunks: status.retrying,
            next_retry_at_unix_seconds: status.next_retry,
            sample_error: status.sample_error,
        }))
    }

    async fn cancel_ingestion_job(
        &self,
        request: Request<v1::IngestionJobRequest>,
    ) -> Reply<v1::IngestionJob> {
        job_action(self.local()?, request.into_inner(), Service::cancel_job).await
    }

    async fn retry_ingestion_job(
        &self,
        request: Request<v1::IngestionJobRequest>,
    ) -> Reply<v1::IngestionJob> {
        job_action(self.local()?, request.into_inner(), Service::retry_job).await
    }

    async fn get_local_queue(&self, _request: Request<v1::Empty>) -> Reply<v1::LocalQueueStatus> {
        let service = self.local()?.clone();
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
        let service = self.local()?.clone();
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
        let mode = search_mode(request.mode, sanctum_service::SearchMode::Lexical)?;
        let filter = request
            .metadata_filter
            .map(metadata_filter::parse)
            .transpose()
            .map_err(status)?;
        let limit = request.limit.unwrap_or(10);
        let result = match &self.0 {
            Backend::Local(service) => {
                service
                    .retrieve(scope.clone(), request.query, limit, filter, mode)
                    .await
            }
            Backend::Hosted(service) => {
                service
                    .retrieve(scope.clone(), request.query, limit, filter, mode)
                    .await
            }
        }
        .map_err(status)?;
        Ok(Response::new(v1::SearchTextResponse {
            hits: result
                .hits
                .into_iter()
                .map(|hit| wire_hit(&scope, hit))
                .collect(),
            mode: wire_search_mode(result.mode).into(),
            embedding_coverage: Some(v1::EmbeddingCoverage {
                profile_id: result.profile,
                total_chunks: result.coverage.total,
                embedded_chunks: result.coverage.embedded,
                failed_chunks: result.coverage.failed,
            }),
            degraded_reason: result.degraded.map(str::to_owned),
        }))
    }

    async fn search_collections(
        &self,
        request: Request<v1::SearchCollectionsRequest>,
    ) -> Reply<v1::SearchTextResponse> {
        let request = request.into_inner();
        let tenant = request.tenant.parse().map_err(status)?;
        let collections = request
            .collections
            .iter()
            .map(|id| id.parse())
            .collect::<Result<Vec<_>>>()
            .map_err(status)?;
        let mode = search_mode(request.mode, sanctum_service::SearchMode::Hybrid)?;
        let filter = request
            .metadata_filter
            .map(metadata_filter::parse)
            .transpose()
            .map_err(status)?;
        let result = self
            .local()?
            .retrieve_collections(
                tenant,
                collections,
                request.query,
                request.limit.unwrap_or(10),
                filter,
                mode,
            )
            .await
            .map_err(status)?;
        Ok(Response::new(v1::SearchTextResponse {
            hits: result
                .hits
                .into_iter()
                .map(|(scope, hit)| wire_hit(&scope, hit))
                .collect(),
            mode: wire_search_mode(result.mode).into(),
            embedding_coverage: Some(v1::EmbeddingCoverage {
                profile_id: result.profile,
                total_chunks: result.coverage.total,
                embedded_chunks: result.coverage.embedded,
                failed_chunks: result.coverage.failed,
            }),
            degraded_reason: result.degraded.map(str::to_owned),
        }))
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
        let hit = match &self.0 {
            Backend::Local(service) => {
                let service = service.clone();
                let scope = scope.clone();
                work(move || service.fetch(&scope, &reference)).await?
            }
            Backend::Hosted(service) => service.fetch(&scope, reference).await.map_err(status)?,
        };
        Ok(Response::new(wire_hit(&scope, hit)))
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
        let service = self.local()?.clone();
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

fn unix_ms(time: std::time::SystemTime) -> Result<u64> {
    let elapsed = time
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| Error::Internal(Box::new(error)))?;
    u64::try_from(elapsed.as_millis()).map_err(|error| Error::Internal(Box::new(error)))
}

async fn submit_local(
    service: &Service,
    mut stream: Streaming<v1::UploadTextFrame>,
) -> std::result::Result<Job, Status> {
    let (scope, upload, request_id) = read_upload_header(&mut stream).await?;
    if request_id.is_some() {
        return Err(Status::invalid_argument(
            "upload request IDs are supported for hosted submissions",
        ));
    }
    let service = service.clone();
    let max_bytes = service.max_source_bytes();
    let (staged, file) = work(move || {
        let staged = service.stage_upload(scope, upload)?;
        let file = staged.reopen()?;
        Ok((staged, file))
    })
    .await?;
    receive_upload(&mut stream, file, max_bytes).await?;
    work(move || staged.enqueue()).await
}

async fn submit_hosted(
    catalog: &HostedCatalog,
    mut stream: Streaming<v1::UploadTextFrame>,
) -> std::result::Result<v1::IngestionJob, Status> {
    let (scope, upload, operation) = read_upload_header(&mut stream).await?;
    let staged = catalog
        .stage_upload(scope.clone(), upload, operation)
        .await
        .map_err(status)?;
    let file = staged.reopen().map_err(status)?;
    receive_upload(&mut stream, file, catalog.max_source_bytes()).await?;
    let accepted = staged.enqueue().await.map_err(status)?;
    Ok(wire_hosted_job(&scope, accepted))
}

async fn read_upload_header(
    stream: &mut Streaming<v1::UploadTextFrame>,
) -> std::result::Result<(Scope, TextUpload, Option<uuid::Uuid>), Status> {
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
    let upload = TextUpload {
        name: header.name,
        external: header.external.map(|value| ExternalSource {
            namespace: value.namespace,
            id: value.id,
        }),
        replacement,
        retain_original: header.retain_original,
    };
    let request_id = parse_optional_id(header.request_id, "invalid request ID")?;
    Ok((scope, upload, request_id))
}

async fn receive_upload(
    stream: &mut Streaming<v1::UploadTextFrame>,
    file: std::fs::File,
    max_bytes: u64,
) -> std::result::Result<(), Status> {
    let mut file = tokio::fs::File::from_std(file);
    let mut written = 0u64;
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
        written = written
            .checked_add(data.len() as u64)
            .filter(|&bytes| bytes <= max_bytes)
            .ok_or_else(|| {
                Status::resource_exhausted("source exceeds the configured upload limit")
            })?;
        file.write_all(&data)
            .await
            .map_err(|error| status(error.into()))?;
    }
    if stream.message().await?.is_some() {
        return Err(Status::invalid_argument(
            "unexpected frame after upload end",
        ));
    }
    file.flush().await.map_err(|error| status(error.into()))
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
        bytes: job.canonical_bytes.unwrap_or(job.bytes),
        chunks: job.chunks?,
        content_blake3: job.hash.clone()?,
        original_blake3: job.original_hash.clone().or_else(|| job.hash.clone())?,
        source_encoding: job
            .source_encoding
            .clone()
            .unwrap_or_else(|| "UTF-8".into()),
        extractor: job.extractor.clone(),
        retain_original: job.retain_original,
        url: job.url.clone().map(wire_url),
        git: job.git.clone().map(wire_git),
    })
}

fn wire_hosted_job(scope: &Scope, job: HostedJob) -> v1::IngestionJob {
    let (state, error_code, error_message) = match job.state {
        HostedJobState::Accepted => (v1::IngestionState::Accepted, None, None),
        HostedJobState::Registered => (v1::IngestionState::Registered, None, None),
        HostedJobState::TextReady => (v1::IngestionState::TextReady, None, None),
        HostedJobState::Failed { code, message } => {
            (v1::IngestionState::Failed, Some(code), Some(message))
        }
    };
    let scope = wire_scope(scope);
    let source_id = job
        .source
        .map_or_else(String::new, |source| source.to_string());
    let revision_id = job.revision.to_string();
    let external = job.external.map(wire_external);
    let text_revision = job.content.map(|content| v1::TextRevision {
        scope: Some(scope.clone()),
        source_id: source_id.clone(),
        revision_id: revision_id.clone(),
        name: job.name.clone(),
        external: external.clone(),
        bytes: content.bytes,
        chunks: content.chunks,
        content_blake3: content.hash,
        original_blake3: content.original_hash,
        source_encoding: content.encoding,
        extractor: content.extractor,
        retain_original: content.retain_original,
        url: content.url.map(wire_url),
        git: content.git.map(wire_git),
    });
    v1::IngestionJob {
        scope: Some(scope),
        operation_id: job.operation.to_string(),
        source_id,
        revision_id,
        name: job.name,
        external,
        total_bytes: job.bytes,
        processed_bytes: if state == v1::IngestionState::TextReady {
            job.bytes
        } else {
            0
        },
        state: state.into(),
        error_code,
        error_message,
        text_revision,
        update_sequence: job.update_sequence,
        created_at_unix_seconds: job.created_at,
        ..Default::default()
    }
}

fn wire_job(job: Job) -> v1::IngestionJob {
    let state = match job.state {
        JobState::Accepted => v1::IngestionState::Accepted,
        JobState::Indexing if job.extracting => v1::IngestionState::Extracting,
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
        created_at_unix_seconds: job.created_at,
        url: job.url.map(wire_url),
        git: job.git.map(wire_git),
    }
}

type Reply<T> = std::result::Result<Response<T>, Status>;

fn wire_folder(folder: WatchedFolder) -> v1::WatchedFolder {
    v1::WatchedFolder {
        id: folder.id.to_string(),
        definition: Some(v1::WatchFolderRequest {
            scope: Some(wire_scope(&folder.scope)),
            path: folder.path,
            include: folder.options.include,
            exclude: folder.options.exclude,
            prune_missing: folder.options.prune_missing,
            retain_original: folder.options.retain_original,
            interval_seconds: Some(folder.interval),
        }),
        last_scan_at_unix_seconds: folder.scanned,
        last_queued: folder.counts.queued,
        last_missing: folder.counts.missing,
        last_deleted: folder.counts.deleted,
        last_failed: folder.counts.failed,
        last_error: folder.error,
    }
}

#[allow(clippy::result_large_err)]
fn search_mode(
    raw: i32,
    default: sanctum_service::SearchMode,
) -> std::result::Result<sanctum_service::SearchMode, Status> {
    use sanctum_service::SearchMode;
    match v1::SearchMode::try_from(raw) {
        Ok(v1::SearchMode::Unspecified) => Ok(default),
        Ok(v1::SearchMode::Lexical) => Ok(SearchMode::Lexical),
        Ok(v1::SearchMode::Semantic) => Ok(SearchMode::Semantic),
        Ok(v1::SearchMode::Hybrid) => Ok(SearchMode::Hybrid),
        Err(_) => Err(Status::invalid_argument("unknown search mode")),
    }
}

fn wire_search_mode(mode: sanctum_service::SearchMode) -> v1::SearchMode {
    match mode {
        sanctum_service::SearchMode::Lexical => v1::SearchMode::Lexical,
        sanctum_service::SearchMode::Semantic => v1::SearchMode::Semantic,
        sanctum_service::SearchMode::Hybrid => v1::SearchMode::Hybrid,
    }
}

fn wire_git(git: GitProvenance) -> v1::GitProvenance {
    v1::GitProvenance {
        repository: git.repository,
        reference: git.reference,
        commit: git.commit,
        path: git.path,
        blob: git.blob,
    }
}

fn wire_url(url: UrlProvenance) -> v1::UrlProvenance {
    v1::UrlProvenance {
        requested_url: url.requested_url,
        final_url: url.final_url,
        etag: url.etag,
        last_modified: url.last_modified,
        content_type: url.content_type,
    }
}

fn wire_import_state(state: ImportState) -> v1::ImportState {
    match state {
        ImportState::Queued => v1::ImportState::Queued,
        ImportState::Pending => v1::ImportState::Pending,
        ImportState::Unchanged => v1::ImportState::Unchanged,
        ImportState::Missing => v1::ImportState::Missing,
        ImportState::Deleted => v1::ImportState::Deleted,
        ImportState::Failed => v1::ImportState::Failed,
    }
}

fn wire_import(event: ImportEvent) -> v1::ImportLocalEvent {
    v1::ImportLocalEvent {
        path: event.path,
        state: wire_import_state(event.state).into(),
        source_id: event.source.map(|id| id.to_string()),
        revision_id: event.revision.map(|id| id.to_string()),
        operation_id: event.operation.map(|id| id.to_string()),
        message: event.message,
    }
}

async fn stream_changes(
    service: Service,
    scope: Scope,
    limit: u32,
    first: v1::ListChangesResponse,
    sender: tokio::sync::mpsc::Sender<std::result::Result<v1::ListChangesResponse, Status>>,
) {
    let mut at_head = first.changes.len() < limit as usize;
    let mut cursor = first.next_cursor;
    if sender.send(Ok(first)).await.is_err() {
        return;
    }
    let mut heartbeat = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        if at_head {
            tokio::select! {
                _ = sender.closed() => return,
                _ = tokio::time::sleep(std::time::Duration::from_millis(500)) => {}
            }
        }
        let Ok(permit) = sender.reserve().await else {
            return;
        };
        match changes_page(service.clone(), scope.clone(), cursor, limit).await {
            Ok(page) => {
                at_head = page.changes.len() < limit as usize;
                if !page.changes.is_empty() || tokio::time::Instant::now() >= heartbeat {
                    cursor = page.next_cursor;
                    permit.send(Ok(page));
                    heartbeat = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
                }
            }
            Err(error) => {
                permit.send(Err(error));
                return;
            }
        }
    }
}

async fn changes_page(
    service: Service,
    scope: Scope,
    after_cursor: u64,
    limit: u32,
) -> std::result::Result<v1::ListChangesResponse, Status> {
    work(move || {
        let changes = service.changes(&scope, after_cursor, limit)?;
        let next_cursor = changes.last().map_or(after_cursor, |change| change.cursor);
        Ok(v1::ListChangesResponse {
            next_cursor,
            changes: changes
                .into_iter()
                .map(|change| v1::SourceChange {
                    cursor: change.cursor,
                    kind: change.kind,
                    source_id: change.source,
                    revision_id: change.revision,
                    previous_revision_id: change.previous_revision,
                    chunk_ordinal: change.ordinal,
                })
                .collect(),
        })
    })
    .await
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
        score: hit.score,
        database_generation: hit.database_generation.map(|id| id.to_string()),
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

#[allow(clippy::result_large_err)]
fn metadata_target(
    target: v1::MetadataTarget,
) -> std::result::Result<(Scope, Option<sanctum_service::RevisionTarget>), Status> {
    let scope = parse_scope(target.scope)?;
    let source = parse_optional_id(target.source_id, "invalid source ID")?;
    let revision = parse_optional_id(target.revision_id, "invalid revision ID")?;
    match (source, revision, target.chunk_ordinal) {
        (None, None, None) => Ok((scope, None)),
        (Some(source), Some(revision), ordinal) => Ok((
            scope,
            Some(sanctum_service::RevisionTarget {
                source,
                revision,
                ordinal,
            }),
        )),
        _ => Err(Status::invalid_argument(
            "source metadata needs both source and revision IDs",
        )),
    }
}

#[allow(clippy::result_large_err)]
fn metadata_edits(
    edits: Vec<v1::MetadataMutation>,
) -> std::result::Result<Vec<sanctum_service::MetadataMutation>, Status> {
    if !(1..=32).contains(&edits.len()) {
        return Err(Status::invalid_argument(
            "metadata batch must contain 1 to 32 mutations",
        ));
    }
    let mut result = Vec::with_capacity(edits.len());
    use sanctum_service::MetadataEdit;
    use v1::metadata_mutation::Edit;
    for edit in edits {
        let parsed = match edit.edit {
            Some(Edit::SetJson(json)) => MetadataEdit::Set(parse_metadata_json(&json)?),
            Some(Edit::MergePatchJson(json)) => MetadataEdit::Patch(parse_metadata_json(&json)?),
            Some(Edit::Remove(_)) => MetadataEdit::Remove,
            None => return Err(Status::invalid_argument("metadata edit is required")),
        };
        result.push(sanctum_service::MetadataMutation {
            namespace: edit.namespace,
            key: edit.key,
            edit: parsed,
        });
    }
    Ok(result)
}

#[allow(clippy::result_large_err)]
fn parse_metadata_json(json: &str) -> std::result::Result<serde_json::Value, Status> {
    if json.len() > 64 * 1024 {
        return Err(Status::invalid_argument("metadata value exceeds 64 KiB"));
    }
    serde_json::from_str(json)
        .map_err(|error| Status::invalid_argument(format!("invalid metadata JSON: {error}")))
}

fn metadata_response(entries: Vec<sanctum_service::MetadataEntry>) -> v1::MetadataResponse {
    v1::MetadataResponse {
        entries: entries
            .into_iter()
            .map(|entry| v1::MetadataEntry {
                namespace: entry.namespace,
                key: entry.key,
                value_json: entry.json,
            })
            .collect(),
    }
}
