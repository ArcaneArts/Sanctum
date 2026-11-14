use sanctum_core::{
    Error, ExternalSource, GitProvenance, Result, Scope, UPLOAD_FRAME_BYTES, UrlProvenance,
    UsageMeasurement, UsageScope,
};
use sanctum_proto::v1::{
    self,
    sanctum_server::{Sanctum, SanctumServer},
    upload_text_frame::Frame,
};
use sanctum_service::{
    ChunkRef, GitRepository, Hit, HostedCatalog, HostedJob, HostedJobState, ImportEvent,
    ImportOptions, ImportState, Job, JobState, Replacement, ScanOptions, ScanStop, Service,
    TextUpload, WatchedFolder,
};
use tokio::io::AsyncWriteExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Code, Request, Response, Status, Streaming};

mod database;
mod error;
mod metadata_filter;
mod metadata_indexes;

use error::{from_error, invalid_argument, operation_status, status};

pub fn grpc(service: Service, shutdown: CancellationToken) -> SanctumServer<Grpc> {
    let runtime = v1::RuntimeInfo {
        storage: "filesystem".into(),
        role: "local".into(),
        max_source_bytes: service.max_source_bytes(),
        url_imports: true,
        ..Default::default()
    };
    grpc_with_runtime(service, shutdown, runtime)
}

pub fn grpc_with_runtime(
    service: Service,
    shutdown: CancellationToken,
    runtime: v1::RuntimeInfo,
) -> SanctumServer<Grpc> {
    // Metadata batches can contain 32 values of up to 64 KiB each.
    SanctumServer::new(Grpc(Backend::Local(service), shutdown, runtime))
        .max_decoding_message_size(3 * 1024 * 1024)
}

pub fn hosted_grpc(catalog: HostedCatalog, shutdown: CancellationToken) -> SanctumServer<Grpc> {
    let runtime = v1::RuntimeInfo {
        storage: "object".into(),
        role: "hosted".into(),
        max_source_bytes: catalog.max_source_bytes(),
        ..Default::default()
    };
    hosted_grpc_with_runtime(catalog, shutdown, runtime)
}

pub fn hosted_grpc_with_runtime(
    catalog: HostedCatalog,
    shutdown: CancellationToken,
    runtime: v1::RuntimeInfo,
) -> SanctumServer<Grpc> {
    SanctumServer::new(Grpc(Backend::Hosted(catalog), shutdown, runtime))
        .max_decoding_message_size(3 * 1024 * 1024)
}

#[derive(Clone)]
enum Backend {
    Local(Service),
    Hosted(HostedCatalog),
}

pub struct Grpc(Backend, CancellationToken, v1::RuntimeInfo);

impl Backend {
    async fn measure_storage(
        &self,
        scope: UsageScope,
        progress: impl FnMut(u64, u64) -> Result<()> + Send + 'static,
    ) -> std::result::Result<UsageMeasurement, Status> {
        match self {
            Self::Hosted(catalog) => catalog
                .storage_usage_with_progress(&scope, progress)
                .await
                .map_err(status),
            Self::Local(service) => {
                let service = service.clone();
                work(move || service.storage_usage_with_progress(&scope, progress)).await
            }
        }
    }
}

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
    async fn get_runtime_info(&self, _: Request<v1::Empty>) -> Reply<v1::RuntimeInfo> {
        let mut info = self.2.clone();
        info.version = env!("CARGO_PKG_VERSION").into();
        info.status = if self.1.is_cancelled() {
            "stopping"
        } else {
            "serving"
        }
        .into();
        let mut capabilities = vec![
            "catalog",
            "upload",
            "ingestion_activity",
            "source_inspection",
            "retrieval",
            "pending_literal_scan",
            "metadata",
            "metadata_indexes",
            "changes",
            "database_sessions",
            "storage_usage",
        ];
        if matches!(self.0, Backend::Local(_)) {
            capabilities.extend(["local_queue", "local_import", "watched_folders"]);
            if info.url_imports {
                capabilities.extend(["url_import", "git_import_https"]);
            }
            capabilities.push("git_import_local");
        }
        if info.mcp {
            capabilities.push("mcp");
        }
        if info.embedding_profile_id.is_some() {
            capabilities.push("semantic_search");
        }
        info.capabilities = capabilities.into_iter().map(str::to_owned).collect();
        Ok(Response::new(info))
    }

    async fn open_database_session(
        &self,
        request: Request<v1::DatabaseTarget>,
    ) -> Reply<v1::DatabaseSession> {
        database::open(&self.0, self.1.child_token(), request.into_inner()).await
    }

    async fn query_database(
        &self,
        request: Request<v1::QueryDatabaseRequest>,
    ) -> Reply<v1::QueryDatabaseResponse> {
        database::query(&self.0, self.1.child_token(), request.into_inner()).await
    }

    type DownloadDatabaseStream = ReceiverStream<std::result::Result<v1::DatabaseFrame, Status>>;

    async fn download_database(
        &self,
        request: Request<v1::DatabaseSessionRef>,
    ) -> Reply<Self::DownloadDatabaseStream> {
        database::download(&self.0, self.1.clone(), request.into_inner()).await
    }

    async fn close_database_session(
        &self,
        request: Request<v1::DatabaseSessionRef>,
    ) -> Reply<v1::Empty> {
        database::close(&self.0, request.into_inner()).await
    }

    async fn get_storage_usage(
        &self,
        request: Request<v1::StorageUsageRequest>,
    ) -> Reply<v1::StorageUsage> {
        let request = request.into_inner();
        let scope = usage_scope(&request)?;
        let measured = self.0.measure_storage(scope, |_, _| Ok(())).await?;
        wire_usage(request, measured).map(Response::new)
    }

    type MeasureStorageUsageStream =
        ReceiverStream<std::result::Result<v1::StorageUsageEvent, Status>>;

    async fn measure_storage_usage(
        &self,
        request: Request<v1::StorageUsageRequest>,
    ) -> Reply<Self::MeasureStorageUsageStream> {
        use v1::storage_usage_event::Event;
        let request = request.into_inner();
        let scope = usage_scope(&request)?;
        let backend = self.0.clone();
        let shutdown = self.1.clone();
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        tokio::spawn(async move {
            let progress = sender.clone();
            let cancel = shutdown.clone();
            let scan = backend.measure_storage(scope, move |bytes, files| {
                if cancel.is_cancelled() || progress.is_closed() {
                    return Err(Error::Busy("storage measurement was canceled".into()));
                }
                // Progress is cumulative. Skip intermediate reports while the client is slow.
                let _ = progress.try_send(Ok(v1::StorageUsageEvent {
                    event: Some(Event::Progress(v1::StorageUsageProgress { bytes, files })),
                }));
                Ok(())
            });
            let result = tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = sender.closed() => return,
                result = scan => result,
            };
            let finished = result
                .and_then(|measured| wire_usage(request, measured))
                .map(|usage| v1::StorageUsageEvent {
                    event: Some(Event::Complete(usage)),
                });
            let _ = sender.send(finished).await;
        });
        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    async fn get_metadata(
        &self,
        request: Request<v1::MetadataTarget>,
    ) -> Reply<v1::MetadataResponse> {
        let (scope, target) = metadata_target(request.into_inner())?;
        let entries = match &self.0 {
            Backend::Local(service) => {
                let service = service.clone();
                work(move || service.metadata(&scope, target, None)).await?
            }
            Backend::Hosted(catalog) => catalog.metadata(&scope, target).await.map_err(status)?,
        };
        Ok(Response::new(metadata_response(entries)))
    }

    async fn mutate_metadata(
        &self,
        request: Request<v1::MutateMetadataRequest>,
    ) -> Reply<v1::MetadataResponse> {
        let request = request.into_inner();
        let (scope, target) = metadata_target(
            request
                .target
                .ok_or_else(|| invalid_argument("target", "metadata target is required"))?,
        )?;
        let edits = metadata_edits(request.edits)?;
        let entries = match &self.0 {
            Backend::Local(service) => {
                let service = service.clone();
                work(move || service.metadata(&scope, target, Some(&edits))).await?
            }
            Backend::Hosted(catalog) => catalog
                .mutate_metadata(&scope, target, edits)
                .await
                .map_err(status)?,
        };
        Ok(Response::new(metadata_response(entries)))
    }

    async fn list_metadata_indexes(
        &self,
        request: Request<v1::DatabaseTarget>,
    ) -> Reply<v1::ListMetadataIndexesResponse> {
        metadata_indexes::list(&self.0, request.into_inner()).await
    }

    async fn create_metadata_index(
        &self,
        request: Request<v1::CreateMetadataIndexRequest>,
    ) -> Reply<v1::Empty> {
        metadata_indexes::create(&self.0, request.into_inner()).await
    }

    async fn drop_metadata_index(
        &self,
        request: Request<v1::DropMetadataIndexRequest>,
    ) -> Reply<v1::Empty> {
        metadata_indexes::drop(&self.0, request.into_inner()).await
    }

    async fn query_metadata(
        &self,
        request: Request<v1::QueryMetadataRequest>,
    ) -> Reply<v1::QueryMetadataResponse> {
        use sanctum_service::metadata_query::Results;
        let request = request.into_inner();
        let tenant: sanctum_core::ScopeId = request.tenant.parse().map_err(status)?;
        let collection = request
            .collection
            .as_ref()
            .map(|id| id.parse())
            .transpose()
            .map_err(status)?;
        let query = metadata_filter::parse_query(request).map_err(status)?;
        let page = match &self.0 {
            Backend::Local(service) => {
                let service = service.clone();
                let tenant = tenant.clone();
                work(move || service.query_metadata(&tenant, collection, query)).await?
            }
            Backend::Hosted(catalog) => catalog
                .query_metadata(&tenant, collection, query)
                .await
                .map_err(status)?,
        };
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
        Ok(Response::new(reply))
    }

    async fn list_changes(
        &self,
        request: Request<v1::ListChangesRequest>,
    ) -> Reply<v1::ListChangesResponse> {
        let request = request.into_inner();
        let scope = parse_scope(request.scope)?;
        changes_page(
            self.0.clone(),
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
        let backend = self.0.clone();
        let first =
            changes_page(backend.clone(), scope.clone(), request.after_cursor, limit).await?;
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let shutdown = self.1.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown.cancelled() => {}
                _ = stream_changes(backend, scope, limit, first, sender) => {}
            }
        });
        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    async fn get_change_cursor(
        &self,
        request: Request<v1::ChangeCursorRequest>,
    ) -> Reply<v1::ChangeCursor> {
        let scope = parse_scope(request.into_inner().scope)?;
        let cursor = match &self.0 {
            Backend::Local(service) => {
                let service = service.clone();
                work(move || service.change_cursor(&scope)).await?
            }
            Backend::Hosted(catalog) => catalog.change_cursor(&scope).await.map_err(status)?,
        };
        Ok(Response::new(v1::ChangeCursor { cursor }))
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
            _ => return Err(invalid_argument("repository", "Git repository is required")),
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
            return Err(invalid_argument("path", "import path is required"));
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
        let after = parse_optional_id(
            request.after_id,
            "after_id",
            "invalid watched folder cursor",
        )?;
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
            .map_err(|_| invalid_argument("id", "invalid watched folder ID"))?;
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
        let operation = parse_optional_id(
            request.request_id,
            "request_id",
            "request ID must be a UUID",
        )?;
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
            ..Default::default()
        }))
    }

    async fn update_collection(
        &self,
        request: Request<v1::UpdateCollectionRequest>,
    ) -> Reply<v1::Collection> {
        let request = request.into_inner();
        let scope = parse_scope(request.scope)?;
        let row = match &self.0 {
            Backend::Local(service) => {
                let service = service.clone();
                let scope = scope.clone();
                work(move || service.update_collection(&scope, &request.display_name)).await?
            }
            Backend::Hosted(catalog) => catalog
                .update_collection(&scope, request.display_name)
                .await
                .map_err(status)?,
        };
        Ok(Response::new(wire_collection(&scope.tenant, row)))
    }

    async fn delete_collection(
        &self,
        request: Request<v1::DeleteCollectionRequest>,
    ) -> Reply<v1::Empty> {
        let request = request.into_inner();
        let scope = parse_scope(request.scope)?;
        let operation = parse_optional_id(
            request.request_id,
            "request_id",
            "request ID must be a UUID",
        )?;
        match &self.0 {
            Backend::Hosted(catalog) => catalog
                .delete_collection(&scope, operation)
                .await
                .map_err(status)?,
            Backend::Local(service) => {
                if operation.is_some() {
                    return Err(invalid_argument(
                        "request_id",
                        "deletion request IDs are supported by hosted storage",
                    ));
                }
                let service = service.clone();
                work(move || service.delete_collection(&scope)).await?;
            }
        }
        Ok(Response::new(v1::Empty {}))
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
            return Err(invalid_argument(
                "limit",
                "collection page limit must be between 1 and 99",
            ));
        }
        let mut rows = match &self.0 {
            Backend::Hosted(catalog) => catalog
                .list_collection_details(&tenant, after.as_ref(), limit + 1)
                .await
                .map_err(status)?,
            Backend::Local(service) => {
                let service = service.clone();
                let tenant = tenant.clone();
                work(move || {
                    service.list_collection_details(
                        &tenant,
                        after.as_ref(),
                        paginated.then_some(limit + 1),
                    )
                })
                .await?
            }
        };
        let next_after = if paginated && rows.len() > limit as usize {
            rows.pop();
            rows.last().map(|row| row.id.clone())
        } else {
            None
        };
        let collections = rows
            .into_iter()
            .map(|row| wire_collection(&tenant, row))
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
        let stream = request.into_inner();
        let service = match &self.0 {
            Backend::Local(service) => service,
            Backend::Hosted(catalog) => {
                let (scope, job) = submit_hosted(catalog, stream).await?;
                return wait_hosted_upload(catalog, &scope, job)
                    .await
                    .map(Response::new);
            }
        };
        let mut job = submit_local(service, stream).await?;
        let operation = job.id;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            match job.state {
                JobState::TextReady => {
                    return text_revision(&job)
                        .map(Response::new)
                        .ok_or_else(|| missing_upload_receipt(operation));
                }
                JobState::Failed => {
                    let failure = job.failure.ok_or_else(|| {
                        operation_status(
                            Code::Internal,
                            "ingestion failed",
                            "internal",
                            false,
                            operation,
                        )
                    })?;
                    let code = match failure.code.as_str() {
                        "invalid_source" => Code::InvalidArgument,
                        "conflict" => Code::AlreadyExists,
                        _ => Code::Unavailable,
                    };
                    return Err(operation_status(
                        code,
                        failure.message,
                        &failure.code,
                        failure.code == "retryable",
                        operation,
                    ));
                }
                JobState::Canceled => {
                    return Err(operation_status(
                        Code::Cancelled,
                        "ingestion canceled",
                        "canceled",
                        false,
                        operation,
                    ));
                }
                _ => {}
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(upload_wait_expired(operation));
            }
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            let service = service.clone();
            job = work_for_operation(Some(operation), move || service.job(&job.scope, operation))
                .await?;
        }
    }

    async fn submit_text(
        &self,
        request: Request<Streaming<v1::UploadTextFrame>>,
    ) -> Reply<v1::IngestionJob> {
        let stream = request.into_inner();
        let job = match &self.0 {
            Backend::Local(service) => wire_job(submit_local(service, stream).await?),
            Backend::Hosted(catalog) => {
                let (scope, accepted) = submit_hosted(catalog, stream).await?;
                wire_hosted_job(&scope, accepted)
            }
        };
        Ok(Response::new(job))
    }

    async fn get_ingestion_job(
        &self,
        request: Request<v1::IngestionJobRequest>,
    ) -> Reply<v1::IngestionJob> {
        let request = request.into_inner();
        if let Backend::Hosted(catalog) = &self.0 {
            let (scope, id) = job_target(request)?;
            let accepted = catalog
                .ingestion_job(&scope, id)
                .await
                .map_err(|error| from_error(error, Some(id)))?;
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
        let limit = request.limit.unwrap_or(25);
        let response = match &self.0 {
            Backend::Hosted(catalog) => {
                if request.before_operation_id.is_some() {
                    return Err(invalid_argument(
                        "before_operation_id",
                        "hosted activity uses page_token",
                    ));
                }
                let page = catalog
                    .ingestion_activity(
                        &tenant,
                        collection.as_ref(),
                        request.page_token.as_deref(),
                        limit,
                    )
                    .await
                    .map_err(status)?;
                v1::ListIngestionJobsResponse {
                    jobs: page
                        .jobs
                        .into_iter()
                        .map(|(scope, job)| wire_hosted_job(&scope, job))
                        .collect(),
                    next_page_token: page.next,
                    ..Default::default()
                }
            }
            Backend::Local(service) => {
                if request.page_token.is_some() {
                    return Err(invalid_argument(
                        "page_token",
                        "local activity uses before_operation_id",
                    ));
                }
                let before = parse_optional_id(
                    request.before_operation_id,
                    "before_operation_id",
                    "invalid activity cursor",
                )?;
                let service = service.clone();
                let (jobs, next) =
                    work(move || service.activity(&tenant, collection.as_ref(), before, limit))
                        .await?;
                v1::ListIngestionJobsResponse {
                    jobs: jobs.into_iter().map(wire_job).collect(),
                    next_before_operation_id: next.map(|id| id.to_string()),
                    ..Default::default()
                }
            }
        };
        Ok(Response::new(response))
    }

    async fn get_embedding_status(
        &self,
        request: Request<v1::EmbeddingStatusRequest>,
    ) -> Reply<v1::EmbeddingStatus> {
        let request = request.into_inner();
        let scope = parse_scope(request.scope)?;
        let source = parse_optional_id(request.source_id, "source_id", "invalid source ID")?;
        let revision =
            parse_optional_id(request.revision_id, "revision_id", "invalid revision ID")?;
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
        let request = request.into_inner();
        if let Backend::Hosted(catalog) = &self.0 {
            let (scope, id) = job_target(request)?;
            let job = catalog
                .cancel_upload(&scope, id)
                .await
                .map_err(|error| from_error(error, Some(id)))?;
            return Ok(Response::new(wire_hosted_job(&scope, job)));
        }
        job_action(self.local()?, request, Service::cancel_job).await
    }

    async fn retry_ingestion_job(
        &self,
        request: Request<v1::IngestionJobRequest>,
    ) -> Reply<v1::IngestionJob> {
        let request = request.into_inner();
        if let Backend::Hosted(catalog) = &self.0 {
            let (scope, id) = job_target(request)?;
            let job = catalog
                .retry_upload(&scope, id)
                .await
                .map_err(|error| from_error(error, Some(id)))?;
            return Ok(Response::new(wire_hosted_job(&scope, job)));
        }
        job_action(self.local()?, request, Service::retry_job).await
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

    type ScanPendingTextStream =
        ReceiverStream<std::result::Result<v1::PendingTextScanEvent, Status>>;

    async fn scan_pending_text(
        &self,
        request: Request<v1::ScanPendingTextRequest>,
    ) -> Reply<Self::ScanPendingTextStream> {
        use v1::pending_text_scan_event::Event;
        let request = request.into_inner();
        let (scope, operation) = job_target(
            request
                .job
                .ok_or_else(|| invalid_argument("job", "job is required"))?,
        )?;
        let options = ScanOptions::new(
            request.literal,
            request.byte_limit.unwrap_or(1024 * 1024),
            request.hit_limit.unwrap_or(10),
        )
        .map_err(status)?;
        let pending = match &self.0 {
            Backend::Local(service) => {
                let service = service.clone();
                let scope = scope.clone();
                work_for_operation(Some(operation), move || {
                    service.pending_text(&scope, operation)
                })
                .await?
            }
            Backend::Hosted(catalog) => catalog
                .pending_text(&scope, operation)
                .await
                .map_err(|error| from_error(error, Some(operation)))?,
        };
        let source = Event::Source(v1::PendingTextScanSource {
            scope: Some(wire_scope(&scope)),
            operation_id: operation.to_string(),
            source_id: pending.source.map(|source| source.to_string()),
            revision_id: pending.revision.to_string(),
            name: pending.name.clone(),
            external: pending.external.clone().map(wire_external),
            total_bytes: pending.total_bytes,
        });
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let shutdown = self.1.clone();
        tokio::spawn(async move {
            let result = tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = sender.closed() => return,
                result = async {
                    if sender.send(Ok(v1::PendingTextScanEvent { event: Some(source) })).await.is_err() {
                        return Ok(());
                    }
                    pending.scan(&options, |page| {
                        let stop = match page.stop {
                            Some(ScanStop::Complete) => v1::PendingTextScanStop::Complete,
                            Some(ScanStop::ByteLimit) => v1::PendingTextScanStop::ByteLimit,
                            Some(ScanStop::HitLimit) => v1::PendingTextScanStop::HitLimit,
                            Some(ScanStop::InvalidText) => v1::PendingTextScanStop::InvalidText,
                            Some(ScanStop::ExtractionPending) => v1::PendingTextScanStop::ExtractionPending,
                            None => v1::PendingTextScanStop::Unspecified,
                        };
                        let event = Event::Progress(v1::PendingTextScanProgress {
                            scanned_bytes: page.scanned_bytes,
                            matches: page.hits.into_iter().map(|range| v1::PendingTextMatch {
                                start_byte: range.start,
                                end_byte: range.end,
                            }).collect(),
                            stop: stop.into(),
                            read_bytes: page.read_bytes,
                        });
                        let sender = sender.clone();
                        async move { sender.send(Ok(v1::PendingTextScanEvent { event: Some(event) })).await.is_ok() }
                    }).await
                } => result,
            };
            if let Err(error) = result {
                let _ = sender.send(Err(from_error(error, Some(operation)))).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(receiver)))
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
        let limit = request.limit.unwrap_or(10);
        let result = match &self.0 {
            Backend::Local(service) => {
                service
                    .retrieve_collections(tenant, collections, request.query, limit, filter, mode)
                    .await
            }
            Backend::Hosted(service) => {
                service
                    .retrieve_collections(tenant, collections, request.query, limit, filter, mode)
                    .await
            }
        }
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
        let (scope, reference) = parse_chunk(request.into_inner().chunk)?;
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

    async fn fetch_context(
        &self,
        request: Request<v1::FetchContextRequest>,
    ) -> Reply<v1::TextContext> {
        let request = request.into_inner();
        let (scope, reference) = parse_chunk(request.chunk)?;
        let before = request.before_bytes;
        let after = request.after_bytes;
        let context = match &self.0 {
            Backend::Local(service) => {
                let service = service.clone();
                let scope = scope.clone();
                work(move || service.fetch_context(&scope, &reference, before, after)).await?
            }
            Backend::Hosted(service) => service
                .fetch_context(&scope, reference, before, after)
                .await
                .map_err(status)?,
        };
        Ok(Response::new(v1::TextContext {
            hit: Some(wire_hit(&scope, context.hit)),
            start_byte: context.start_byte,
            end_byte: context.end_byte,
            text: context.text,
        }))
    }

    async fn list_sources(
        &self,
        request: Request<v1::ListSourcesRequest>,
    ) -> Reply<v1::ListSourcesResponse> {
        let request = request.into_inner();
        let scope = parse_scope(request.scope)?;
        let after = parse_optional_id(
            request.after_source_id,
            "after_source_id",
            "invalid source cursor",
        )?;
        let limit = request.limit.unwrap_or(25);
        if !(1..=100).contains(&limit) {
            return Err(invalid_argument(
                "limit",
                "source page limit must be between 1 and 100",
            ));
        }
        let (mut sources, generation) = match &self.0 {
            Backend::Local(service) => {
                let service = service.clone();
                let scope = scope.clone();
                (
                    work(move || service.list_sources(&scope, after, limit + 1)).await?,
                    None,
                )
            }
            Backend::Hosted(catalog) => {
                let (rows, generation) = catalog
                    .list_sources(&scope, after, limit + 1)
                    .await
                    .map_err(status)?;
                (rows, Some(generation.to_string()))
            }
        };
        let next_after_source_id = if sources.len() > limit as usize {
            sources.pop();
            sources.last().map(|revision| revision.source.to_string())
        } else {
            None
        };
        Ok(Response::new(v1::ListSourcesResponse {
            sources: sources
                .into_iter()
                .map(|revision| wire_revision(&scope, revision))
                .collect(),
            next_after_source_id,
            database_generation: generation,
        }))
    }

    async fn get_source(
        &self,
        request: Request<v1::GetSourceRequest>,
    ) -> Reply<v1::GetSourceResponse> {
        let request = request.into_inner();
        let scope = parse_scope(request.scope)?;
        let source = request
            .source_id
            .parse()
            .map_err(|_| invalid_argument("source_id", "invalid source ID"))?;
        let expected = parse_optional_id(
            request.expected_revision_id,
            "expected_revision_id",
            "invalid expected revision ID",
        )?;
        let (revision, generation) = match &self.0 {
            Backend::Local(service) => {
                let service = service.clone();
                let scope = scope.clone();
                (
                    work(move || service.source(&scope, source, expected)).await?,
                    None,
                )
            }
            Backend::Hosted(catalog) => {
                let (revision, generation) = catalog
                    .source(&scope, source, expected)
                    .await
                    .map_err(status)?;
                (revision, Some(generation.to_string()))
            }
        };
        Ok(Response::new(v1::GetSourceResponse {
            current_revision: Some(wire_revision(&scope, revision)),
            database_generation: generation,
        }))
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
            .map_err(|_| invalid_argument("source_id", "invalid source ID"))?;
        let expected = parse_optional_id(
            request.expected_revision_id,
            "expected_revision_id",
            "invalid expected revision ID",
        )?;
        let request_id = parse_optional_id(request.request_id, "request_id", "invalid request ID")?;
        let revision = match &self.0 {
            Backend::Local(service) => {
                if request_id.is_some() {
                    return Err(invalid_argument(
                        "request_id",
                        "deletion request IDs are supported by hosted storage",
                    ));
                }
                let service = service.clone();
                work(move || service.delete_source(&scope, source, expected)).await?
            }
            Backend::Hosted(service) => service
                .delete_source(&scope, source, expected, request_id)
                .await
                .map_err(status)?,
        };
        Ok(Response::new(v1::DeletedSource {
            source_id: source.to_string(),
            revision_id: revision.to_string(),
        }))
    }
}

#[allow(clippy::result_large_err)]
fn usage_scope(request: &v1::StorageUsageRequest) -> std::result::Result<UsageScope, Status> {
    use v1::storage_usage_request::Scope as WireScope;
    match &request.scope {
        Some(WireScope::Collection(scope)) => {
            parse_scope(Some(scope.clone())).map(UsageScope::Collection)
        }
        Some(WireScope::Tenant(tenant)) => tenant.parse().map(UsageScope::Tenant).map_err(status),
        Some(WireScope::Cluster(_)) => Ok(UsageScope::Cluster),
        None => Err(invalid_argument("scope", "storage usage scope is required")),
    }
}

#[allow(clippy::result_large_err)]
fn wire_usage(
    request: v1::StorageUsageRequest,
    measured: UsageMeasurement,
) -> std::result::Result<v1::StorageUsage, Status> {
    Ok(v1::StorageUsage {
        scope: Some(request),
        bytes: measured.bytes,
        files: measured.files,
        started_at_unix_ms: unix_ms(measured.started_at).map_err(status)?,
        finished_at_unix_ms: unix_ms(measured.finished_at).map_err(status)?,
    })
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
    let service = service.clone();
    let max_bytes = service.max_source_bytes();
    let (staged, file) = work_for_operation(request_id, move || {
        let staged = service.stage_upload(scope, upload)?;
        let file = staged.reopen()?;
        Ok((staged, file))
    })
    .await?;
    receive_upload(&mut stream, file, max_bytes).await?;
    work_for_operation(request_id, move || staged.enqueue_with_request(request_id)).await
}

async fn submit_hosted(
    catalog: &HostedCatalog,
    mut stream: Streaming<v1::UploadTextFrame>,
) -> std::result::Result<(Scope, HostedJob), Status> {
    let (scope, upload, operation) = read_upload_header(&mut stream).await?;
    let staged = catalog
        .stage_upload(scope.clone(), upload, operation)
        .await
        .map_err(|error| from_error(error, operation))?;
    let file = staged
        .reopen()
        .map_err(|error| from_error(error, operation))?;
    receive_upload(&mut stream, file, catalog.max_source_bytes()).await?;
    let accepted = staged
        .enqueue()
        .await
        .map_err(|error| from_error(error, operation))?;
    Ok((scope, accepted))
}

async fn wait_hosted_upload(
    catalog: &HostedCatalog,
    scope: &Scope,
    mut job: HostedJob,
) -> std::result::Result<v1::TextRevision, Status> {
    use std::time::Duration;
    let operation = job.operation;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        match job.state {
            HostedJobState::TextReady => {
                return wire_hosted_job(scope, job)
                    .text_revision
                    .ok_or_else(|| missing_upload_receipt(operation));
            }
            HostedJobState::Failed { code, message } => {
                let grpc_code = match code.as_str() {
                    "invalid_source" | "invalid_argument" => Code::InvalidArgument,
                    "conflict" => Code::AlreadyExists,
                    "not_found" => Code::NotFound,
                    "stale_revision" | "expired" => Code::FailedPrecondition,
                    _ => Code::Unavailable,
                };
                return Err(operation_status(
                    grpc_code, message, &code, false, operation,
                ));
            }
            HostedJobState::Canceled => {
                return Err(operation_status(
                    Code::Cancelled,
                    "upload was canceled",
                    "canceled",
                    false,
                    operation,
                ));
            }
            HostedJobState::Accepted | HostedJobState::Registered => {}
        }
        let pending = async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            catalog.ingestion_job(scope, operation).await
        };
        job = match tokio::time::timeout_at(deadline, pending).await {
            Ok(Ok(job)) => job,
            Ok(Err(error)) => return Err(from_error(error, Some(operation))),
            Err(_) => return Err(upload_wait_expired(operation)),
        };
    }
}

fn upload_wait_expired(operation: uuid::Uuid) -> Status {
    operation_status(
        Code::DeadlineExceeded,
        format!("upload is still processing; query operation {operation}"),
        "upload_pending",
        true,
        operation,
    )
}

fn missing_upload_receipt(operation: uuid::Uuid) -> Status {
    operation_status(
        Code::Internal,
        "completed revision receipt is missing",
        "internal",
        false,
        operation,
    )
}

async fn read_upload_header(
    stream: &mut Streaming<v1::UploadTextFrame>,
) -> std::result::Result<(Scope, TextUpload, Option<uuid::Uuid>), Status> {
    let header = match stream.message().await?.and_then(|message| message.frame) {
        Some(Frame::Header(header)) => header,
        _ => {
            return Err(invalid_argument(
                "frame",
                "first upload frame must be a header",
            ));
        }
    };
    let scope = parse_scope_fields(
        header.scope,
        "header.scope",
        "header.scope.tenant",
        "header.scope.collection",
    )?;
    let replacement = if let Some(replacement) = header.replacement {
        Some(Replacement {
            source: parse_optional_id(
                replacement.source_id,
                "header.replacement.source_id",
                "invalid source ID",
            )?,
            expected_revision: parse_optional_id(
                replacement.expected_revision_id,
                "header.replacement.expected_revision_id",
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
        initial_metadata: initial_metadata(header.source_metadata)?,
        initial_chunk_metadata: header
            .chunk_metadata
            .into_iter()
            .map(|chunk| {
                Ok(sanctum_service::ChunkMetadata {
                    ordinal: chunk.ordinal,
                    entries: initial_metadata(chunk.entries)?,
                })
            })
            .collect::<std::result::Result<_, Status>>()?,
    };
    let request_id =
        parse_optional_id(header.request_id, "header.request_id", "invalid request ID")?;
    Ok((scope, upload, request_id))
}

#[allow(clippy::result_large_err)]
fn initial_metadata(
    entries: Vec<v1::MetadataEntry>,
) -> std::result::Result<Vec<sanctum_service::MetadataMutation>, Status> {
    entries
        .into_iter()
        .map(|entry| {
            Ok(sanctum_service::MetadataMutation {
                namespace: entry.namespace,
                key: entry.key,
                edit: sanctum_service::MetadataEdit::Set(parse_metadata_json(&entry.value_json)?),
            })
        })
        .collect()
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
                return Err(invalid_argument(
                    "frame",
                    "expected data or explicit upload end",
                ));
            }
        };
        if data.len() > UPLOAD_FRAME_BYTES {
            return Err(invalid_argument("data", "upload frame exceeds 256 KiB"));
        }
        written = written
            .checked_add(data.len() as u64)
            .filter(|&bytes| bytes <= max_bytes)
            .ok_or_else(error::upload_limit)?;
        file.write_all(&data)
            .await
            .map_err(|error| status(error.into()))?;
    }
    if stream.message().await?.is_some() {
        return Err(invalid_argument(
            "frame",
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
    let (scope, id) = job_target(request)?;
    let service = service.clone();
    work_for_operation(Some(id), move || action(&service, &scope, id))
        .await
        .map(wire_job)
        .map(Response::new)
}

#[allow(clippy::result_large_err)]
fn job_target(
    request: v1::IngestionJobRequest,
) -> std::result::Result<(Scope, uuid::Uuid), Status> {
    let scope = parse_scope(request.scope)?;
    let id = request
        .operation_id
        .parse()
        .map_err(|_| invalid_argument("operation_id", "invalid operation ID"))?;
    Ok((scope, id))
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
        chunker: job.chunking.identifier(),
    })
}

fn wire_revision(scope: &Scope, revision: sanctum_service::Revision) -> v1::TextRevision {
    let content = revision.content;
    v1::TextRevision {
        scope: Some(wire_scope(scope)),
        source_id: revision.source.to_string(),
        revision_id: revision.revision.to_string(),
        name: revision.name,
        external: revision.external.map(wire_external),
        bytes: content.bytes,
        chunks: content.chunks,
        content_blake3: content.hash,
        original_blake3: content.original_hash,
        source_encoding: content.encoding,
        extractor: content.extractor,
        retain_original: content.retain_original,
        url: content.url.map(wire_url),
        git: content.git.map(wire_git),
        chunker: content.chunker,
    }
}

fn wire_hosted_job(scope: &Scope, job: HostedJob) -> v1::IngestionJob {
    let (state, error_code, error_message) = match job.state {
        HostedJobState::Accepted => (v1::IngestionState::Accepted, None, None),
        HostedJobState::Registered => (v1::IngestionState::Registered, None, None),
        HostedJobState::TextReady => (v1::IngestionState::TextReady, None, None),
        HostedJobState::Canceled => (v1::IngestionState::Canceled, None, None),
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
        chunker: content.chunker,
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
        embedding_work: job.embedding_work.map(|work| v1::HostedEmbeddingWork {
            next_batch: work.next_batch,
            not_before_unix_seconds: work.not_before,
        }),
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
        embedding_work: None,
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
        Err(_) => Err(invalid_argument("mode", "unknown search mode")),
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
    backend: Backend,
    scope: Scope,
    limit: u32,
    first: v1::ListChangesResponse,
    sender: tokio::sync::mpsc::Sender<std::result::Result<v1::ListChangesResponse, Status>>,
) {
    let interval = match backend {
        Backend::Local(_) => std::time::Duration::from_millis(500),
        Backend::Hosted(_) => std::time::Duration::from_secs(2),
    };
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
                _ = tokio::time::sleep(interval) => {}
            }
        }
        let Ok(permit) = sender.reserve().await else {
            return;
        };
        match changes_page(backend.clone(), scope.clone(), cursor, limit).await {
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
    backend: Backend,
    scope: Scope,
    after_cursor: u64,
    limit: u32,
) -> std::result::Result<v1::ListChangesResponse, Status> {
    let changes = match backend {
        Backend::Local(service) => {
            work(move || service.changes(&scope, after_cursor, limit)).await?
        }
        Backend::Hosted(catalog) => catalog
            .changes(&scope, after_cursor, limit)
            .await
            .map_err(status)?,
    };
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
}

async fn work<T: Send + 'static>(
    operation: impl FnOnce() -> Result<T> + Send + 'static,
) -> std::result::Result<T, Status> {
    work_for_operation(None, operation).await
}

async fn work_for_operation<T: Send + 'static>(
    id: Option<uuid::Uuid>,
    operation: impl FnOnce() -> Result<T> + Send + 'static,
) -> std::result::Result<T, Status> {
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| {
            tracing::error!(%error, "blocking operation failed");
            Status::internal("server operation failed")
        })?
        .map_err(|error| from_error(error, id))
}

#[allow(clippy::result_large_err)]
fn parse_chunk(reference: Option<v1::ChunkRef>) -> std::result::Result<(Scope, ChunkRef), Status> {
    let reference = reference.ok_or_else(|| invalid_argument("chunk", "chunk is required"))?;
    let scope = parse_scope_fields(
        reference.scope,
        "chunk.scope",
        "chunk.scope.tenant",
        "chunk.scope.collection",
    )?;
    if reference.ordinal > i64::MAX as u64 {
        return Err(invalid_argument(
            "chunk.ordinal",
            "chunk ordinal exceeds the supported range",
        ));
    }
    let reference = ChunkRef {
        source: reference
            .source_id
            .parse()
            .map_err(|_| invalid_argument("chunk.source_id", "invalid source ID"))?,
        revision: reference
            .revision_id
            .parse()
            .map_err(|_| invalid_argument("chunk.revision_id", "invalid revision ID"))?,
        ordinal: reference.ordinal,
    };
    Ok((scope, reference))
}

#[allow(clippy::result_large_err)]
fn parse_scope(scope: Option<v1::Scope>) -> std::result::Result<Scope, Status> {
    parse_scope_fields(scope, "scope", "scope.tenant", "scope.collection")
}

#[allow(clippy::result_large_err)]
fn parse_scope_fields(
    scope: Option<v1::Scope>,
    field: &str,
    tenant_field: &str,
    collection_field: &str,
) -> std::result::Result<Scope, Status> {
    let scope = scope.ok_or_else(|| invalid_argument(field, "scope is required"))?;
    Ok(Scope {
        tenant: scope
            .tenant
            .parse()
            .map_err(|error: Error| invalid_argument(tenant_field, error.to_string()))?,
        collection: scope
            .collection
            .parse()
            .map_err(|error: Error| invalid_argument(collection_field, error.to_string()))?,
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

fn wire_collection(
    tenant: &sanctum_core::ScopeId,
    row: sanctum_service::CollectionDetails,
) -> v1::Collection {
    v1::Collection {
        scope: Some(v1::Scope {
            tenant: tenant.to_string(),
            collection: row.id,
        }),
        display_name: Some(row.name),
        created_at_unix_seconds: Some(row.created_at),
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
        representation: "canonical_utf8".into(),
        metadata: hit
            .metadata
            .entries
            .into_iter()
            .map(|entry| {
                use sanctum_service::metadata_filter::Level;
                let level = match entry.level {
                    Level::Collection => v1::MetadataLevel::Collection,
                    Level::Source => v1::MetadataLevel::Source,
                    Level::Chunk => v1::MetadataLevel::Chunk,
                };
                v1::HitMetadataEntry {
                    level: level.into(),
                    entry: Some(v1::MetadataEntry {
                        namespace: entry.namespace,
                        key: entry.key,
                        value_json: entry.json,
                    }),
                }
            })
            .collect(),
        metadata_omitted: hit.metadata.omitted,
        embedding_state: {
            use sanctum_service::ChunkEmbeddingState;
            match hit.embedding_state {
                ChunkEmbeddingState::Disabled => v1::ChunkEmbeddingState::Disabled,
                ChunkEmbeddingState::Pending => v1::ChunkEmbeddingState::Pending,
                ChunkEmbeddingState::Ready => v1::ChunkEmbeddingState::Ready,
                ChunkEmbeddingState::Retrying => v1::ChunkEmbeddingState::Retrying,
                ChunkEmbeddingState::Failed => v1::ChunkEmbeddingState::Failed,
            }
            .into()
        },
    }
}

#[allow(clippy::result_large_err)]
fn parse_optional_id(
    value: Option<String>,
    field: &'static str,
    error: &'static str,
) -> std::result::Result<Option<uuid::Uuid>, Status> {
    value
        .map(|id| id.parse().map_err(|_| invalid_argument(field, error)))
        .transpose()
}

#[allow(clippy::result_large_err)]
fn metadata_target(
    target: v1::MetadataTarget,
) -> std::result::Result<(Scope, Option<sanctum_service::RevisionTarget>), Status> {
    let scope = parse_scope(target.scope)?;
    let source = parse_optional_id(target.source_id, "source_id", "invalid source ID")?;
    let revision = parse_optional_id(target.revision_id, "revision_id", "invalid revision ID")?;
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
            None => return Err(invalid_argument("edits.edit", "metadata edit is required")),
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
