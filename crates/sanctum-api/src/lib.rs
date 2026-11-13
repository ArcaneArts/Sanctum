use sanctum_core::{Error, ExternalSource, Result, Scope, UPLOAD_FRAME_BYTES};
use sanctum_proto::v1::{
    self,
    sanctum_server::{Sanctum, SanctumServer},
    upload_text_frame::Frame,
};
use sanctum_service::{ChunkRef, Hit, Service, TextUpload};
use tokio::io::AsyncWriteExt;
use tonic::{Request, Response, Status, Streaming};

pub fn grpc(service: Service) -> SanctumServer<Grpc> {
    SanctumServer::new(Grpc(service)).max_decoding_message_size(UPLOAD_FRAME_BYTES + 8192)
}

pub struct Grpc(Service);

#[tonic::async_trait]
impl Sanctum for Grpc {
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
        let mut stream = request.into_inner();
        let header = match stream.message().await?.and_then(|message| message.frame) {
            Some(Frame::Header(header)) => header,
            _ => {
                return Err(Status::invalid_argument(
                    "first upload frame must be a header",
                ));
            }
        };
        let scope = parse_scope(header.scope)?;
        let wire_scope = wire_scope(&scope);
        let service = self.0.clone();
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

        let revision = work(move || staged.commit()).await?;
        Ok(Response::new(v1::TextRevision {
            scope: Some(wire_scope),
            source_id: revision.source.to_string(),
            revision_id: revision.revision.to_string(),
            name: revision.name,
            external: revision.external.map(wire_external),
            bytes: revision.bytes,
            chunks: revision.chunks,
            content_blake3: revision.hash,
        }))
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
}

type Reply<T> = std::result::Result<Response<T>, Status>;

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
        Error::Busy(message) => Status::unavailable(message),
        Error::Io(_) | Error::Internal(_) => {
            tracing::error!(%error, "application operation failed");
            Status::internal("storage or database operation failed")
        }
    }
}
