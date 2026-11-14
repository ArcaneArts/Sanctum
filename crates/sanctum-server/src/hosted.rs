use std::{net::SocketAddr, sync::Arc, time::Duration};

use sanctum_config::{CloudRunRole, Config, ObjectStorageConfig};
use sanctum_service::HostedCatalog;
use sanctum_storage::{Key, Storage};
use sanctum_storage_gcs::GcsStorage;
use sanctum_storage_s3::{S3Config, S3Storage};
use tokio_util::sync::CancellationToken;

const CLOUD_RUN_DRAIN: Duration = Duration::from_secs(7);

pub(crate) async fn serve(
    config: Config,
    storage: ObjectStorageConfig,
    embedding: Option<Arc<dyn sanctum_embedding::Provider>>,
) -> anyhow::Result<()> {
    let (grpc_address, worker_address) = if let Some(role) = config.cloud_run_role {
        let port = std::env::var("PORT")?.parse::<u16>()?;
        anyhow::ensure!(port != 0, "Cloud Run PORT must be positive");
        let address = ([0, 0, 0, 0], port).into();
        match role {
            CloudRunRole::Api => (Some(address), None),
            CloudRunRole::Worker => (None, Some(address)),
        }
    } else {
        (Some(config.grpc_listen), config.hosted_worker_listen)
    };
    let mut runtime = crate::runtime_info(&config, embedding.as_deref());
    runtime.storage = match &storage {
        ObjectStorageConfig::S3 { .. } => "s3",
        ObjectStorageConfig::Gcs { .. } => "gcs",
    }
    .into();
    runtime.role = if config.cloud_run_role.is_some() {
        "cloud_run_api"
    } else {
        "hosted"
    }
    .into();
    runtime.local_worker_concurrency = None;
    runtime.url_imports = false;
    runtime.snapshot_cache_bytes = Some(
        config
            .hosted_snapshot_cache_bytes
            .unwrap_or(sanctum_service::DEFAULT_SNAPSHOT_CACHE_BYTES),
    );
    runtime.cloud_task_dispatch = config.cloud_tasks.is_some();
    runtime.standalone_object_worker = config.standalone_object_worker;
    runtime.mcp = grpc_address.is_some();
    let (storage, prefix): (Arc<dyn Storage>, _) = match storage {
        ObjectStorageConfig::S3 {
            bucket,
            region,
            endpoint,
            prefix,
            allow_http,
            virtual_hosted_style,
        } => (
            Arc::new(S3Storage::new(S3Config {
                bucket,
                region,
                endpoint,
                allow_http,
                virtual_hosted_style,
            })?),
            prefix,
        ),
        ObjectStorageConfig::Gcs { bucket, prefix } => (Arc::new(GcsStorage::new(bucket)?), prefix),
    };
    let mut catalog = HostedCatalog::new(
        storage,
        prefix.map(Key::parse).transpose()?,
        config.max_source_bytes,
    )?
    .with_embedding(embedding)
    .with_snapshot_page_deltas(config.hosted_snapshot_page_deltas)
    .with_chunking(config.chunking)?;
    if let Some(max_bytes) = config.hosted_snapshot_cache_bytes {
        catalog = catalog.with_snapshot_cache_bytes(max_bytes);
    }
    if let Some(config) = config.cloud_tasks {
        catalog = catalog.with_dispatch(Arc::new(crate::cloud_tasks::CloudTasks::new(config)?));
    }
    let wake = config
        .standalone_object_worker
        .then(|| Arc::new(tokio::sync::Notify::new()));
    if let Some(wake) = &wake {
        catalog = catalog.with_standalone_worker(wake.clone());
    }
    let grpc = bind(grpc_address).await?;
    let worker = bind(worker_address).await?;
    let address = |listener: &Option<tokio::net::TcpListener>| {
        listener
            .as_ref()
            .map(tokio::net::TcpListener::local_addr)
            .transpose()
    };
    let grpc_address = address(&grpc)?;
    let worker_address = address(&worker)?;
    if crate::managed() {
        println!(
            "{}",
            serde_json::json!({"grpc": grpc_address, "mcp": grpc_address, "worker": worker_address})
        );
    }
    if let Some(address) = grpc_address {
        tracing::info!(%address, "Sanctum object-backed gRPC and MCP listening");
    }
    if let Some(address) = worker_address {
        tracing::info!(%address, "Sanctum hosted worker listening");
    }

    let cancellation = CancellationToken::new();
    let grpc = async {
        if let Some(grpc) = grpc {
            let grpc_routes = tonic::service::Routes::new(sanctum_api::hosted_grpc_with_runtime(
                catalog.clone(),
                cancellation.child_token(),
                runtime,
            ));
            let direct_ip = if config.cloud_run_role.is_none() {
                Some(grpc.local_addr()?.ip())
            } else {
                None
            };
            let routes = grpc_routes
                .into_axum_router()
                .route("/healthz", axum::routing::get(health))
                .merge(sanctum_mcp::hosted_router(
                    catalog.clone(),
                    direct_ip,
                    cancellation.child_token(),
                ));
            tonic::transport::Server::builder()
                .accept_http1(true)
                .timeout(std::time::Duration::from_secs(600))
                .add_routes(routes.into())
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(grpc),
                    cancellation.clone().cancelled_owned(),
                )
                .await?;
        } else {
            cancellation.cancelled().await;
        }
        Ok::<(), anyhow::Error>(())
    };
    let worker = async {
        if let Some(worker) = worker {
            axum::serve(
                worker,
                crate::hosted_worker::router(catalog.clone())
                    .route("/healthz", axum::routing::get(health)),
            )
            .with_graceful_shutdown(cancellation.clone().cancelled_owned())
            .await?;
        } else if let Some(wake) = wake {
            crate::standalone_worker::run(catalog.clone(), wake, cancellation.child_token()).await;
        } else {
            cancellation.cancelled().await;
        }
        Ok::<(), anyhow::Error>(())
    };
    tokio::pin!(grpc, worker);
    let result = tokio::select! {
        result = &mut grpc => {
            return drain(&cancellation, config.cloud_run_role, async {
                result.and(worker.await)
            }).await;
        }
        result = &mut worker => {
            return drain(&cancellation, config.cloud_run_role, async {
                result.and(grpc.await)
            }).await;
        }
        signal = crate::shutdown_signal() => signal,
    };
    drain(&cancellation, config.cloud_run_role, async {
        let (grpc, worker) = tokio::join!(grpc, worker);
        result.map_err(anyhow::Error::from).and(grpc).and(worker)
    })
    .await
}

async fn drain(
    cancellation: &CancellationToken,
    role: Option<CloudRunRole>,
    work: impl Future<Output = anyhow::Result<()>>,
) -> anyhow::Result<()> {
    cancellation.cancel();
    if let Some(role) = role {
        match tokio::time::timeout(CLOUD_RUN_DRAIN, work).await {
            Ok(result) => result,
            Err(_) => {
                tracing::warn!(
                    event = "cloud_run_drain_timeout",
                    ?role,
                    elapsed_seconds = CLOUD_RUN_DRAIN.as_secs(),
                    "stopped waiting for requests during Cloud Run shutdown"
                );
                Ok(())
            }
        }
    } else {
        work.await
    }
}

async fn bind(address: Option<SocketAddr>) -> std::io::Result<Option<tokio::net::TcpListener>> {
    match address {
        Some(address) => tokio::net::TcpListener::bind(address).await.map(Some),
        None => Ok(None),
    }
}

async fn health() -> axum::http::StatusCode {
    axum::http::StatusCode::NO_CONTENT
}

#[cfg(test)]
mod tests {
    use std::{future::pending, time::Instant};

    use super::*;

    #[tokio::test]
    async fn cloud_run_drain_returns_when_work_is_still_pending() {
        let cancellation = CancellationToken::new();
        let started = Instant::now();
        tokio::time::timeout(
            CLOUD_RUN_DRAIN + Duration::from_secs(2),
            drain(&cancellation, Some(CloudRunRole::Worker), pending()),
        )
        .await
        .expect("Cloud Run drain must be bounded")
        .unwrap();
        assert!(cancellation.is_cancelled());
        assert!(started.elapsed() >= CLOUD_RUN_DRAIN);
    }
}
