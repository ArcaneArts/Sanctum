use std::{net::SocketAddr, sync::Arc};

use sanctum_config::{CloudRunRole, Config, ObjectStorageConfig};
use sanctum_service::HostedCatalog;
use sanctum_storage::{Key, Storage};
use sanctum_storage_gcs::GcsStorage;
use sanctum_storage_s3::{S3Config, S3Storage};
use tokio_util::sync::CancellationToken;

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
    .with_embedding(embedding);
    if let Some(max_bytes) = config.hosted_snapshot_cache_bytes {
        catalog = catalog.with_snapshot_cache_bytes(max_bytes);
    }
    if let Some(config) = config.cloud_tasks {
        catalog = catalog.with_dispatch(Arc::new(crate::cloud_tasks::CloudTasks::new(config)?));
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
            let grpc_routes = tonic::service::Routes::new(sanctum_api::hosted_grpc(
                catalog.clone(),
                cancellation.child_token(),
            ));
            let direct_ip = if config.cloud_run_role.is_none() {
                Some(grpc.local_addr()?.ip())
            } else {
                None
            };
            let routes = grpc_routes
                .into_axum_router()
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
            axum::serve(worker, crate::hosted_worker::router(catalog.clone()))
                .with_graceful_shutdown(cancellation.clone().cancelled_owned())
                .await?;
        } else {
            cancellation.cancelled().await;
        }
        Ok::<(), anyhow::Error>(())
    };
    tokio::pin!(grpc, worker);
    tokio::select! {
        result = &mut grpc => {
            cancellation.cancel();
            result.and(worker.await)
        }
        result = &mut worker => {
            cancellation.cancel();
            result.and(grpc.await)
        }
        signal = crate::shutdown_signal() => {
            cancellation.cancel();
            let (grpc, worker) = tokio::join!(grpc, worker);
            signal.map_err(anyhow::Error::from).and(grpc).and(worker)
        }
    }
}

async fn bind(address: Option<SocketAddr>) -> std::io::Result<Option<tokio::net::TcpListener>> {
    match address {
        Some(address) => tokio::net::TcpListener::bind(address).await.map(Some),
        None => Ok(None),
    }
}
