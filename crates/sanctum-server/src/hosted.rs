use std::{net::SocketAddr, sync::Arc};

use sanctum_config::ObjectStorageConfig;
use sanctum_service::HostedCatalog;
use sanctum_storage::{Key, Storage};
use sanctum_storage_gcs::GcsStorage;
use sanctum_storage_s3::{S3Config, S3Storage};
use tokio_util::sync::CancellationToken;

pub(crate) async fn serve(
    address: SocketAddr,
    worker_address: Option<SocketAddr>,
    config: ObjectStorageConfig,
    max_source_bytes: u64,
) -> anyhow::Result<()> {
    let (storage, prefix): (Arc<dyn Storage>, _) = match config {
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
    let catalog = HostedCatalog::new(
        storage,
        prefix.map(Key::parse).transpose()?,
        max_source_bytes,
    )?;
    let listener = tokio::net::TcpListener::bind(address).await?;
    let address = listener.local_addr()?;
    let worker = if let Some(worker_address) = worker_address {
        Some(tokio::net::TcpListener::bind(worker_address).await?)
    } else {
        None
    };
    let worker_address = worker
        .as_ref()
        .map(tokio::net::TcpListener::local_addr)
        .transpose()?;
    if crate::managed() {
        println!(
            "{}",
            serde_json::json!({"grpc": address, "mcp": null, "worker": worker_address})
        );
    }
    tracing::info!(%address, "Sanctum object-backed gRPC listening");
    if let Some(worker_address) = worker_address {
        tracing::info!(%worker_address, "Sanctum hosted worker listening");
    }

    let cancellation = CancellationToken::new();
    let grpc = tonic::transport::Server::builder()
        .add_service(sanctum_api::hosted_grpc(
            catalog.clone(),
            cancellation.child_token(),
        ))
        .serve_with_incoming_shutdown(
            tokio_stream::wrappers::TcpListenerStream::new(listener),
            cancellation.clone().cancelled_owned(),
        );
    let worker = async {
        if let Some(worker) = worker {
            axum::serve(worker, crate::hosted_worker::router(catalog))
                .with_graceful_shutdown(cancellation.clone().cancelled_owned())
                .await?;
        } else {
            cancellation.cancelled().await;
        }
        Ok::<(), anyhow::Error>(())
    };
    let shutdown = async {
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = crate::host_shutdown() => Ok(()),
        }
    };
    tokio::pin!(grpc, worker);
    tokio::select! {
        result = &mut grpc => {
            cancellation.cancel();
            result.map_err(anyhow::Error::from).and(worker.await)
        }
        result = &mut worker => {
            cancellation.cancel();
            result.and(grpc.await.map_err(anyhow::Error::from))
        }
        signal = shutdown => {
            cancellation.cancel();
            let (grpc, worker) = tokio::join!(grpc, worker);
            signal.map_err(anyhow::Error::from).and(grpc.map_err(anyhow::Error::from)).and(worker)
        }
    }
}
