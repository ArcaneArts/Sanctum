use std::{net::SocketAddr, sync::Arc};

use sanctum_config::ObjectStorageConfig;
use sanctum_service::HostedCatalog;
use sanctum_storage::{Key, Storage};
use sanctum_storage_gcs::GcsStorage;
use sanctum_storage_s3::{S3Config, S3Storage};
use tokio_util::sync::CancellationToken;

pub(crate) async fn serve(
    address: SocketAddr,
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
    if crate::managed() {
        println!("{}", serde_json::json!({"grpc": address, "mcp": null}));
    }
    tracing::info!(%address, "Sanctum object-backed gRPC listening");
    let cancellation = CancellationToken::new();
    let shutdown = cancellation.clone();
    tonic::transport::Server::builder()
        .add_service(sanctum_api::hosted_grpc(catalog, cancellation))
        .serve_with_incoming_shutdown(
            tokio_stream::wrappers::TcpListenerStream::new(listener),
            async move {
                tokio::select! {
                    result = tokio::signal::ctrl_c() => {
                        if let Err(error) = result {
                            tracing::warn!(%error, "could not listen for shutdown signal");
                        }
                    }
                    _ = crate::host_shutdown() => {}
                }
                shutdown.cancel();
            },
        )
        .await?;
    Ok(())
}
