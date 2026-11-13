use sanctum_config::Config;
use sanctum_service::Service;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    let config = Config::from_env()?;
    let service = Service::open(&config.storage_root, config.max_source_bytes)?;
    let listener = tokio::net::TcpListener::bind(config.grpc_listen).await?;
    tracing::info!(address = %listener.local_addr()?, "Sanctum gRPC listening");
    tonic::transport::Server::builder()
        .add_service(sanctum_api::grpc(service))
        .serve_with_incoming_shutdown(
            tokio_stream::wrappers::TcpListenerStream::new(listener),
            async {
                if let Err(error) = tokio::signal::ctrl_c().await {
                    tracing::error!(%error, "cannot listen for Ctrl-C");
                    std::future::pending::<()>().await;
                }
            },
        )
        .await?;
    Ok(())
}
