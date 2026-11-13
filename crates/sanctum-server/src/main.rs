use std::sync::Arc;

use sanctum_config::{Config, EmbeddingConfig};
use sanctum_service::{Service, run_embedding_worker, run_local_maintenance, run_local_worker};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    let config = Config::from_env()?;
    let embedding = config
        .embedding
        .map(|embedding| match embedding {
            EmbeddingConfig::Ollama(config) => sanctum_embedding_ollama::Ollama::new(config)
                .map(|provider| Arc::new(provider) as Arc<_>),
        })
        .transpose()?;
    let service =
        Service::open_with_embedding(&config.storage_root, config.max_source_bytes, embedding)?;
    let listener = tokio::net::TcpListener::bind(config.grpc_listen).await?;
    let (stop, shutdown) = tokio::sync::watch::channel(false);
    let mut workers = tokio::task::JoinSet::new();
    for _ in 0..config.local_worker_concurrency {
        workers.spawn(run_local_worker(service.clone(), shutdown.clone()));
    }
    workers.spawn(run_embedding_worker(service.clone(), shutdown.clone()));
    workers.spawn(run_local_maintenance(service.clone(), shutdown));
    tracing::info!(address = %listener.local_addr()?, "Sanctum gRPC listening");
    let result = tonic::transport::Server::builder()
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
        .await;
    let _ = stop.send(true);
    while let Some(result) = workers.join_next().await {
        result?;
    }
    result?;
    Ok(())
}
