use std::sync::Arc;

use sanctum_config::{Config, EmbeddingConfig};
use sanctum_service::{
    Service, run_embedding_worker, run_local_maintenance, run_local_worker, run_watched_folders,
};
use tokio_util::sync::CancellationToken;

mod cloud_tasks;
mod hosted;
mod hosted_worker;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    let mut config = Config::from_env()?;
    let embedding: Option<Arc<dyn sanctum_embedding::Provider>> = config
        .embedding
        .take()
        .map(|embedding| match embedding {
            EmbeddingConfig::Ollama(config) => sanctum_embedding_ollama::Ollama::new(config)
                .map(|provider| Arc::new(provider) as Arc<dyn sanctum_embedding::Provider>),
            EmbeddingConfig::OpenAi(config) => sanctum_embedding_openai::OpenAi::new(config)
                .map(|provider| Arc::new(provider) as Arc<dyn sanctum_embedding::Provider>),
            EmbeddingConfig::Google(config) => sanctum_embedding_google::Vertex::new(config)
                .map(|provider| Arc::new(provider) as Arc<dyn sanctum_embedding::Provider>),
        })
        .transpose()?;
    if let Some(storage) = config.object_storage.take() {
        return hosted::serve(config, storage, embedding).await;
    }
    let mut runtime = runtime_info(&config, embedding.as_deref());
    let service =
        Service::open_with_embedding(&config.storage_root, config.max_source_bytes, embedding)?
            .with_url_origins(config.url_import_origins)?;
    let listener = tokio::net::TcpListener::bind(config.grpc_listen).await?;
    let mcp_listener = if let Some(address) = config.mcp_listen {
        anyhow::ensure!(
            address.ip().is_loopback(),
            "the local MCP listener requires a loopback address"
        );
        Some(tokio::net::TcpListener::bind(address).await?)
    } else {
        None
    };
    runtime.mcp = mcp_listener.is_some();
    let cancellation = CancellationToken::new();
    let (stop, shutdown) = tokio::sync::watch::channel(false);
    let mut workers = tokio::task::JoinSet::new();
    for _ in 0..config.local_worker_concurrency {
        workers.spawn(run_local_worker(service.clone(), shutdown.clone()));
    }
    workers.spawn(run_embedding_worker(service.clone(), shutdown.clone()));
    workers.spawn(run_watched_folders(service.clone(), shutdown.clone()));
    workers.spawn(run_local_maintenance(service.clone(), shutdown));

    if let Some(listener) = &mcp_listener {
        tracing::info!(address = %listener.local_addr()?, "Sanctum MCP listening");
    }
    if managed() {
        let mcp = mcp_listener
            .as_ref()
            .map(|listener| listener.local_addr())
            .transpose()?;
        println!(
            "{}",
            serde_json::json!({"grpc": listener.local_addr()?, "mcp": mcp})
        );
    }
    let mcp = serve_mcp(mcp_listener, service.clone(), cancellation.child_token());
    tracing::info!(address = %listener.local_addr()?, "Sanctum gRPC listening");
    let grpc = tonic::transport::Server::builder()
        .add_service(sanctum_api::grpc_with_runtime(
            service,
            cancellation.child_token(),
            runtime,
        ))
        .serve_with_incoming_shutdown(
            tokio_stream::wrappers::TcpListenerStream::new(listener),
            cancellation.clone().cancelled_owned(),
        );
    tokio::pin!(grpc, mcp);
    let result = tokio::select! {
        result = &mut grpc => {
            cancellation.cancel();
            let _ = stop.send(true);
            result.map_err(anyhow::Error::from).and(mcp.await)
        }
        result = &mut mcp => {
            cancellation.cancel();
            let _ = stop.send(true);
            result.and(grpc.await.map_err(anyhow::Error::from))
        }
        signal = shutdown_signal() => {
            cancellation.cancel();
            let _ = stop.send(true);
            let (grpc, mcp) = tokio::join!(grpc, mcp);
            signal.map_err(anyhow::Error::from).and(grpc.map_err(anyhow::Error::from)).and(mcp)
        }
    };
    while let Some(result) = workers.join_next().await {
        result?;
    }
    result?;
    Ok(())
}

fn runtime_info(
    config: &Config,
    embedding: Option<&dyn sanctum_embedding::Provider>,
) -> sanctum_proto::v1::RuntimeInfo {
    sanctum_proto::v1::RuntimeInfo {
        max_source_bytes: config.max_source_bytes,
        embedding_profile_id: embedding.map(|provider| provider.profile_id().to_owned()),
        embedding_dimensions: embedding.and_then(|provider| provider.dimensions().try_into().ok()),
        storage: "filesystem".into(),
        role: "local".into(),
        local_worker_concurrency: Some(config.local_worker_concurrency as u32),
        url_imports: config
            .url_import_origins
            .as_ref()
            .is_none_or(|origins| !origins.is_empty()),
        ..Default::default()
    }
}

async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    #[cfg(unix)]
    let termination = terminate.recv();
    #[cfg(not(unix))]
    let termination = std::future::pending::<Option<()>>();
    tokio::select! {
        signal = tokio::signal::ctrl_c() => signal,
        _ = termination => Ok(()),
        _ = host_shutdown() => Ok(()),
    }
}

async fn host_shutdown() {
    if !managed() {
        std::future::pending::<()>().await;
        return;
    }
    let (stopped, stop) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let _ = std::io::Read::read(&mut std::io::stdin(), &mut [0u8]);
        let _ = stopped.send(());
    });
    let _ = stop.await;
}

fn managed() -> bool {
    std::env::var_os("SANCTUM_MANAGED_STDIO").is_some()
}

async fn serve_mcp(
    listener: Option<tokio::net::TcpListener>,
    service: Service,
    cancellation: CancellationToken,
) -> anyhow::Result<()> {
    if let Some(listener) = listener {
        let router = sanctum_mcp::router(
            service,
            listener.local_addr()?.ip(),
            cancellation.child_token(),
        );
        axum::serve(listener, router)
            .with_graceful_shutdown(cancellation.cancelled_owned())
            .await?;
    } else {
        cancellation.cancelled().await;
    }
    Ok(())
}
