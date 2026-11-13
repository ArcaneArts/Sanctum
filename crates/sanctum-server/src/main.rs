use std::sync::Arc;

use sanctum_config::{Config, EmbeddingConfig};
use sanctum_service::{
    Service, run_embedding_worker, run_local_maintenance, run_local_worker, run_watched_folders,
};
use tokio_util::sync::CancellationToken;

mod hosted;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    let mut config = Config::from_env()?;
    if let Some(storage) = config.object_storage.take() {
        return hosted::serve(config.grpc_listen, storage).await;
    }
    let embedding: Option<Arc<dyn sanctum_embedding::Provider>> = config
        .embedding
        .map(|embedding| match embedding {
            EmbeddingConfig::Ollama(config) => sanctum_embedding_ollama::Ollama::new(config)
                .map(|provider| Arc::new(provider) as Arc<dyn sanctum_embedding::Provider>),
            EmbeddingConfig::OpenAi(config) => sanctum_embedding_openai::OpenAi::new(config)
                .map(|provider| Arc::new(provider) as Arc<dyn sanctum_embedding::Provider>),
            EmbeddingConfig::Google(config) => sanctum_embedding_google::Vertex::new(config)
                .map(|provider| Arc::new(provider) as Arc<dyn sanctum_embedding::Provider>),
        })
        .transpose()?;
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
        .add_service(sanctum_api::grpc(service, cancellation.child_token()))
        .serve_with_incoming_shutdown(
            tokio_stream::wrappers::TcpListenerStream::new(listener),
            cancellation.clone().cancelled_owned(),
        );
    tokio::pin!(grpc, mcp);
    let shutdown = async {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => signal,
            _ = host_shutdown() => Ok(()),
        }
    };
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
        signal = shutdown => {
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
