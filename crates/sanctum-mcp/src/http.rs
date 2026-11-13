use std::net::IpAddr;

use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use sanctum_service::{HostedCatalog, Service};
use tokio_util::sync::CancellationToken;

use crate::tools::Tools;

pub fn router(service: Service, ip: IpAddr, stop: CancellationToken) -> axum::Router {
    endpoint(
        Tools::new(service),
        config(stop).with_allowed_hosts(["localhost".to_owned(), ip.to_string()]),
    )
}

pub fn hosted_router(
    catalog: HostedCatalog,
    direct_ip: Option<IpAddr>,
    stop: CancellationToken,
) -> axum::Router {
    let config = config(stop)
        .with_legacy_session_mode(false)
        .with_json_response(true);
    let config = match direct_ip {
        Some(ip) => config.with_allowed_hosts(["localhost".to_owned(), ip.to_string()]),
        // Cloud Run IAM authenticates the caller before forwarding its authority.
        None => config.disable_allowed_hosts(),
    };
    endpoint(Tools::hosted(catalog), config)
}

fn config(stop: CancellationToken) -> StreamableHttpServerConfig {
    StreamableHttpServerConfig::default()
        .enforce_origin_validation()
        .with_max_request_body_bytes(2 * 1024 * 1024)
        .with_cancellation_token(stop)
}

fn endpoint(tools: Tools, config: StreamableHttpServerConfig) -> axum::Router {
    let endpoint = StreamableHttpService::<Tools, LocalSessionManager>::new(
        move || Ok(tools.clone()),
        Default::default(),
        config,
    );
    axum::Router::new().route_service("/mcp", endpoint)
}
