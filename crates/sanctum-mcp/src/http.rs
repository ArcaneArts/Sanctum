use std::net::IpAddr;

use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use sanctum_service::Service;
use tokio_util::sync::CancellationToken;

use crate::tools::Tools;

pub fn router(service: Service, ip: IpAddr, stop: CancellationToken) -> axum::Router {
    let endpoint = StreamableHttpService::<Tools, LocalSessionManager>::new(
        move || Ok(Tools::new(service.clone())),
        Default::default(),
        StreamableHttpServerConfig::default()
            .with_allowed_hosts(["localhost".to_owned(), ip.to_string()])
            .enforce_origin_validation()
            .with_max_request_body_bytes(2 * 1024 * 1024)
            .with_cancellation_token(stop),
    );
    axum::Router::new().route_service("/mcp", endpoint)
}
