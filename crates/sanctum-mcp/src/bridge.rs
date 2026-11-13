use std::{net::IpAddr, time::Duration};

use rmcp::{
    ErrorData, RoleServer, ServerHandler, ServiceExt,
    model::{
        CallToolRequestParams, CallToolResponse, ClientConfig, ListToolsResult,
        PaginatedRequestParams, ServerCapabilities, ServerConfig,
    },
    service::{Peer, RequestContext, RoleClient},
    transport::{
        StreamableHttpClientTransport, io::stdio,
        streamable_http_client::StreamableHttpClientTransportConfig,
    },
};

struct Bridge {
    upstream: Peer<RoleClient>,
}

impl ServerHandler for Bridge {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_instructions(crate::INSTRUCTIONS)
    }

    async fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        self.upstream
            .list_tools(request)
            .await
            .map_err(upstream_error)
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        tokio::select! {
            result = self.upstream.call_tool(request) => result.map(Into::into).map_err(upstream_error),
            _ = context.ct.cancelled() => Err(ErrorData::internal_error("tool call canceled", None)),
        }
    }
}

fn upstream_error(error: rmcp::service::ServiceError) -> ErrorData {
    match error {
        rmcp::service::ServiceError::McpError(error) => error,
        _ => ErrorData::internal_error("the running Sanctum MCP server could not answer", None),
    }
}

pub async fn run_stdio_bridge(endpoint: &str) -> anyhow::Result<()> {
    let url = reqwest_mcp::Url::parse(endpoint)?;
    let loopback = url.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .trim_matches(['[', ']'])
                .parse::<IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    anyhow::ensure!(
        url.scheme() == "http"
            && loopback
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none(),
        "the local MCP bridge requires a loopback HTTP URL without credentials, query, or fragment"
    );
    let client = reqwest_mcp::Client::builder()
        .no_proxy()
        .redirect(reqwest_mcp::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(5))
        .build()?;
    let transport = StreamableHttpClientTransport::with_client(
        client,
        StreamableHttpClientTransportConfig::with_uri(url.as_str())
            .max_sse_event_size(2 * 1024 * 1024),
    );
    let upstream = tokio::time::timeout(
        Duration::from_secs(10),
        ClientConfig::default().serve(transport),
    )
    .await??;
    let bridge = Bridge {
        upstream: upstream.peer().clone(),
    };
    bridge.serve(stdio()).await?.waiting().await?;
    upstream.cancel().await?;
    Ok(())
}
