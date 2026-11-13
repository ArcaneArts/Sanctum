use std::io::Write;

use tokio::io::AsyncWriteExt;

use rmcp::{
    ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{CallToolResult, ServerCapabilities, ServerConfig},
    schemars, tool, tool_handler, tool_router,
};
use sanctum_core::{Error, ExternalSource, Result as SanctumResult, Scope, ScopeId};
use sanctum_service::{
    ChunkRef, Hit, HostedCatalog, HostedJob, HostedJobState, Job, JobState, MetadataEdit,
    MetadataEntry, MetadataMutation, Replacement, RevisionTarget, SearchMode, Service, TextUpload,
};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::filter::MetadataFilter;

const INLINE_LIMIT: usize = 128 * 1024;

#[derive(Clone)]
enum Backend {
    Local(Service),
    Hosted(HostedCatalog),
}

#[derive(Clone)]
pub(crate) struct Tools {
    backend: Backend,
    tool_router: ToolRouter<Self>,
}

impl Tools {
    pub(crate) fn new(service: Service) -> Self {
        Self {
            backend: Backend::Local(service),
            tool_router: Self::tool_router(),
        }
    }

    pub(crate) fn hosted(catalog: HostedCatalog) -> Self {
        Self {
            backend: Backend::Hosted(catalog),
            tool_router: Self::tool_router(),
        }
    }
}

async fn local<T: Send + 'static>(
    service: &Service,
    work: impl FnOnce(Service) -> SanctumResult<T> + Send + 'static,
) -> SanctumResult<T> {
    let service = service.clone();
    tokio::task::spawn_blocking(move || work(service))
        .await
        .map_err(|error| Error::Internal(Box::new(error)))?
}

#[derive(Deserialize, schemars::JsonSchema)]
struct Collection {
    /// Tenant ID. Defaults to the local tenant.
    #[serde(default = "default_tenant")]
    tenant: String,
    /// Collection ID.
    collection: String,
}

impl Collection {
    fn scope(&self) -> SanctumResult<Scope> {
        Ok(Scope {
            tenant: self.tenant.parse()?,
            collection: self.collection.parse()?,
        })
    }
}

fn default_tenant() -> String {
    "default".into()
}

#[derive(Deserialize, schemars::JsonSchema)]
struct Collections {
    #[serde(default = "default_tenant")]
    tenant: String,
    /// Resume after this collection ID using next_after from the previous response.
    after: Option<String>,
    /// Maximum collections to return. Defaults to 25 and allows 1 to 99.
    limit: Option<u32>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct SourceLink {
    namespace: String,
    id: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct Ingest {
    #[serde(flatten)]
    scope: Collection,
    /// Name of the text source, with a file extension when applicable.
    name: String,
    /// UTF-8 text, up to 128 KiB. Use the gRPC upload for larger inputs.
    text: String,
    external: Option<SourceLink>,
    /// Reuse the ID of an existing source by replacing its content.
    replace_source_id: Option<String>,
    /// Reject the replacement if the current revision differs.
    expected_revision_id: Option<String>,
    /// Optional hosted request UUID. Retry with the same UUID and identical input.
    request_id: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
enum Mode {
    Lexical,
    Semantic,
    Hybrid,
}

impl From<Mode> for SearchMode {
    fn from(mode: Mode) -> Self {
        match mode {
            Mode::Lexical => Self::Lexical,
            Mode::Semantic => Self::Semantic,
            Mode::Hybrid => Self::Hybrid,
        }
    }
}

#[derive(Deserialize, schemars::JsonSchema)]
struct Search {
    #[serde(flatten)]
    scope: Collection,
    query: String,
    /// Maximum hits. Defaults to 5 and allows 1 to 10.
    limit: Option<u32>,
    /// Defaults to hybrid. The result reports the mode used and any fallback.
    mode: Option<Mode>,
    /// Optional metadata predicate applied before ranking.
    metadata_filter: Option<MetadataFilter>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct SearchCollections {
    #[serde(default = "default_tenant")]
    tenant: String,
    /// One to eight distinct collection IDs in the tenant.
    collections: Vec<String>,
    query: String,
    /// Maximum hits. Defaults to 5 and allows 1 to 10.
    limit: Option<u32>,
    /// Defaults to hybrid.
    mode: Option<Mode>,
    metadata_filter: Option<MetadataFilter>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct Fetch {
    #[serde(flatten)]
    scope: Collection,
    source_id: String,
    revision_id: String,
    ordinal: u64,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct FetchContext {
    #[serde(flatten)]
    chunk: Fetch,
    /// Maximum UTF-8 bytes before the chunk, default 1024 and maximum 16384.
    before_bytes: Option<u32>,
    /// Maximum UTF-8 bytes after the chunk, default 1024 and maximum 16384.
    after_bytes: Option<u32>,
}

impl Fetch {
    fn reference(&self) -> SanctumResult<ChunkRef> {
        Ok(ChunkRef {
            source: id(&self.source_id)?,
            revision: id(&self.revision_id)?,
            ordinal: self.ordinal,
        })
    }
}

#[derive(Deserialize, schemars::JsonSchema)]
struct Operation {
    #[serde(flatten)]
    scope: Collection,
    operation_id: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct Target {
    #[serde(flatten)]
    scope: Collection,
    /// Provide both IDs for source or chunk metadata. Omit both for collection metadata.
    source_id: Option<String>,
    revision_id: Option<String>,
    /// Provide the ordinal for chunk metadata, including zero.
    ordinal: Option<u64>,
}

impl Target {
    fn revision(&self) -> SanctumResult<Option<RevisionTarget>> {
        match (&self.source_id, &self.revision_id, self.ordinal) {
            (None, None, None) => Ok(None),
            (Some(source), Some(revision), ordinal) => Ok(Some(RevisionTarget {
                source: id(source)?,
                revision: id(revision)?,
                ordinal,
            })),
            _ => Err(Error::Invalid(
                "metadata requires both source and revision IDs".into(),
            )),
        }
    }
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(tag = "operation", rename_all = "snake_case")]
enum Edit {
    Set { value: Value },
    Patch { value: Value },
    Remove,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct Mutation {
    namespace: String,
    key: String,
    #[serde(flatten)]
    edit: Edit,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct MetadataUpdate {
    #[serde(flatten)]
    target: Target,
    edits: Vec<Mutation>,
}

#[tool_router]
impl Tools {
    #[tool(description = "List a page of collection IDs within a tenant.")]
    async fn list_collections(&self, Parameters(args): Parameters<Collections>) -> CallToolResult {
        let result = async {
            let tenant: ScopeId = args.tenant.parse()?;
            let after = args.after.as_deref().map(str::parse).transpose()?;
            let limit = args.limit.unwrap_or(25);
            if !(1..=99).contains(&limit) {
                return Err(Error::Invalid(
                    "collection limit must be between 1 and 99".into(),
                ));
            }
            let mut ids = match &self.backend {
                Backend::Local(service) => {
                    local(service, {
                        let tenant = tenant.clone();
                        move |service| {
                            service.list_collections_page(&tenant, after.as_ref(), limit + 1)
                        }
                    })
                    .await?
                }
                Backend::Hosted(catalog) => {
                    catalog
                        .list_collections(&tenant, after.as_ref(), limit + 1)
                        .await?
                }
            };
            let next = if ids.len() > limit as usize {
                ids.pop();
                ids.last().cloned()
            } else {
                None
            };
            Ok(json!({"tenant": tenant.as_str(), "collections": ids, "next_after": next}))
        }
        .await;
        response(result)
    }

    #[tool(description = "Create a collection to hold searchable text in a tenant.")]
    async fn create_collection(&self, Parameters(args): Parameters<Collection>) -> CallToolResult {
        let result = async {
            let scope = args.scope()?;
            match &self.backend {
                Backend::Local(service) => {
                    local(service, {
                        let scope = scope.clone();
                        move |service| service.create_collection(&scope)
                    })
                    .await?
                }
                Backend::Hosted(catalog) => catalog.create_collection(&scope, None).await?,
            }
            Ok(scope_value(&scope))
        }
        .await;
        response(result)
    }

    #[tool(description = "Queue inline UTF-8 text for indexing and return a durable operation ID.")]
    async fn ingest_text(&self, Parameters(args): Parameters<Ingest>) -> CallToolResult {
        let result = async {
            let scope = args.scope.scope()?;
            let max_source_bytes = match &self.backend {
                Backend::Local(service) => service.max_source_bytes(),
                Backend::Hosted(catalog) => catalog.max_source_bytes(),
            };
            if args.text.len() > INLINE_LIMIT || args.text.len() as u64 > max_source_bytes {
                return Err(Error::Invalid(
                    "text exceeds the inline or configured source limit".into(),
                ));
            }
            if args.expected_revision_id.is_some() && args.replace_source_id.is_none() {
                return Err(Error::Invalid(
                    "expected revision requires a replacement source ID".into(),
                ));
            }
            let replacement = args
                .replace_source_id
                .as_deref()
                .map(|source| -> SanctumResult<_> {
                    Ok(Replacement {
                        source: Some(id(source)?),
                        expected_revision: args
                            .expected_revision_id
                            .as_deref()
                            .map(id)
                            .transpose()?,
                    })
                })
                .transpose()?;
            let upload = TextUpload {
                name: args.name,
                external: args.external.map(|external| ExternalSource {
                    namespace: external.namespace,
                    id: external.id,
                }),
                replacement,
                retain_original: false,
            };
            let request = args.request_id.as_deref().map(id).transpose()?;
            match &self.backend {
                Backend::Local(service) => {
                    if request.is_some() {
                        return Err(Error::Invalid(
                            "request ID is supported on hosted ingestion".into(),
                        ));
                    }
                    local(service, move |service| {
                        let upload = service.stage_upload(scope, upload)?;
                        upload.reopen()?.write_all(args.text.as_bytes())?;
                        Ok(job_value(upload.enqueue()?))
                    })
                    .await
                }
                Backend::Hosted(catalog) => {
                    let staged = catalog.stage_upload(scope.clone(), upload, request).await?;
                    let mut file = tokio::fs::File::from_std(staged.reopen()?);
                    file.write_all(args.text.as_bytes()).await?;
                    file.flush().await?;
                    Ok(hosted_job_value(&scope, staged.enqueue().await?))
                }
            }
        }
        .await;
        response(result)
    }

    #[tool(description = "Search one collection and return excerpts with exact source references.")]
    async fn search(&self, Parameters(args): Parameters<Search>) -> CallToolResult {
        let result = async {
            let scope = args.scope.scope()?;
            let limit = args.limit.unwrap_or(5);
            if !(1..=10).contains(&limit) {
                return Err(Error::Invalid("search limit must be between 1 and 10".into()));
            }
            let filter = args.metadata_filter.map(TryInto::try_into).transpose()?;
            let mode = args.mode.unwrap_or(Mode::Hybrid).into();
            let result = match &self.backend {
                Backend::Local(service) => service.retrieve(scope.clone(), args.query, limit, filter, mode).await?,
                Backend::Hosted(catalog) => catalog.retrieve(scope.clone(), args.query, limit, filter, mode).await?,
            };
            let mode = match result.mode {
                SearchMode::Lexical => "lexical",
                SearchMode::Semantic => "semantic",
                SearchMode::Hybrid => "hybrid",
            };
            Ok(json!({
                "mode": mode,
                "degraded_reason": result.degraded,
                "embedding_coverage": {
                    "profile_id": result.profile,
                    "total_chunks": result.coverage.total,
                    "embedded_chunks": result.coverage.embedded,
                    "failed_chunks": result.coverage.failed,
                },
                "hits": result.hits.into_iter().map(|hit| hit_value(&scope, hit)).collect::<Vec<_>>(),
            }))
        }.await;
        response(result)
    }

    #[tool(description = "Read the exact canonical UTF-8 range for a chunk reference from search.")]
    async fn fetch_chunk(&self, Parameters(args): Parameters<Fetch>) -> CallToolResult {
        let result = async {
            let scope = args.scope.scope()?;
            let reference = args.reference()?;
            let hit = match &self.backend {
                Backend::Local(service) => {
                    local(service, {
                        let scope = scope.clone();
                        move |service| service.fetch(&scope, &reference)
                    })
                    .await?
                }
                Backend::Hosted(catalog) => catalog.fetch(&scope, reference).await?,
            };
            Ok(hit_value(&scope, hit))
        }
        .await;
        response(result)
    }

    #[tool(description = "Read bounded canonical UTF-8 context around a cited chunk.")]
    async fn fetch_context(&self, Parameters(args): Parameters<FetchContext>) -> CallToolResult {
        let result = async {
            let scope = args.chunk.scope.scope()?;
            let reference = args.chunk.reference()?;
            let before = args.before_bytes.unwrap_or(1024);
            let after = args.after_bytes.unwrap_or(1024);
            let context = match &self.backend {
                Backend::Local(service) => {
                    local(service, {
                        let scope = scope.clone();
                        move |service| service.fetch_context(&scope, &reference, before, after)
                    })
                    .await?
                }
                Backend::Hosted(catalog) => {
                    catalog
                        .fetch_context(&scope, reference, before, after)
                        .await?
                }
            };
            Ok(json!({
                "hit": hit_value(&scope, context.hit),
                "start_byte": context.start_byte,
                "end_byte": context.end_byte,
                "text": context.text,
            }))
        }
        .await;
        response(result)
    }

    #[tool(
        description = "Search up to eight collections within a single tenant and merge their ranked results."
    )]
    async fn search_collections(
        &self,
        Parameters(args): Parameters<SearchCollections>,
    ) -> CallToolResult {
        let result = async {
            let limit = args.limit.unwrap_or(5);
            if !(1..=10).contains(&limit) {
                return Err(Error::Invalid("search limit must be between 1 and 10".into()));
            }
            let collections = args.collections.iter().map(|id| id.parse()).collect::<SanctumResult<_>>()?;
            let filter = args.metadata_filter.map(TryInto::try_into).transpose()?;
            let tenant: ScopeId = args.tenant.parse()?;
            let mode = args.mode.unwrap_or(Mode::Hybrid).into();
            let result = match &self.backend {
                Backend::Local(service) => service.retrieve_collections(tenant, collections, args.query, limit, filter, mode).await?,
                Backend::Hosted(catalog) => catalog.retrieve_collections(tenant, collections, args.query, limit, filter, mode).await?,
            };
            let mode = match result.mode {
                SearchMode::Lexical => "lexical",
                SearchMode::Semantic => "semantic",
                SearchMode::Hybrid => "hybrid",
            };
            Ok(json!({
                "mode": mode,
                "degraded_reason": result.degraded,
                "embedding_coverage": {
                    "profile_id": result.profile,
                    "total_chunks": result.coverage.total,
                    "embedded_chunks": result.coverage.embedded,
                    "failed_chunks": result.coverage.failed,
                },
                "hits": result.hits.into_iter().map(|(scope, hit)| hit_value(&scope, hit)).collect::<Vec<_>>(),
            }))
        }.await;
        response(result)
    }

    #[tool(description = "Check a durable ingestion operation for progress and errors.")]
    async fn ingestion_status(&self, Parameters(args): Parameters<Operation>) -> CallToolResult {
        let result = async {
            let scope = args.scope.scope()?;
            let operation = id(&args.operation_id)?;
            match &self.backend {
                Backend::Local(service) => {
                    local(service, move |service| {
                        Ok(job_value(service.job(&scope, operation)?))
                    })
                    .await
                }
                Backend::Hosted(catalog) => Ok(hosted_job_value(
                    &scope,
                    catalog.ingestion_job(&scope, operation).await?,
                )),
            }
        }
        .await;
        response(result)
    }

    #[tool(
        description = "Read namespaced JSON metadata on a collection, current source, or chunk."
    )]
    async fn get_metadata(&self, Parameters(args): Parameters<Target>) -> CallToolResult {
        let result = async {
            let scope = args.scope.scope()?;
            let target = args.revision()?;
            let entries = match &self.backend {
                Backend::Local(service) => {
                    local(service, move |service| {
                        service.metadata(&scope, target, None)
                    })
                    .await?
                }
                Backend::Hosted(catalog) => catalog.metadata(&scope, target).await?,
            };
            metadata_value(entries)
        }
        .await;
        response(result)
    }

    #[tool(description = "Atomically set, merge-patch, or remove namespaced JSON metadata.")]
    async fn mutate_metadata(
        &self,
        Parameters(args): Parameters<MetadataUpdate>,
    ) -> CallToolResult {
        let result = async {
            let scope = args.target.scope.scope()?;
            let target = args.target.revision()?;
            let edits: Vec<_> = args
                .edits
                .into_iter()
                .map(|edit| MetadataMutation {
                    namespace: edit.namespace,
                    key: edit.key,
                    edit: match edit.edit {
                        Edit::Set { value } => MetadataEdit::Set(value),
                        Edit::Patch { value } => MetadataEdit::Patch(value),
                        Edit::Remove => MetadataEdit::Remove,
                    },
                })
                .collect();
            let entries = match &self.backend {
                Backend::Local(service) => {
                    local(service, move |service| {
                        service.metadata(&scope, target, Some(&edits))
                    })
                    .await?
                }
                Backend::Hosted(catalog) => catalog.mutate_metadata(&scope, target, edits).await?,
            };
            metadata_value(entries)
        }
        .await;
        response(result)
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for Tools {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_instructions(crate::INSTRUCTIONS)
    }
}

fn id(value: &str) -> SanctumResult<Uuid> {
    Uuid::parse_str(value).map_err(|_| Error::Invalid("expected a UUID".into()))
}

fn scope_value(scope: &Scope) -> Value {
    json!({"tenant": scope.tenant.as_str(), "collection": scope.collection.as_str()})
}

fn job_value(job: Job) -> Value {
    let state = match job.state {
        JobState::Accepted => "accepted",
        JobState::Indexing if job.extracting => "extracting",
        JobState::Indexing => "indexing",
        JobState::Publishing => "publishing",
        JobState::TextReady => "text_ready",
        JobState::Failed => "failed",
        JobState::Canceled => "canceled",
    };
    json!({
        "scope": scope_value(&job.scope),
        "operation_id": job.id.to_string(),
        "source_id": job.source.to_string(),
        "revision_id": job.revision.to_string(),
        "state": state,
        "total_bytes": job.bytes,
        "processed_bytes": job.processed_bytes,
        "attempts": job.attempts,
        "created_at_unix_seconds": job.created_at,
        "failure": job.failure.map(|error| json!({"code": error.code, "message": error.message})),
    })
}

fn hosted_job_value(scope: &Scope, job: HostedJob) -> Value {
    let (state, failure) = match job.state {
        HostedJobState::Accepted => ("accepted", None),
        HostedJobState::Registered => ("registered", None),
        HostedJobState::TextReady => ("text_ready", None),
        HostedJobState::Failed { code, message } => {
            ("failed", Some(json!({"code": code, "message": message})))
        }
    };
    json!({
        "scope": scope_value(scope),
        "operation_id": job.operation.to_string(),
        "source_id": job.source.map(|source| source.to_string()),
        "revision_id": job.revision.to_string(),
        "state": state,
        "total_bytes": job.bytes,
        "processed_bytes": if job.content.is_some() { job.bytes } else { 0 },
        "update_sequence": job.update_sequence,
        "created_at_unix_seconds": job.created_at,
        "failure": failure,
    })
}

fn hit_value(scope: &Scope, hit: Hit) -> Value {
    let chunk = hit.chunk;
    let mut value = json!({
        "tenant": scope.tenant.as_str(),
        "collection": scope.collection.as_str(),
        "source_id": chunk.reference.source.to_string(),
        "revision_id": chunk.reference.revision.to_string(),
        "ordinal": chunk.reference.ordinal,
        "name": chunk.name,
        "start_byte": chunk.range.start,
        "end_byte": chunk.range.end,
        "content_blake3": chunk.hash,
        "external": chunk.external.map(|source| json!({"namespace": source.namespace, "id": source.id})),
        "excerpt": hit.excerpt,
        "score": hit.score,
    });
    if let Some(generation) = hit.database_generation {
        value["database_generation"] = json!(generation);
    }
    value
}

fn metadata_value(entries: Vec<MetadataEntry>) -> SanctumResult<Value> {
    let mut metadata = Vec::with_capacity(entries.len());
    for entry in entries {
        let value: Value =
            serde_json::from_str(&entry.json).map_err(|error| Error::Internal(Box::new(error)))?;
        metadata.push(json!({"namespace": entry.namespace, "key": entry.key, "value": value}));
    }
    Ok(json!({"entries": metadata}))
}

fn response(result: SanctumResult<Value>) -> CallToolResult {
    match result {
        Ok(value) => CallToolResult::structured(value),
        Err(error) => {
            let code = match &error {
                Error::Invalid(_) => "invalid_argument",
                Error::NotFound(_) => "not_found",
                Error::Conflict(_) => "conflict",
                Error::StaleRevision(_) => "stale_revision",
                Error::Busy(_) => "unavailable",
                Error::Io(_) | Error::Internal(_) => {
                    tracing::warn!(%error, "MCP tool failed");
                    return CallToolResult::structured_error(
                        json!({"code": "internal", "message": "operation failed"}),
                    );
                }
            };
            CallToolResult::structured_error(json!({"code": code, "message": error.to_string()}))
        }
    }
}
