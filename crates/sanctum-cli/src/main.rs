use std::path::PathBuf;

use anyhow::{Context, anyhow};
use clap::{Parser, Subcommand};
use sanctum_proto::{
    upload::send_upload,
    v1::{self, sanctum_client::SanctumClient},
};
use tokio::io::AsyncReadExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Status, metadata::MetadataValue, transport::Endpoint};

#[derive(Parser)]
#[command(about = "Sanctum text ingestion and search")]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:47550")]
    endpoint: String,
    #[arg(long, default_value = "default")]
    tenant: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Serve MCP on stdio by connecting to the running Sanctum server.
    McpStdio {
        #[arg(long, default_value = "http://127.0.0.1:47551/mcp")]
        mcp_url: String,
    },
    /// Report the API process health and redacted effective settings.
    Info,
    Create {
        collection: String,
        #[arg(long)]
        request_id: Option<String>,
    },
    RenameCollection {
        collection: String,
        name: String,
    },
    DeleteCollection {
        collection: String,
        #[arg(long)]
        request_id: Option<String>,
    },
    List,
    Usage {
        collection: Option<String>,
        #[arg(long, conflicts_with = "collection")]
        cluster: bool,
    },
    Import {
        collection: String,
        path: PathBuf,
        #[arg(long)]
        include: Vec<String>,
        #[arg(long)]
        exclude: Vec<String>,
        #[arg(long)]
        prune_missing: bool,
        #[arg(long)]
        retain_original: bool,
    },
    /// Import committed text from a local Git repository or an HTTPS remote.
    ImportGit {
        collection: String,
        #[arg(
            long,
            required_unless_present = "https_url",
            conflicts_with = "https_url"
        )]
        path: Option<PathBuf>,
        #[arg(long)]
        https_url: Option<String>,
        #[arg(long = "ref", default_value = "HEAD")]
        reference: String,
        #[arg(long)]
        include: Vec<String>,
        #[arg(long)]
        exclude: Vec<String>,
        #[arg(long)]
        prune_missing: bool,
        #[arg(long)]
        retain_original: bool,
    },
    /// Fetch text or RTF from an HTTP(S) URL and queue it for indexing.
    ImportUrl {
        collection: String,
        url: String,
        #[arg(long)]
        retain_original: bool,
    },
    /// Keep a directory synchronized with a collection.
    Watch {
        collection: String,
        path: PathBuf,
        #[arg(long)]
        include: Vec<String>,
        #[arg(long)]
        exclude: Vec<String>,
        #[arg(long)]
        prune_missing: bool,
        #[arg(long)]
        retain_original: bool,
        #[arg(long, default_value_t = 60)]
        interval_seconds: u32,
    },
    /// List directory watches and the outcome of their latest scan.
    Watches {
        collection: String,
    },
    /// Stop watching a directory.
    Unwatch {
        collection: String,
        id: String,
    },
    Upload {
        collection: String,
        path: PathBuf,
        #[arg(long, requires = "external_id")]
        namespace: Option<String>,
        #[arg(long, requires = "namespace")]
        external_id: Option<String>,
        #[arg(long)]
        background: bool,
        /// Replay an accepted submission with the same UUID and input.
        #[arg(long)]
        request_id: Option<String>,
        /// JSON array of namespaced metadata entries to attach to the revision.
        #[arg(long)]
        source_metadata: Option<PathBuf>,
        /// JSON array of chunk ordinal and metadata entry groups.
        #[arg(long)]
        chunk_metadata: Option<PathBuf>,
        #[arg(long)]
        retain_original: bool,
        #[arg(long)]
        replace_source: Option<String>,
        #[arg(long, requires = "namespace")]
        replace_external: bool,
        #[arg(long)]
        expected_revision: Option<String>,
    },
    Delete {
        collection: String,
        source_id: String,
        #[arg(long)]
        expected_revision: Option<String>,
        #[arg(long)]
        request_id: Option<String>,
    },
    Job {
        collection: String,
        operation_id: String,
    },
    /// List a page of ingestion activity in this tenant or a collection.
    Jobs {
        collection: Option<String>,
        #[arg(long, default_value_t = 25)]
        limit: u32,
        #[arg(long, conflicts_with = "before")]
        page_token: Option<String>,
        #[arg(long)]
        before: Option<String>,
    },
    Cancel {
        collection: String,
        operation_id: String,
    },
    Retry {
        collection: String,
        operation_id: String,
    },
    Queue {
        #[arg(value_parser = ["status", "pause", "resume"])]
        action: String,
    },
    /// Find a case-sensitive literal in the staged text of a pending upload.
    ScanPending {
        collection: String,
        operation_id: String,
        literal: String,
        #[arg(long)]
        byte_limit: Option<u64>,
        #[arg(long)]
        hit_limit: Option<u32>,
    },
    Search {
        collection: String,
        query: String,
        #[arg(long, default_value_t = 10)]
        limit: u32,
        #[arg(long, value_parser = ["lexical", "semantic", "hybrid"], default_value = "lexical")]
        mode: String,
        /// Include additional collections in the same tenant.
        #[arg(long = "also")]
        also: Vec<String>,
    },
    Fetch {
        collection: String,
        source_id: String,
        revision_id: String,
        ordinal: u64,
    },
    /// Write the exact canonical UTF-8 byte range of a source revision to stdout.
    FetchRange {
        collection: String,
        source_id: String,
        revision_id: String,
        start_byte: u64,
        end_byte: u64,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if let Command::McpStdio { mcp_url } = args.command {
        return sanctum_mcp::run_stdio_bridge(&mcp_url).await;
    }
    let scope = |collection| {
        Some(v1::Scope {
            tenant: args.tenant.clone(),
            collection,
        })
    };
    let endpoint = Endpoint::new(args.endpoint)?;
    let authorization = std::env::var_os("SANCTUM_GRPC_ID_TOKEN")
        .map(|token| {
            anyhow::ensure!(
                endpoint.uri().scheme_str() == Some("https"),
                "SANCTUM_GRPC_ID_TOKEN requires an HTTPS endpoint"
            );
            let token = token
                .into_string()
                .map_err(|_| anyhow!("invalid ID token"))?;
            let mut value = format!("Bearer {token}")
                .parse::<MetadataValue<_>>()
                .map_err(|_| anyhow!("invalid ID token"))?;
            value.set_sensitive(true);
            Ok(value)
        })
        .transpose()?;
    let mut client = SanctumClient::with_interceptor(
        endpoint.connect().await?,
        move |mut request: Request<()>| -> Result<Request<()>, Status> {
            if let Some(value) = &authorization {
                request
                    .metadata_mut()
                    .insert("authorization", value.clone());
            }
            Ok(request)
        },
    );
    match args.command {
        Command::McpStdio { .. } => unreachable!("handled before opening a gRPC connection"),
        Command::Watch {
            collection,
            path,
            include,
            exclude,
            prune_missing,
            retain_original,
            interval_seconds,
        } => {
            let path = std::path::absolute(path)?
                .into_os_string()
                .into_string()
                .map_err(|_| anyhow!("watch path is not UTF-8"))?;
            let folder = client
                .watch_folder(v1::WatchFolderRequest {
                    scope: scope(collection),
                    path,
                    include,
                    exclude,
                    prune_missing,
                    retain_original,
                    interval_seconds: Some(interval_seconds),
                })
                .await?
                .into_inner();
            println!("{}", folder.id);
        }
        Command::Watches { collection } => {
            let mut after = None;
            loop {
                let response = client
                    .list_watched_folders(v1::ListWatchedFoldersRequest {
                        scope: scope(collection.clone()),
                        after_id: after,
                        limit: Some(25),
                    })
                    .await?
                    .into_inner();
                for folder in response.folders {
                    let definition = folder
                        .definition
                        .context("server omitted watch definition")?;
                    println!(
                        "{} {} ({} queued, {} missing, {} deleted, {} failed)",
                        folder.id,
                        definition.path,
                        folder.last_queued,
                        folder.last_missing,
                        folder.last_deleted,
                        folder.last_failed
                    );
                    if let Some(error) = folder.last_error {
                        eprintln!("{error}");
                    }
                }
                after = response.next_after_id;
                if after.is_none() {
                    break;
                }
            }
        }
        Command::Unwatch { collection, id } => {
            client
                .unwatch_folder(v1::UnwatchFolderRequest {
                    scope: scope(collection),
                    id,
                })
                .await?;
        }
        Command::Usage {
            collection,
            cluster,
        } => {
            use v1::storage_usage_request::Scope as WireScope;
            let scope = match collection {
                Some(collection) => WireScope::Collection(v1::Scope {
                    tenant: args.tenant,
                    collection,
                }),
                None if cluster => WireScope::Cluster(v1::Empty {}),
                None => WireScope::Tenant(args.tenant),
            };
            let usage = client
                .get_storage_usage(v1::StorageUsageRequest { scope: Some(scope) })
                .await?
                .into_inner();
            println!(
                "{} bytes in {} files ({}..{} Unix ms)",
                usage.bytes, usage.files, usage.started_at_unix_ms, usage.finished_at_unix_ms
            );
        }
        Command::ImportGit {
            collection,
            path,
            https_url,
            reference,
            include,
            exclude,
            prune_missing,
            retain_original,
        } => {
            use v1::import_git_request::Repository;
            let repository = if let Some(url) = https_url {
                Repository::HttpsUrl(url)
            } else {
                let path = std::path::absolute(path.context("repository path is required")?)?
                    .into_os_string()
                    .into_string()
                    .map_err(|_| anyhow!("Git repository path is not UTF-8"))?;
                Repository::LocalPath(path)
            };
            let mut events = client
                .import_git(v1::ImportGitRequest {
                    scope: scope(collection),
                    repository: Some(repository),
                    reference,
                    include,
                    exclude,
                    prune_missing,
                    retain_original,
                })
                .await?
                .into_inner();
            let mut failures = 0;
            while let Some(event) = events.message().await? {
                let file = event
                    .file
                    .context("server omitted Git import file result")?;
                let state = file.state();
                println!(
                    "{}\t{}\t{}\t{}",
                    state.as_str_name(),
                    file.path,
                    file.operation_id.as_deref().unwrap_or_default(),
                    event
                        .git
                        .as_ref()
                        .map(|git| git.commit.as_str())
                        .unwrap_or_default()
                );
                if let Some(message) = file.message {
                    eprintln!("{message}");
                }
                if state == v1::ImportState::Failed {
                    failures += 1;
                }
            }
            if failures > 0 {
                return Err(anyhow!("{failures} Git import entries failed"));
            }
        }
        Command::ImportUrl {
            collection,
            url,
            retain_original,
        } => {
            let response = client
                .import_url(v1::ImportUrlRequest {
                    scope: scope(collection),
                    url,
                    retain_original,
                })
                .await?
                .into_inner();
            let provenance = response
                .url
                .as_ref()
                .context("server omitted URL provenance")?;
            println!(
                "{}\t{}\t{}\t{}\t{}",
                response.state().as_str_name(),
                response.source_id,
                response.revision_id,
                response.operation_id.as_deref().unwrap_or_default(),
                provenance.final_url,
            );
        }
        Command::Import {
            collection,
            path,
            include,
            exclude,
            prune_missing,
            retain_original,
        } => {
            let path = std::path::absolute(path)?
                .into_os_string()
                .into_string()
                .map_err(|_| anyhow!("import path is not UTF-8"))?;
            let mut events = client
                .import_local(v1::ImportLocalRequest {
                    scope: scope(collection),
                    path,
                    include,
                    exclude,
                    prune_missing,
                    retain_original,
                })
                .await?
                .into_inner();
            let mut failures = 0;
            while let Some(event) = events.message().await? {
                let state = event.state();
                println!(
                    "{}\t{}\t{}",
                    state.as_str_name(),
                    event.path,
                    event.operation_id.as_deref().unwrap_or_default()
                );
                if let Some(message) = event.message {
                    eprintln!("{message}");
                }
                if state == v1::ImportState::Failed {
                    failures += 1;
                }
            }
            if failures > 0 {
                return Err(anyhow!("{failures} import entries failed"));
            }
        }
        Command::Info => {
            let info = client.get_runtime_info(v1::Empty {}).await?.into_inner();
            println!("Sanctum {} ({}, {})", info.version, info.status, info.role);
            println!("Storage: {}", info.storage);
            println!("Source limit: {} bytes", info.max_source_bytes);
            println!(
                "Chunk window: {} bytes, {} bytes overlap",
                info.chunk_window_bytes, info.chunk_overlap_bytes
            );
            if let Some(tokens) = info.chunk_max_tokens {
                println!("Chunk token budget: {tokens} estimated tokens");
            }
            if let Some(concurrency) = info.local_worker_concurrency {
                println!("Local workers: {concurrency}");
            }
            if let Some(bytes) = info.snapshot_cache_bytes {
                println!("Snapshot cache: {bytes} bytes");
            }
            if let (Some(profile), Some(dimensions)) =
                (info.embedding_profile_id, info.embedding_dimensions)
            {
                println!("Embedding profile: {profile} ({dimensions} dimensions)");
            }
            if info.cloud_task_dispatch {
                println!("Cloud Tasks dispatch enabled");
            }
            if info.standalone_object_worker {
                println!("Standalone object worker enabled");
            }
            println!("Capabilities: {}", info.capabilities.join(", "));
        }
        Command::Create {
            collection,
            request_id,
        } => {
            client
                .create_collection(v1::CreateCollectionRequest {
                    scope: scope(collection.clone()),
                    request_id,
                })
                .await?;
            println!("{collection}");
        }
        Command::RenameCollection { collection, name } => {
            client
                .update_collection(v1::UpdateCollectionRequest {
                    scope: scope(collection),
                    display_name: name,
                })
                .await?;
        }
        Command::DeleteCollection {
            collection,
            request_id,
        } => {
            client
                .delete_collection(v1::DeleteCollectionRequest {
                    scope: scope(collection),
                    request_id,
                })
                .await?;
        }
        Command::List => {
            let mut after = None;
            loop {
                let response = client
                    .list_collections(v1::ListCollectionsRequest {
                        tenant: args.tenant.clone(),
                        after,
                        limit: Some(50),
                    })
                    .await?
                    .into_inner();
                for collection in response.collections {
                    println!(
                        "{}",
                        collection
                            .scope
                            .context("server omitted collection scope")?
                            .collection
                    );
                }
                let Some(next) = response.next_after else {
                    break;
                };
                after = Some(next);
            }
        }
        Command::Upload {
            collection,
            path,
            namespace,
            external_id,
            background,
            request_id,
            source_metadata,
            chunk_metadata,
            retain_original,
            replace_source,
            replace_external,
            expected_revision,
        } => {
            let file = tokio::fs::File::open(&path)
                .await
                .with_context(|| format!("open {}", path.display()))?;
            let name = path
                .file_name()
                .context("upload path must name a file")?
                .to_string_lossy()
                .into_owned();
            let replacement = if replace_source.is_some() || replace_external {
                Some(v1::Replacement {
                    source_id: replace_source,
                    expected_revision_id: expected_revision,
                })
            } else if expected_revision.is_some() {
                return Err(anyhow!("--expected-revision requires a replacement target"));
            } else {
                None
            };
            let header = v1::UploadTextHeader {
                scope: scope(collection),
                name,
                external: namespace
                    .zip(external_id)
                    .map(|(namespace, id)| v1::ExternalSource { namespace, id }),
                replacement,
                retain_original,
                request_id,
                source_metadata: match source_metadata {
                    Some(path) => wire_initial_metadata(read_metadata_file(path).await?)?,
                    None => Vec::new(),
                },
                chunk_metadata: match chunk_metadata {
                    Some(path) => {
                        let chunks: Vec<ChunkMetadata> = read_metadata_file(path).await?;
                        chunks
                            .into_iter()
                            .map(|chunk| {
                                Ok(v1::UploadChunkMetadata {
                                    ordinal: chunk.ordinal,
                                    entries: wire_initial_metadata(chunk.entries)?,
                                })
                            })
                            .collect::<anyhow::Result<_>>()?
                    }
                    None => Vec::new(),
                },
            };
            let (sender, receiver) = tokio::sync::mpsc::channel(2);
            if background {
                let request = client.submit_text(ReceiverStream::new(receiver));
                let (job, ()) =
                    tokio::try_join!(async { Ok(request.await?.into_inner()) }, async {
                        send_upload(sender, header, file)
                            .await
                            .context("stream upload file")
                    },)?;
                print_job(job);
                return Ok(());
            }
            let request = client.upload_text(ReceiverStream::new(receiver));
            let (revision, ()) =
                tokio::try_join!(async { Ok(request.await?.into_inner()) }, async {
                    send_upload(sender, header, file)
                        .await
                        .context("stream upload file")
                },)?;
            println!(
                "{} {} ({} chunks, {} bytes)",
                revision.source_id, revision.revision_id, revision.chunks, revision.bytes
            );
        }
        Command::Delete {
            collection,
            source_id,
            expected_revision,
            request_id,
        } => {
            let deleted = client
                .delete_source(v1::DeleteSourceRequest {
                    scope: scope(collection),
                    source_id,
                    expected_revision_id: expected_revision,
                    request_id,
                })
                .await?
                .into_inner();
            println!("{} {}", deleted.source_id, deleted.revision_id);
        }
        Command::Job {
            collection,
            operation_id,
        } => {
            print_job(
                client
                    .get_ingestion_job(v1::IngestionJobRequest {
                        scope: scope(collection),
                        operation_id,
                    })
                    .await?
                    .into_inner(),
            );
        }
        Command::Jobs {
            collection,
            limit,
            page_token,
            before,
        } => {
            let page = client
                .list_ingestion_jobs(v1::ListIngestionJobsRequest {
                    tenant: args.tenant.clone(),
                    collection,
                    limit: Some(limit),
                    before_operation_id: before,
                    page_token,
                })
                .await?
                .into_inner();
            for job in page.jobs {
                let scope = job.scope.as_ref().context("server omitted job scope")?;
                print!("{}/{} ", scope.tenant, scope.collection);
                print_job(job);
            }
            if let Some(next) = page.next_page_token {
                eprintln!("Next page: --page-token {next}");
            }
            if let Some(next) = page.next_before_operation_id {
                eprintln!("Next page: --before {next}");
            }
        }
        Command::Cancel {
            collection,
            operation_id,
        } => {
            print_job(
                client
                    .cancel_ingestion_job(v1::IngestionJobRequest {
                        scope: scope(collection),
                        operation_id,
                    })
                    .await?
                    .into_inner(),
            );
        }
        Command::Retry {
            collection,
            operation_id,
        } => {
            print_job(
                client
                    .retry_ingestion_job(v1::IngestionJobRequest {
                        scope: scope(collection),
                        operation_id,
                    })
                    .await?
                    .into_inner(),
            );
        }
        Command::Queue { action } => {
            let queue = if action == "status" {
                client.get_local_queue(v1::Empty {}).await?.into_inner()
            } else {
                client
                    .set_local_queue(v1::SetLocalQueueRequest {
                        paused: action == "pause",
                    })
                    .await?
                    .into_inner()
            };
            println!("{}", if queue.paused { "paused" } else { "running" });
        }
        Command::ScanPending {
            collection,
            operation_id,
            literal,
            byte_limit,
            hit_limit,
        } => {
            use v1::pending_text_scan_event::Event;
            let mut scan = client
                .scan_pending_text(v1::ScanPendingTextRequest {
                    job: Some(v1::IngestionJobRequest {
                        scope: scope(collection),
                        operation_id,
                    }),
                    literal,
                    byte_limit,
                    hit_limit,
                })
                .await?
                .into_inner();
            while let Some(event) = scan.message().await? {
                match event.event.context("server omitted scan event")? {
                    Event::Source(source) => eprintln!(
                        "{} {} ({} staged bytes)",
                        source.operation_id, source.name, source.total_bytes
                    ),
                    Event::Progress(progress) => {
                        let stop = progress.stop();
                        for hit in progress.matches {
                            println!("[{}..{})", hit.start_byte, hit.end_byte);
                        }
                        if stop != v1::PendingTextScanStop::Unspecified {
                            eprintln!(
                                "Scanned {} bytes, read {} bytes ({})",
                                progress.scanned_bytes,
                                progress.read_bytes,
                                stop.as_str_name()
                            );
                        }
                    }
                }
            }
        }
        Command::Search {
            collection,
            query,
            limit,
            mode,
            mut also,
        } => {
            let mode = match mode.as_str() {
                "semantic" => v1::SearchMode::Semantic,
                "hybrid" => v1::SearchMode::Hybrid,
                _ => v1::SearchMode::Lexical,
            }
            .into();
            let multiple = !also.is_empty();
            let response = if multiple {
                also.insert(0, collection);
                client
                    .search_collections(v1::SearchCollectionsRequest {
                        tenant: args.tenant,
                        collections: also,
                        query,
                        limit: Some(limit),
                        metadata_filter: None,
                        mode,
                    })
                    .await?
            } else {
                client
                    .search_text(v1::SearchTextRequest {
                        scope: scope(collection),
                        query,
                        limit: Some(limit),
                        metadata_filter: None,
                        mode,
                    })
                    .await?
            }
            .into_inner();
            if let Some(reason) = response.degraded_reason {
                eprintln!("Search coverage: {reason}");
            }
            for hit in response.hits {
                let chunk = hit.chunk.context("server omitted chunk reference")?;
                if multiple {
                    let scope = chunk.scope.as_ref().context("server omitted chunk scope")?;
                    print!("{}/{} ", scope.tenant, scope.collection);
                }
                println!(
                    "{} {} {} [{}..{}) {}\n{}",
                    chunk.source_id,
                    chunk.revision_id,
                    chunk.ordinal,
                    hit.start_byte,
                    hit.end_byte,
                    hit.name,
                    hit.excerpt
                );
            }
        }
        Command::Fetch {
            collection,
            source_id,
            revision_id,
            ordinal,
        } => {
            let hit = client
                .fetch_chunk(v1::FetchChunkRequest {
                    chunk: Some(v1::ChunkRef {
                        scope: scope(collection),
                        source_id,
                        revision_id,
                        ordinal,
                    }),
                })
                .await?
                .into_inner();
            print!("{}", hit.excerpt);
        }
        Command::FetchRange {
            collection,
            source_id,
            revision_id,
            start_byte,
            end_byte,
        } => {
            let range = client
                .fetch_source_range(v1::FetchSourceRangeRequest {
                    scope: scope(collection),
                    source_id,
                    revision_id,
                    start_byte,
                    end_byte,
                })
                .await?
                .into_inner();
            print!("{}", range.text);
        }
    }
    Ok(())
}

#[derive(serde::Deserialize)]
struct SourceMetadata {
    namespace: String,
    key: String,
    value: serde_json::Value,
}

#[derive(serde::Deserialize)]
struct ChunkMetadata {
    ordinal: u64,
    entries: Vec<SourceMetadata>,
}

async fn read_metadata_file<T: serde::de::DeserializeOwned>(path: PathBuf) -> anyhow::Result<T> {
    const MAX_FILE_BYTES: u64 = 1024 * 1024;
    let file = tokio::fs::File::open(&path)
        .await
        .with_context(|| format!("open {}", path.display()))?;
    let mut input = Vec::new();
    file.take(MAX_FILE_BYTES + 1)
        .read_to_end(&mut input)
        .await?;
    if input.len() as u64 > MAX_FILE_BYTES {
        return Err(anyhow!("metadata file exceeds 1 MiB"));
    }
    serde_json::from_slice(&input)
        .with_context(|| format!("parse metadata from {}", path.display()))
}

fn wire_initial_metadata(metadata: Vec<SourceMetadata>) -> anyhow::Result<Vec<v1::MetadataEntry>> {
    metadata
        .into_iter()
        .map(|entry| {
            Ok(v1::MetadataEntry {
                namespace: entry.namespace,
                key: entry.key,
                value_json: serde_json::to_string(&entry.value)?,
            })
        })
        .collect()
}

fn print_job(job: v1::IngestionJob) {
    println!(
        "{} {} {}/{} bytes (attempts: {})",
        job.operation_id,
        job.state().as_str_name(),
        job.processed_bytes,
        job.total_bytes,
        job.attempts,
    );
    if let Some(preparation) = job.preparation.and_then(|preparation| preparation.outcome) {
        match preparation {
            v1::hosted_preparation::Outcome::Ready(text) => {
                println!(
                    "Awaiting indexing ({} canonical bytes, {} chunks)",
                    text.canonical_bytes, text.chunks
                );
            }
            v1::hosted_preparation::Outcome::InvalidSource(message) => {
                println!("Source validation failed during preparation: {message}");
            }
        }
    }
    if let Some(work) = job.embedding_work {
        println!("Next embedding batch: {}", work.next_batch);
        if let Some(when) = work.not_before_unix_seconds {
            println!("Embedding retry at Unix second: {when}");
        }
    }
    if let Some(git) = job.git {
        println!("Git commit: {}", git.commit);
        println!("Git blob: {}", git.blob);
    }
    if let Some(url) = job.url {
        println!("Final URL: {}", url.final_url);
        if let Some(etag) = url.etag {
            println!("ETag: {etag}");
        }
        if let Some(last_modified) = url.last_modified {
            println!("Last-Modified: {last_modified}");
        }
    }
    if let (Some(code), Some(message)) = (job.error_code, job.error_message) {
        println!("{code}: {message}");
    }
}
