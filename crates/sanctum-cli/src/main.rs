use std::path::PathBuf;

use anyhow::{Context, anyhow};
use clap::{Parser, Subcommand};
use sanctum_core::UPLOAD_FRAME_BYTES;
use sanctum_proto::v1::{self, sanctum_client::SanctumClient, upload_text_frame::Frame};
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
    Create {
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
        /// Replay an accepted hosted submission with the same UUID and input.
        #[arg(long)]
        request_id: Option<String>,
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
            };
            let (sender, receiver) = tokio::sync::mpsc::channel(2);
            if background {
                let request = client.submit_text(ReceiverStream::new(receiver));
                let (job, ()) = tokio::try_join!(
                    async { Ok(request.await?.into_inner()) },
                    send_upload(sender, header, file),
                )?;
                print_job(job);
                return Ok(());
            }
            let request = client.upload_text(ReceiverStream::new(receiver));
            let (revision, ()) = tokio::try_join!(
                async { Ok(request.await?.into_inner()) },
                send_upload(sender, header, file),
            )?;
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
    }
    Ok(())
}

async fn send_upload(
    sender: tokio::sync::mpsc::Sender<v1::UploadTextFrame>,
    header: v1::UploadTextHeader,
    mut file: tokio::fs::File,
) -> anyhow::Result<()> {
    sender
        .send(v1::UploadTextFrame {
            frame: Some(Frame::Header(header)),
        })
        .await?;
    let mut buffer = vec![0; UPLOAD_FRAME_BYTES];
    loop {
        let read = file.read(&mut buffer).await.context("read upload file")?;
        if read == 0 {
            sender
                .send(v1::UploadTextFrame {
                    frame: Some(Frame::End(v1::UploadTextEnd {})),
                })
                .await?;
            return Ok(());
        }
        if sender
            .send(v1::UploadTextFrame {
                frame: Some(Frame::Data(buffer[..read].to_vec())),
            })
            .await
            .is_err()
        {
            return Err(anyhow!("server closed the upload stream"));
        }
    }
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
