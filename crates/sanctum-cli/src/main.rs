use std::path::PathBuf;

use anyhow::{Context, anyhow};
use clap::{Parser, Subcommand};
use sanctum_core::UPLOAD_FRAME_BYTES;
use sanctum_proto::v1::{self, sanctum_client::SanctumClient, upload_text_frame::Frame};
use tokio::io::AsyncReadExt;
use tokio_stream::wrappers::ReceiverStream;

#[derive(Parser)]
#[command(about = "Sanctum local text ingestion and search")]
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
    Create {
        collection: String,
    },
    List,
    Upload {
        collection: String,
        path: PathBuf,
        #[arg(long, requires = "external_id")]
        namespace: Option<String>,
        #[arg(long, requires = "namespace")]
        external_id: Option<String>,
    },
    Search {
        collection: String,
        query: String,
        #[arg(long, default_value_t = 10)]
        limit: u32,
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
    let scope = |collection| {
        Some(v1::Scope {
            tenant: args.tenant.clone(),
            collection,
        })
    };
    let mut client = SanctumClient::connect(args.endpoint).await?;
    match args.command {
        Command::Create { collection } => {
            client
                .create_collection(v1::CreateCollectionRequest {
                    scope: scope(collection.clone()),
                })
                .await?;
            println!("{collection}");
        }
        Command::List => {
            let response = client
                .list_collections(v1::ListCollectionsRequest {
                    tenant: args.tenant,
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
        }
        Command::Upload {
            collection,
            path,
            namespace,
            external_id,
        } => {
            let file = tokio::fs::File::open(&path)
                .await
                .with_context(|| format!("open {}", path.display()))?;
            let name = path
                .file_name()
                .context("upload path must name a file")?
                .to_string_lossy()
                .into_owned();
            let header = v1::UploadTextHeader {
                scope: scope(collection),
                name,
                external: namespace
                    .zip(external_id)
                    .map(|(namespace, id)| v1::ExternalSource { namespace, id }),
            };
            let (sender, receiver) = tokio::sync::mpsc::channel(2);
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
        Command::Search {
            collection,
            query,
            limit,
        } => {
            let response = client
                .search_text(v1::SearchTextRequest {
                    scope: scope(collection),
                    query,
                    limit: Some(limit),
                })
                .await?
                .into_inner();
            for hit in response.hits {
                let chunk = hit.chunk.context("server omitted chunk reference")?;
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
