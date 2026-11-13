# Sanctum

Sanctum is a planned Rust service for storing, indexing, and retrieving text for
agents and other applications. It exposes an MCP server and a Protobuf/gRPC API
so an application can ingest files, attach metadata, and retrieve relevant
chunks with their source references.

**Status:** planning. This repository currently contains the architecture and
implementation plan; the server and desktop application are not implemented.

The first usable milestone is a desktop tray application running a local MCP
server, ready to connect to an LLM client for personal use.

## What Sanctum will do

- Ingest text, Markdown, RTF, source code, and text configuration files through
  a common text pipeline with overlapping chunks.
- Support uploads, local file and directory imports, watched folders, URL
  retrieval, and Git repository ingestion across the initial release milestones.
- Store source text in a user-selected directory or object storage, with chunk
  byte ranges allowing retrieval of only the required content.
- Keep metadata and search indexes in SQLite, grouped into independent
  collections under tenant IDs.
- Offer full-text and embedding search across one or more collections belonging
  to the same tenant, including useful retrieval before embeddings are ready.
- Connect to Ollama, Google Cloud embedding APIs, and OpenAI-compatible embedding
  endpoints with a configurable base URL.
- Reuse embeddings for unchanged chunk inputs when files change, then remove
  superseded content after the replacement is ready.
- Let integrations attach and query arbitrary namespaced metadata, retain their
  own source IDs, and use an exceptional direct SQL query interface.
- Report processing progress and storage usage for a collection, tenant, or the
  whole managed storage root.

“Collection” is a working name. The permanent name for a grouping of information
has not been chosen.

## Local and hosted operation

The desktop application will use **Tauri, React, and shadcn/ui**, with PNPM for
frontend tooling and development commands. Business logic, configuration
handling, and server lifecycle management will live in Rust. The tray UI will
show activity, progress, storage usage, and settings. The planned `pnpm devtop`
command will launch the desktop development environment and its server; that
command does not exist yet.

The hosted deployment targets **Google Cloud Run, Cloud Storage, and Cloud
Tasks**, with Linux server builds. Filesystem storage and S3 storage through
MinIO provide local development and integration-test paths. Hosted instances
will download database snapshots on demand and reuse them while warm, with
durable queued work and coordinated database publication.

Sanctum is always multi-tenant internally. A local installation normally uses
the tenant ID `default`. The hosted service is operated by trusted applications;
end-user accounts, signups, and authorization belong to those applications.

## Scope

Sanctum retrieves content and provenance. Answer generation, automatic
summarization, knowledge types, confidence scoring, contradiction handling,
healing, and persistent agent context are outside the initial release.
Integrations may store their own derived information as metadata.

See [the full project plan](docs/PLAN.md) for architecture, data lifecycle,
interfaces, delivery milestones, and validation criteria. Repository changes
follow the [manual change protocol](docs/MANUAL_CHANGE_PROTOCOL.md).
