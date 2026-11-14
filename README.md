# Sanctum

Sanctum is a Rust service for storing, indexing, and retrieving text for
agents and other applications. Its Protobuf/gRPC API supports ingestion and
retrieval with source references and namespaced integration metadata. Agents
can connect to the local server through MCP HTTP or stdio.

The desktop tray and its managed MCP server have been exercised on macOS
and Ubuntu with an X11 tray host.
The object-backed server has been tested against MinIO. Live Google Cloud
verification and Windows tray testing are still open.
[Implementation status and acceptance evidence](docs/IMPLEMENTATION_STATUS.md).

## Get started

Run the desktop tray app and its managed Rust server from the repository
root after installing the native prerequisites.

```sh
pnpm install --frozen-lockfile
pnpm devtop
```

Open **Connect** in the app for the active MCP URL and stdio configuration.
Use **Activity** to create a collection and import files. The headless
server and CLI expose the same underlying services.
[Desktop setup and packaging](docs/DESKTOP.md), [MCP connection](docs/MCP.md),
[gRPC and CLI quick start](docs/LOCAL_API.md).

## Features

- Ingest text, Markdown, RTF, source code, and text configuration files through
  a common text pipeline with overlapping chunks.
- Stream uploads and import from local files, directories, watched folders,
  URLs, and Git repositories through the local service.
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

## Local and hosted operation

The desktop application uses **Tauri, React, and shadcn/ui**, with PNPM for
frontend tooling and development commands. Rust handles configuration, the
server lifecycle, and API calls. The headless server reads its configuration
from `SANCTUM_CONFIG_JSON`. The desktop app persists its own settings and
passes them to the managed server.

The hosted deployment targets **Google Cloud Run, Cloud Storage, and Cloud
Tasks**, with Linux builds for separate API and worker roles. Object-backed
instances download database snapshots on demand and reuse them while warm.
Queued work and committed snapshots survive process restarts. Filesystem
storage and S3-compatible MinIO are also supported.
[Storage behavior](docs/STORAGE.md), [Cloud Run deployment](docs/CLOUD_RUN.md),
[capacity measurements](docs/CAPACITY.md).

Sanctum is always multi-tenant internally. A local installation normally uses
the tenant ID `default`. Trusted hosted callers select the tenant ID and
manage authorization for their own end users.

## Scope

Sanctum retrieves content and provenance. Answer generation, automatic
summarization, knowledge types, confidence scoring, contradiction handling,
healing, and persistent agent context are outside the initial release.
Integrations may store their own derived information as metadata.

See [the full project plan](docs/PLAN.md) for architecture, data lifecycle,
interfaces, delivery milestones, and validation criteria. Repository changes
follow the [manual change protocol](docs/MANUAL_CHANGE_PROTOCOL.md).
