# Implementation status

[PLAN.md](PLAN.md) defines the scope and acceptance criteria. This record tracks
which parts have working implementations and where to find the evidence.

| Milestone | Current implementation | Remaining work |
| --- | --- | --- |
| 1. Local desktop and MCP | Local Rust service and gRPC CLI with a tenant catalog, streamed UTF-8 uploads, SQLite FTS5, byte-range fetch, and restartable lexical jobs | RTF extraction, local imports, metadata, replacement and cleanup, usage, Ollama and vector search, MCP, tray app, desktop acceptance |
| 2. Providers and local integrations | Pending | Hosted provider adapters, rebuilds, watchers, URL and Git imports, metadata queries and cursors, SQL access, multi-collection retrieval, platform packaging and verification |
| 3. Object storage and publication | Pending | MinIO and GCS, backend contract tests, ownership, snapshots, durable publication, and recovery |
| 4. Cloud Run and Cloud Tasks | Pending | Linux deployment, dispatch and repair, identity, status, hosted ingress, crash and replay scenarios |
| 5. Capacity | Pending | Representative 200 MB sources, GB-scale collections and database snapshots, cold/warm cloud performance, measured optimization |

## Local text ingestion and retrieval

Merged in [PR #2](https://github.com/ArcaneArts/Sanctum/pull/2).

- The Cargo workspace separates configuration, scope validation, chunking,
  filesystem I/O, SQLite, shared service behavior, and transport. The Protobuf
  build generates Rust client and server code and a descriptor.
- Local storage uses `<tenant>/root.db`, `<tenant>/<collection>/meta.db`, and
  revision files. The service holds an OS ownership lock. Contentless FTS5 keeps
  the lexical index in SQLite. Each result reads the exact UTF-8 bytes from the
  source file and verifies their BLAKE3 digest.
- Uploads stream bounded frames to staging and explicitly finish before indexing.
- The Rust integration tests in `crates/sanctum-api/tests/local_grpc.rs` run the
  real transport and database. They cover persistence across service restart,
  tenant-scoped search and fetch, the ownership lock, Unicode split across
  frames, rejection of incomplete/oversized/invalid uploads, and exact excerpts.
  The chunker tests cover long lines, overlap, and content hashing.
- The standalone server and CLI smoke run exercised collection creation,
  listing, upload, search, and byte-exact fetch over loopback.

## Durable local lexical work

- `SubmitText` acknowledges after persisting the input and the queue record in
  `runtime.db`. Configurable workers claim jobs atomically. The queue records
  progress, failures, attempts, and pause state. gRPC and the CLI expose status,
  cancellation, retries, and queue controls.
- The collection's immutable revision is the receipt for a completed publication.
  If the process stops after SQLite commits but before queue completion is
  recorded, the next attempt recovers that receipt without indexing twice.
- The gRPC tests cover persisted pause and acceptance, tenant-scoped cancellation,
  recovery from a missing receipt, and retry after a storage read failure.
- Reclamation of orphan files from before acceptance and caller-provided request
  idempotency remain open. The next cycle adds source replacement and deletion,
  then uses that lifecycle for RTF and directory imports.
