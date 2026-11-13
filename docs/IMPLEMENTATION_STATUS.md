# Implementation status

[PLAN.md](PLAN.md) defines the scope and acceptance criteria. This record tracks
which parts have working implementations and where to find the evidence.

| Milestone | Current implementation | Remaining work |
| --- | --- | --- |
| 1. Local desktop and MCP | Local Rust service with gRPC, MCP HTTP and stdio, and a Tauri tray app for storage, imports, activity, settings, and Ollama-backed retrieval | End-to-end acceptance in an LLM client |
| 2. Providers and local integrations | Local profile rebuilds and multi-collection retrieval | Hosted provider adapters, watchers, URL and Git imports, advanced metadata queries and cursors, SQL access, platform packaging and verification |
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

Merged in [PR #3](https://github.com/ArcaneArts/Sanctum/pull/3).

- `SubmitText` acknowledges after persisting the input and the queue record in
  `runtime.db`. Configurable workers claim jobs atomically. The queue records
  progress, failures, attempts, and pause state. gRPC and the CLI expose status,
  cancellation, retries, and queue controls.
- The collection's immutable revision is the receipt for a completed publication.
  If the process stops after SQLite commits but before queue completion is
  recorded, the next attempt recovers that receipt without indexing twice.
- The gRPC tests cover persisted pause and acceptance, tenant-scoped cancellation,
  recovery from a missing receipt, and retry after a storage read failure.
- Caller-provided request idempotency remains open.

## Local source lifecycle

Merged in [PR #4](https://github.com/ArcaneArts/Sanctum/pull/4).

- gRPC and the CLI can replace a source by its ID or external reference, with
  an optional expected revision. The collection writer serializes registration
  and publication, and the queue persists the registered replacement sequence.
- Deletion hides the source immediately. The worker rejects pending writes to
  deleted or superseded sources. A short grace period protects old fetch handles
  during lexical replacement, and the cleanup queue persists across restarts.
- Startup reclaims abandoned uploads while preserving committed revisions and
  queued or retryable inputs. Existing v1 databases migrate in place.
- The local gRPC tests cover both replacement targets, stale expected revisions,
  competing queued replacements, deletion during queued work, and retirement
  of old index rows and files. A v1 fixture verifies migration and orphan cleanup.
- Retirement now also waits for the configured embedding profile's readiness
  when embeddings are enabled.

## Local file and directory imports

Merged in [PR #5](https://github.com/ArcaneArts/Sanctum/pull/5).

- `ImportLocal` and `sanctum-cli import` walk files incrementally, apply include
  and exclude globs, and stream per-file results. Selected UTF-8 files enter
  the existing durable queue.
- A stable import root and relative path identify each source. Content digests
  skip unchanged input and reuse identical pending operations. Edits use the
  same source ID and replacement sequence as uploads.
- The default scan reports missing files. Explicit pruning deletes selected
  missing sources and supersedes their pending imports. The gRPC tests cover
  selection, repeated imports, edits, pending work, and deletion.

## Streaming RTF and original retention

Merged in [PR #6](https://github.com/ArcaneArts/Sanctum/pull/6).

- Local uploads and imports extract RTF in bounded buffers before indexing
  canonical UTF-8. Revision records store both digests, input encoding, the
  extractor version, and retention choice. Byte ranges use the canonical text.
- Accepted input remains durable until the lexical commit. Recovery can
  complete from that commit after original cleanup. Retained originals are
  reclaimed with their revisions.
- Parser tests exercise Unicode, split legacy code-page sequences, skipped
  destinations and binary payloads, and malformed input. gRPC tests cover
  restart, retention, exact excerpts, and directory re-import.
- Full size and memory measurements remain part of the capacity milestone.

## Revision-aware metadata and change polling

Merged in [PR #7](https://github.com/ArcaneArts/Sanctum/pull/7).

- The gRPC API reads and atomically mutates namespaced JSON on collections,
  current source revisions, and their chunks. Batch edits support set,
  merge patch, and remove.
- Replacement resets source and chunk metadata in the publication transaction.
  Scoped cursor polling returns lifecycle and metadata events and rejects
  expired cursors. Collection metadata stays independent of source changes.
- The gRPC integration test covers JSON null and merge behavior, transaction
  rollback, tenant scope, cursor pages, and stale writes after replacement.
- Metadata sorting, aggregation, and optional subscriptions remain open.

## Lexical candidate filtering

Merged in [PR #8](https://github.com/ArcaneArts/Sanctum/pull/8).

- `SearchText` supports structured collection, source, and chunk metadata
  predicates with nested paths, typed comparisons, array membership, and
  Boolean composition. SQLite applies source and chunk predicates to
  candidate rows before the rank limit.
- The gRPC test demonstrates a matching lower-ranked candidate under a
  one-hit limit. It also covers literal path segments, large integer
  comparisons, JSON null versus missing, and tenant scope.
- Semantic and hybrid search reuse these candidate filtering semantics.

## Local storage usage

Merged in [PR #9](https://github.com/ArcaneArts/Sanctum/pull/9).

- `GetStorageUsage` and `sanctum-cli usage` measure real filesystem lengths
  at the collection, tenant, and cluster scopes. The response includes file
  count and the scan's start and finish timestamps.
- The integration test compares all three scopes with an independent
  filesystem walk and verifies staging, retained originals, symbolic links,
  and refresh after removal.
- Object storage usage remains part of the object storage milestone.

## Local embeddings and retrieval

Merged in [PR #10](https://github.com/ArcaneArts/Sanctum/pull/10).

- The server can use an existing Ollama endpoint with a configured model
  revision, dimensions, and separate document and query prefixes. The
  background worker reuses matching inputs within a collection, tracks
  failures and retry times in SQLite, and keeps provider calls outside
  SQLite transactions.
- Single-collection search supports lexical, semantic, and hybrid modes.
  Vector ranking uses exact cosine similarity. A profile change excludes
  previous vectors immediately, and hybrid search reports lexical fallback
  or partial coverage during a rebuild.
- The gRPC integration test exercises search, filtering before ranking,
  lexical fallback, vector reuse, failed replacement retention, restart,
  and profile isolation. It uses a deterministic test provider.
- A standalone server and CLI smoke test used the installed Ollama
  `embeddinggemma-2:740m` model, verified two persisted 768-dimensional
  vectors, and ranked a relevant source for a query with no lexical match.
  It also exercised hybrid search.
- Explicit retry of permanent failures and capacity measurement remain open.

## Local MCP

Merged in [PR #11](https://github.com/ArcaneArts/Sanctum/pull/11).

- The server hosts MCP Streamable HTTP on a configurable loopback listener.
  The CLI's `mcp-stdio` bridges tool discovery and calls to that listener.
- MCP tools call the same application service as gRPC for collection
  operations, inline ingestion, status, search, exact fetch, and metadata.
  The [tool reference](MCP.md) documents pagination, response limits, and
  client configuration.
- The standalone integration check starts the server and CLI, verifies
  tool discovery and shared state over HTTP, stdio, and gRPC, and checks
  cross-tenant fetch rejection and HTTP Host and Origin validation.

## Local activity and embedding status

Merged in [PR #12](https://github.com/ArcaneArts/Sanctum/pull/12).

- `ListIngestionJobs` pages durable ingestion activity across a tenant or
  within one collection, using the stored acceptance timestamp and operation
  ID. The existing queue operations still control individual jobs.
- `GetEmbeddingStatus` reports the configured profile's coverage and failure
  information for a collection or one current source revision without
  running a search or contacting the embedding provider.
- The local gRPC tests cover pagination across restart, cursor tenant
  isolation, current and stale revision checks, and recorded embedding
  failures.

## Desktop tray and first-use flow

Merged in [PR #13](https://github.com/ArcaneArts/Sanctum/pull/13).

- `pnpm devtop` builds server and CLI sidecars and starts the Tauri/React
  app. The host persists settings and owns one server process. It passes
  configuration through the environment and stops the child through its
  control pipe.
- Activity shows persistent ingestion operations, embedding coverage, and
  measured storage usage. The app can create collections and queue file
  and directory imports. Settings cover the local store, Ollama, worker
  concurrency, and listener ports. Connect shows live MCP settings.
- Setup and build commands are in the [desktop guide](DESKTOP.md).
- On Apple Silicon macOS, the unsigned debug app bundle was exercised with
  a temporary store. It created a collection, imported a folder through
  the native picker, showed indexing and storage totals, applied Ollama
  settings on restart, and reported a real embedded chunk. A protocol
  client searched the live desktop MCP endpoint and fetched the citation
  through the bundled stdio bridge. Closing the window kept the app
  running, and quitting stopped the sidecar.
- A model-driven client workflow, Windows and Linux tray behavior, and
  release signing and installers remain unverified.

## Search across collections

- gRPC, CLI, and MCP can search up to eight distinct collections in a
  tenant. Each collection applies the metadata filter before ranking.
  The service embeds the query at most once and combines ranked lists
  while retaining the collection ID on each citation.
- Hybrid retrieval reports partial coverage and includes lexical candidates
  from collections still waiting for embeddings.
- The gRPC integration test verifies aggregate coverage, one query
  embedding per request, stable ranking, metadata filters, and tenant
  isolation. The standalone MCP check searches multiple collections
  through the stdio bridge.
