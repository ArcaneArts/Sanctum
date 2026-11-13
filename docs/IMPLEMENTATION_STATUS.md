# Implementation status

[PLAN.md](PLAN.md) defines the scope and acceptance criteria. This record tracks
which parts have working implementations and where to find the evidence.

| Milestone | Current implementation | Remaining work |
| --- | --- | --- |
| 1. Local desktop and MCP | Local Rust service with gRPC, MCP HTTP and stdio, and a Tauri tray app for storage, imports, activity, settings, and Ollama-backed retrieval | End-to-end acceptance in an LLM client |
| 2. Providers and local integrations | Local profile rebuilds, multi-collection retrieval, embedding adapters and desktop settings, watched folders, and URL and Git imports | Live hosted provider checks, platform packaging and verification |
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
- Metadata sorting and aggregation use the structured query API described below.

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

Merged in [PR #14](https://github.com/ArcaneArts/Sanctum/pull/14).

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

## OpenAI-compatible embeddings

Merged in [PR #15](https://github.com/ArcaneArts/Sanctum/pull/15).

- The server supports a configurable OpenAI-compatible embedding endpoint,
  optional dimensions in the request, and credentials read by environment
  variable name. Response indices map embeddings back to their inputs
  before the common shape and normalization checks.
- The standalone local server exercises the adapter against Ollama's
  OpenAI-compatible endpoint. Hosted OpenAI and OpenRouter verification
  remains open.

## Google Cloud embeddings

Merged in [PR #16](https://github.com/ArcaneArts/Sanctum/pull/16).

- The adapter uses Vertex AI text prediction with Application Default
  Credentials, distinct query and document tasks, requested dimensions, and
  truncation disabled. Its tests exercise the wire contract and response
  validation using local fixtures.
- A standalone run with missing credentials verified that retry state
  persists while ingestion and lexical fallback remain available.
- Live prediction requires an authorized Sanctum Google Cloud project and
  a credential with Vertex AI prediction access.

## Desktop embedding settings

Merged in [PR #17](https://github.com/ArcaneArts/Sanctum/pull/17).

- The desktop supports Ollama, OpenAI-compatible endpoints, and Google
  Cloud using the headless server's provider configuration. Earlier
  desktop Ollama settings load with their saved profile intact.
- The Rust settings test covers old settings, provider round trips, and
  credential references. Hosted provider verification remains open.

## Watched folders

Merged in [PR #18](https://github.com/ArcaneArts/Sanctum/pull/18).

- Watch definitions persist in the local queue and reconcile with the
  existing directory importer after restart and at configurable intervals.
  Watches preserve source IDs on edits and support explicit removal
  handling.
- The gRPC integration test verifies queue pause, tenant scoping, filters,
  and edits and removals made while the server was offline. A standalone
  CLI smoke test also detected an edit while running and verified that the
  source ID stayed the same.

## URL imports

Merged in [PR #19](https://github.com/ArcaneArts/Sanctum/pull/19).

- The gRPC `ImportUrl` method and CLI stream HTTP(S) responses to the existing
  local queue. The normalized requested URL identifies a source. The queue and
  committed revision record the final URL, ETag, and Last-Modified observed
  during acceptance. Re-import compares response bytes to preserve source
  identity and skip duplicate indexing.
- The operator can restrict destinations and configure a bearer token for an
  exact HTTPS origin. Redirects reapply the origin policy. Each import makes
  a full GET and HTML is indexed as source text.
- The gRPC integration tests exercise redirects, real upstream errors, size
  limits for chunked responses, RTF extraction, provenance across restart,
  tenant isolation, and digest-based replacement. The standalone server and
  CLI also verified exact UTF-8 fetch, recorded ETag, unchanged re-import,
  and a surfaced HTTP 404 against a loopback HTTP server.

## Git imports

Merged in [PR #20](https://github.com/ArcaneArts/Sanctum/pull/20).

- `ImportGit` and `sanctum-cli import-git` stream committed regular files
  selected by ref and path filters from a local repository or an HTTPS
  shallow fetch. The Rust adapter uses Git plumbing without a checkout.
- Each accepted operation and revision records the repository, ref, commit,
  path, and blob. Matching blob identity skips blob transfer from Git into
  staging. The existing queue handles content digests, replacements,
  source identity, and explicit pruning.
- The gRPC test uses a real local repository and verifies annotated refs,
  tenant scope, exclusion of worktree edits, unchanged input, replacements,
  size failures, and explicit pruning. A standalone CLI smoke test fetched
  and indexed the public Sanctum README over HTTPS and reused its revision
  on a second fetch. Authenticated private Git retrieval remains unverified.

## Resumable changes and Python gRPC client

Merged in [PR #21](https://github.com/ArcaneArts/Sanctum/pull/21).

- `WatchChanges` streams the same retained records as `ListChanges`.
  A bounded channel applies backpressure and an idle heartbeat preserves
  the resume cursor. `GetChangeCursor` captures the position to resume
  from after a rescan. Server shutdown closes live subscriptions.
- Local gRPC tests verify tenant scope, replay, reconnect after a single
  event page, cursor expiry, rescan recovery, and shutdown with an open
  stream.
- The [Python client example](../examples/python/README.md) generates
  code from the repository's Protobuf, streams uploads, waits for
  publication, observes the change event, and queries the resulting
  collection.
- A generated Python client used the standalone server to upload a UTF-8
  file spanning multiple frames, receive the publication event, and
  retrieve matching Unicode text. A second connection verified that a
  live subscription closed cleanly when the server shut down.
- The watched-folder regression test waits for the scan's deletion
  count before checking that the removed source is absent. Publishing
  an edited source can finish before the same scan reconciles removals.

## Structured metadata queries

Merged in [PR #22](https://github.com/ArcaneArts/Sanctum/pull/22).

- `QueryMetadata` lists current collections, sources, and chunks with
  typed filters, optional sorting, and bounded result pages. Counts can
  be grouped by a field on the target or an ancestor.
- The SQLite query reuses the retrieval filter compiler and preserves
  numeric precision and the distinction between JSON null and missing
  values. Each page observes current committed data.
- The gRPC test covers nested literal paths, pagination, large integers,
  grouping numerically equal values, null and missing values, ancestor
  metadata, and tenant scope.
- Local storage explicitly releases its ownership lock when the final
  owner drops. A regression test exercises restart with a duplicate
  descriptor still open, as can happen during a concurrent process launch.

## Read-only SQL sessions and snapshot export

- The operator gRPC API pins one catalog or collection generation with
  SQLite's online backup API, exposes its schema, and serves typed
  parameterized SELECT pages and streamed snapshot downloads.
- Session copies expire after five minutes and share a four-copy limit.
  Closing a session removes its handle. In-flight downloads retain their
  copy until completion. Startup reclaims abandoned copies.
- The gRPC test replaces a source while the original session stays open,
  verifies scope and SQL restrictions, and opens the streamed database
  directly to check the original revision and SQLite integrity.
- Hosted snapshot publication and generation fencing remain part of
  the object storage milestone.
- A generated Python client opened a session on the standalone server,
  ran a parameterized query, downloaded the advertised byte count,
  and read the same revision using Python's SQLite library.
