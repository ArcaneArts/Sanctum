# Implementation status

[PLAN.md](PLAN.md) defines the scope and acceptance criteria. This record tracks
which parts have working implementations and where to find the evidence.

| Milestone | Current implementation | Remaining work |
| --- | --- | --- |
| 1. Local desktop and MCP | Local Rust service with gRPC, MCP HTTP and stdio, and a Tauri tray app for storage, imports, activity, settings, and Ollama-backed retrieval | End-to-end acceptance in an LLM client |
| 2. Providers and local integrations | Local profile rebuilds, multi-collection retrieval, embedding adapters and desktop settings, watched folders, and URL and Git imports | Live hosted provider checks, platform packaging and verification |
| 3. Object storage and publication | Shared object contract, filesystem and MinIO-tested S3 adapters, and a fenced SQLite snapshot executor, a native GCS adapter, and a hosted catalog | Live GCS verification, hosted source operations, and deployment recovery |
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

Merged in [PR #23](https://github.com/ArcaneArts/Sanctum/pull/23).

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

## Shared object storage and MinIO

Merged in [PR #24](https://github.com/ArcaneArts/Sanctum/pull/24).

- `sanctum-storage` defines streaming objects, bounded exact ranges,
  conditional control writes, paginated listing, deletion, and actual
  stored-byte measurement. The filesystem implementation shares the
  existing range reader and filesystem ownership lock.
- The live S3 contract ran against an isolated MinIO container. It
  streamed a 17 MiB multipart payload, read an exact UTF-8 byte range,
  raced two ETag-conditional writers, rejected mismatched lengths,
  and verified pagination, prefix isolation, byte totals, and deletion.
- The same contract runs against the filesystem during `pnpm check`.
  The server still uses local storage while remote publication and
  backend configuration are implemented.

## Fenced object-backed database publication

Merged in [PR #25](https://github.com/ArcaneArts/Sanctum/pull/25).

- `sanctum-db` stores the lease, epoch, and visible snapshot in one
  conditional HEAD object. It validates each downloaded SQLite snapshot
  and publishes a new backup only after storing the snapshot object.
- Prepared command outcomes are committed alongside mutations. An
  identical retry reads its saved outcome and reuses the generation.
  Cleanup marks retired or abandoned snapshots and gives readers a grace
  period before deletion.
- The filesystem and live MinIO publication tests cover independent
  catalog/collection scopes, takeover after expiry, stale preparation,
  duplicate and out-of-order delivery, rollback, and retained readers.
  A fault-injection test simulates losing the response after publication
  and verifies receipt recovery. The filesystem test also exercises
  deletion after grace.
- Hosted application commands and Cloud Tasks dispatch still need to
  use the executor.

## Native Google Cloud Storage adapter

Merged in [PR #26](https://github.com/ArcaneArts/Sanctum/pull/26).

- The GCS adapter uses the JSON API, Google Application Default
  Credentials, generation preconditions, streaming downloads, bounded
  resumable upload parts, and paginated recursive listing.
- Loopback HTTP fixture tests verify the exact wire requests, generation
  fencing even with unchanged ETags, upload offsets, range and encoding
  validation, and list continuation. They use a fixture credential.
- The [storage guide](STORAGE.md) documents the live contract command.
  Actual GCS verification still requires an authorized test bucket and
  ADC identity. Hosted catalog configuration now selects the adapter.

## Hosted catalog

- `SANCTUM_CONFIG_JSON.object_storage` selects S3 or GCS for the hosted
  gRPC catalog. The server publishes the initial collection database
  before activating it in the tenant catalog. An optional request ID
  replays the recorded activation outcome.
- The hosted gRPC test resumes an interrupted creation without rebuilding
  the collection database. It also covers tenant isolation, pagination,
  request ID replay, and usage.
- A standalone server and CLI run against live MinIO created collections
  for two tenants, restarted, replayed creation, and matched an independent
  bucket listing at collection, tenant, and cluster scopes.
- Delivered in [PR #27](https://github.com/ArcaneArts/Sanctum/pull/27).

## Hosted upload acceptance

- Hosted `SubmitText` uses the same framed stream reader as local uploads.
  It spools and hashes the exact input on disk, uploads it under a unique
  key, and creates a scoped operation envelope with the pending registration
  stage. `GetIngestionJob` reads that envelope after an API restart.
- The filesystem-backed gRPC test checks malformed streams, configured size
  limits, exact Unicode bytes, concurrent submission replay, different input
  or options under the same request ID, tenant isolation, and replacement
  by external reference. A second test interrupts the write before and
  after envelope creation and verifies recovery after restart.
- A standalone server and CLI against MinIO replayed a Unicode upload
  after restart. An independent S3 client verified a byte range, the
  complete SHA-256 of a 201 MiB multipart input, and the envelope's byte
  count. The debug server accepted the large upload in 47.45 seconds
  with a sampled peak RSS of 43.3 MiB on Apple Silicon. This measures
  acceptance through durable storage.
- Delivered in [PR #28](https://github.com/ArcaneArts/Sanctum/pull/28).

## Hosted revision registration

- The optional private worker listener accepts a scoped operation reference
  and registers the revision under the collection writer lease. The update
  sequence and receipt commit together in the collection snapshot. A
  conditional envelope update checkpoints the result for ingestion status.
- The hosted gRPC integration test interrupts that checkpoint, restarts the
  API, and verifies receipt replay without publishing another snapshot. It
  also verifies replacement ordering, an unchanged current revision, and
  rejection of a stale expected revision.
- A standalone server and CLI against MinIO exercised the private HTTP
  endpoint with out-of-order deliveries and a conflicting external ID.
  After restart, both deliveries replayed without changing the published
  snapshot. The CLI reported registered and failed states.
- Delivered in [PR #29](https://github.com/ArcaneArts/Sanctum/pull/29).

## Hosted lexical publication and ranges

- The worker validates and indexes registered UTF-8 and RTF uploads.
  A fenced transaction commits the lexical index, canonical object
  locator, and indexing receipt. A second conditional write advances
  the envelope to `TEXT_READY`. A stale replacement loses inside the
  transaction, even if it already prepared its input.
- Hosted `SearchText` and `FetchChunk` resolve the committed snapshot
  and read validated, half-open byte ranges from object storage. Hybrid
  currently falls back to lexical results.
- The gRPC integration test interrupts the indexing checkpoint, replays
  the receipt after restart, and checks the exact Unicode range, RTF
  original retention, replacement fencing, and invalid-input recovery.
- A standalone server and CLI against MinIO verified both worker stages,
  Unicode byte-range fetch, RTF replacement, hybrid fallback, and prior
  revision fetch. After restart, duplicate indexing left the published
  snapshot unchanged.
- Delivered in [PR #30](https://github.com/ArcaneArts/Sanctum/pull/30).

## Cloud Tasks dispatch and repair

- Hosted upload acceptance records a repair hint before committing the
  operation envelope. The API attempts Cloud Tasks delivery after acceptance,
  and the worker attempts the next dispatch after committing its stage.
  The envelope records delivery by stage and time window.
- The private maintenance endpoint scans a bounded page of a single shard
  and persists its cursor in object storage. It reads operation envelopes
  directly and can retry after an API or worker restart.
- The HTTP fixture checks Google Cloud Tasks request format, OIDC audience
  and account, deduplication, and retryable service errors. The local
  integration test interrupts dispatch and resumes the repair cursor across
  a fresh service instance, then advances both worker stages.
- A standalone server with MinIO and deliberately unavailable ADC accepted
  an upload, resumed repair after restart, and cleared its dispatch hint
  after manual worker delivery reached text ready.
- Actual Cloud Run ingress, Cloud Tasks delivery, Cloud Scheduler invocations,
  and live GCS still require an authorized Sanctum Google Cloud project.
  Hosted embedding work, cleanup, and additional retrieval APIs remain open.

- Delivered in [PR #31](https://github.com/ArcaneArts/Sanctum/pull/31).

## Cloud Run runtime and packaging

- The Linux image supports separate Cloud Run API and worker roles. Each
  binds the injected PORT and handles SIGTERM. The API uses gRPC over
  HTTP/2, and the worker serves private task and repair routes.
- The CLI supports TLS and attaches SANCTUM_GRPC_ID_TOKEN on HTTPS.
  The [deployment guide](CLOUD_RUN.md) records IAM, queue and Scheduler
  setup, and an authenticated deployment smoke test.
- Local process tests verified port binding and graceful SIGTERM shutdown.
  A cross-compiled Linux amd64 release executable ran as both roles in
  non-root Debian containers and completed upload, HTTP registration,
  indexing, and gRPC search against MinIO. The Dockerfile static check
  passed.
- Live Cloud Run and GCS verification are still open.

- Delivered in [PR #32](https://github.com/ArcaneArts/Sanctum/pull/32).

## Hosted snapshot reuse

- Hosted catalog reads and lexical retrieval resolve the current HEAD on
  each request and reuse its verified immutable SQLite file. Cache limits
  bound retained files by size and count. Active readers can finish after
  eviction, and an oversized snapshot is read without caching.
- Hosted search and fetch hits report the generation that supplied their
  byte offsets. SQLite opens cached collection snapshots read-only.
- Tests verify a shared cold download, warm file reuse, refresh after
  publication, eviction, and an active reader surviving refresh. The
  MinIO publication contract also checks warm reuse.
- Hosted reads currently reopen SQLite connections on the warm file.
  Capacity measurements and the hosted vector path remain open.

- Delivered in [PR #33](https://github.com/ArcaneArts/Sanctum/pull/33).

## Hosted embedding batches and search

- The hosted API and worker use the configured server-level provider.
  A private worker request processes up to 16 distinct pending chunk
  inputs and publishes one SQLite snapshot. Its transaction writes only
  vectors whose inputs are still current. Collection and revision status
  report coverage for the configured profile.
- Hosted single-collection search supports lexical, semantic, and hybrid
  modes. Hybrid stays lexical when coverage or the provider is unavailable.
  Incompatible profiles are excluded immediately.
- The hosted gRPC test verifies outage fallback, unchanged-input reuse,
  and a profile change followed by a rebuild. A standalone MinIO run used
  Ollama's 768-dimensional embeddinggemma-2:740m model and verified
  semantic and hybrid retrieval, a no-work replay, and restart persistence.
- Delivered in [PR #34](https://github.com/ArcaneArts/Sanctum/pull/34).

## Hosted embedding dispatch

- Lexical publication records the first embedding step before returning.
  Each worker delivery processes one bounded batch, checkpoints the next
  step, and uses the same partitioned dispatch repair as uploads. Transient
  provider failures persist retry times and can schedule delayed tasks.
- Batch receipts are keyed by operation, step, and provider profile.
  The integration test interrupts an envelope checkpoint after vector
  publication, then verifies redelivery without publishing another
  snapshot. It also verifies out-of-order rejection and hint cleanup.
- Standalone worker deliveries against MinIO and real Ollama verified
  both embedding steps, duplicate delivery, and semantic search after
  restart. The Cloud Tasks payload fixture checks delayed delivery.
- Delivered in [PR #35](https://github.com/ArcaneArts/Sanctum/pull/35).

## Hosted search across collections

- Hosted gRPC can search up to eight collections belonging to one tenant.
  It shares a query vector across those collections, aggregates coverage,
  and uses the same rank merge as local search. Each returned hit keeps
  its collection and database generation.
- The integration test verifies one provider query, partial coverage,
  byte-range fetches for results from both collections, and tenant
  isolation.
- A standalone MinIO and Ollama run exercised hybrid search across an
  embedded collection and a collection awaiting vectors. Semantic search
  returned the embedded collection, and restart preserved both results.
- Delivered in [PR #36](https://github.com/ArcaneArts/Sanctum/pull/36).

## Hosted embedding profile rebuilds

- Scheduled discovery detects a changed server profile and scans object
  manifests with a durable cursor. Each discovered collection gets
  bounded rebuild tasks through the existing dispatch repair index.
- The integration test covers an idle collection, a restart between
  discovery pages, worker profile mismatch, duplicate delivery, and
  rebuilt coverage for the new profile.
- A standalone MinIO and real Ollama run changed the document prefix
  of the configured profile. The API excluded the old vectors and
  preserved hybrid fallback. The discovery route and manually delivered
  worker steps restored semantic search for the idle collection.
- Discovery lists object metadata across the deployment prefix. Its
  scan cost and live Cloud Scheduler delivery remain to be measured.

- Delivered in [PR #37](https://github.com/ArcaneArts/Sanctum/pull/37).

## Hosted source deletion and reclamation

- Hosted deletion uses a fenced SQLite publication with an expected
  revision, a durable request receipt, and a tombstone. It immediately
  removes hits and fences registered replacements.
- The source sweep persists a scan cursor. It retires locators under
  the writer, keeps immutable bytes for a 15-minute reader grace, and
  retries physical deletions from a durable cleanup table. Successful
  text-only RTF indexing also drops the accepted original.
- The integration test verifies a stale deletion, receipt replay,
  replacement fencing, stale references, both reclamation phases,
  and an empty repeat sweep. It advances timestamps inside test snapshots.
- A standalone MinIO smoke verified text-only RTF retention, deletion
  replay after restart, search exclusion, and the worker sweep preserving
  bytes during the grace period.
- Delivered in [PR #38](https://github.com/ArcaneArts/Sanctum/pull/38).

## Scheduled hosted snapshot reclamation

- The private worker snapshot sweep persists discovery and per-unit
  cursors. It marks and deletes superseded catalog and collection
  snapshots under their writer fences, with a 15-minute grace from the
  first observed retirement.
- The integration test exercises a busy writer, cursor resumption,
  marker aging, and preservation of the current snapshot.
- A standalone MinIO smoke verified that the scheduled HTTP endpoint
  marks the retired catalog, preserves the current generation and
  reader grace, and continues after restart.
- Abandoned input and extraction candidates still need a cleanup path.
- Delivered in [PR #39](https://github.com/ArcaneArts/Sanctum/pull/39).

## Hosted MCP ingress

- The object-backed API shares its port between gRPC and stateless MCP.
  The Cloud Run worker role keeps its separate HTTP task listener.
- Hosted MCP can create and list collections, durably accept inline
  text with an optional request UUID, poll ingestion, search one or
  several collections, and fetch cited ranges. Tools call the same
  object-backed service as gRPC and preserve the database generation.
- The MinIO smoke uses two API processes and a separate worker. It
  verifies request replay across instances, shared gRPC and MCP data,
  exact UTF-8 ranges, tenant scope, and HTTP/1.1 and h2c on one port.
- The deployment guide includes an authenticated MCP request. Actual
  Cloud Run ingress remains open.
- Delivered in [PR #40](https://github.com/ArcaneArts/Sanctum/pull/40).

## Hosted metadata

- Hosted gRPC and MCP read and mutate collection, current source, and
  chunk metadata through their shared fenced publication service.
  The transaction returns its own metadata rows while the durable
  receipt stores only a short outcome.
- Hosted gRPC lists, sorts, counts, and groups metadata matches from
  read-only snapshots. Retrieval evaluates metadata filters against
  current data. Source and chunk edits record change events.
- The gRPC integration test verifies rollback, restart, merge-patch,
  JSON null grouping, tenant scope, and revision reset. The MinIO MCP
  smoke verifies collection and chunk writes across API instances
  and search filtered by published catalog metadata.
- Delivered in [PR #41](https://github.com/ArcaneArts/Sanctum/pull/41).

## Hosted change cursors and streams

- gRPC polls and streams the collection change log from published
  snapshots, with the same cursor contract as the filesystem service.
  An idle hosted stream checks for new generations every two seconds.
- The integration test subscribes before indexing, verifies that
  registration has no visible event, observes worker publication,
  reconnects through a new API instance, and replays replacement and
  metadata reset in order. It also checks cursor and tenant scope.
- A generated Python gRPC client used two MinIO-backed API processes
  to observe publication and resume replacement and reset events.
- Delivered in [PR #42](https://github.com/ArcaneArts/Sanctum/pull/42).

## Hosted database inspection

- Scoped operator handles store the immutable publication identity
  in object storage. Any API instance can query or download the
  pinned SQLite generation with the same read-only SQL limits as the
  local API. Hosted sessions reuse verified snapshots from the cache.
- The snapshot sweep removes expired handle records. The handle
  lasts five minutes, within the fifteen-minute reader grace for
  retired generations.
- The gRPC integration test covers restart, exact exported bytes,
  scope and SQL restrictions, close, expiry, and eventual removal.
- A generated Python client queried and exported the pinned catalog
  from MinIO after the opening process exited. It compared the export
  byte for byte with the object and queried the collection schema.
- The live check found that SQLite's columnless read for `count(*)`
  omits the schema. The SQL authorizer now handles that case while
  retaining the same write and attachment restrictions.
- Delivered in [PR #43](https://github.com/ArcaneArts/Sanctum/pull/43).

## Abandoned upload candidates

- The worker upload sweep persists a bounded cursor. It reclaims
  losing input attempts after an envelope identifies the winner
  and old extracted text without a published locator or retirement
  entry. It keeps winning input, current extracted text, and
  recent attempts.
- The integration test ages objects, restarts between bounded batches,
  checks which objects survived, and verifies exact retrieval.
- The live MinIO worker route preserved a recent attempt and published
  RTF text across restart. The local object-store test verifies deletion
  using controlled object ages.
- A follow-up adds an acceptance fence for inputs without an accepted
  envelope.
- Delivered in [PR #44](https://github.com/ArcaneArts/Sanctum/pull/44).

## Hosted synchronous uploads

- Hosted `UploadText` uses the same durable admission as `SubmitText`.
  It waits up to 30 seconds after acceptance for worker publication.
  A wait failure reports the operation UUID for polling or replay.
- The gRPC test confirms the call waits through registration, returns
  the committed revision, and reports a terminal validation failure
  with the accepted operation ID. Both outcomes replay with the same
  request UUID.
- The live MinIO check exercised a worker-driven synchronous upload,
  a matching replay, and a timeout that left the operation available
  for subsequent worker delivery and status polling.
- Delivered in [PR #45](https://github.com/ArcaneArts/Sanctum/pull/45).

## Fenced reclamation of unaccepted inputs

- Reclamation and acceptance compete on the same conditional envelope
  write. The sweep creates or advances a small fence before deleting an
  old input that has no accepted envelope. A later submission can replace
  the fence only with a new attempt. Workers still see the operation as
  absent until an accepted envelope exists.
- The integration test delays an admission at its conditional write,
  reclaims the candidate, rejects the delayed write, and publishes a
  fresh attempt with the same operation UUID.
- The live MinIO smoke seeded a fence, replaced it using the server's
  ETag precondition, replayed the accepted request, and indexed it
  through the worker.
- Delivered in [PR #46](https://github.com/ArcaneArts/Sanctum/pull/46).

## Hosted collection tombstones

- `DeleteCollection` removes the collection from the tenant catalog
  and fences its manifest. The scheduled snapshot sweep reconciles
  a tombstone left before the collection fence. The tenant catalog
  reserves deleted IDs to prevent old tasks from reaching a new collection
  with the same scope.
- The gRPC integration test holds the collection lease to interrupt
  deletion between the two commits, restarts the service, and checks
  the repair, request replay, and retired ID behavior.
- A generated Python client exercised deletion and replay across a
  MinIO-backed API restart and verified the stored manifest fence.
- Delivered in [PR #47](https://github.com/ArcaneArts/Sanctum/pull/47).

## Hosted collection reclamation

- The hosted manifest records the time it was fenced. After a
  15-minute grace, the snapshot sweep removes the collection's
  source, operation, and database objects in bounded pages and
  leaves its permanent fence. Uploads recheck the catalog after
  staging and remove their input if deletion won.
- The integration test verifies a scoped reader within the grace
  period, resumes across multiple cleanup pages after restart, and
  queries an already materialized snapshot after object removal.
- The live MinIO worker route preserved a recently retired snapshot.
  After advancing the stored deletion timestamp in the test fixture,
  the worker removed the collection objects and preserved the fence.
- Delivered in [PR #48](https://github.com/ArcaneArts/Sanctum/pull/48).

## Filesystem collection deletion

- Local gRPC and the CLI delete a collection by committing its tenant
  tombstone, removing its runtime jobs and watches, and reclaiming its
  directory. The catalog tracks unfinished reclamation for startup and
  periodic repair. An open operator session keeps its SQLite copy.
- The integration test verifies tenant scope, removal of queued work,
  preservation of an open snapshot, retry, and recovery from a stop
  between tombstone publication and physical reclamation.
- A standalone CLI smoke deleted and replayed a local collection and
  checked that the other tenant's database was preserved.
- Delivered in [PR #49](https://github.com/ArcaneArts/Sanctum/pull/49).

## Source listing and inspection

- Both gRPC backends list current published sources with UUID
  pagination and inspect a revision with an optional expected ID.
  Responses preserve provenance, exact canonical size, hashes, and
  original retention. Hosted reads include the snapshot generation.
- The hosted integration test verifies pagination, RTF provenance and
  retention, pending and deleted source visibility, and tenant scope.
- A generated Python client against MinIO verified that an accepted
  upload is absent from listing until publication and inspected the
  extracted UTF-8 size and external ID afterward.
- Delivered in [PR #50](https://github.com/ArcaneArts/Sanctum/pull/50).

## Bounded citation context

- gRPC and MCP can fetch context around an exact chunk reference.
  Both backends read one bounded byte range and verify the cited
  chunk within it. The returned context is trimmed at UTF-8 code
  point boundaries and identifies its own canonical byte range.
- The hosted integration test exercises split UTF-8 code points,
  clipping at source boundaries, and rejection of oversized budgets.
- Local stdio and hosted Streamable HTTP smokes verified that the
  context tool returns the same cited hit and exact source text.
- Delivered in [PR #51](https://github.com/ArcaneArts/Sanctum/pull/51).

## Runtime information

- gRPC and the CLI expose process health, storage mode, effective
  limits, and feature capabilities. The response constructs an
  explicit allowlist of fields. Hosted API and worker listeners have
  HTTP health endpoints. Process health reads do not contact storage or providers.
- A standalone smoke verified the local and hosted gRPC responses,
  the CLI, and both hosted health routes with unreachable dependency
  endpoints. The response omitted configured resource names and
  credential environment references.
