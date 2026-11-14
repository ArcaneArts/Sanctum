# Implementation status

[PLAN.md](PLAN.md) defines the scope and acceptance criteria. This record tracks
which parts have working implementations and where to find the evidence.

| Milestone | Current implementation | Remaining work |
| --- | --- | --- |
| 1. Local desktop and MCP | Local Rust service with gRPC, MCP HTTP and stdio, and a Tauri tray app for storage, imports, activity, settings, and Ollama-backed retrieval | Personal workflow verified on macOS. Cross-platform packaging and tray verification are tracked in milestone 2. |
| 2. Providers and local integrations | Local profile rebuilds, multi-collection retrieval, embedding adapters and desktop settings, watched folders, URL and Git imports, and a Windows x64 installer build | Live hosted provider checks, Windows installation and tray verification, distribution signing |
| 3. Object storage and publication | Shared storage contract, filesystem and MinIO-tested S3, native GCS adapter, and fenced SQLite snapshot publication for hosted catalog, source, and metadata operations | Live GCS verification and cloud recovery scenarios |
| 4. Cloud Run and Cloud Tasks | Linux API and worker containers, durable staged uploads, dispatch and repair, scheduled maintenance, and hosted gRPC and MCP | Live IAM, Cloud Run ingress, and Tasks and Scheduler delivery |
| 5. Capacity | [200 MiB ingestion, GiB-scale snapshots, 10,000 MinIO tenants, and real embedding benchmarks](CAPACITY.md) | Cold/warm Cloud Run performance and production rollout limits |

## Acceptance and next verification

- **Local desktop and integrations.** The [macOS desktop workflow](#desktop-tray-and-first-use-flow)
  exercised configuration, imports, and tray lifecycle. The
  [Codex CLI check](#codex-mcp-retrieval) used real Ollama vectors and retrieved
  exact citations across a source edit and server restart. The Ubuntu ARM64
  package passed the [X11 tray workflow](#native-desktop-package-verification).
  The [Windows x64 installer](#windows-x64-installer-build) needs installation,
  WebView2 startup, and tray lifecycle verification on a licensed Windows
  machine or an authorized evaluation guest. Distribution signing and macOS
  notarization also remain open.
- **Embedding providers.** Ollama and the OpenAI-compatible adapter have
  returned real embeddings. The latter used Ollama's compatibility endpoint.
  Vertex AI's next check is a real document and query prediction with the
  configured dimensions and subsequent semantic retrieval. It needs a
  Sanctum Google Cloud project and a prediction-enabled identity.
  [Provider evidence](#google-cloud-embeddings).
- **Object storage and recovery.** Filesystem and MinIO exercised exact
  ranges, writer takeover, interrupted publication, and recovery from a
  lost response. The live GCS contract needs a Sanctum test bucket and an
  authorized ADC identity. [Storage contract](STORAGE.md#google-cloud-storage-adapter),
  [publication evidence](#fenced-object-backed-database-publication).
- **Cloud Run.** The API and worker containers and task dispatch are in
  the repository. Local role processes on MinIO exercised stage replay
  and HTTP handlers. The hosted deployment still needs live Cloud Run IAM,
  gRPC and MCP ingress, real Cloud Tasks and Scheduler delivery, observed
  scale-to-zero recovery, and cold/warm timings and memory at the target
  database sizes. This requires the authorized Sanctum project, deployment
  region, bucket, and deployer and invocation identities.
  [Hosted acceptance runbook](CLOUD_RUN.md#verify-a-deployment).
- **Capacity.** The recorded workloads cover 200 MiB UTF-8 and RTF sources, a 2.34 GiB
  collection, 10,000 mostly idle MinIO tenants, concurrent active
  collections, and real embeddings. On Linux ARM64 with MinIO, concurrent
  cold gRPC searches against two 2 GiB snapshots took 5.96–6.40 s and
  reached 4.55 GiB of cgroup memory. Cloud Run still needs its own
  transfer, database open, warm reuse, and memory measurements before
  setting production limits. [Benchmark results](CAPACITY.md).

The sections below record individual delivery slices. Later measurements
and recovery checks extend the initial evidence for each feature.

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
- Caller-provided upload request IDs were added in a later cycle.

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
  on a second fetch. The [private HTTPS check](#authenticated-git-over-https)
  exercises operator-supplied credentials through a real Git backend.

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
- Direct Dockerfile builds for Linux amd64 and arm64 ran both non-root
  roles against MinIO. The container smoke verified upload, task delivery,
  gRPC search, and SIGTERM shutdown on each architecture. Live Cloud Run
  and GCS verification are still open.
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
- Delivered in [PR #52](https://github.com/ArcaneArts/Sanctum/pull/52).

## Large SQLite snapshot measurements

- The opt-in MinIO benchmark uses the production writer and cache.
  It records exact snapshot sizes, three cold and warm samples,
  SQLite point query latency, and the cost of publishing a one-row
  update. [Method and measurements](CAPACITY.md).
- Representative text and vector query workloads, concurrent
  collections, and Cloud Run capacity were outside that run.
  [Subsequent capacity measurements](CAPACITY.md).
- Delivered in [PR #53](https://github.com/ArcaneArts/Sanctum/pull/53).

## Retrieval metadata

- gRPC and MCP hits identify their canonical UTF-8 representation.
  Filtered searches include the relevant stored metadata fields from
  the snapshot used for ranking. Each hit has a bounded metadata
  budget and reports omitted values. Full metadata reads remain
  available through the target APIs.
- The gRPC test checks typed values and omission of a value over
  budget. Local and hosted MCP checks verify filter metadata on
  cited results, including a hosted multi-collection query.
- Delivered in [PR #54](https://github.com/ArcaneArts/Sanctum/pull/54).

## Collection names

- Catalog lists expose display names and creation times. gRPC and
  the CLI can update the display name without changing the stable
  collection ID or its source paths. Old snapshots default to their
  existing IDs as names.
- Local and hosted integration checks verify the stable scope. A
  CLI and generated Python MinIO check verifies Unicode display names
  and an unchanged creation time after restart.
- Delivered in [PR #55](https://github.com/ArcaneArts/Sanctum/pull/55).

## Configurable chunking

- Server JSON controls the bounded streaming UTF-8 window and overlap.
  Local jobs and hosted envelopes persist the settings at acceptance.
  Legacy jobs retain the original defaults, and revision inspection
  records the chunking strategy used.
- Integration checks reopen the filesystem executor and deliver hosted
  work through a worker with different settings. Both preserve the
  accepted window. The hosted test verifies the resulting UTF-8 range.
- A generated Python client verified the configured chunker identifier
  on a published RTF revision in live MinIO.
- Delivered in [PR #56](https://github.com/ArcaneArts/Sanctum/pull/56).

## Hosted operation retention

- Scheduled maintenance compacts terminal upload envelopes after
  45 days. It preserves canonical input references and reserves old
  request UUIDs. Status and worker calls reject expired operations.
- The filesystem integration test ages terminal records and their input,
  runs the sweep, and verifies pending work, duplicate rejection, and
  retrieval. A live MinIO run installs a compact record fixture and
  verifies duplicate rejection and retrieval after server restart.
- Delivered in [PR #57](https://github.com/ArcaneArts/Sanctum/pull/57).

## Per-hit embedding state

- Local and hosted search, chunk fetch, and context fetch report the
  active profile's cache state on each hit. Status comes from the same
  SQLite read as the chunk reference and does not call the provider.
- Integration checks cover pending, ready, retrying, failed, profile
  change, and a query outage with ready document vectors.
- The local MCP HTTP and stdio check verifies disabled status. Live
  MinIO and Ollama verify pending, ready, and pending after a profile
  change over MCP HTTP, including status on fetched context.
- Delivered in [PR #58](https://github.com/ArcaneArts/Sanctum/pull/58).

## Hosted upload cancellation

- Hosted gRPC and CLI cancel accepted or registered uploads. Cancellation
  and indexing share a fenced SQLite receipt, and worker replay completes
  cleanup if the operation envelope update was interrupted. MCP status
  reports canceled operations.
- Integration checks cover lost checkpoints on either side of publication,
  duplicate cancellation, tenant isolation, and reuse of an external ID
  after canceling a registered upload.
- Live MinIO verified cancellation before and after registration, duplicate
  delivery across restart, input reclamation, and refusal after commit.
- Delivered in [PR #59](https://github.com/ArcaneArts/Sanctum/pull/59).

## Hosted upload retry

- Hosted gRPC and CLI can request a fresh dispatch for pending upload work
  while retaining its operation, source, revision, and SQLite receipts.
- The transport fixture verifies a distinct Cloud Tasks name and identical
  payload for a manual retry. The hosted integration loses the response to
  the durable retry checkpoint and verifies dispatch repair after restart.
- A live MinIO and CLI check verified the retry checkpoint across restart,
  stable upload identity, and duplicate worker delivery after retry.
- Delivered in [PR #60](https://github.com/ArcaneArts/Sanctum/pull/60).

## Local operation retention

- Local maintenance removes at most 256 terminal queue records per pass
  after 45 days. The migration gives existing terminal records the same
  window. Pending and retryable jobs retain their input and status.
- The local gRPC test runs maintenance, restarts the service, and verifies
  expiration, searchable published bytes, and retained pending inputs.
- Delivered in [PR #61](https://github.com/ArcaneArts/Sanctum/pull/61).

## Pending literal scan

- Local and hosted gRPC and MCP scan accepted UTF-8 input by operation ID
  while it awaits lexical publication. The response reports exact match
  ranges, bytes read and searched, and completion or a partial reason.
- The gRPC tests exercise exact matches across a 64 KiB boundary, limits,
  invalid text, empty uploads, tenant scope, and unextracted RTF. Dropping
  the gRPC stream cancels an in-flight range read in the storage fixture.
- The hosted MCP smoke on live MinIO returns the exact UTF-8 byte range
  of a literal before any indexing task has run.
- Delivered in [PR #62](https://github.com/ArcaneArts/Sanctum/pull/62).

## Source metadata on upload

- Upload headers and MCP deposits accept namespaced JSON source metadata.
  Local queue records and hosted envelopes carry it through restart.
  SQLite commits the metadata and lexical publication together.
- Local integration verifies that the previous metadata stays visible
  until replacement publication. Hosted integration verifies restart,
  replay with reordered metadata, and rejection of changed replay values.
- Live MinIO verifies that metadata submitted through one hosted MCP
  process is visible after the worker publishes the revision.
- Delivered in [PR #63](https://github.com/ArcaneArts/Sanctum/pull/63).

## Chunk metadata on upload

- The upload header, CLI, and MCP accept namespaced entries per chunk
  ordinal. Source and chunk metadata share one upload budget and publish
  with the new revision. An invalid ordinal aborts publication.
- Local and hosted tests verify durable metadata and a failed replacement
  that preserves the old revision and its metadata.
- The live MinIO MCP smoke submits and reads chunk metadata after the
  hosted worker publishes the revision.
- Delivered in [PR #64](https://github.com/ArcaneArts/Sanctum/pull/64).

## Hosted job activity

- Hosted gRPC and CLI page through retained job records by tenant or
  collection. The storage cursor is scope-bound and listing work is
  bounded per request. Deleted collections are excluded.
- The gRPC test resumes on another instance and checks short and empty
  pages, tenant isolation, and pending and canceled operation states.
- Live MinIO and the CLI verify that a hosted continuation survives a
  restart and returns pending and canceled records across collections.
- Delivered in [PR #65](https://github.com/ArcaneArts/Sanctum/pull/65).

## Local upload request replay

- Local uploads and MCP deposits accept a scoped request UUID. The queue
  commits its receipt with the accepted job, and finished-job maintenance
  keeps a compact reservation after expiry.
- The local gRPC test verifies concurrent replay, distinct tenant scope,
  restart, conflicting bytes, and UUID expiry with published text intact.
- Local MCP HTTP smoke submits the same request twice and verifies the
  original operation and revision IDs.
- Delivered in [PR #66](https://github.com/ArcaneArts/Sanctum/pull/66).

## Desktop sidecar rebuilds

- `pnpm devtop` watches server and CLI crates, Protobuf, and workspace
  manifests. The Tauri build prepares sidecars before restarting the host.
  Tauri owns the Vite process and the desktop host owns the server.
- The Apple Silicon `pnpm devtop` smoke edited a server log string,
  verified the new binary and child PID, and checked that a collection
  survived both rebuilds. Ctrl+C released the frontend and API ports.
- Delivered in [PR #67](https://github.com/ArcaneArts/Sanctum/pull/67).

## Hosted embedding job progress

- Job status and activity expose the durable embedding batch checkpoint
  and retry time through gRPC. MCP and the CLI show the same checkpoint.
- Hosted integration checks the checkpoint before embedding, after replay
  of an interrupted batch, during a persisted retry delay, and after work ends.
- A real MinIO and Ollama smoke observed batch 0 before embedding and batch 1
  after worker progress, then checked completion, semantic and hybrid search,
  and semantic retrieval after restart.

## Codex MCP retrieval

- Read-only MCP tools declare their behavior in `tools/list`. HTTP and stdio
  report the same annotations. A live MinIO smoke verified hosted discovery and
  retrieval across separate API processes.
- Codex CLI 0.160.1 used Streamable HTTP and an existing ChatGPT login in two
  ephemeral sessions. A filesystem-backed server with real 768-dimensional
  Ollama embeddings imported a test directory, then reimported it after an edit.
  The model used hybrid search and `fetch_chunk`. Both excerpts, source and
  revision IDs, ordinals, and byte ranges matched the gRPC result. The second
  lookup ran after server restart, with the same source ID and a new revision.
- The earlier macOS desktop smoke separately exercised the directory picker,
  persisted settings, the tray, and the managed sidecar. See
  [Codex CLI connection steps](MCP.md#codex-cli).

## Standalone object-backed worker

- Set `standalone_object_worker` to process durable object-backed uploads in
  the same server as gRPC and MCP. The worker uses the existing publication
  fences and stage receipts, runs embedding profile discovery and maintenance,
  and resumes accepted jobs from persisted hints.
- A hosted fixture restarted between registration and indexing and verified
  that the accepted revision ID survived and the finished hint was removed.
- A real MinIO and Ollama run recovered a previously accepted job, published a
  synchronous upload, retrieved exact UTF-8 through MCP, and rebuilt embeddings
  after the configured profile changed.
- Delivered in [PR #70](https://github.com/ArcaneArts/Sanctum/pull/70).

## Desktop object storage

- Desktop settings select a filesystem folder, S3-compatible storage, or
  Google Cloud Storage. The Rust host passes object settings to its sidecar,
  starts its standalone worker, and shows the active gRPC/MCP endpoint.
- On Apple Silicon, the native settings switched a disposable server from a
  filesystem folder to live MinIO. The UI created a collection, streamed a
  file through the native picker, displayed its registered and text-ready
  states, and measured stored object sizes. gRPC and MCP returned the same
  41-byte UTF-8 chunk. Google Cloud Storage was checked through configuration
  tests. Live GCS remains open.
- `pnpm check` and `pnpm desktop:check` passed. The desktop settings tests
  cover prior filesystem configuration and both object backend configurations.

## Large text ingestion measurements

- A release benchmark streamed and indexed a deterministic 200 MiB UTF-8
  source through the production filesystem and standalone MinIO servers.
  Exact MCP range reads matched the final 2,631 bytes on both backends.
- Local publication took 6.018 s at a sampled peak server RSS of 29,392,896
  bytes. MinIO took 9.130 s at 46,415,872 bytes. The workload used lexical
  indexing with embeddings disabled. [Reproduction and results](CAPACITY.md).

## Exact vector ranking measurements

- Local, hosted, and cross-collection retrieval use the `VectorIndex` boundary.
  The production implementation retains the SQLite BLOB format and exact
  Rust scorer, with current-revision and metadata conditions applied before
  top-k.
- A release comparison with pinned sqlite-vec v0.1.9 at 262,144 vectors and
  both 768 and 1,536 dimensions matched all 240 returned top-ten neighbors.
  The scalar extension did not improve median latency.
  [Method and results](CAPACITY.md#exact-vector-ranking-in-sqlite),
  [subsequent retrieval measurements](CAPACITY.md#retrieval-relevance-and-embedding-reuse).

## Native desktop package verification

- Tauri built the Apple Silicon `Sanctum.app` release bundle with the desktop
  executable and both ARM64 sidecars. The bundled server started with an
  isolated application data directory, and the bundled CLI saw a collection
  created before the app restart. The build used local ad-hoc signing.
- The macOS tray and native file picker were exercised in the earlier debug
  builds. Distribution signing and notarization remain open.
- The ARM64 Debian package built and installed on Ubuntu 24.04.5 with
  WebKitGTK 4.1. In an isolated X11 session with Openbox and the Xfce 4.18
  tray host, the installed app registered its tray menu and displayed
  running status. Closing the window hid it while the bundled server still
  accepted gRPC and MCP connections. **Open Sanctum** restored the window,
  a second launch kept one server child, and **Quit Sanctum** released both
  listeners. The collection survived relaunch. The tray actions were
  exercised through the session's D-Bus menu.
- The Linux package evidence covers Ubuntu ARM64 and this X11 tray host.
  Windows, other Linux desktop environments, and signed distribution remain
  open. [Native package commands](DESKTOP.md#native-packages).

## Mostly idle tenant capacity

- A release build initialized 1,000 tenants and exercised four active collections
  concurrently against live MinIO. Repeated searches had an 18.32 ms p50 and
  54.88 ms p95. Eight source replacements reached lexical readiness with a
  658.74 ms p50 and 860.10 ms p95. Source identity and tenant isolation held.
- The measured cluster usage matched a separate MinIO listing across 4,052
  objects. [Method and results](CAPACITY.md#concurrent-collections-among-1000-tenants).

## Multi-GiB collection replacement

- Twelve 200 MiB sources published through the standalone MinIO server.
  First search after restart took 4.20 s. Three warm searches took 17–34 ms.
- Changing eight bytes in the final source preserved its ID, replaced the
  old search hit, and returned the exact new MCP range. Text readiness took
  69.73 s. Registration and indexing each wrote a complete SQLite snapshot.
  [Method and measurements](CAPACITY.md#small-edit-in-a-multi-gib-collection).

## Publication snapshot reuse

- Hosted receipt lookups, readers, and writers share the verified snapshot
  cache. A writer modifies a private copy and caches the new SQLite backup
  after the manifest compare-and-swap succeeds.
- The regression test counted zero object downloads for cached reads and
  subsequent writes, preserved active readers of prior generations, and
  refreshed after publication from another instance. The filesystem and
  live MinIO publication contract tests passed, including recovery after a
  lost publish response.
- A release rerun with 2.34 GiB of text on MinIO reduced the measured
  eight-byte replacement time from 69.73 s to 29.56 s. It still wrote two
  full snapshots. [Method and measurements](CAPACITY.md#reusing-verified-snapshots-during-publication).

## Retrieval relevance and provider work avoided

- A release filesystem server indexed all 8,674 BEIR ArguAna documents
  through the generated Python gRPC client. Ollama supplied 768-dimensional
  EmbeddingGemma 2 vectors for every current chunk.
- Across 1,406 judged queries, semantic and hybrid each measured 0.91038
  Recall@10 and 0.63994 nDCG@10. The all-term lexical mode returned results
  for 63 queries and reached 0.00071 Recall@10 on the paragraph queries.
- An eight-byte edit changed one of 11 chunks. The live Ollama request
  contained one input, with the other ten chunks immediately able to
  reuse vectors from the same profile. [Method and results](CAPACITY.md#retrieval-relevance-and-embedding-reuse).

## Windows x64 installer build

- The desktop build hook can use `cargo xwin` for the Rust sidecars when
  Tauri uses the same Windows cross-compilation toolchain. The default build
  path still uses Cargo for native development and packaging.
- A release build on Apple Silicon produced a 14.24 MiB unsigned x64 NSIS
  installer. Extracting its payload confirmed three x64 PE executables.
  Both packaged sidecars matched the compiler output byte for byte.
- A disposable x64 Windows 11 Enterprise evaluation guest booted from
  checksum-verified Microsoft media into Windows Setup. Installing the
  app and exercising the tray, sidecar lifecycle, and WebView2 still
  require a licensed Windows environment. [Build commands](DESKTOP.md#native-packages).

## SQLite page-delta feasibility

- The release MinIO benchmark repeated the eight-byte replacement with
  2.34 GiB of current source text. The latest three SQLite snapshots
  identified the prior generation and the two replacement stages.
- Registration changed three pages. Indexing changed 35,413 pages.
  The offline delta script reconstructed both snapshots byte for byte.
  Together the two delta files were 10.14% of the published snapshot bytes.
- This measurement supports trying bounded delta publication with fenced
  commits, verified replay, and dependency-aware retirement.
  [Method and results](CAPACITY.md#sqlite-page-changes-during-a-source-replacement).

## Bounded hosted SQLite page deltas

- Hosted writers can publish ordered page deltas under the same fenced
  HEAD protocol as full backups. Readers verify each object and the
  reconstructed SQLite digest. The option defaults off and the first
  base remains a standard SQLite backup.
- The filesystem and live MinIO contract tests covered four deltas,
  full-backup compaction, idempotent receipts, cold reads, and retirement
  grace. A fault fixture verified an uploaded but unpublished delta, a
  lost HEAD response after commit, and rejection of a corrupted delta.
- A gRPC database session opened against a delta generation exported a
  complete SQLite file on another instance after a full backup replaced
  its chain. The MinIO hosted MCP smoke ran across two API processes and
  a worker with delta publication enabled.
  [Storage format and rollout](STORAGE.md#optional-sqlite-page-deltas).
- In the 2.34 GiB MinIO benchmark the eight-byte edit reached lexical
  readiness in 20.38 s and uploaded 145,351,400 bytes of database data.
  The full-backup comparison took 30.00 s and uploaded 1,432,829,952
  bytes. Cold search increased from 2.68 s to 3.71 s in these runs.
  [Measurement](CAPACITY.md#publishing-the-deltas-on-minio).

## Partial lexical matches for hybrid queries

- The local and object-backed retrieval paths try the same all-term FTS5
  query. Hybrid can use partial matches when both the all-term and
  semantic searches have zero eligible chunks in a collection.
- The gRPC tests exercised local strict and hybrid queries and hosted
  multi-collection retrieval. The MCP smoke compared explicit lexical
  search over HTTP with hybrid over stdio, then fetched the exact
  matching UTF-8 range. [Search modes](LOCAL_API.md#ollama-and-search-modes).
- The full ArguAna run with embeddings disabled reached 0.31437 Recall@10
  and returned results for 662 of 1,406 queries. The strict lexical
  baseline returned results for 63. [Method](CAPACITY.md#partial-lexical-matches-for-prose-queries).
- Fusing partial matches with available vectors lowered Recall@10 on
  ArguAna from 0.91038 to 0.88549. The fallback is limited to collections
  with zero eligible semantic hits.

## Local collection write contention

- New uploads stage bytes and write their durable queue record without a
  collection write transaction. Replacement registration, import identity
  checks, indexing, embedding commits, and revision metadata writes
  coordinate through the same collection-specific writer lock.
- A gRPC regression test held a SQLite writer open while submitting a new
  source. The server accepted the operation and indexed it after the test
  released the SQLite lock. `pnpm check` and the local MCP smoke passed.
- The release ArguAna run completed all 8,674 uploads with 16 workers
  and zero SQLite-busy retries. It reached full Ollama coverage for
  8,695 chunks in 464.96 s. [Method](CAPACITY.md#local-ingestion-with-16-workers).

## Named metadata indexes

- The gRPC API manages optional per-database indexes for top-level
  existence and equality filters on collection, source, and chunk
  metadata. Local collection writes share the collection lock, and
  hosted changes use fenced publication.
- Bundled SQLite's query plan used the partial index at all three
  metadata levels. The test checked identical results before creation,
  after creation, and after removal, including numeric equality.
- `pnpm check` passed with local and hosted gRPC persistence tests. A
  generated Python client created catalog and source indexes against
  live MinIO, queried them after a server restart, and dropped them.
  [Index API](LOCAL_API.md#named-metadata-indexes).

## Structured operation diagnostics

- Local and hosted service events report queue and operation age, stage
  outcomes, provider request latency, search coverage, committed vector
  reuse, snapshot transfer and cache behavior, and bounded cleanup work.
  Cloud Run roles emit JSON to stderr. Local servers can select JSON and
  enable scoped diagnostic events with `RUST_LOG`.
- `pnpm check` passed. A generated Python gRPC client exercised the
  filesystem server and live MinIO with Ollama. It checked the emitted
  JSON fields, exact range bytes, cold downloads and warm reuse after
  restart, and one reused vector on each replacement. The fixture's
  source text, filename, and query were absent from the logs.
  [Event reference](LOCAL_API.md#server-diagnostics).

## Hosted writer renewal during publication

- The publication executor renews ownership while downloading a snapshot,
  preparing a SQLite backup, or uploading it. The same task serializes
  renewal with publication. A renewal that exceeds its deadline or loses
  its conditional write stops that attempt from publishing.
- Filesystem and live MinIO tests delayed each publication phase past the
  original lease. A competing owner remained fenced until release. A
  separate fault test withheld a successful renewal response and verified
  that the timed-out writer could only proceed by reacquiring ownership.
  The suspended-owner test still verifies takeover and stale rejection.
- `pnpm check` passed on rerun. Two local polling tests timed out on the
  first run and passed on the rerun. All 11 publication tests passed with
  the live MinIO cases enabled.

## Profile rebuilds on collection access

- After a worker activates a new embedding profile, hosted searches can
  queue a collection before scheduled discovery reaches its manifest.
  The API uses the existing rebuild record and repair hint. Discovery
  and concurrent reads can safely converge on the same operation.
- The gRPC test paused discovery before the collection, verified strict
  semantic unavailability and hybrid lexical fallback, and recovered
  the read-triggered work through reconciliation on a fresh worker.
- A real MinIO and Ollama run held discovery on page one, checked the
  persisted hint and rebuild record, restarted the server, and ran the
  worker's HTTP task handler to restore semantic retrieval. Live Cloud
  Tasks delivery was not exercised. `pnpm check` passed.

## Profile-bound hosted task delivery

- Cloud Tasks embedding and rebuild payloads carry the target profile
  ID. The task name also includes that ID, and the upload envelope
  records the profile of its last dispatch. The new profile can
  redispatch a pending batch within the same window.
- The dispatch fixture verified distinct names and profile fields.
  A hosted service test delivered stale and legacy payloads, checked
  that they left the pending batch intact, and redispatched it under
  the new profile. The matching worker then published the vector.
- A standalone worker HTTP smoke on MinIO and real Ollama exercised
  stale, legacy, and current profile deliveries across restart. It
  verified that only the matching payload advanced embedding progress
  and enabled semantic retrieval. Live Cloud Tasks delivery remains open.

## Repeatable hosted deployment verification

- The generated Python client check can submit and verify an operation
  in separate invocations. It reports request and revision IDs, exact
  retrieved bytes, and client timings. The runbook identifies which
  platform observations are required to establish scaling to zero and
  Cloud Scheduler delivery.
- A release API and worker ran locally against live MinIO. A 200 MiB
  upload survived a restart of both processes and an out-of-order
  delivery. The generated client and MCP returned the same exact
  3,652-byte range. Duplicate task deliveries preserved the published
  generation. The test removed its object prefix.
- A local TLS ingress fixture verified authorized gRPC/MCP requests and
  both unauthenticated responses. This did not exercise Cloud Run IAM.
  The local MCP check also passed with default logging and with logs
  disabled after switching it to the server's managed startup output.
  [Runbook](CLOUD_RUN.md#repeatable-ingestion-and-range-check).

## Capacity across 10,000 mostly idle tenants

- The release API created 10,000 tenant catalogs and collections against
  live MinIO. Eight active collections exercised search and replacement.
  Repeated search measured 12.66 ms p50 and 21.90 ms p95. Replacement
  reached lexical readiness in 873.08 ms p50 and 1,304.38 ms p95.
- The cluster usage API counted 40,100 objects and 1,995,800,329 bytes.
  A separate paginated listing matched both numbers. The run used a
  dedicated prefix and removed its objects after verification.
  [Method and results](CAPACITY.md#10000-mostly-idle-tenants).

## SQLite vector scan order and read-only memory mapping

- Three release runs used 262,144 vectors at both 768 and 1,536
  dimensions. The two tested join orders returned identical ranked
  references and scores across all 72 comparisons.
- Without fixture planner statistics, automatic planning reached
  498.92 ms and 837.13 ms median unfiltered at the two sizes. The
  earlier multi-second result was from an analyzed fixture.
- A read-only memory mapping experiment reduced those medians to
  273.22 ms and 456.82 ms. The experiment leaves production on
  automatic joins and ordinary SQLite reads pending hosted memory
  and concurrency measurements.
  [Method and results](CAPACITY.md#query-plan-and-read-only-memory-mapping).

## Concurrent hosted retrieval with real embeddings

- A release API published 1,026 chunks across two collections against
  live MinIO and real Ollama. After API restart, a generated Python
  client queried both collections concurrently and checked the bytes and
  collection provenance of every excerpt. The first searches took
  230.62 and 185.06 ms. Snapshot download and replay accounted for
  56.47 and 59.63 ms respectively.
- Warm semantic p95 rose from 138.14 ms with serial requests to
  187.43 ms with two concurrent collections. Warm concurrent hybrid p95
  was 197.08 ms. All four passes found the judged document in the first
  ten unique sources for 79 of 80 queries. These collections used about
  4.3 MB of SQLite storage each. Cloud Run performance with simultaneous
  GiB-scale databases remains open.
  [Method and results](CAPACITY.md#concurrent-hosted-retrieval-with-real-embeddings).

## Durable hosted source preparation

- Cloud Tasks dispatches preparation and lexical publication as separate
  steps. The preparation checkpoint stores the canonical source digest,
  size, chunk count, and RTF object reference. A retry of lexical
  publication can use this checkpoint on another instance. Registration,
  cancellation, and publication retain their existing fencing and receipts.
- `pnpm check` passed. The hosted regression test lost the preparation
  checkpoint response, started a fresh service, and reconciled the indexing
  task. Indexing then returned the exact canonical excerpt while reads of
  the original RTF were blocked.
- Local Cloud Run role processes and live MinIO processed an RTF source
  and a 200 MiB text upload with a restart between preparation and indexing.
  Out-of-order requests returned 503, and duplicate deliveries preserved
  the final generation. The generated client and MCP returned the same
  exact 3,652-byte text range. The 200 MiB preparation request took 1.86 s
  and the indexing request took 6.31 s. The latter includes one canonical
  download and one SQLite publication. Live Cloud Run and Cloud Tasks
  timing remains open.
  [Storage contract](STORAGE.md#hosted-lexical-publication).

## Two simultaneous GiB-scale snapshot reads

- The release snapshot benchmark ran against live MinIO with two 1 GiB
  databases and again with two 2 GiB databases. Cold resolutions took
  6.09–6.71 s and 12.85–20.04 s respectively. Warm reads resolved in
  1.12–1.96 ms with a cache budget large enough for both databases.
- Both collections returned their distinct SQLite markers through cold
  reads, warm reuse, and eviction. The 2 GiB fixture reached 6,450,941,952
  bytes of temporary files while three readers held their verified copies.
  The run removed its fixture objects. Cloud Run memory and latency remain
  open. [Method and results](CAPACITY.md#two-simultaneous-cold-snapshot-reads).

## Hosted SQLite read handle reuse

- Ordinary hosted collection reads can keep one idle SQLite connection
  with each verified snapshot. Concurrent reads can open their own
  connections. A failed read drops its connection, and successful reads
  clear query-specific vector and metadata functions before reuse.
- The focused tests verified idle handle reuse, independent overlapping
  reads, error disposal, and release of query-specific state. `pnpm check`
  and the release server and CLI builds passed.
- The release MCP and gRPC smoke against live MinIO recorded eight
  collection reads across three snapshot generations, including four
  handle reuses. A metadata write published a new generation and the next
  reader opened its own handle. The filesystem MCP HTTP and stdio smoke
  also passed. [Snapshot cache behavior](STORAGE.md#hosted-snapshot-reuse).

## Shared range reads for hosted search excerpts

- Hosted search reads touching excerpts from one immutable source in one
  bounded range, then verifies and returns each excerpt in ranked order.
  The read trace records individual requested bytes, transferred bytes,
  and the number of excerpts served by the transfer.
- `pnpm check` passed. A hosted gRPC regression test blocked a second range
  read, compared every hit with its standalone fetch, and confirmed a
  corrupted merged response fails fingerprint verification.
- On live MinIO, a generated Python gRPC client retrieved six overlapping
  UTF-8 chunks in one 3,240-byte range request. Six individual fetches
  returned identical excerpts and would total 5,795 bytes. The test
  removed its fixture prefix. [Range behavior](STORAGE.md#hosted-lexical-publication).

## Structured gRPC errors

- gRPC application errors include a versioned Protobuf detail with a
  reason, retryability, and the relevant operation ID or request field
  where available. Existing status codes and messages and the hosted
  binary operation-ID trailer remain compatible.
- Local and hosted Rust gRPC tests verified validation fields,
  failed-source receipts, and the legacy trailer. A generated Python
  client unpacked the same detail from the filesystem server and the
  MinIO-backed API. It recovered a real 30-second upload wait timeout
  using the returned operation ID, resumed the local queue, and replayed
  the same upload successfully. [Error contract](LOCAL_API.md#grpc-error-details),
  [Python example](../examples/python/README.md#read-structured-errors).

## Estimated token budget for chunking

- `chunking.max_tokens` adds a model-independent estimated token limit
  alongside the byte cap. Revision identifiers record the estimator and
  its settings. Local queued jobs and hosted envelopes persist the
  limit they accepted. Existing operations and omitted settings retain
  the byte-only chunker.
- `pnpm check` and locked release builds passed. Local and hosted tests
  resumed accepted jobs with a different runtime configuration and
  retained the original budget. A generated Python client checked 15
  exact multibyte excerpts against live MinIO after worker restart.
- A release filesystem server indexed a 200 MiB file with a 1,000-token
  estimate in 7.061 s at 29,114,368 bytes peak sampled process RSS.
  The final MCP excerpt matched the source bytes. Provider token limits
  remain model-specific. [Capacity results](CAPACITY.md#estimated-token-budget).

## Storage refresh progress

- `MeasureStorageUsage` streams cumulative scanned counts and a final
  timestamped measurement for the same scopes as `GetStorageUsage`.
  The desktop displays progress during a refresh and keeps the last
  completed total labeled with its measurement time.
- `pnpm check` and `pnpm desktop:check` passed. Filesystem and object
  integration tests compared streamed and unary totals. A generated
  client checked that live MinIO reported progress after its first
  1,000 objects and finished at 1,137 objects and 35,448 bytes.
- The sidecar launched by `pnpm devtop` on macOS streamed 46 progress
  events and a final count of 12,002 files. Chromium exercised the
  desktop progress, error, and collection-switch states with simulated
  Tauri IPC. [Storage API](LOCAL_API.md#storage-usage).

## Source readiness and pending cleanup

- `GetSource` returns active-profile embedding coverage and errors,
  readiness, and cleanup counts from the same snapshot as the current
  revision. Hosted snapshots track the source ID of newly queued objects.
  The per-source hosted count is omitted when legacy entries lack that ID.
- `pnpm check` passed. The local gRPC regression covered failed
  embeddings retaining the previous revision and cleanup after a profile
  rebuild. The hosted regression covered an unfinished replacement,
  readiness, reader grace, object deletion, and legacy cleanup entries.
- A generated Python client on live MinIO and Ollama checked lexical
  readiness before enrichment and then verified the changed source became
  ready after its new vector was committed. The previous revision remained
  held in both replacement states. The run removed all 16 fixture objects.
  [Source inspection](LOCAL_API.md#source-inspection).

## Large RTF ingestion measurements

- The ingestion benchmark exercised 200 MiB RTF uploads through the
  release gRPC CLI, lexical publication, and exact MCP retrieval on the
  filesystem and live MinIO. It compared both excerpts with the expected
  canonical UTF-8 bytes and checked that hidden RTF text was absent from
  lexical results.
- Text readiness took 6.865 s on the filesystem and 13.715 s on MinIO.
  Peak sampled server RSS was 29,294,592 and 46,006,272 bytes,
  respectively. These measurements cover the server process on macOS.
  [Reproduction and results](CAPACITY.md#200-mib-rtf-extraction).
- `pnpm check` and the locked release build passed. The original
  benchmark also passed a 1 MiB UTF-8 smoke run.

## First collection catalog reads

- The large RTF benchmark exposed a first-use race between catalog initialization
  and background cleanup. Collection listing and existence reads use a read-only
  SQLite transaction so the schema version and rows come from the same snapshot.
  A new catalog with no committed schema is empty until initialization commits.
- A two-connection SQLite regression reproduced the missing-table error before
  the fix and verified empty lists and a missing collection before commit, then
  a visible collection after commit. A committed schema with a damaged table
  still reports the database error. `pnpm check` passed.

## Authenticated Git over HTTPS

- Exact-origin import configuration accepts an HTTP Basic username and an
  environment variable for the password or token alongside the existing
  bearer configuration. Git passes the Authorization header through the
  child environment. A private CA bundle can be supplied through
  `GIT_SSL_CAINFO`.
- The standalone check runs Git's smart HTTP backend over TLS. It exercises
  authenticated upload-pack discovery and fetch, exact MCP retrieval,
  unchanged re-import, replacement after a new commit, rejection without a
  credential, and rejection of redirects, unlisted origins, and an
  untrusted certificate. The check passed on macOS ARM64 and Debian
  ARM64. `pnpm check` passed.
  [Configuration and reproduction](LOCAL_API.md#git-imports).

## Repair after task creation fails

- The worker logs dispatch scan results and successful upload task handoffs.
  The deployment check includes the operation's dispatch shard so operators
  can correlate Scheduler, worker, and queue evidence.
- The filesystem and live MinIO regression used the production gRPC
  service, worker routes, and Cloud Tasks request formatting against a
  local HTTP task endpoint. An upload remained accepted after task creation
  returned HTTP 503. An early index delivery returned HTTP 503, and a repair
  pass recorded the failed handoff. After restart, the next repair enqueued
  registration, and delivered stages published the exact UTF-8 text. Repeat
  scans and task deliveries preserved the source revision and generation.
- The live runbook stages an actual task creation failure on a dedicated
  deployment and checks that an authenticated Scheduler invocation creates
  the missing task. It still needs an authorized Google Cloud project.
  [Dispatch repair exercise](CLOUD_RUN.md#repair-an-upload-that-never-reached-the-queue).

## Search against deployed SQLite snapshots

- The generated Python benchmark seeds synthetic text through gRPC until
  the published databases reach the requested size. It records a manifest
  of actual snapshot lengths and generations. A separate invocation runs
  simultaneous lexical searches and verifies the exact UTF-8 excerpts and
  subsequent fetches against the fixture.
- SQLite read-handle logs include time to open a new connection or select
  the cached connection. The runbook explains how to correlate these
  events with object transfer times, generation and instance identity,
  and Cloud Run memory and tmpfs metrics.
  [Deployed capacity procedure](CLOUD_RUN.md#measure-gib-scale-snapshot-reads).
- The release API on live MinIO verified separate seed and measure runs
  across two collections, exact range retrieval, and idempotent replay
  of a seed with multiple sources. `pnpm check` passed.
- The target-size run published two databases of about 1.05 GiB and
  searched both concurrently. The Linux ARM64 Docker API with tmpfs
  returned cold results in 5.96–6.23 s with a 2.41 GiB cgroup peak.
  [Measured results](CAPACITY.md#concurrent-grpc-search-on-1-gib-fts-snapshots).

## GCS resumable transfer recovery

- The native adapter retains a bounded upload part and probes the same
  session after retryable failures. It resumes at the persisted byte
  offset, verifies final object metadata, and stops after bounded retries
  without progress.
- The loopback JSON API fixture interrupted a part after an unaligned
  offset, failed the first probe, and dropped the response after the final
  commit. The next probe recovered the committed generation. The fixture
  compared every resent byte and verified cancellation for invalid offsets
  and exhaustion. The live test still requires an authorized GCS bucket
  and Application Default Credentials.
  [GCS storage contract](STORAGE.md#google-cloud-storage-adapter).

## Consistent reads during GCS manifest replacement

- Full reads restart with fresh metadata when a concurrent write changes
  the generation targeted by the media request. Four unsuccessful attempts
  return a retryable busy error. Deletion observed on the metadata read
  returns not found.
- The loopback JSON API tests reproduced a failed read after concurrent
  replacement. They now verify that the returned bytes, length, and
  generation come from the same version, and cover deletion and repeated
  churn. The same publication and page-delta contracts used with MinIO can
  now be run against an authorized GCS bucket. `pnpm check` and the live
  MinIO publication, renewal, and page-delta tests passed.
  [GCS contract commands](STORAGE.md#google-cloud-storage-adapter).

## Hosted worker task deadlines

- Worker task routes return HTTP 503 after 570 seconds, leaving time to
  respond before Cloud Tasks and the example Cloud Run worker reach their
  600-second deadlines. Maintenance routes retain their own scheduler
  deadlines. [Worker deployment](CLOUD_RUN.md#deploy-the-services).
- The filesystem and live MinIO regression stalled a registration request
  after SQLite publication and before the operation checkpoint. The worker
  canceled the stalled write and returned 503. A fresh worker reused the
  registration receipt without publishing another database generation.
  It then delivered preparation and indexing and verified exact UTF-8
  retrieval through gRPC. This run used a loopback Cloud Tasks fixture.

## Concurrent reads on two 2 GiB FTS snapshots

- The release server published 78 sources of 200 MiB to MinIO and
  produced two snapshots of roughly 2.05 GiB each. Fresh Linux ARM64
  APIs on tmpfs served simultaneous cold gRPC searches in 5.96–6.40 s
  with a 4.55 GiB cgroup peak. A generated Python client checked exact
  UTF-8 ranges, source provenance, and snapshot generation.
- A 5 GiB cache reused both SQLite connections on every repeat. With the
  default 2 GiB budget, both snapshots were downloaded for every search
  and peak memory was 4.52 GiB. The Cloud Run benchmark still needs the
  authorized deployment.
  [Timings and memory](CAPACITY.md#concurrent-grpc-search-on-2-gib-fts-snapshots).

## GCS downloads interrupted after streaming starts

- A full read resumes at the next byte within the original object generation.
  The adapter checks the complete expected interval and length for each
  resumed request and bounds attempts that make no progress.
- The loopback JSON API interrupted a response after the reader received
  its first bytes, rejected the first retry, and truncated the next
  response. The final stream matched the fixture byte for byte. Separate
  cases checked replacement, a different total length, and stalled
  transfers. The live GCS bucket contract still needs authorized ADC.
  [GCS adapter](STORAGE.md#google-cloud-storage-adapter).

## Dispatch repair after collection deletion

- The worker uses the collection manifest's durable tombstone to remove
  upload and profile rebuild hints. Late worker deliveries to a retired
  collection complete without publishing. An active collection can
  keep its hint through the acceptance gap before an envelope exists.
- The filesystem and live MinIO tests retired a collection while uploads
  were queued. They checked cleanup before and after reclaiming the
  envelopes, HTTP acknowledgment of late tasks after a worker restart,
  and successful repair of the same upload ID in an active collection.
  Queue creation used a loopback Cloud Tasks fixture.
  [Cloud Tasks dispatch](STORAGE.md#cloud-tasks-dispatch).

## Dispatch repair under a stalled handoff

- Hosted repair processes up to four independent hints concurrently.
  It gives each attempt 30 seconds and the whole page 240 seconds.
  Unfinished attempts remain discoverable after a timeout.
- The filesystem regression accepted 17 uploads under one shard. While
  the first dispatch stalled, 15 other handoffs completed. A new worker
  resumed after a page timeout, recorded the per-hint failure, moved to
  the next page, and recovered the timed-out hint on the next pass.
  [Scheduler setup](CLOUD_RUN.md#schedule-dispatch-repair).

## Batching hosted embedding publications

- Up to 16 distinct chunk inputs share one fenced SQLite publication.
  Requests still follow the provider's batch limit. The worker can publish
  completed requests if preparation reaches its 120-second budget, and
  leaves unprocessed inputs for the next step.
- The hosted regression used three single-input provider calls and
  verified one new snapshot. After a failed operation checkpoint, a
  fresh worker reused the committed receipt with no provider calls
  or additional snapshot. A separate regression stalled the second
  provider request and verified that preparation returned the first result.
- A generated Python client against live MinIO and Ollama verified
  coverage for 18 inputs, grouped into 16 and two in separate worker
  requests. Those requests published one SQLite snapshot each, of
  258,048 and 270,336 bytes. An empty follow-up published nothing.
  Semantic search and gRPC fetch returned the exact UTF-8 excerpt.
  Live Vertex verification remains open.
  [Embedding batches](STORAGE.md#hosted-embedding-batches).

## Concurrent embedding outcomes

- A worker records a failure only when the input's cache entry still
  matches the retry state it read before making the provider call.
  Successful predictions keep the first committed vector for each
  current input and profile.
- A filesystem-backed gRPC regression ordered competing workers'
  responses for the initial attempt and a scheduled retry. The late
  failure left the original error, retry time, and attempt count intact.
  The late success left semantic retrieval using the first vector.
- Two standalone workers on live MinIO reproduced the ordering over
  HTTP with a controlled OpenAI-compatible endpoint. A generated Python
  client verified unchanged retry status, a cosine score of 1.0 from the
  first vector, and exact UTF-8 text from the cited chunk.
  [Embedding batches](STORAGE.md#hosted-embedding-batches).

## Durable prepared embedding batches

- Upload and profile rebuild tasks conditionally stage their results in object
  storage under a stable step and profile ID before acquiring the SQLite
  writer lease. Retries use the prepared results until a committed database
  receipt is available. Publication removes the staging object. The scheduled
  upload sweep deletes abandoned batches after 45 days.
- A filesystem regression lost the response to the staging write, held the
  writer lease through redelivery, then restarted the worker and interrupted
  its operation checkpoint after publication. The next delivery used the
  SQLite receipt. The three provider calls produced one committed snapshot.
  The sweep regression preserved a 31-day-old batch and removed a 46-day-old
  batch after resuming its scan.
- Two deliveries against live MinIO and Ollama returned HTTP 503 while a
  competing lease held the collection. Ollama embedded three inputs in one
  request and the worker staged 12,863 bytes. After a restart, the same step
  published one 167,936-byte SQLite snapshot, removed staging, and made no
  document embedding requests. A third process replayed the task without a
  new request or snapshot. A generated Python gRPC client verified coverage
  and fetched the exact cited UTF-8 bytes from semantic search.
  [Embedding batches](STORAGE.md#hosted-embedding-batches).

## Hosted uploads past the processing window

- Dispatch repair, the standalone worker, and the scheduled upload sweep
  settle expired pending stages through the fenced ingestion path. Failed
  uploads release their external ID and unreferenced input. Indexing
  recovers published content from the exact revision if the receipt has
  aged out. Settled records follow terminal retention.
- The filesystem regression interrupted the indexing checkpoint, removed
  its receipt, and verified that a fresh worker recovered committed text.
  It also exercised registration and indexing expiry, an embedding step
  with future backoff, dispatch hint removal, external ID reuse, and
  replay after the terminal envelope was compacted.
- A generated Python gRPC client with live MinIO verified the standalone
  restart path. A registered unpublished upload became failed and left
  zero input objects. A committed upload with a stale envelope recovered
  as text-ready, kept its one canonical object, and returned byte-exact
  UTF-8 from lexical search. Both old dispatch hints were removed. The
  live Cloud Tasks and Scheduler checks remain open.
  [Hosted operation retention](STORAGE.md#hosted-operation-retention).

## Dispatch hints for individual upload attempts

- A durable upload hint names its staging attempt. Repair keeps recent
  hints while admission is in flight. Once an unaccepted hint passes
  the 15-minute grace period, repair uses the conditional acceptance
  fence before removing that attempt's hint. It can remove a losing
  hint immediately after another attempt commits.
- The filesystem regression paused admission before the envelope write.
  A fresh repair service retained the recent hint, fenced and removed it
  after aging, and rejected the paused admission. A retry with the same
  operation ID got its own hint. After an injected dispatch failure,
  repair advanced the retry to indexing and retrieval returned the original text.
- The live MinIO dispatch test exercised the worker against a loopback
  Cloud Tasks fixture. It removed a losing attempt hint, preserved the
  winning attempt through a restart, and still handled operation-only
  hints from older releases. Deploy updated repair workers before APIs
  begin writing the new hint format.
  [Dispatch repair](STORAGE.md#cloud-tasks-dispatch).

## Maintenance recovery for hosted collection creation

- Hosted creation first records a tenant-scoped intent. Scheduled
  snapshot maintenance can initialize a missing collection database,
  activate an already-published initial database, or clean up an
  intent left after catalog activation.
- The filesystem-backed gRPC test interrupted creation before initial
  publication, before catalog activation, and after activation. A fresh
  maintenance service recovered all three with no client retries.
  The original request IDs replayed their outcomes without publishing
  another catalog generation.
- With live MinIO, a generated Python client received `UNAVAILABLE`
  while another writer held the catalog lease. A fresh process in the
  Cloud Run worker role ran the HTTP snapshot sweep and reported one
  recovered creation. The next API process listed the collection and
  replayed its original request ID. The initial database generation and
  recovered catalog generation were reused. Actual Cloud Run ingress
  and Scheduler delivery remain open.
  [Snapshot maintenance](STORAGE.md#scheduled-snapshot-reclamation).

## S3 validation after an interrupted download

- The S3 adapter validates every resumed response against the requested
  range and the original length, ETag, and version ID when present. It
  also checks identity encoding and the final byte count.
- The loopback HTTP fixture interrupted full and bounded downloads after
  delivering initial bytes. It verified exact continuation and rejection
  of a full response, wrong range, changed total length, changed version
  with the same ETag, and unsupported encoding.
- The shared storage contract passed against live MinIO, directly and
  through a proxy that closed a 17 MiB multipart object's download after
  1,048,576 bytes. The next request started at that offset and the contract
  verified all 17,825,799 bytes. [S3 storage behavior](STORAGE.md#storage-backends).

## Hosted preparation progress

- gRPC job status and activity, MCP ingestion status, and the CLI expose
  the durable preparation outcome while lexical publication is pending.
  Successful preparation reports canonical bytes and chunk count.
  Invalid input exposes the saved error before the worker commits the
  terminal result. Desktop activity shows the same preparation state.
- The filesystem-backed gRPC test recovered the preparation checkpoint
  after a lost write response. Status and activity returned matching RTF
  counts before any lexical hit was visible. After index publication,
  the exact UTF-8 text became searchable. The invalid-input case verified
  that its preparation error flowed into the terminal job failure.
  [Hosted lexical publication](STORAGE.md#hosted-lexical-publication).
- With live MinIO, the hosted MCP smoke test verified the counts before
  publication and the gRPC CLI displayed `Awaiting indexing`. A generated
  Python client saw the persisted 32 input bytes, 17 canonical bytes, and
  one chunk after restarting the API. After indexing, the job exposed
  the published text revision and cleared the preparation field.
- Delivered in [PR #121](https://github.com/ArcaneArts/Sanctum/pull/121).

## Desktop ingestion activity pages

- Desktop activity follows the server's local or hosted continuation
  cursor. A hosted page with no jobs can still advance. The current page
  refreshes in place, and changing the collection or restarting the
  managed server opens the first page.
- A Chromium check against the production React build with a controlled
  Tauri IPC fixture exercised local and hosted navigation, empty hosted
  pages, an expired cursor, a scoped cancel action, collection changes,
  and managed server restart.
  [Desktop activity](DESKTOP.md#first-use).
