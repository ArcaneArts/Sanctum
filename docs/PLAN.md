# Sanctum implementation plan

**Status:** planned; no implementation is implied by this document.

**Planning baseline:** 2026-11-12.

**Repository:** [ArcaneArts/Sanctum](https://github.com/ArcaneArts/Sanctum).

**First usable milestone:** a desktop tray application running a local MCP
server that can be connected to an LLM client for personal use.

This plan incorporates the project discussion and the owner's subsequent
clarifications. Those clarifications replace the earlier concepts of permanent
version history, typed knowledge, internal summarization, healing, and persistent
context. “Collection” is a temporary name for a grouping of information; choosing
the product term does not block implementation.

## Contents

- [Product contract](#1-product-contract) and [scale](#2-scale-and-operating-assumptions)
- [Architecture](#3-architecture-and-module-boundaries), [storage](#4-storage-layout-and-byte-range-contract), [source retention](#5-original-retention-and-external-provenance), and [databases](#6-database-choice-and-record-model)
- [Ingestion](#7-ingestion-and-streaming-chunking), [replacement](#8-source-replacement-and-metadata-invalidation), and [metadata](#9-metadata-and-integration-queries)
- [Embeddings](#10-embedding-configuration-and-lifecycle), [retrieval](#11-retrieval-and-provenance), and [APIs](#12-protobufgrpc-and-mcp-contracts)
- [Local work](#13-local-durable-work) and [cloud persistence](#14-cloud-work-ownership-and-publication)
- [Desktop/configuration](#15-configuration-and-desktop-lifecycle), [imports](#16-import-adapters), and [storage totals](#17-storage-totals-and-observability)
- [Milestones](#18-delivery-milestones), [validation](#19-validation-strategy), and [remaining implementation choices](#20-decisions-to-finalize-during-implementation)

## 1. Product contract

Sanctum accepts text-bearing sources, stores a retrievable text representation,
chunks and indexes it, computes embeddings, and returns matching content with
provenance. Other applications and their agents decide what that content means.

The server and all business logic are Rust. Native dependencies such as SQLite
are acceptable: the requirement concerns where Sanctum's logic is implemented,
not rewriting every dependency in Rust. The desktop frontend uses React and
shadcn/ui inside Tauri; Rust owns configuration, process management, ingestion,
storage, queries, and scheduling.

The service is always multi-tenant. A tenant is an identifier supplied by the
controlling application, not a signup, login, or billing account. A personal
installation normally uses `default`. Every collection belongs to exactly one
tenant. Multi-collection searches remain within one tenant.

The hosted server trusts its operator and calling applications. The parent
application restricts and injects tenant scope before forwarding requests. The
local MCP endpoint can be accessed directly. There is no single-user/multi-user
mode switch, end-user account system, or collection permission model in v1.

### Included in the initial release roadmap

- Text, Markdown, RTF, and other text files, including code and configuration.
- Generic overlapping text chunks, with streaming ingestion of large sources.
- Local folders, S3-compatible object storage tested with MinIO, and native
  Google Cloud Storage for hosted operation.
- SQLite metadata and indexes, plus a vector implementation behind a Rust trait.
- Uploads, local imports, watched folders, URLs, and Git sources, delivered in
  stages rather than all being prerequisites for the first desktop milestone.
- Full-text, vector, and combined retrieval with explicit processing coverage.
- Arbitrary integration metadata and caller-owned external source IDs.
- MCP and a documented, versioned Protobuf/gRPC API from the first usable slice.
- Durable local background work; Cloud Tasks and Cloud Run for hosted work.
- Incremental embedding reuse, source replacement, and eventual eviction of old
  content after its replacement is ready.
- Progress, errors, retries, storage totals, and a configuration-focused tray UI.

### Outside the initial release

Answer generation; automatic summaries; fact/thought/wish/observation/opinion
types; confidence; dependency reasoning; contradiction detection; healing;
persistent agent context; permanent revision audits; image/audio/video processing;
syntax-aware code chunking; public user accounts; remote-server management in
the desktop UI; arbitrary executable plugins; automatic sharding or cold tiers.

Applications may store summaries, extracted facts, or other derived data as
metadata. Special rules for showing that metadata instead of chunk text are
deferred. Cloud-drive synchronization of a live data directory is also deferred;
choosing an iCloud or Google Drive folder does not establish multi-device database
coordination or support for files that have been offloaded.

## 2. Scale and operating assumptions

| Dimension | Planning target |
| --- | --- |
| Hosted users | Approximately 1,000 initially, with a path toward 10,000 |
| Collections per user | Usually at most 10; this is not a product limit |
| Collection source content | Several GB |
| Collection database | Potentially 1–2 GB |
| Individual input | At least 200 MB of text |
| Cold collection opening | Aim for roughly 10 seconds; measure rather than promise |
| Warm access | Reuse downloaded databases and open handles when current |
| Desktop platforms | macOS, Windows, Linux |
| Hosted server | Linux containers on Google Cloud Run |

Bounded streaming must cover upload, extraction, hashing, chunking, and import.
Do not materialize a 200 MB source as a single string or Protobuf message.
Embedding batches are independently bounded by the provider's actual limits.

A 1–2 GB database may not download and open within ten seconds. Measure cold
transfer, SQLite opening, vector loading, warm queries, and snapshot publication
separately. Do not extrapolate those results from source-data size or assume
vectors occupy less than 5% of storage.

Cloud Run's writable container filesystem uses instance memory and is ephemeral.
Database copies, SQLite working space, and simultaneous open collections therefore
affect memory sizing. Warm reuse is opportunistic, not guaranteed routing back to
the same instance. [Cloud Run container contract](https://docs.cloud.google.com/run/docs/container-contract#file_system_access).

Temporary download cleanup is not a standalone v1 feature: instances will
eventually disappear. Bound active working sets and expose actual resource errors;
do not add speculative disk-space checks that prevent otherwise valid operations.
Authoritative source/snapshot cleanup is a separate, required data-lifecycle task.

## 3. Architecture and module boundaries

```text
LLM / parent application                  Desktop React UI
       |                                        |
       | MCP or Protobuf/gRPC                   | Tauri commands/events
       v                                        v
Transport adapters                       Desktop Rust host
       |                                        |
       +------------ Rust application services --+
                              |
            +-----------------+------------------+
            |                 |                  |
     Ingest / extract      Search / metadata   Jobs / progress
            |                 |                  |
            +-------- Collection executor -------+
                              |
                  SQLite + vector index adapter
                              |
                Filesystem / S3 / GCS storage

Embedding adapters: Ollama | Google Cloud | OpenAI-compatible
Job dispatch adapters: durable local runner | Google Cloud Tasks
```

Transports call the same application services. MCP must not implement separate
ingestion or search semantics. Desktop commands delegate to Rust services or a
Rust API client; React renders state and collects input.

Parallel work is allowed for uploads, extraction, chunking, provider calls, and
result preparation. Database mutations are serialized per database owner. A
network call to an embedding provider must not hold a database write transaction
or ownership lease while waiting for inference.

Suggested workspace boundaries, introduced as their responsibilities appear:

| Crate / package | Responsibility |
| --- | --- |
| `sanctum-core` | IDs, records, errors, locators, shared traits |
| `sanctum-config` | Versioned configuration schema and decoding |
| `sanctum-storage` | Streaming objects, range reads, listing, conditional mutation |
| `sanctum-storage-fs` | Filesystem implementation |
| `sanctum-storage-s3` | S3 implementation and MinIO coverage |
| `sanctum-storage-gcs` | Native GCS implementation and generation preconditions |
| `sanctum-db` | Database ownership, transactions, snapshots, migrations |
| `sanctum-db-sqlite` | SQLite queries and full-text indexing |
| `sanctum-vector` | Vector storage/search interface and initial implementation |
| `sanctum-extract` | Text decoding and RTF extraction |
| `sanctum-chunk` | Streaming, overlapping text chunking |
| `sanctum-ingest` | Source lifecycle and ingestion orchestration |
| `sanctum-import` | Directory, watcher, URL, and Git adapters |
| `sanctum-embedding` | Provider contract, profiles, batching, reuse keys |
| `sanctum-embedding-ollama` | Ollama provider |
| `sanctum-embedding-google` | Google Cloud provider |
| `sanctum-embedding-openai` | OpenAI-compatible provider with base URL |
| `sanctum-search` | Filtering, retrieval, ranking, excerpts, provenance |
| `sanctum-jobs` | Work records, progress, retry/cancel semantics |
| `sanctum-jobs-local` | Durable local queue and runner |
| `sanctum-jobs-gcp` | Cloud Tasks dispatch and task handlers |
| `sanctum-service` | Application use cases shared by all transports |
| `sanctum-proto` | Protobuf definitions and generated Rust bindings |
| `sanctum-api` | gRPC service adapters and cloud HTTP task endpoints |
| `sanctum-mcp` | MCP tools and transport adapters |
| `sanctum-client` | Rust client used by CLI, desktop, and integrations |
| `sanctum-server` | Standalone executable and dependency wiring |
| `sanctum-cli` | Imports, diagnostics, and stdio MCP bridge |
| `apps/desktop` | Tauri Rust host and React/shadcn frontend |

Use a Cargo workspace for Rust and a PNPM workspace for frontend/development
orchestration. There is no Node server in the production architecture. Prefer
traits and explicit composition over a runtime plugin loader. Do not create
empty crates solely to reach a target crate count.

## 4. Storage layout and byte-range contract

The filesystem layout starts with the owner's requested organization:

```text
<storage-root>/
  <tenant-id>/
    root.db
    <collection-id>/
      meta.db
      sources/
        <source-id>/
          <revision-id>/
            text.utf8
            original           # Optional retention
      staging/                 # Incomplete uploads and computed results
```

Paths are assembled from validated/encoded IDs. Display names and external IDs
never become unchecked filesystem paths. Operations always carry tenant and
collection scope. Prefixes are an organizational boundary, not end-user auth.

The object-store layout preserves those scopes but publishes immutable database
snapshots through manifests, for example:

```text
<prefix>/<tenant-id>/
  catalog/HEAD.json
  catalog/snapshots/<generation>/root.db
  <collection-id>/
    HEAD.json
    snapshots/<generation>/meta.db
    sources/<source-id>/<revision-id>/text.utf8
    sources/<source-id>/<revision-id>/original
    operations/<operation-id>/...
```

`root.db` and `meta.db` remain the logical database names. If additional database
files are introduced, one collection manifest identifies their complete set.
Source objects have immutable revision keys until deliberate cleanup.

The storage trait includes streaming put/get, `read_range`, paginated recursive
listing with sizes, deletion, and conditional object replacement. Use native GCS
generation preconditions for GCS; do not assume an S3-compatible endpoint provides
identical coordination behavior. Test filesystem and MinIO locally and GCS in
the actual hosted integration environment.

Chunk ranges are `[start_byte, end_byte)`, use 64-bit offsets, and reference a
specific text representation and revision. Convert to inclusive HTTP ranges at
the adapter boundary. Chunk boundaries must land on UTF-8 character boundaries.
Line numbers may be stored for display but never replace byte locators.

S3 and GCS both expose byte-range downloads. GCS can ignore ranges under some
transcoding conditions, so store canonical text without whole-object content
encoding and inspect actual range responses. Validate returned length/range and
the recorded chunk fingerprint; do not assume a full-object checksum validates
a partial read. [S3 GetObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html),
[GCS object downloads](https://docs.cloud.google.com/storage/docs/xml-api/get-object-download).

Read adjacent hit ranges together when that reduces request overhead; cap merged
ranges so one retrieval does not silently download a huge source. Record requested
and transferred bytes in diagnostics. Future compression must preserve seekable
ranges and explicit logical-to-physical mapping; whole-file compression is not
part of v1 source storage.

## 5. Original retention and external provenance

Every searchable source has a Sanctum-managed canonical UTF-8 representation.
The original upload is optional after extraction. Two ingestion policies cover
the initial requirement:

| Policy | Retained by Sanctum |
| --- | --- |
| `retain_original` | Original plus canonical text when a separate representation is necessary |
| `text_only` | Canonical text and provenance; original upload discarded after successful extraction |

For already suitable plain text, the original and canonical representation can
share one stored object. Preserve bytes and line endings when no conversion is
needed. Converted RTF or non-UTF-8 input uses offsets into extracted UTF-8 text,
not fictitious offsets into the original document. Record extractor version,
encoding, content hash, and representation relationship.

An application can supply an external namespace and source ID. Results return
that opaque reference; Sanctum does not interpret it, fetch an application-owned
file by that ID, or require it to be a public URL. The application decides how to
open the original. An immutable source locator and range also identify the text
Sanctum actually used.

Pure reference-only indexing with no retained text is outside v1: reliable
full-text fallback and range retrieval require a managed text representation.
Temporary upload bytes must remain durable until extraction can be retried, even
when the final retention policy is `text_only`.

## 6. Database choice and record model

Use SQLite for the initial implementation. Its embedded file model matches local
operation and downloadable snapshots. PGlite is not an initial second backend;
its documented package embeds WASM PostgreSQL through a JavaScript/TypeScript
client. [SQLite overview](https://www.sqlite.org/about.html),
[PGlite overview](https://pglite.dev/docs/about).

Start with `root.db` per tenant and one `meta.db` per collection. This gives
atomic updates between chunk identity, metadata, lexical indexing, job markers,
and vector references. Do not split databases by metadata category without
measuring a benefit.

Keep vector operations behind `VectorIndex`; evaluate `sqlite-vec` as the first
implementation candidate. Its Rust bindings and SQL integration are useful, but
the pinned release must be exercised with real filters, dimensions, and target
sizes before the implementation is finalized. No unmeasured ANN or latency claim
is part of this plan. [sqlite-vec documentation](https://alexgarcia.xyz/sqlite-vec/).

If profiling supports a separate `vectors.db`, use the same collection writer,
publish the files as one generation, and join candidates to chunk metadata in
the Rust query layer. A downloaded manifest must never pair metadata from one
generation with vectors from another. Cross-file transactions cannot simply be
assumed equivalent to one SQLite transaction. There is no `summaries.db` in v1.

Suggested logical records:

| Scope | Records and important fields |
| --- | --- |
| Tenant catalog | Collection ID/name, lifecycle state, custom metadata, timestamps, schema version |
| Source | Stable source ID, external namespace/ID, import origin, current searchable revision |
| Revision | Content digest, representation keys/sizes, retention policy, extractor/chunker versions, readiness |
| Chunk | Revision ID, ordinal, byte range, content/input digest, optional line range |
| Embedding | Chunk/input key, server profile ID, dimensions, state, vector or index reference |
| Custom metadata | Entity scope/ID, namespace, typed JSON value, current revision where applicable |
| Job / operation | Idempotency key, stage, attempt, progress, error, desired revision/profile |
| Change event | Cursor, source/revision, event type, metadata-reset information |
| Applied command | Durable operation/batch ID used to make retries harmless |

Do not put complete source bodies or duplicate chunk bodies in SQL tables.
Derived facts and summaries supplied as metadata are allowed, but are not copied
onto every chunk automatically. Metadata and index sizes are observable. Set
documented configurable request/value limits and return actionable errors rather
than silently truncating arbitrary metadata.

Only transient source revisions and processing generations are versioned as
needed for safe replacement. Ordinary metadata updates happen in place. Job
receipts and change events have bounded operational retention, not permanent
audit-history semantics. Schema migrations remain explicit and restart-safe.

Keep idempotency receipts and deletion markers for at least the supported task
retry lifetime. After their expiry, missing/expired operation envelopes must be
rejected, not treated as instructions to create new state. Never reuse operation
or revision IDs. This prevents delayed deliveries from resurrecting deleted data
without retaining an indefinite audit log.

## 7. Ingestion and streaming chunking

All import routes feed one pipeline:

```text
Receive + durably stage → extract/decode → stream chunks → publish lexical index
                                                        → embed missing inputs
                                                        → publish readiness
                                                        → reclaim replaced data
```

An accepted response means the source input and an identifiable operation are
durable enough to resume. It does not mean the content is already searchable.
Return an operation ID, source ID, revision ID, and processing state. Queryable
status distinguishes accepted, extracting, indexing, text-ready, embedding,
ready, failed/retryable, canceled, and cleanup-pending states.

Use streaming UTF-8 decoding with carry buffers for split code points and a
stateful RTF extractor. Reject unsupported or malformed input with a source-level
error rather than replacing bytes without disclosure. Unsupported binary files
are not fed into the text chunker merely because they have a familiar extension.

Use bounded overlapping windows with sensible paragraph/newline boundaries.
Initial tuning defaults can target roughly 1,000 tokens with roughly 100 tokens
of overlap, subject to measurement and the provider's supported input size.
Those are configurable starting points, not promised universal settings. Keep
a byte cap as well as token-aware sizing so very long lines remain bounded.

Code and configuration use the same generic text strategy. Do not require
Tree-sitter, classes, methods, pages, or format-specific ASTs for v1. Record the
chunker configuration so later strategies can intentionally rebuild chunks.

An embedding-only subdivision may be necessary when a provider accepts less
input than a canonical chunk. Its locators must still point to exact source
ranges, and every changed input must receive a distinct cache key. Never quietly
truncate content to satisfy a provider.

Small inline text deposits can be implemented as uploads of a virtual text file.
They are not a separate knowledge model and are not required to expose a novel
concept to desktop users.

## 8. Source replacement and metadata invalidation

Stable source identity is separate from content identity. Callers can update by
source ID or an external namespace/ID pair unique within the collection. Directory,
Git, and URL adapters maintain their own mapping to those IDs. A source revision
identifies immutable content/representation; retrying the same operation must not
create another logical update.

Replacement procedure:

1. Persist the candidate input while keeping the old searchable revision usable.
2. Extract and chunk the candidate; compute exact embedding-input fingerprints.
3. Match old/new chunk inputs by digest and processing profile, including repeated
   text occurrences. Reuse vector values without reusing obsolete byte locators.
4. Publish the complete candidate lexical index and current-source pointer
   atomically. Searches now return that revision, with embedding coverage shown.
5. Compute missing embeddings in the background and ignore late results whose
   revision/profile is no longer desired.
6. Once all required built-in stages succeed, mark the replacement ready and
   schedule deletion of obsolete chunks, vectors, representations, and snapshots.

“Fully ready” means canonical text, chunks, lexical indexing, and all required
embeddings for the active profile are committed. It does not wait for arbitrary
external metadata or application-generated summaries. Provider failure leaves
lexical retrieval usable and the incomplete replacement visible in progress;
old retained data is not silently declared eligible for eviction.

Embedding reuse requires identical input bytes and compatible model/preprocessing
identity. A moved chunk may reuse a vector; modified context supplied to the
provider may not. Do not use offsets or ordinal alone as content identity.
Changes near window boundaries may alter neighboring chunks, so generic overlap
does not guarantee optimal reuse for every edit. Measure reuse on representative
edits before introducing more elaborate chunking.

Collection metadata persists independently. Integration metadata attached to
chunks is cleared on a changed source revision, even if its embeddings are
reused. Source-level integration metadata also resets by default on replacement;
caller identity/provenance remains, and metadata supplied for the new revision
is installed with it. Emit `source_replaced` / `metadata_reset` events and expose
missing-key filters so integrations can reattach data. A metadata patch targeting
an obsolete revision returns a conflict instead of modifying a new chunk.

Concurrent updates use an optional expected revision and an update sequence
assigned when the collection writer registers the revision intent. This order
is durable; task execution order and caller clocks do not establish it. The
newest registered replacement wins, and status exposes that sequence. Slower jobs
cannot resurrect an older candidate. Deletion uses a tombstone/operation state
while cleanup runs so late tasks cannot recreate the source. Canceled or
superseded candidate jobs are safe to retry as no-ops. Clients that require
optimistic concurrency supply the expected revision instead of relying on timing.

Eviction is eventual and respects bounded in-flight readers. Old search handles
expire; a retrieval for reclaimed content returns an explicit stale-revision
response. This is temporary overlap for consistency, not permanent source history.

## 9. Metadata and integration queries

Support collection metadata in `root.db` and source/chunk metadata in `meta.db`.
Use namespaced JSON values with stable types: null, booleans, strings, numbers,
arrays, and objects. Keep built-in operational fields separate so a caller's
`status` key cannot overwrite Sanctum's processing state.

Provide set, merge/patch, remove, and batch operations. Query filters cover
equality, ordered comparisons, nested paths, array membership, existence/missing,
Boolean combinations, sorting, and basic aggregation. Use parameterized database
queries and documented null/missing/type semantics. Optional named metadata
indexes can be added for frequent access paths; accepting a new key must not
require a schema migration.

Provide a cursor-based changes API and optional streaming notifications backed
by the same retained change records. Reconnection resumes from a cursor; an
expired cursor returns a rescan instruction. Do not require outbound webhooks
for v1. This lets integrations discover metadata resets without Sanctum performing
their application-specific derivation work.

Direct database access is an exceptional operator API:

- Open a read-only SQL session against a named tenant catalog or collection
  generation; return schema and generation information.
- Execute parameterized SELECT queries with pagination, cancellation, and resource
  limits. Scope the connection to that database rather than a server-wide SQL
  connection that can attach unrelated tenant files.
- Allow export/download of a consistent snapshot for offline inspection, with
  required extension/schema information and an expiry for the handle.
- Keep managed writes on normal mutation APIs and their serialized commit path.
  Arbitrary SQL writes and replacing live databases are not implied by query
  access and are outside v1.

If vector storage later separates, SQL sessions identify each database explicitly.
The ordinary query API continues to provide the common joined view.

## 10. Embedding configuration and lifecycle

One server-level embedding profile applies to all tenants and collections.
There are no tenant or collection overrides and no requirement to ship a local
model runtime. Ollama supplies local inference.

Initial adapters:

| Provider | Required configuration and behavior |
| --- | --- |
| Ollama | Endpoint, model, timeouts; communicate with an existing Ollama service |
| Google Cloud | Project/location/model and appropriate cloud credentials; use its embedding API |
| OpenAI-compatible | Base URL, model, credentials, supported dimensions/options; accommodate compatible services such as OpenRouter |

Define provider traits for document and query embedding, batching, dimensions,
distance/normalization requirements, and cancellation. The adapter translates
the contract into the provider's actual API. Compatibility is verified by real
embedding requests, not by assuming every endpoint accepts the same options.

The embedding profile ID fingerprints provider/model identity, model revision when
available, dimensions, distance metric, preprocessing, and document/query task
settings. Credentials are not part of that fingerprint. Model aliases that can
change invisibly need an explicit profile revision supplied by configuration.

Changing the profile immediately excludes vectors from previous profiles. Search
falls back to full text while active-profile embeddings rebuild in the background.
Do not call the old provider or mix equal-length vectors from different models.
Partially rebuilt collections may use new vectors with explicit coverage and a
lexical fallback for the rest.

Persist desired/applied profile information and schedule re-embedding across the
catalog in resumable batches. Accessing an unvisited collection also detects a
profile mismatch and ensures its work is scheduled. Cloud tasks carry the target
profile; old tasks cannot publish into the new profile. Rollouts must configure
API and worker revisions consistently.

Reuse is initially scoped within a collection. Cross-tenant deduplication and
sharing provider results between unrelated users are not required. Store provider
errors and retry timing; stop retrying permanent input/configuration errors until
corrected. A provider outage must not disable ingestion or lexical search.

## 11. Retrieval and provenance

Provide `lexical`, `semantic`, and `hybrid` search. The normal default can be hybrid
when compatible vectors exist and lexical otherwise, reporting the strategy
actually used and any degraded/partial state. Strict semantic requests can
explicitly return an unavailable result instead of silently changing semantics.

Use SQLite FTS5 without a second copy of full source/chunk text, selecting the
contentless-delete form where supported by the pinned SQLite build. Read excerpts
from stored ranges and maintain the index transactionally with chunks. FTS5's
contentless support stores index entries while omitting full column content.
[FTS5 documentation](https://sqlite.org/fts5.html#external_content_and_contentless_tables).

Full-text retrieval is the primary non-embedding path. Add a bounded literal
scan fallback for content not yet indexed, with cancellation, scanned-byte
progress, and explicit partial results. A regex engine and unrestricted exhaustive
grep are not prerequisites for v1. Do not describe token search as arbitrary
substring search, or pretend a scan has examined objects it has not read.

Search filters must influence candidate selection, not only discard a small
top-k afterward. For multiple collections, bound fan-out, query each database,
and merge ranked lists. Use a documented rank-fusion strategy rather than comparing
raw lexical scores from unrelated indexes as though they were globally calibrated.
Deduplicate overlapping hits while preserving exact provenance.

Each hit includes tenant/collection/source/revision/chunk IDs, external source
reference, representation, byte range, excerpt, relevant metadata, score/rank
information, and embedding/processing status. A separate fetch operation retrieves
additional context under a response byte/token budget. Sources remain content,
not instructions that Sanctum executes or promotes into tool control.

No search request spans tenants. Operator-wide storage reporting is a separate
API and does not expand search scope.

## 12. Protobuf/gRPC and MCP contracts

Define a versioned `sanctum.v1` Protobuf package early. Generate Rust services and
clients with `prost`/`tonic`; publish the `.proto` sources, descriptors, examples,
and generation instructions for other languages. Tonic is a Rust gRPC/HTTP2
implementation. [Tonic documentation](https://docs.rs/tonic/latest/tonic/).

Transport messages map to domain types rather than becoming the entire domain
model. Preserve field numbers and reserve removed fields. Version incompatible
contracts. Use typed errors with retryability, operation ID, and field context.

| Service area | Initial operations |
| --- | --- |
| Catalog | Create/list/update/delete collections for a tenant |
| Ingestion | Begin/upload/finalize, import, replace, cancel |
| Sources | List/inspect/delete sources; inspect revisions and processing state |
| Retrieval | Search collections; fetch chunk/range/context |
| Metadata | Set/patch/delete/query; manage selected indexes |
| Jobs | Status/progress, retry/cancel, local pause/resume |
| Changes | Cursor polling and resumable notifications |
| Usage | Collection, tenant, and managed-cluster size calculation |
| Operator database | Schema, read-only query session, snapshot export |
| Runtime | Effective redacted configuration, health, capabilities, diagnostics |

Large uploads use bounded streaming messages or resumable storage upload sessions;
Cloud Tasks payloads carry operation references, never large files. The cloud
HTTP ingestion route may use direct resumable object uploads where transport or
request duration makes that preferable. Finalization is idempotent and detects
incomplete input through the actual stored upload state.

MCP provides a compact set of collection, ingest, search, fetch, metadata, and
status tools over the same services. Publish a tool reference with examples and
pagination/response limits. Support Streamable HTTP for the running desktop or
hosted server and a Rust stdio bridge for clients requiring subprocess transport.
The bridge connects to the existing desktop server instead of starting another
writer. MCP remains its standard JSON-RPC protocol; Protobuf is the separate
application API. [MCP transports](https://modelcontextprotocol.io/specification/latest/basic/transports).

Cloud Run routing must accommodate gRPC HTTP2, MCP HTTP, and Cloud Tasks HTTP
handlers, either through deliberate multiplexing or separate deployment entry
points in the same Rust codebase. Verify those real paths in deployment rather
than assuming a local multi-port setup maps directly to Cloud Run ingress.

## 13. Local durable work

The local executor uses a persistent queue in a service-owned runtime database,
such as `<app-state>/runtime.db`. This queue belongs to the server, not React.
The GUI observes and controls it through the API. In-memory channels may wake
workers but never serve as the sole record of accepted work.

Acquire an actual operating-system lock for the local service's managed store
to prevent two server processes from becoming owners. Report real lock contention;
do not infer ownership from a stale PID file or a guessed process name. Inside
the owner process, collection executors serialize their own database mutations.

Persist work descriptors and stage receipts in each relevant collection as well
so recovery can reconcile collection work with the runtime queue. Idempotent
operation IDs bridge separate database commits; do not assume a transaction spans
the runtime database, collection database, and source files.

The default local worker concurrency is one. The GUI can raise it to 8, 16, 32,
or another configured value. Compute/upload concurrency does not change the
single-writer rule for a collection database. Persist paused state, retries,
attempt counts, and progress sufficiently to resume after a crash or restart.

Use actual execution outcomes to report unavailable files/providers. Configuration
decoding and operation-specific range/type validation are real requirements;
unrelated credential probes, port guesses, or estimated resource checks must not
block the operation before it attempts to run.

## 14. Cloud work, ownership, and publication

Deploy the Linux server/worker roles on Cloud Run, store authoritative state in
Cloud Storage, and use Cloud Tasks for dispatch. Initial deployment concurrency
can be one or two requests/tasks, adjusted through Cloud Tasks and Cloud Run.
Local worker-concurrency settings are ignored by the cloud dispatcher. Correctness
constraints such as database serialization still apply at any resource concurrency.

Cloud Tasks can execute work out of order and more than once. It is neither a
per-collection lock nor an exactly-once commit log. Do not create one task queue
for every collection to simulate ownership. [Cloud Tasks limitations](https://docs.cloud.google.com/tasks/docs/common-pitfalls).

### Durable acceptance and dispatch

1. Store upload bytes and an operation envelope at a stable tenant/collection key.
2. Persist dispatch intent in that envelope before acknowledging acceptance.
3. Submit a task naming the operation and current stage; retry dispatch safely.
   Before dependent stages publish, register revision intent with the collection
   writer and persist its update sequence. A durable upload acceptance is distinct
   from this ordered registration; report both through operation status.
4. Keep a durable dispatch-repair path for the gap between object storage and
   Cloud Tasks. A scheduled Cloud Run reconciliation invocation, triggered by
   Cloud Scheduler, can redispatch pending operations even after all instances
   scale to zero. Request-triggered repair is useful but insufficient by itself.
5. A task returns success only after its result or next durable handoff is stored.
   Expensive stages checkpoint into smaller operations before request deadlines.

The scheduled reconciler is deployment plumbing, not a user-facing scheduling
feature. Task-name deduplication may reduce duplicate deliveries but the durable
operation identity is the source of idempotency. Provider calls can still repeat
after an ambiguous timeout; do not claim exactly-once provider billing.

Partition reconciliation and retain pagination cursors over operation prefixes;
do not open every collection database on each scheduled invocation. Conditional
operation-state transitions prevent duplicate dispatchers from overwriting newer
stage receipts. Failed dispatch remains discoverable until durably handed off.

### Per-database ownership

Treat each collection and each tenant catalog as a separate coordination unit.
One `HEAD.json` per unit contains the committed snapshot reference and writer
ownership state: owner token, monotonically increasing epoch, and lease expiry.
Acquire/renew/release ownership by conditional replacement of this same object.
Publish commits through that same conditional state so a stale lease holder
cannot update a separate unprotected pointer.

Use GCS object generations as the compare-and-swap token. A conditional write
fails if the stored generation changed; a create can require generation zero.
These are conditions on the actual write, not a speculative check followed by
an unconditional mutation. [GCS request preconditions](https://docs.cloud.google.com/storage/docs/request-preconditions).

A worker losing ownership must stop publishing, refresh state, and retry the
idempotent command if still relevant. Lease timing improves liveness; generation
fencing establishes publication safety even when an expired worker resumes.
Serialize lease renewal and publication inside the owner so its own heartbeat
does not race its commit. Test takeover and crash behavior explicitly.

### Applying prepared writes

Uploads, extraction results, and embedding batches can be produced in parallel
and staged durably. A collection executor acquires ownership, opens the current
database snapshot, and applies a bounded batch of prepared commands sequentially.
Each command includes operation ID, intended source revision, profile, and
expected state; stale results become no-ops or explicit conflicts.

Persist applied command IDs in the same SQLite transaction as the mutation.
Build a consistent snapshot, upload it under an immutable key, then conditionally
publish the new committed generation. Do not acknowledge a write as committed
while its only copy is the instance's local SQLite file. Snapshot creation must
use SQLite's supported backup mechanism rather than copying a live file without
its journal state. [SQLite online backup](https://sqlite.org/backup.html).

On conditional-publication failure, discard the unpublished candidate, reopen the
winning generation, and reapply only commands that remain valid. An uploaded but
unpublished snapshot is reclaimable after a safe interval. Batch database changes
so one embedding result does not trigger a 2 GB upload.

This snapshot strategy is the correctness baseline. If measured write amplification
is unacceptable, introduce a durable ordered mutation journal plus periodic
snapshots through the same ownership interface. Do not quietly postpone durability
until shutdown. That optimization needs its own replay, compaction, and recovery
design before hosted scale is claimed.

### Reading and creating collections

Reads resolve a committed generation and may reuse its local database. Bound
staleness and report the generation; a stale cache must not remain authoritative
indefinitely. Read-only snapshots need no writer lease. Source range references
must remain valid for the lifetime of bounded query/fetch handles.

Collection creation is a resumable operation: allocate the collection, publish an
initial database, and add/activate its record in the tenant catalog. Deletion uses
the reverse lifecycle with a tombstone. Neither root/catalog updates nor source
object writes are atomic with a separate collection database; retries and repair
reconcile interrupted transitions. Do not hold several collection leases while
making unrelated remote calls.

## 15. Configuration and desktop lifecycle

The server's authoritative startup configuration is a serialized, versioned JSON
document in `SANCTUM_CONFIG_JSON`. It is the configuration itself, not a path to
a required file next to the executable. Credentials can be referenced through
named environment variables or platform cloud identity rather than copied into
logs or UI status responses.

The desktop Rust host stores an editable configuration file in the platform's
application configuration directory. GUI changes are decoded and atomically saved
by Rust. When starting the child server, the host serializes that configuration
into `SANCTUM_CONFIG_JSON`. A configuration change requiring restart is shown as
pending until a controlled server restart applies it; runtime queue controls can
operate through normal APIs without rewriting startup configuration.

Configuration groups include storage backend/location, embedding provider/model/
dimensions, API and MCP listeners, chunk settings, local work concurrency, cloud
queue/project settings, and operational limits. There is no tenant-mode field.
Choosing local execution or Cloud Tasks selects a dispatch adapter, not a
different multi-tenancy model.

The Tauri Rust host owns exactly one managed server process, restart/backoff,
shutdown, configuration persistence, and the API client. Use a sidecar executable
so the same server also runs headlessly. Closing the settings/activity window
leaves the tray service running; explicit Quit shuts it down cleanly. Persisted
jobs resume on next launch. [Tauri sidecars](https://v2.tauri.app/develop/sidecar/).

The first UI includes:

- Tray status and a small activity window showing ingestion and embedding work.
- Progress counters with failed/retryable stages and useful errors.
- Storage usage and a manual refresh action.
- Settings for storage location/backend, embedding connection, and local
  concurrency, plus start/stop or pause/resume controls as appropriate.
- Minimal collection creation/import selection and an MCP connection snippet so
  the first milestone is usable without assembling configuration by hand.

It is not initially a document editor, chat app, full knowledge browser, or remote
cloud administration console. Tray behavior must be exercised on macOS, Windows,
and supported Linux desktop environments; packaging with Tauri alone does not
prove identical tray behavior everywhere. [Tauri tray documentation](https://v2.tauri.app/learn/system-tray/).

`pnpm devtop` will build/watch the Rust server as needed, launch Tauri and the React
development frontend, and let the Tauri host own the server/tray lifecycle. Its
exit path cleans up only processes it launched. Restarting development code must
not silently launch a second writer against the same local store.

## 16. Import adapters

All adapters invoke the same ingestion/update APIs and supply stable provenance.

| Adapter | Behavior |
| --- | --- |
| File upload | Stream or resumably stage bytes, choose retention policy, attach external ID and metadata |
| Local file/directory | Walk incrementally, apply include/exclude rules, stream accepted text files |
| Watched folder | Persist watch definitions, debounce repeated events, reconcile missed changes after restart |
| URL | Fetch text-bearing content, record final URL and change validators, expose real HTTP failures |
| Git repository | Import selected refs/paths; use commit/path/blob identity to detect changes and deletions |

Use content hashes to skip genuinely unchanged files; filesystem timestamps are
hints, not proof of unchanged content. Watchers re-scan after event loss and do
not follow directory/symlink cycles indefinitely. Record removed inputs and apply
the adapter's explicit delete policy rather than silently retaining unrelated old
source records.

URL and Git credentials belong to the controlling operator. Fetch/import work
does not execute retrieved code, Git hooks, or content instructions. The operator
can restrict fetch destinations where the parent application's trust boundary
requires it. These are adapters, not special document types or parallel databases.

## 17. Storage totals and observability

The meaning of storage usage is the sum of stored file/object lengths under the
requested managed prefix:

- Collection: every file/object under that collection, including its database(s),
  source representations, staging, retained snapshots, and pending cleanup data.
- Tenant: every collection plus `root.db`/catalog files and other tenant objects.
- Cluster: every tenant plus any shared files/objects under the managed root.

Use actual filesystem metadata or paginated object listings. Do not estimate from
row counts, vector dimensions, or source logical sizes. A cached total may serve
the tray quickly only if labeled with its calculation time; explicit refresh
enumerates the backing store and reports progress/errors.

Each physical object is counted once; shared root/catalog files are not charged
to every collection. Deleted-but-retained objects still count while present.
Local runtime/config files and disposable cloud download caches outside the
managed storage root are reported separately if needed, not mixed into tenant
totals. Counts represent stored file lengths, not filesystem allocation blocks
or the provider's billing statement. Provider-managed hidden object versions or
soft-delete retention require a separate physical-billing audit if enabled.

Concurrent writes can change the total during enumeration. Return start/end time
and scope, never imply a cross-object transactional snapshot. This is still a sum
of real observed sizes, not a heuristic gate on subsequent operations.

Structured logs and metrics carry tenant/collection/operation identifiers without
raw content or secrets. Track queue age, stage progress, bytes transferred, cache
reuse, snapshot size/publication time, provider latency/errors, vector reuse,
search latency/coverage, and cleanup backlog. These drive capacity decisions and
the tray's state rather than fabricated percentage estimates.

## 18. Delivery milestones

Each milestone below is a planning unit. Implementation PRs should be coherent
and independently reviewable; this planning change does not implement them.

### Milestone 1 — Personal desktop with local MCP

1. Establish Cargo/PNPM workspaces, Rust configuration/domain types, and Protobuf
   contracts with generated Rust clients and service adapters.
2. Implement filesystem storage, tenant catalog, collection database, migrations,
   canonical text, source IDs, and bounded streaming chunks.
3. Add full-text search, range fetch, external provenance, metadata CRUD/filtering,
   durable local work, progress reporting, and local storage totals for the tray.
4. Integrate Ollama, server-wide profile identity, vector search, and lexical
   fallback when embeddings are pending or unavailable.
5. Expose MCP HTTP and the stdio bridge over the same service layer.
6. Ship the Tauri tray/settings/activity flow and `pnpm devtop`, with uploads and
   local file/directory imports available to the owner.
7. Complete source replacement/reuse, stale task handling, metadata reset signals,
   and cleanup before treating repeated personal use as ready.

Acceptance: the owner can launch the desktop app, select storage, configure
Ollama, ingest a directory, connect an LLM client, retrieve cited chunks, edit a
source, restart the app, and see correct persisted state and resumable progress.
The core protocol/CLI path is documented; there is no requirement to wait for
cloud deployment or all import adapters to start using Sanctum.

### Milestone 2 — Complete local integrations and providers

Add Google Cloud and OpenAI-compatible adapters; model-switch rebuilds; watched
folders; URL and Git imports; advanced metadata queries and changes cursors;
read-only SQL sessions/snapshot export; multi-collection search; broader usage reporting;
and macOS/Windows/Linux packaging and tray validation.

Acceptance: all three provider adapters make real embedding requests, provider
outages preserve lexical retrieval, import updates are idempotent, old profiles
are excluded immediately, and another language can use published Protobuf
contracts without going through the desktop frontend.

### Milestone 3 — Object storage and durable publication

Implement MinIO-tested S3 and native GCS backends, range contracts, database
snapshot manifests, per-unit ownership, conditional publication, batched prepared
writes, and interrupted-operation recovery. Exercise local and remote adapters
through the same storage contract suite.

Acceptance: crashes and competing writers cannot lose an acknowledged committed
mutation or publish stale results; range fetches return exact text; source
replacement and cleanup preserve active readers; catalog creation/deletion
recovers across each interruption point.

### Milestone 4 — Cloud Run and Cloud Tasks

Deploy Linux API/worker roles, durable operation envelopes, Cloud Tasks delivery,
scheduled dispatch repair, resumable stage execution, service-to-service identity,
and hosted progress endpoints. Tune initial external concurrency to one or two
and demonstrate that desktop concurrency settings do not cap cloud dispatch.

Acceptance: an accepted ingestion survives scale-to-zero, duplicate/out-of-order
tasks, instance loss, and retries. MCP, gRPC uploads/search, and HTTP task handlers
work through actual Cloud Run ingress. Warm instances reuse current databases;
cold instances recover from published state.

### Milestone 5 — Capacity and publication optimization

Measure 200 MB sources, GB-scale collections, 1–2 GB databases, realistic update
patterns, many mostly idle tenants, and concurrent active collections. Record
cold/warm latency, memory, snapshot write amplification, range transfer volume,
index size, recall/quality, and provider work avoided by reuse.

Choose a split vector database, improved index, durable mutation journal, or
later sharding only in response to measured limits. The cold-open target is an
acceptance measurement with documented results, not an assumed guarantee. Any
added journal or shard design must preserve tenant scope, source provenance,
query semantics, and crash recovery.

## 19. Validation strategy

Validation focuses on behavior and real I/O, not checks that infer failure from
secondary evidence. Use contract tests for adapters, state-transition tests for
durability, and end-to-end scenarios for user-visible workflows.

| Area | Required scenarios |
| --- | --- |
| Text/ranges | Multibyte UTF-8 split across buffers, CRLF, empty/large files, long lines, RTF extraction, exact boundary reads |
| Storage | Real filesystem and MinIO range reads; GCS generation conflicts and returned range lengths; interrupted uploads |
| Ingestion | 200 MB input with bounded memory, restart during each stage, duplicate finalize, unavailable provider |
| Updates | Insert/move/delete text, repeated identical chunks, stable vector reuse, new offsets, metadata reset, late old jobs |
| Readiness/cleanup | Lexical availability before embeddings, replacement failure, old data retained until ready, stale fetch after eviction |
| Search | Missing/partial embeddings, metadata filtering before top-k, overlap deduplication, same-tenant multi-collection ranking |
| Model change | New profile/dimensions, old provider absent, partial rebuild, stale model tasks, no cross-profile similarity |
| Local queue | Crash between queue/collection commits, pause/restart, concurrency changes, no duplicate child server |
| Cloud queue | Duplicate/unordered tasks, dispatch failure, missed handoff, timeout/resume, scale-to-zero repair |
| Ownership | Two writers, expired owner resuming, renewal/publication race, failed compare-and-swap, idempotent reapply |
| Databases | Consistent snapshot under writes, migrations, interrupted publication, optional multi-file generation consistency |
| API | Generated non-Rust client, streamed upload, errors/cancel, changes cursor expiry, SQL session generation |
| Tenants | Catalog isolation, no cross-tenant search, path encoding, operator usage across tenants |
| Desktop | All target OS trays, close-to-tray vs Quit, settings persistence, progress/errors, LLM client connection |
| Usage | Sum real object/file lengths, root overhead once, staging/cleanup bytes, listing pagination/errors, concurrent changes |

Keep provider response fixtures for repeatable tests and separate opt-in real
provider/deployment checks for credentials, dimensions, and compatibility. Never
label a mocked provider test as a successful real embedding request. Do not add
hosted PR/push validation workflows contrary to the manual change protocol;
ordinary development checks run locally, with release/deployment validation at
those respective stages.

## 20. Decisions to finalize during implementation

These are bounded implementation choices, not a reopening of the owner's scope:

- Permanent name for collections and final public naming before API stabilization.
- Pinned SQLite/vector implementation after local target-size measurements.
- Concrete RTF decoder and non-UTF-8 handling after streaming correctness tests.
- Exact chunk defaults, lexical ranking, and vector metric based on selected model.
- Snapshot batch sizes, ownership lease timing, and query-handle retention after
  fault injection and cloud measurements.
- Whether hosted write amplification requires a journal before wider deployment.
- Whether vector storage needs its own database, while retaining one publication
  generation and one mutation owner.
- Specific Google Cloud model/location and provider limits in deployable examples.
- Production memory/concurrency settings, packaging/signing, and Linux tray support.

Do not turn these choices into preflight gates that block ordinary ingestion or
search without exercising the relevant operation. Ship the desktop path first,
retain explicit recovery semantics, and use observed behavior to refine the
capacity work.
