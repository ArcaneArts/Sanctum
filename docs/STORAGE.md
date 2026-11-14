# Storage backends

The `sanctum-storage` crate defines validated object keys, streaming
uploads and downloads, exact half-open byte ranges, versioned writes,
recursive listing, and deletion. The filesystem and S3 implementations
share a contract test. Database publication will use small conditionally
written control objects and stream data to caller-owned unique keys.

S3 uses `If-None-Match` for create and `If-Match` with the returned ETag
for replacement. A control write is limited to 1 MiB. Large immutable
payloads use sequential 8 MiB multipart writes. An interrupted upload
can leave incomplete parts, so configure the bucket's incomplete multipart
upload lifecycle rule. [S3 multipart uploads](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html).

Canonical objects use identity encoding. Range reads check the response
range and body length and cap requested bytes at 4 MiB. After a body
interruption, the S3 client can resume from the next byte. The adapter
validates each response's interval, total length, encoding, ETag, and
version ID when present before releasing resumed bytes. The stream also
enforces the declared length. Higher-level retrieval verifies the chunk
digest against its database record. [S3 GetObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html).
Listing returns sizes and a continuation cursor. Usage sums actual stored
object lengths and includes its measurement window.

## Local validation

The filesystem contract runs in `pnpm check`. To run the same contract
against MinIO, start an isolated MinIO server and create a test bucket.
The test writes under a unique prefix and deletes its objects.

```sh
export SANCTUM_MINIO_ENDPOINT=http://127.0.0.1:9000
export SANCTUM_MINIO_BUCKET=sanctum-contract
export AWS_ACCESS_KEY_ID=YOUR_TEST_ACCESS_KEY
export AWS_SECRET_ACCESS_KEY=YOUR_TEST_SECRET_KEY
cargo test --locked -p sanctum-storage-s3 --test minio -- --ignored
```

The server can select S3 for its hosted catalog. See the
[configuration example](#hosted-server-configuration).

## Object-backed database publication

The database executor uses a `HEAD.json` for each coordination unit.
Tenant catalogs use `<tenant>/catalog` and collection databases use
`<tenant>/collections/<collection>`. This keeps a collection named
`catalog` independent of the tenant catalog. Each HEAD records its
snapshot, writer token, lease expiry, epoch, and a unique update ID.

Writers acquire, renew, publish, and release through conditional writes
on that same object. While downloading, preparing, or uploading a
snapshot, the executor renews at half the lease interval. The same task
serializes renewal and publication. If renewal misses the deadline or
loses its conditional write, the attempt cannot publish. Once a worker
stops, another worker can acquire the unit after the last renewal
expires. Snapshot downloads verify length and BLAKE3. The executor
applies at most 64 prepared
commands and their receipts in one SQLite transaction. It uploads a
consistent SQLite backup under a fresh key before attempting to publish
it. Replaying a batch of previously applied commands returns the saved
outcomes and current generation. Only the successful HEAD write makes a
new database visible. [SQLite online backup](https://www.sqlite.org/backup.html).

Cleanup marks a noncurrent snapshot when it first observes it, then
waits at least five minutes before removing it. The grace period starts
when retirement is observed, so it also covers long-lived current
generations that were only recently replaced. Sweep each unit with
the returned continuation cursor. Uploaded snapshots left behind by
failed publication follow the same cleanup process. Command receipts
are retained for 45 days. The dispatch layer must reject expired
operation envelopes when it invokes this executor.

The executor has a shared publication contract for the filesystem,
MinIO, and GCS. Hosted creation stores a request under
`<tenant>/catalog/creations/<request-id>.json` before initializing the
collection database. Maintenance can resume from this intent even if
the caller or initial API instance never retries. Activation is a
separate fenced catalog commit with a request receipt. A committed
receipt takes precedence on replay, and an interrupted activation
reuses the published initial database. The intent is removed once
the catalog outcome is committed. A pending request ID is reserved
for the named collection within its tenant.

### Optional SQLite page deltas

Set `"hosted_snapshot_page_deltas": true` to publish small changes as
immutable `pages.delta` objects. The writer still creates a consistent
SQLite backup and records the stage receipt in that backup. It uploads
the changed pages before the conditional HEAD write. A reader downloads
the verified base and ordered deltas, applies them to a private file,
and checks the reconstructed database against the committed digest.
Read-only database sessions still expose complete SQLite files.

Each HEAD can reference up to four deltas. Their combined size must be
at most half the resulting database size. A new full backup starts the
next chain when either limit is reached, the database shrinks, or the
page size changes. Cleanup retains every object in the current chain.
After a full backup replaces the chain, its objects enter ordinary
reader grace. Existing session records include the chain they opened.

Deploy servers that support HEAD format 2 to all roles on a storage
prefix before enabling the option. Older servers only understand
format 1. Setting the option back to `false` keeps existing chains
readable and resumes full backups on subsequent writes. Allow old
read-only sessions to expire and confirm every HEAD uses format 1
before rolling back to a server without delta support.
[Capacity measurements](CAPACITY.md#sqlite-page-changes-during-a-source-replacement).

## Google Cloud Storage adapter

The native adapter uses the GCS JSON API and Application Default
Credentials. It creates control objects with `ifGenerationMatch=0`
and replaces them using the returned generation. Full downloads fence
the media request to the generation from the metadata response. When a
writer replaces or removes that generation between the requests, the
adapter reads fresh metadata before retrying the media request. It returns
a retryable error if the generation keeps changing across four attempts.
Once a full download starts, the adapter can resume an interrupted body
at the next undelivered byte using a range request pinned to the original
generation. It checks the resumed interval and object length and attempts
at most four resumes without forward progress. If the pinned generation
is replaced midstream, the reader fails so the caller can restart from
fresh metadata. Range reads require a 206 response and verify the exact
interval and length. A transient HTTP error or broken connection can
retry the same bounded range up to three total attempts within 15 seconds.
Each attempt has five seconds and starts with an empty buffer. The GCS
range log counts received body bytes across attempts. Missing objects
and invalid range responses return immediately.
[GCS object downloads](https://docs.cloud.google.com/storage/docs/json_api/v1/objects/get),
[retry behavior](https://docs.cloud.google.com/storage/docs/retry-strategy).
Bulk transfers use bounded 8 MiB parts in a GCS resumable upload
session. After an interrupted request or retryable HTTP failure,
the adapter asks GCS for its persisted offset and resumes from that byte.
A status response can also confirm an upload whose final response was lost.
The adapter bounds retries without progress and attempts to cancel the
session on failure. The caller can then start a new transfer with a fresh
object key. [GCS generation preconditions](https://docs.cloud.google.com/storage/docs/request-preconditions), [resumable uploads](https://docs.cloud.google.com/storage/docs/performing-resumable-uploads).

Run the live contract against an authorized test bucket with ADC that
can create, read, list, replace, and delete objects. Use a bucket without
a retention rule that prevents test cleanup.

```sh
export SANCTUM_GCS_BUCKET=YOUR_TEST_BUCKET
cargo test --locked -p sanctum-storage-gcs --test live -- --ignored
cargo test --locked -p sanctum-db --test publication gcs_ -- --ignored
```

The publication tests use unique prefixes and remove their test objects
on success. They verify fenced takeover, replay, and snapshot reads,
including reconstruction from page deltas. The regular test suite checks
the JSON API with a loopback HTTP fixture. Live bucket verification remains
open.

## Hosted server configuration

Set `object_storage` in `SANCTUM_CONFIG_JSON` to select the backend.
For a local MinIO bucket, start the server with a configuration such as
the following. AWS credentials come from the process environment.

```json
{
  "version": 1,
  "grpc_listen": "127.0.0.1:47550",
  "object_storage": {
    "backend": "s3",
    "endpoint": "http://127.0.0.1:9000",
    "bucket": "sanctum",
    "region": "us-east-1",
    "prefix": "development",
    "allow_http": true
  }
}
```

For GCS, set `object_storage` to
`{"backend":"gcs","bucket":"your-bucket","prefix":"sanctum"}`.
The prefix is optional. Use a dedicated bucket or prefix for each deployment.
Omit `object_storage` and provide `storage_root` for the local filesystem
service. The hosted server listens on gRPC and supports collection
creation, listing, scoped storage usage, and durable upload acceptance.
Keep this listener within the controlling application's trusted network.

To process object-backed uploads in the same standalone server, set
`"standalone_object_worker": true` at the top level. The embedded worker
reads durable dispatch hints from the configured bucket prefix and runs
registration, lexical indexing, embedding, profile rebuilds, and cleanup.
It runs one stage at a time. New submissions wake it, and it also polls
for pending work after restart. It shares the server's gRPC/MCP listener
configuration and needs no worker HTTP listener. Use a dedicated prefix for
this setup. Cloud Run and Cloud Tasks have their own worker configuration.

The hosted create operation initializes the collection database before
activating its catalog record. If a request is interrupted between those
steps, repeat it to activate the existing database. Pass a UUID through
`CreateCollectionRequest.request_id` or `sanctum-cli create --request-id`
to replay a successful activation after an ambiguous response. The
request ID applies within the tenant catalog. Hosted lists default to
50 items. Pass `next_after` as `after` to continue the same tenant's list.

### Hosted uploads

Use `SubmitText` with the [upload frame protocol](LOCAL_API.md), or submit
from the CLI in background mode. Supply a nonzero UUID to recover from an
ambiguous response. Repeat the same header and source bytes with that UUID
within the same tenant and collection. The server returns the same operation
and revision IDs. Reusing the UUID with different input or options returns
`ALREADY_EXISTS`.

```sh
cargo run --locked -p sanctum-cli -- upload notes /path/to/notes.md \
  --background --request-id "$(uuidgen)"
cargo run --locked -p sanctum-cli -- job notes OPERATION_ID
```

The server spools the stream to a temporary file and honors
`max_source_bytes`. It streams the original bytes to a unique input key under
`<tenant>/collections/<collection>/operations/<operation>/inputs/`.
The sibling `operation.json` is created conditionally or replaces an
abandoned-input fence by version. An accepted envelope records the input's length and BLAKE3 digest, ingestion options, and
pending registration stage. A failed conditional write with an uncertain
outcome preserves its input for recovery. Concurrent requests with the same
ID adopt the winning operation and discard their losing input when possible.

The `ACCEPTED` status can be queried after restart through `GetIngestionJob`.
A replacement targeting an external reference has an empty `source_id` until
registration resolves it. Hosted MCP uses the same acceptance path for inline
text.

`UploadText` accepts the same frames and waits up to 30 seconds after acceptance
for a worker to publish the lexical revision. The worker continues after an
RPC disconnect or timeout. Hosted failures after acceptance include the raw
16-byte UUID in gRPC trailing metadata under `sanctum-operation-id-bin`.
Use that UUID with `GetIngestionJob`. Supplying a UUID as `--request-id` also
lets the CLI safely repeat a synchronous upload after a lost response.
Use `SubmitText` for long-running uploads or deployments that dispatch work
outside the waiting period.
[Hosted MCP](MCP.md#hosted-connection).

### Hosted revision registration

Set `hosted_worker_listen` in `SANCTUM_CONFIG_JSON` to enable the private
HTTP worker listener. For local testing use `127.0.0.1:47552`. Restrict this
listener to trusted callers. See the Cloud Tasks configuration below for
automatic delivery.

```sh
curl --fail-with-body http://127.0.0.1:47552/internal/tasks/register-upload \
  -H 'Content-Type: application/json' \
  -d '{"tenant":"default","collection":"notes","operation_id":"OPERATION_UUID"}'
```

The task applies a revision intent under the collection writer lease. The
published database contains the resulting update sequence and operation
receipt. The worker then conditionally advances the envelope. If it stops
between these steps, redelivery reuses the database receipt and checkpoints
the same result. Duplicate or conflicting deliveries cannot advance the
sequence twice for one operation.

After registration, `GetIngestionJob` reports `REGISTERED` and the assigned
update sequence. Existing revisions stay current until indexing publishes a
replacement. A stale expected revision or a duplicate external source
returns a durable failed status. HTTP 204 confirms the outcome was recorded.
An operation without a recorded registration outcome expires after 30 days.
A recorded outcome can be replayed while its database receipt is retained.

### Hosted lexical publication

Cloud Tasks sends the registered operation to `/internal/tasks/prepare-upload`.
The worker verifies the accepted input, counts overlapping chunks, and writes
canonical UTF-8 for RTF to an immutable object. A conditional write records the
canonical digest, length, and locator in the operation envelope. A retry can
resume from this checkpoint on another worker. Plain text preparation refers
to the accepted input object.

The next task delivers the same operation reference to
`/internal/tasks/index-upload`. It indexes overlapping chunks from the
canonical object and verifies its digest, length, and chunk count as it reads.
If a saved RTF extraction is missing or fails verification, the worker rebuilds
it from the accepted RTF. It publishes a fresh object only if the reconstructed
text and processing metadata match the recorded extraction.
When the request retains its original RTF, the worker verifies those bytes
before publishing the original locator. A missing or changed original leaves
the operation registered and retryable.
The worker checks the registered sequence inside the publishing transaction.
An older replacement cannot displace a newer registered revision.
The snapshot commits text-object locators, the lexical index, and the
indexing receipt together. Replaying a published receipt avoids rebuilding
its index.

`GetIngestionJob` keeps the job `REGISTERED` while indexing is pending.
After successful preparation, `preparation.ready` reports the canonical byte
and chunk counts, and `processed_bytes` reports the original bytes processed.
If preparation found unusable input, `preparation.invalid_source` carries the
saved error until the index stage commits the failure. The job reports
`TEXT_READY` with the committed revision after lexical publication. While
embedding work remains queued, `embedding_work` reports the next durable
batch number and its earliest eligible dispatch time, when scheduled.
`ListIngestionJobs` and MCP `ingestion_status` report the same checkpoint.
Use `GetEmbeddingStatus` for vector coverage and failures.
`SearchText` serves collection-scoped lexical hits, including hybrid
requests when hosted embeddings are unavailable. `FetchChunk` reads
the hit's exact byte range and verifies its fingerprint. Search can read
overlapping or adjacent excerpts from the same immutable source object in
one range request of up to 64 KiB. Each search reads up to four
independent ranges concurrently, verifies each fingerprint separately,
and returns excerpts in ranked order. Hosted
`SearchCollections` accepts one to eight collections in a tenant and
returns the aggregate coverage and scoped references. `FetchSourceRange`
resolves a published revision's object in the collection snapshot and
reads the requested range, up to 64 KiB. A pending literal scan reads the
accepted UTF-8 input or saved RTF extraction in bounded ranges and reports
the exact scan coverage. [Range API](LOCAL_API.md#fetch-text-by-source-range),
[pending scan API](LOCAL_API.md#literal-scan-of-pending-text).

### Cloud Tasks dispatch

Set `cloud_tasks` on the hosted API and worker. Use the full queue resource
name, the private worker service's HTTPS origin, and the service account
that Cloud Tasks will use to invoke it.

```json
{
  "queue": "projects/PROJECT/locations/REGION/queues/QUEUE",
  "worker_url": "https://WORKER.run.app",
  "service_account": "sanctum-tasks@PROJECT.iam.gserviceaccount.com"
}
```

The API and worker use Application Default Credentials to create tasks.
Grant those identities Cloud Tasks Enqueuer on the queue and permission
to act as the task service account. Grant that account Cloud Run Invoker
on the private worker. The task carries an OIDC token with the worker's
origin as its audience. [Creating tasks](https://docs.cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues.tasks/create), [Cloud Run service identity](https://docs.cloud.google.com/run/docs/authenticating/service-to-service).

A dispatch error leaves accepted work discoverable in object storage. Invoke
`POST /internal/maintenance/dispatch/{shard}` on the private worker for each
shard from 0 through 15 with an authorized Cloud Scheduler identity. Each
request processes at most 16 hints, persists the continuation, and reports
`dispatched`, `failed`, and `more`. The next invocation continues the scan
or begins a new pass when the previous pass ended. Independent hints can
run concurrently. A hint that exceeds its request budget counts as failed
for this page and stays available for the next pass. Dispatch retries keep
the same task name within a six-hour window. A later window uses a new
name so repair can recover tasks that exhausted their queue retries.
Configure the scheduler frequency against the measured queue depth.
Once the collection manifest is retired, repair removes its upload and
rebuild hints, including hints whose operation envelopes were already
reclaimed. A late delivery to a retired collection is acknowledged
without publishing anything. New upload hints include the staged attempt
UUID. Repair keeps a fresh hint while admission writes its envelope. After
15 minutes, repair can fence an unaccepted envelope and remove that attempt's
hint. If another attempt already won, repair removes only the losing attempt's
hint. It also removes hints left by terminal or compacted operations.
Operation-only hints from older servers still dispatch accepted work and
remain discoverable while their envelope is absent. Upgrade repair workers
before enabling new writers that create attempt-scoped hints.
[Cloud Tasks HTTP acknowledgments](https://docs.cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues.tasks#HttpRequest).

The private listener accepts manual registration, preparation, and indexing
when `cloud_tasks` is omitted. Index deliveries queued before this change and
the standalone object worker can still prepare and index in one request.
During an upgrade, deploy the worker with the preparation endpoint before
deploying the API. Operations accepted before dispatch hints were
introduced can be resubmitted with the same request ID and input to record
a hint. Live Google Cloud deployment verification remains open.

### Hosted snapshot reuse

The API resolves the current HEAD for each catalog or collection read.
It reuses a downloaded snapshot when the generation and digest match,
with at most 16 retained files and a 2 GiB default byte budget. Set
`hosted_snapshot_cache_bytes` to change the budget or set it to zero to
disable retention between transfers. Concurrent readers of the same verified
generation share an active download, including when the file exceeds the budget
or its cache entry is evicted during the download. Once the transfer and its
waiters finish, an unretained generation is downloaded again on a later request.
Each materialized collection can retain one idle read-only
SQLite connection for search, fetch, and other ordinary collection reads.
Simultaneous reads can open additional connections and release them after
use. The handle stays with its own verified snapshot when HEAD advances.
Oversized snapshots and files still in use can add to active temporary
storage. Hosted gRPC search and fetch hits report the
database generation that supplied their chunk ranges.

Receipt reads and writers can use the same verified snapshot cache. A
writer resolves the generation from its acquired lease, copies a retained
snapshot to a private working file, and publishes a SQLite backup. Only
successful conditional publication makes the new backup eligible for the
cache. Reads continue to resolve HEAD, including after another instance
publishes or a collection is deleted. Full backups are the default wire
format. [Optional SQLite page deltas](#optional-sqlite-page-deltas).

### Hosted embedding batches

Set the same server-level `embedding` configuration on the API and
worker. For a private worker, use the following request to prepare
and publish a group of up to 16 distinct chunk inputs. Repeat until
`processed` is zero.

```sh
curl --fail-with-body http://127.0.0.1:47552/internal/tasks/embed-collection \
  -H 'Content-Type: application/json' \
  -d '{"tenant":"default","collection":"notes"}'
```

`GetEmbeddingStatus` reports coverage and retry state for the configured
profile. Changing that profile immediately excludes previous vectors.
Semantic queries require matching vectors. Hybrid queries fall back to
lexical results while embeddings are pending or the query provider is
unavailable.

With Cloud Tasks configured, lexical publication schedules bounded
embedding steps automatically. The worker checkpoints each step and
schedules retryable provider work at its recorded retry time. The
worker respects each provider's request batch size and groups completed
requests in one fenced SQLite publication. Its preparation budget is
120 seconds per group, including chunk reads and provider calls. If the
budget expires after some requests finish, the worker publishes those
results and leaves the remaining inputs pending. Upload and profile
rebuild tasks conditionally create a prepared batch under
`prepared-embeddings/<batch-id>.json` in the collection's object prefix
before taking the writer lease. The batch ID stays the same across
deliveries of the same step and profile. A retry on another instance
uses the staged results when no committed receipt exists. Two requests
can still make provider calls before either stages results. A committed
step's SQLite receipt takes precedence if an envelope write fails.
The worker removes staged results after publication. The scheduled
upload sweep reclaims abandoned batches after 45 days. When workers
complete the same input concurrently, the first
committed valid vector is kept for that input and profile. A failure
only updates the retry state the worker observed when it selected the
input. The reconciler can recover a missing delivery. Embedding and rebuild
task payloads include the target profile ID, which also participates in the
Cloud Tasks name. A delivery for a different profile leaves the current
batch alone. Dispatch repair can enqueue the pending upload batch under
the new profile in the same dispatch window. An old upload task without
a profile ID is handed back to dispatch on a Cloud Tasks worker. The
manual worker endpoint can omit the profile when Cloud Tasks is absent.
The scheduled embedding
profile endpoint scans one page of object keys after a profile change
and queues collections from their manifests. A persisted cursor resumes
discovery on the next invocation. Once a worker starts discovery for the
configured profile, a search can enqueue a collection with incomplete
coverage before the discovery cursor reaches it. It persists the same
repair hint and rebuild record as scheduled discovery. Search gives
each collection's enqueue attempt up to one second and can return its
normal result if that attempt fails. Strict semantic search still
requires compatible vectors. Workers rebuild each collection in bounded
steps and ignore old run IDs. See the
[Cloud Run schedule](CLOUD_RUN.md#schedule-dispatch-repair).

### Hosted source deletion and reclamation

`DeleteSource` removes a source from the active index, invalidates
registered replacements, and records a tombstone. Supply an expected
revision to reject a stale deletion and a request UUID to replay a
committed deletion after a lost response. The CLI accepts these as
`--expected-revision` and `--request-id`.

The scheduled source sweep removes retired locators and records their
object keys in the same SQLite publication. Physical deletion waits
another 15 minutes so requests holding an older snapshot can finish.
Hosted gRPC requests have a 10-minute server deadline.
Each collection sweep attempts up to 64 due deletions, with up to four
requests in flight and a ten-second limit per request. The worker
acknowledges successful deletions and delays failed objects for at least
five minutes in the same SQLite publication. Other due objects can still
make progress. An already absent object is safe to acknowledge after
a restart or a lost delete response.

An obsolete replacement is eligible once its successor has coverage for
the active profile. Text-only RTF ingestion discards the original after
committing its canonical revision.
[Cloud Run schedule](CLOUD_RUN.md#schedule-dispatch-repair).

The source sweep saves the unprocessed collection manifests along with
the storage continuation token. Its `more` field covers both forms of
remaining work. The next invocation drains the saved manifests before
requesting another storage page.

### Scheduled snapshot reclamation

The worker's snapshot sweep scans catalog and collection manifests and
pending creation intents. Each invocation processes up to eight entries
and stores the remaining work and pagination cursors. It acquires the
unit's writer lease to mark snapshots that are no longer current. The sweep
renews the lease during storage operations and stops when renewal fails.
Deletion is eligible 15 minutes after that observation, including for uploaded
snapshots that never committed.
Busy units are revisited on the next pass. The sweep preserves the
published HEAD generation and leaves already downloaded reader files usable.
The maintenance response includes `recovered_creations` for creation intents
resolved with a successful catalog outcome.

The snapshot, source, and upload sweeps preserve items interrupted by the
invocation's work deadline in their saved pages.
[Scheduler and sweep budgets](CLOUD_RUN.md#schedule-dispatch-repair).

### Hosted metadata

MCP and gRPC can read or edit namespaced JSON on collections, current
sources, and chunks. A collection edit commits to the tenant catalog.
A source or chunk edit commits to its collection database and records
a metadata change in the same transaction. Each write publishes through
that database's existing writer lease. An overlapping writer can
return a retryable busy error. Mutations check revision identity inside
the transaction and stale revision targets require a fresh reference.

gRPC `QueryMetadata` filters, sorts, and counts against published
snapshots with the same JSON type rules as the local API. Search uses
these fields when evaluating metadata predicates. Source and chunk
metadata reset on replacement. Collection metadata remains attached
to the collection.
[Metadata and changes](LOCAL_API.md#metadata-and-changes).

### Hosted database inspection

`OpenDatabaseSession` pins the current catalog or collection generation for
five minutes. An object-backed handle can be used by any API instance with
access to the same deployment prefix. It refers to the existing published
SQLite generation. API processes verify the complete file against its recorded size and
BLAKE3 digest and reuse it within the configured snapshot cache budget.
The regular snapshot sweep removes expired session records after the
reader grace period. Close a session early to release its handle.
[Operator SQL and export](LOCAL_API.md#read-only-sql-sessions-and-snapshots).

### Abandoned upload candidates

The private upload sweep advances a durable scan cursor. It checks up to
eight candidates per invocation. An input older than 15 minutes is
eligible when a different attempt won the operation envelope. An RTF
extraction candidate is eligible after the 30-day indexing window and
15-minute reader grace if it has neither a published locator nor a
pending source-reclamation entry. To reclaim an input without an accepted
envelope, the sweep writes a fence to the same key that acceptance uses.
If acceptance wins the conditional write, the sweep checks again on a
later pass. A retry can replace a fence only with its own newly staged
input. A fence alone is invisible to worker and status requests.
[Cloud Scheduler configuration](CLOUD_RUN.md#schedule-dispatch-repair).

### Hosted collection deletion

`DeleteCollection` publishes a tombstone in the tenant catalog and removes
the collection from catalog reads. It then fences the collection manifest
so delayed tasks cannot publish. Supply a request UUID to resume the
same deletion after an ambiguous response. If a current writer delays
the fence, retry the request or let the snapshot sweep finish it from
the catalog tombstone. Deleted collection IDs remain reserved.
Existing database inspection handles can read their pinned generation
until expiry. The worker waits 15 minutes after the manifest is fenced,
then removes the retired collection's snapshots, operation objects,
and source bytes in bounded pages. It retains the small manifest fence.
An already materialized snapshot remains readable for the life of its
local reader.

### Hosted ingestion activity

`ListIngestionJobs` and `sanctum-cli jobs [COLLECTION]` list retained
operations in a tenant or one of its collections. Hosted activity follows
object-key order and reads current job records. Pass `next_page_token` back
as `page_token` with the same scope. Continue when a short or empty page
still has a token. The server bounds object listing per request, and skips
expired operations and deleted collections. The page is a live view of
storage, so concurrent submissions or cleanup can change later pages.

### Hosted upload cancellation

Use `CancelIngestionJob` or `sanctum-cli cancel COLLECTION OPERATION` for
an accepted or registered upload. Cancellation and indexing compete for
one SQLite receipt under the collection writer lease. Once text has been
published, cancellation fails and the revision remains available.

A successful cancellation releases any reserved external ID for a new
source, discards the staged upload, and returns `CANCELED`. Repeating the
request returns the same canceled status. If the worker or API stops
between committing the receipt and updating the operation envelope,
repeating the request or delivering the next worker task recovers the
receipt and finishes cancellation. Late tasks cannot publish canceled
input. Cancellation uses the same 30-day window as indexing.

### Hosted upload retry

`RetryIngestionJob` and `sanctum-cli retry COLLECTION OPERATION` request
another delivery of pending registration, indexing, or embedding within
the upload's 30-day window. They preserve the operation and revision IDs.
Each retry has a distinct Cloud Tasks name, even within the current
six-hour dispatch window. The worker keeps the same input and publication
receipt, so an earlier delivery is safe to replay. Existing embedding
backoff still applies.

The retry counter and dispatch hint survive restarts. The dispatch repair
scan can recover an interrupted or unavailable Cloud Tasks handoff. Without
Cloud Tasks configured, deliver the pending stage to the private worker.
Terminal operations require a new submission to try again.

### Hosted operation retention

The upload sweep compacts terminal envelopes after 45 days without an
envelope update. A compact record reserves the old request UUID and
keeps the input locator if that input serves published UTF-8 text or
a retained original. Status and worker calls treat the old operation
as absent. A new submission must use a new UUID.

Dispatch repair and the standalone worker settle pending uploads once
their 30-day processing window ends. The upload sweep also handles pending
envelopes left untouched for 30 days. They reuse registration and indexing
receipts, or check the exact revision in the fenced collection database if
the indexing receipt has aged out. Unpublished work becomes `FAILED` with
code `expired`, releases any reserved external ID, and drops unreferenced
input. Committed text remains searchable, including after an interrupted
envelope checkpoint. An expired embedding step stops scheduling work for
that upload, including a step awaiting provider backoff. Existing vectors
and canonical source data remain available. Once the operation settles,
the usual 45-day terminal retention applies.
