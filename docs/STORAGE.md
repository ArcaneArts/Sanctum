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
range and body length and cap requested bytes at 4 MiB. The higher-level
retrieval path also verifies the chunk digest against its database record.
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
on that same object. A lease verifies snapshot length and BLAKE3 after
downloading a generation. The executor applies at most 64 prepared
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

The executor runs against the filesystem and MinIO in its contract
tests. Hosted catalog operations use it to publish initial collection
databases and activate them in the tenant catalog.

## Google Cloud Storage adapter

The native adapter uses the GCS JSON API and Application Default
Credentials. It creates control objects with `ifGenerationMatch=0`
and replaces them using the returned generation. Full downloads fence
the media request to the generation from the metadata response. Range
reads require a 206 response and verify the exact interval and length.
Bulk transfers use bounded 8 MiB parts in a GCS resumable upload
session. If a transfer fails, the caller receives the error and can
start another attempt with a fresh object key. [GCS generation preconditions](https://docs.cloud.google.com/storage/docs/request-preconditions), [resumable uploads](https://docs.cloud.google.com/storage/docs/performing-resumable-uploads).

Run the live contract against an authorized test bucket with ADC that
can create, read, list, replace, and delete objects. Use a bucket without
a retention rule that prevents test cleanup.

```sh
export SANCTUM_GCS_BUCKET=YOUR_TEST_BUCKET
cargo test --locked -p sanctum-storage-gcs --test live -- --ignored
```

The regular test suite checks the JSON API contract with a loopback HTTP
fixture. Live bucket verification remains open.

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

After registration, deliver the same reference to
`/internal/tasks/index-upload`. The worker verifies the accepted input,
extracts canonical UTF-8 for RTF, and indexes overlapping chunks. It
checks the registered sequence inside the publishing transaction.
An older replacement cannot displace a newer registered revision.
The snapshot commits text-object locators, the lexical index, and the
indexing receipt together. Replaying a published receipt avoids rebuilding
its index.

`GetIngestionJob` reports `TEXT_READY` with the committed revision. While
embedding work remains queued, `embedding_work` reports the next durable
batch number and its earliest eligible dispatch time, when scheduled.
`ListIngestionJobs` and MCP `ingestion_status` report the same checkpoint.
Use `GetEmbeddingStatus` for vector coverage and failures.
`SearchText` serves collection-scoped lexical hits, including hybrid
requests when hosted embeddings are unavailable. `FetchChunk` reads
the hit's exact byte range and verifies its fingerprint. Hosted
`SearchCollections` accepts one to eight collections in a tenant and
returns the aggregate coverage and scoped references. The pending literal
scan uses bounded range reads on the staged input object and reports the
exact scan coverage. See [the scan API](LOCAL_API.md#literal-scan-of-pending-text).

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
or begins a new pass when the previous pass ended. Dispatch retries keep
the same task name within a six-hour window. A later window uses a new
name so repair can recover tasks that exhausted their queue retries.
Configure the scheduler frequency against the measured queue depth.

Manual registration and indexing remain available with the private listener
when `cloud_tasks` is omitted. Operations accepted before dispatch hints were
introduced can be resubmitted with the same request ID and input to record
a hint. Live Google Cloud deployment verification remains open.

### Hosted snapshot reuse

The API resolves the current HEAD for each catalog or collection read.
It reuses a downloaded snapshot when the generation and digest match,
with at most 16 retained files and a 2 GiB default byte budget. Set
`hosted_snapshot_cache_bytes` to change the budget or set it to zero to
disable reuse. Oversized snapshots and files still in use can add to
active temporary storage. Hosted gRPC search and fetch hits report the
database generation that supplied their chunk ranges.

### Hosted embedding batches

Set the same server-level `embedding` configuration on the API and
worker. For a private worker, submit one bounded batch with the
following request. Repeat until `processed` is zero.

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
reconciler can recover a missing delivery. The scheduled embedding
profile endpoint scans one page of object keys after a profile change
and queues collections from their manifests. A persisted cursor resumes
discovery on the next invocation. Workers rebuild each collection in
bounded steps and ignore old run IDs. See the
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
A failed object deletion stays in the durable cleanup table. An obsolete
replacement is eligible once its successor has coverage for the active
profile. Text-only RTF ingestion discards the original after committing
its canonical revision. The [Cloud Run guide](CLOUD_RUN.md#schedule-dispatch-repair)
shows the scheduler configuration.

### Scheduled snapshot reclamation

The worker's snapshot sweep scans catalog and collection manifests. Each
invocation processes up to eight units and stores the remaining work and
pagination cursors. It acquires the unit's writer lease to mark snapshots
that are no longer current. Deletion is eligible after 15 minutes from
that observation, including for uploaded snapshots that never committed.
Busy units are revisited on the next pass. The sweep preserves the
published HEAD generation and leaves already downloaded reader files usable.

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
SQLite file. API processes verify the file against its recorded size and
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
as absent. A new submission must use a new UUID. Pending registration,
indexing, or embedding work remains available for recovery.
