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
[configuration example](#hosted-catalog-configuration).

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

## Hosted catalog configuration

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
service. The hosted catalog listens on gRPC and supports collection
creation, listing, and scoped storage usage. Hosted ingestion, retrieval,
and MCP are still in progress. Keep this listener within the controlling
application's trusted network.

The hosted create operation initializes the collection database before
activating its catalog record. If a request is interrupted between those
steps, repeat it to activate the existing database. Pass a UUID through
`CreateCollectionRequest.request_id` or `sanctum-cli create --request-id`
to replay a successful activation after an ambiguous response. The
request ID applies within the tenant catalog. Hosted lists default to
50 items. Pass `next_after` as `after` to continue the same tenant's list.
