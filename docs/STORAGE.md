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

The server currently uses its filesystem service. S3 can be used through the
storage crate while database publication and server integration are in progress.

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
tests. It is available as a Rust library while the hosted service
wires its application commands to the executor.
