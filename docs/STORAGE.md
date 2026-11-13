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
