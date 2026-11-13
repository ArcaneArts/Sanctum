# Local gRPC API

The versioned [Protobuf definition](../proto/sanctum/v1/sanctum.proto) is the
wire contract. The `sanctum-proto` crate generates Rust clients and servers and
exports the serialized descriptor as `v1::FILE_DESCRIPTOR_SET`.

Run the server on the default loopback address:

```sh
SANCTUM_CONFIG_JSON='{"version":1,"storage_root":"./local-data"}' \
  cargo run --locked -p sanctum-server
```

`grpc_listen` defaults to `127.0.0.1:47550`. `max_source_bytes` defaults to
1 GiB. This local service trusts callers. Keep it on loopback for local use.
`local_worker_concurrency` defaults to one and accepts 1 to 32 workers.

In another terminal, create a collection, upload a UTF-8 file, and search:

```sh
cargo run --locked -p sanctum-cli -- create notes
cargo run --locked -p sanctum-cli -- upload notes /path/to/notes.md
cargo run --locked -p sanctum-cli -- search notes 'search terms'
cargo run --locked -p sanctum-cli -- fetch notes SOURCE_ID REVISION_ID ORDINAL
```

The CLI defaults to the `default` tenant. Set `--tenant` and `--endpoint` before
the subcommand to change the scope or address. Optional `--namespace` and
`--external-id` on uploads provide an opaque reference returned in matching
results.

Each `UploadText` stream starts with one header containing the tenant,
collection, and source name. It follows with data frames of up to 256 KiB and
an explicit end frame. Close the sending side of the stream after the end
frame. A successful response identifies the committed revision. In this
initial version, retrying an upload with the same external namespace and ID
returns `ALREADY_EXISTS`. `UploadText` waits up to 30 seconds for lexical publication.
If its wait expires, the response message contains the operation ID. The
accepted work can continue after the client disconnects.

Use `SubmitText` for background ingestion. It returns a durable operation
ID and source and revision IDs once the input and queue record are stored.
Poll `GetIngestionJob` in the same tenant and collection to see processed
bytes, attempts, failure details, or the committed lexical revision. The
local queue persists through process restarts.

```sh
cargo run --locked -p sanctum-cli -- upload notes /path/to/notes.md --background
cargo run --locked -p sanctum-cli -- job notes OPERATION_ID
cargo run --locked -p sanctum-cli -- queue pause
cargo run --locked -p sanctum-cli -- queue resume
cargo run --locked -p sanctum-cli -- cancel notes OPERATION_ID
cargo run --locked -p sanctum-cli -- retry notes OPERATION_ID
```

Pause prevents new claims and lets active jobs finish. Cancellation can win
until the worker starts publication. Retry applies to failures marked
`retryable` and preserves the operation and revision IDs.

`SearchText` matches the supplied words in a single chunk, using SQLite FTS5
tokenization. Results include byte offsets as `[start_byte, end_byte)` into
the canonical UTF-8 representation. Pass the result's `ChunkRef` unchanged
to `FetchChunk` to retrieve that range.
