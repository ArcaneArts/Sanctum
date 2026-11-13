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

In another terminal, create a collection, upload a UTF-8 or RTF file, and search:

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

A source name ending in `.rtf` selects streaming RTF text extraction. Uploads
and directory imports use text-only retention by default. Set
`retain_original` in the request or pass `--retain-original` to keep the
uploaded RTF alongside the searchable text.

```sh
cargo run --locked -p sanctum-cli -- upload notes report.rtf --retain-original
```

An RTF revision reports the original and canonical BLAKE3 digests, the
document's declared code page, and the extractor version. `TextRevision.bytes`
and search offsets refer to canonical UTF-8. Job progress counts input bytes.
For UTF-8 sources, the original and canonical bytes share a single file.

The extractor indexes text and common paragraph, line, and table separators.
It skips formatting destinations, image and object data, and field
instructions. It supports Unicode escapes and common ANSI code pages.
Malformed input and unsupported declared code pages fail the ingestion job.

To replace a source, submit an upload with a `Replacement` target. Provide
either its Sanctum source ID or its external namespace and ID. An optional
expected revision rejects updates based on a stale view. Competing replacements
receive a durable `update_sequence` and only the newest registered update can
publish. The original remains searchable until its replacement is indexed.

```sh
cargo run --locked -p sanctum-cli -- upload notes /path/to/notes.md \
  --replace-source SOURCE_ID --expected-revision REVISION_ID
cargo run --locked -p sanctum-cli -- delete notes SOURCE_ID --expected-revision REVISION_ID
```

For an external target, pass `--replace-external --namespace NAME --external-id ID`.
Deletion immediately removes the source from search and prevents queued
replacements from publishing. The local lexical service retains replaced
bytes for five minutes before reclaiming them. Fetching a reclaimed revision
returns `FAILED_PRECONDITION` while the source or its deletion receipt is known.

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

## Local file and directory imports

`ImportLocal` accepts a path on the local server's filesystem and streams
one event per selected or missing file. The CLI resolves relative arguments
to absolute paths before sending them. For example, import Markdown files
and skip the drafts directory.

```sh
cargo run --locked -p sanctum-cli -- import notes ./library \
  --include '**/*.md' --exclude drafts
```

Include and exclude globs match paths relative to the import root. Without
include patterns, the importer selects RTF and common UTF-8 text, code, and
configuration files. Directory traversal skips symbolic links, `.git`,
`node_modules`, and the managed storage directory.

A file's import identity combines the canonical import root with its
relative path. Repeating an import reuses a pending operation or skips an
unchanged file using its content digest. An edit keeps the source ID and
queues a replacement. Each queued event includes an operation ID for
`sanctum-cli job` or `GetIngestionJob`.

Missing files are reported and kept in search by default. Pass
`--prune-missing` on a directory import to delete missing sources selected
by the current filters and supersede their pending imports. A scan with
file or traversal errors skips missing-file reconciliation.
