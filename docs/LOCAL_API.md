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
See [Local MCP](MCP.md) for the server's Streamable HTTP and stdio interfaces.

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

`ListIngestionJobs` accepts a tenant, optional collection, and a page limit
from 1 to 100 (default 25). Results sort by acceptance time, newest first,
using the operation ID to break ties. Reuse `next_before_operation_id` as
`before_operation_id` with the same scope to read the next page. A fresh
first page shows newly accepted operations.

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
replacements from publishing. For replacement, a five-minute grace period
starts after the new revision is ready for lexical search and all of its
chunks have embeddings for the configured profile, if enabled. Fetching a reclaimed revision
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

`SearchText` defaults to lexical search. It matches the supplied words in a
single chunk using SQLite FTS5 tokenization. Results include byte offsets as
`[start_byte, end_byte)` into
the canonical UTF-8 representation. Pass the result's `ChunkRef` unchanged
to `FetchChunk` to retrieve that range.

## Ollama and search modes

Add an `embedding` block to the JSON configuration to enable background
embedding for every collection on the server. Run the model in Ollama before
requesting semantic search. For example, this configuration uses a local
installation of `embeddinggemma-2:740m` with 768 dimensions.

```json
{
  "version": 1,
  "storage_root": "./local-data",
  "embedding": {
    "provider": "ollama",
    "endpoint": "http://127.0.0.1:11434",
    "timeout_seconds": 30,
    "profile": {
      "model": "embeddinggemma-2:740m",
      "revision": "eaff2253e2ba",
      "dimensions": 768
    }
  }
}
```

Set `revision` to identify the exact model installed on your Ollama instance
and change it when the contents of the model tag change. The example uses
the digest abbreviation reported by `ollama list` on the development machine.
Optional `document_prefix` and `query_prefix` apply model-specific prefixes
and are part of profile identity. Restart the server to apply profile changes.

```sh
cargo run --locked -p sanctum-cli -- search notes 'how crystals form' --mode semantic
cargo run --locked -p sanctum-cli -- search notes 'quartz formation' --mode hybrid
```

- `lexical` uses FTS5 independently of the provider.
- `semantic` orders compatible chunks by exact cosine similarity. It
  returns `UNAVAILABLE` if query embedding fails or the collection has no
  compatible vectors.
- `hybrid` combines lexical and semantic ranks with reciprocal rank fusion
  (constant 60). It falls back to lexical when the provider or compatible
  vectors are unavailable. Semantic and hybrid modes collapse overlapping
  byte ranges from the same source revision within the retrieved candidates.

The response includes the mode used, optional scores, and coverage for the
active profile across current chunks in the collection, before any metadata
filter. A partial coverage message indicates that semantic ranking covered
only the chunks that already have compatible vectors. Failed counts include
chunks awaiting retry and permanently rejected inputs. Ingestion and lexical
publication proceed while embedding is pending.

Vectors are reused within a collection when both the chunk's bytes and
profile match, even when its offset or revision changes. After a profile
change, background work resumes from persisted results for the new profile.
Ollama requests disable truncation. A rejected input is recorded separately
so other chunks can proceed. Transient failures back off and retry. Correct
a permanently rejected input with a new source revision or use a corrected
embedding profile.

`SearchCollections` accepts one tenant and up to eight distinct collection
IDs. It defaults to hybrid, reports aggregate coverage across the requested
collections, and embeds the query at most once. Each collection applies the
metadata filter and ranks its candidates before results are combined.
Combined scores use reciprocal rank, `1 / (60 + rank)`, with one-based
rank within the collection. Equal ranks sort by collection ID. Semantic
and hybrid searches use only the active profile for vector ranking.
Overlapping hits from the same revision are collapsed. Each reference
carries the collection it came from.

```sh
cargo run --locked -p sanctum-cli -- search notes quartz --also reference --mode hybrid
```

`GetEmbeddingStatus` reports total, embedded, failed, and retrying chunks,
the earliest retry time, and a sample error without invoking Ollama. Pass
only the scope for collection coverage, or add a source ID and its current
revision ID for source coverage. The retrying count is a subset of the failed
count. A missing profile ID indicates embedding is disabled.

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

## Metadata and changes

`GetMetadata` and `MutateMetadata` accept a scope and an optional source
target. A source target requires the current revision ID. Supply a chunk
ordinal to target that chunk, including ordinal zero. With scope alone, the
request targets collection metadata.

Each value has a namespace and key, with a UTF-8 JSON value. A mutation
batch applies in order and commits atomically. Set replaces the value,
merge patch recursively merges objects and removes properties whose patch
value is null, and remove deletes the entire entry. Set can store JSON
null as a value. Batches allow up to 32 edits and values up to 64 KiB.
Each target allows up to 64 entries and 512 KiB of serialized values.

Source and chunk metadata starts empty on replacement. Collection metadata
persists across source changes. Requests for a superseded revision receive
`FAILED_PRECONDITION`.

Poll `ListChanges` with the last processed cursor for that collection. It
returns source creation, replacement, deletion, and source or chunk metadata
changes. Replacement records a `source_replaced` event followed by a
`metadata_reset` event in the same commit. The change log keeps the most
recent 10,000 events. An expired cursor returns `FAILED_PRECONDITION` and
requires a rescan.

`SearchText.metadata_filter` accepts a tree of `test`, `all`, `any`, and
`not` expressions. A test selects a namespace and key at the collection,
source, or chunk level. An optional path selects object properties and
zero-based array indices. Property names are literal, including dots and
quotes. A source or chunk test reads metadata on the candidate's revision.

Equality compares JSON values by type and structure, with numeric comparison
across integer and floating-point values. Ordered comparisons accept numbers
with numbers or strings with strings. Strings sort lexically. `contains`
tests for an element in an array. `exists` includes JSON null and `missing`
matches an absent entry or path. Use the JSON string `null` as the operand
to match an explicit null. Existence tests omit the operand.

SQLite applies the predicates to lexical and semantic candidates before returning the
requested number of ranked hits. The filter allows 64 expression nodes,
eight expression levels, and 16 segments per path.

## Storage usage

`GetStorageUsage` walks the local storage directory for the selected
collection, tenant, or cluster. It returns regular-file counts and byte
lengths, including databases, staging files, and retained revisions still
on disk. The cluster total also includes shared files inside the storage
root. Symbolic links are skipped. Each response includes its scope and
start and finish timestamps in Unix milliseconds. Concurrent writes may
change files during a measurement.

```sh
cargo run --locked -p sanctum-cli -- usage notes
cargo run --locked -p sanctum-cli -- --tenant default usage
cargo run --locked -p sanctum-cli -- usage --cluster
```
