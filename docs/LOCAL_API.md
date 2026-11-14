# Local gRPC API

The versioned [Protobuf definition](../proto/sanctum/v1/sanctum.proto) is the
wire contract. The `sanctum-proto` crate generates Rust clients and servers and
exports the serialized descriptor as `v1::FILE_DESCRIPTOR_SET`. A
[Python example](../examples/python/README.md) generates its client from the
same contract.

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

For retryable collection creation, pass a nonzero UUID as the
`CreateCollection` request ID or use `sanctum-cli create notes --request-id UUID`.
Within a tenant, replaying the same ID and collection returns the original
success receipt. Reusing the ID for a different collection returns a conflict.
The local catalog keeps the intent before initializing the collection database.
Startup resumes pending intents, and the collection becomes visible only after
initialization commits. Local success receipts become eligible for cleanup
after 45 days. Generate a fresh ID for each new operation.

Each `UploadText` stream starts with one header containing the tenant,
collection, and source name. It follows with data frames of up to 256 KiB and
an explicit end frame. Close the sending side of the stream after the end
frame. A successful response identifies the committed revision. Creating a
new source with an existing external namespace and ID returns `ALREADY_EXISTS`.
`UploadText` waits up to 30 seconds for lexical publication.
If its wait expires, the response includes the operation ID. The
accepted work can continue after the client disconnects.

Local upload acceptance records a BLAKE3 digest of the staged input.
The worker checks it before publishing UTF-8 or RTF. A storage digest
mismatch leaves the job retryable so it can resume after the original
bytes are restored.

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

Hosted jobs expose the durable preparation result in the optional
`preparation` field while lexical indexing is pending. A successful
preparation reports canonical bytes and chunks. `processed_bytes` then
equals the original input size. An invalid source reports its preparation
error and remains `REGISTERED` until the worker commits the terminal
result. `text_revision` becomes available with `TEXT_READY`.

New uploads can enter the durable queue while the collection database is
indexing another source. The local server serializes collection mutations
and replacement registration for each collection. Uploading bytes and
computing embeddings run outside that writer lock.

```sh
cargo run --locked -p sanctum-cli -- upload notes /path/to/notes.md --background
cargo run --locked -p sanctum-cli -- job notes OPERATION_ID
cargo run --locked -p sanctum-cli -- queue pause
cargo run --locked -p sanctum-cli -- queue resume
cargo run --locked -p sanctum-cli -- cancel notes OPERATION_ID
cargo run --locked -p sanctum-cli -- retry notes OPERATION_ID
```

Pause prevents new claims and lets active jobs finish. Cancellation can win
until the worker starts publication. A repeated cancellation returns the
same canceled job and retries any pending file cleanup. At startup, Sanctum
settles recorded cancellations even when the queue is paused. It also
retries cleanup of unpublished files from canceled or permanently failed
jobs during periodic maintenance. Retry applies to failures marked
`retryable` and preserves the operation and revision IDs.
Local maintenance removes completed, canceled, and permanently failed job
records after 45 days once their staged cleanup has finished. Pending and
retryable jobs remain available. A cursor
whose job has expired must restart from the newest page. Published source
bytes and search results follow the independent revision lifecycle.
`sanctum-cli jobs [COLLECTION]` lists local activity newest first. Pass
`--before OPERATION_ID` with the cursor shown for the next page.

The local RTF worker saves the canonical UTF-8 file and its digest before
indexing. After a restart or retry, the worker verifies the saved file and
can resume indexing without rerunning extraction. If the saved text is
missing or fails verification, it extracts the original again. An intact
extraction checkpoint also lets a pending literal scan work while the
restarted queue is paused.

`SearchText` defaults to lexical search. It matches the supplied words in a
single chunk using SQLite FTS5 tokenization. Results include byte offsets as
`[start_byte, end_byte)` into
the canonical UTF-8 representation. Pass the result's `ChunkRef` unchanged
to `FetchChunk` to retrieve that range.

Search hits identify the `canonical_utf8` representation and include
stored metadata fields referenced by the query filter. Entries preserve
their collection, source, or chunk level and JSON type. Missing keys
are absent. If a value would exceed the 4 KiB per-hit metadata budget,
the hit sets `metadata_omitted`. Fetch the complete target metadata
with `GetMetadata`.

Use `FetchContext` with the same reference and up to 16 KiB before
and after the cited chunk. Omitted gRPC budgets default to zero. The
server clips the window to the source and trims incomplete UTF-8 code
points at either outer boundary. The returned half-open range identifies
the context. The nested hit still describes the exact cited chunk and
its verified content hash.

## Fetch text by source range

Use `FetchSourceRange` with the tenant and collection scope, source and
revision IDs, and half-open byte offsets for direct access to a slice
of the source. The response includes the exact canonical
UTF-8 text, the range, and the database generation in hosted mode. The
range must span 1 to 65,536 bytes within the revision and start and end
on UTF-8 character boundaries. This lets a client fetch offsets reported
by `ScanPendingText` once the revision is published. Retained revisions
can be read until reclaimed. A reclaimed or deleted source revision
returns `stale_revision`.

```sh
cargo run --locked -p sanctum-cli -- fetch-range notes SOURCE_ID REVISION_ID 12 17
```

## gRPC error details

Application errors can include a `sanctum.v1.RpcError` message in
`google.rpc.Status.details`, carried by the `grpc-status-details-bin`
trailer. The gRPC status code and message remain available as usual.
Clients can unpack the detail by its type URL
`type.googleapis.com/sanctum.v1.RpcError`.
[gRPC richer error model](https://grpc.io/docs/guides/error/#richer-error-model).

- `reason` is a machine-readable reason such as `invalid_argument`,
  `stale_revision`, `invalid_source`, or `upload_pending`.
- `field` identifies a request field for validation errors when the
  server can attribute the error to one field.
- `operation_id` identifies relevant upload or job work when the UUID
  is known. An ID alone does not establish acceptance. Call
  `GetIngestionJob` in the original scope to check the durable record.
- `retryable` marks temporary failures or pending work. For
  `upload_pending`, poll the reported operation. If resending an upload,
  use the same request ID and input. For a failed job whose reason is
  `retryable`, use `RetryIngestionJob`.

Errors generated by the transport or an ingress can arrive without a
Sanctum detail. Clients should handle unrecognized reasons using the
gRPC code and message. The existing hosted
`sanctum-operation-id-bin` trailer continues to carry the raw UUID.

## Upload request IDs

Supply a nonzero `request_id` UUID on the upload header or use
`sanctum-cli upload --request-id UUID`. An identical retry in the same
collection returns the original operation and revision, including after a
server restart. Reuse with different content, metadata, or upload settings
returns `ALREADY_EXISTS`. When an operation expires, its request UUID
stays reserved. Use a fresh UUID for new work.

## Metadata on upload

`SubmitText` and `UploadText` accept `source_metadata` and `chunk_metadata`
on the upload header. Each entry contains `namespace`, `key`, and `value_json`.
Use unique namespace/key pairs within each target, up to 32 entries across
all targets, 64 KiB per JSON value, and 512 KiB of values in total. The queue
stores the entries at acceptance. The same transaction that makes
text searchable attaches them to the new revision. On replacement, readers
see the previous revision and its metadata until the replacement commits.
The new revision starts with the metadata supplied for that upload.

The CLI reads a JSON array from `--source-metadata FILE`. For example,
`[{"namespace":"editor","key":"tags","value":["sample",null]}]` attaches one
array value. Hosted replay with the same request UUID requires the same
metadata values and accepts a different entry order.

For chunk metadata, supply each zero-based ordinal once with its entries.
The CLI accepts `--chunk-metadata FILE` with a JSON array such as
`[{"ordinal":0,"entries":[{"namespace":"editor","key":"section","value":2}]}]`.
Ordinals refer to the canonical text and the chunk settings captured at
acceptance. An ordinal outside the computed revision fails the entire
publication and preserves the prior revision and metadata.

## Literal scan of pending text

`ScanPendingText` searches a single pending operation from the same tenant
and collection. Pass the operation ID returned by `SubmitText`. The first
stream event identifies the staged source and revision. Later events report
nonoverlapping case-sensitive UTF-8 matches as half-open byte ranges. They
also report bytes searched and bytes read, which can differ at a UTF-8
boundary or when the hit limit stops the search inside the current buffer.

The default byte budget is 1 MiB and the maximum is 32 MiB. The literal can
be 1 to 1024 UTF-8 bytes. The default hit limit is 10 and the maximum is 100.
The final event reports `COMPLETE`, `BYTE_LIMIT`, `HIT_LIMIT`,
`INVALID_TEXT`, or `EXTRACTION_PENDING` with zero bytes read when a pending
RTF upload has no scan-ready text. The first event reports the original
upload length in that case. RTF becomes scan-ready after the local worker
saves canonical UTF-8 to disk or the hosted preparation task saves it to
object storage, even while lexical indexing is pending. The reported
length and ranges then refer to those UTF-8 bytes. A match is provisional
until its source is published. Closing the gRPC stream stops the scan.
Published text uses the ordinary search path.

```sh
cargo run --locked -p sanctum-cli -- scan-pending notes OPERATION_ID '=value' --byte-limit 65536
```

## Chunk configuration

Set `chunking.window_bytes` and `chunking.overlap_bytes` in
`SANCTUM_CONFIG_JSON` to change the streaming UTF-8 window. The
defaults are 4096 and 400 bytes. Windows can be 256 to 16384 bytes.
Overlap can be zero through half the window. Boundaries follow UTF-8
code points and prefer nearby newlines.

Set `chunking.max_tokens` to add an estimated token budget. For example,
`"chunking":{"window_bytes":4096,"overlap_bytes":400,"max_tokens":1000}`
limits both byte length and estimated token density. The budget accepts
16–16,384 estimated tokens. The estimate counts runs of up to four ASCII
letters, digits, or horizontal whitespace characters and counts each
punctuation character, newline, or non-ASCII Unicode scalar separately.
For a shortened window, overlap is capped at half that window.

The estimate is shared across providers. Leave room for document prefixes
and model-specific tokenization when configuring it. An omitted token
budget uses byte-only chunking. Embedding providers still enforce their
own input limits.

Accepted uploads retain their chunk settings through retries and
worker restarts. Existing published revisions keep their byte ranges.
A replacement uses the settings effective at its acceptance. Revision
inspection reports the algorithm identifier and the configured budget
when applicable. `GetRuntimeInfo` reports the current defaults.

## Runtime information

`GetRuntimeInfo` reports the serving state and version of the API process,
its storage backend and role, and effective limits and capabilities.
For object storage, it reports whether Cloud Tasks or the standalone
object worker is configured.
It includes the configured embedding profile ID and dimensions when
enabled. This call describes process configuration without contacting
external dependencies. Use actual storage and retrieval requests to
check those dependencies. The response omits paths, endpoints, resource
names, credentials, and environment variable names. Hosted API and
worker listeners also expose `GET /healthz` for process health.

```sh
cargo run --locked -p sanctum-cli -- info
```

## Source inspection

`ListSources` returns the current published revision of each source in
one collection. Pass `next_after_source_id` as `after_source_id` to
continue in source UUID order. Pages default to 25 and allow up to 100
sources. A page reads current data at the time of that request.

`GetSource` takes a source UUID and an optional expected revision UUID.
It returns the current revision, including external reference, provenance,
canonical and original digests, size, chunk count, and retention policy.
A mismatched revision returns `FAILED_PRECONDITION`. Unpublished or
deleted sources are absent. Use `GetIngestionJob` for pending work and
`GetEmbeddingStatus` for processing coverage. Hosted results also
include the database generation used for the read.

`GetSource.processing` reports readiness, embedding coverage and errors,
and pending cleanup from the same snapshot as the current revision.
`ready` means lexical publication succeeded and the active profile has
every required embedding. A ready source can still have older data held
for readers. `retained_revisions` counts superseded revisions still in
SQLite. `pending_revision_cleanup` counts local revisions whose files
remain queued for deletion. `pending_object_cleanup` counts hosted objects
after their locators leave the index and before deletion is acknowledged.
The last field is absent if an older snapshot or legacy garbage records
prevent an exact per-source object count.

## Collection names

`ListCollections` returns each collection's stable scope, display name,
and creation time. Existing collections default to their ID as the
display name. `UpdateCollection` changes the display name in the tenant
catalog. It trims surrounding whitespace and accepts 1–256 UTF-8 bytes.
Existing chunk references and source paths keep their collection IDs.

```sh
cargo run --locked -p sanctum-cli -- rename-collection notes 'Field notes'
```

## Collection deletion

`DeleteCollection` removes a collection, its sources, and its queued
work from the selected tenant. Deleted IDs remain reserved. A local
restart or periodic maintenance resumes cleanup if the process stops
after committing the catalog tombstone. Previously opened local
database sessions keep their independent snapshots until expiry.

```sh
cargo run --locked -p sanctum-cli -- delete-collection notes
```

Pass the same nonzero UUID in `--request-id` to replay an ambiguous response.
A replay resumes pending cleanup and returns success once it finishes. Reusing
the ID for a different collection in the same tenant returns a conflict. Local
deletion receipts become eligible for cleanup after 45 days.
[Hosted collection lifecycle](STORAGE.md#hosted-collection-deletion).

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
  (constant 60). Within each collection, its lexical leg first requires all
  quoted terms in the same chunk. If neither lexical nor semantic ranking
  returns an eligible hit, it searches for any of those terms. When the provider or compatible vectors
  are unavailable, it reports lexical mode and a degradation reason.

All search modes keep the higher-ranked chunk when byte ranges from the
same source revision overlap. Touching ranges are eligible independently.
Search considers up to four times the requested limit per ranked list,
with a minimum of 32 candidates, to make room for lower-ranked,
nonoverlapping hits. A result can contain fewer hits than requested when
the candidate window is exhausted.

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

## OpenAI-compatible embeddings

Set `embedding.provider` to `openai` and give `base_url` as the API prefix.
Sanctum appends `/embeddings`. Set `api_key_env` to the name of the environment
variable holding the bearer key. Omit it for an unauthenticated loopback
service. The URL can use HTTPS or loopback HTTP.

```json
{
  "provider": "openai",
  "base_url": "https://api.openai.com/v1",
  "api_key_env": "OPENAI_API_KEY",
  "send_dimensions": true,
  "profile": {
    "model": "text-embedding-3-small",
    "revision": "1",
    "dimensions": 1536
  }
}
```

Set `send_dimensions` when the provider supports a requested output size.
It defaults to false and the returned size must still match the configured
profile. The provider and endpoint are included in the profile fingerprint.
For OpenRouter, use `https://openrouter.ai/api/v1`, the provider-qualified
model name, and the name of the variable holding the OpenRouter key.
[OpenAI embeddings](https://developers.openai.com/api/reference/resources/embeddings/methods/create), [OpenRouter embeddings](https://openrouter.ai/docs/api/api-reference/embeddings/submit-an-embedding-request).

The local Ollama compatibility endpoint uses `http://127.0.0.1:11434/v1`.
Its native adapter remains available under `provider: ollama`.
[Ollama compatibility](https://docs.ollama.com/api/openai-compatibility).

## Google Cloud embeddings

Set `embedding.provider` to `google` with an authorized Google Cloud project
and a Vertex AI location serving the selected model. The server uses
Application Default Credentials for the caller identity. The project must
allow Vertex AI prediction for that identity.

```json
{
  "provider": "google",
  "project": "your-sanctum-project",
  "location": "us-central1",
  "profile": {
    "model": "gemini-embedding-001",
    "revision": "1",
    "dimensions": 768
  }
}
```

This adapter uses the Vertex AI text embedding `predict` endpoint. It sends
one input per prediction, sets separate retrieval tasks for documents and
queries, and disables truncation. The profile includes the project,
location, and preprocessing settings. Credential discovery and refresh
happen when an embedding request needs them, so the lexical service can
start before credentials become available.
[Vertex AI text embeddings](https://docs.cloud.google.com/gemini-enterprise-agent-platform/reference/models/text-embeddings-api).

## Search across collections

`SearchCollections` accepts one tenant and up to eight distinct collection
IDs. It defaults to hybrid, reports aggregate coverage across the requested
collections, and embeds the query at most once. Each collection applies the
metadata filter and ranks its candidates before results are combined.
Combined scores use reciprocal rank, `1 / (60 + rank)`, with one-based
rank within the collection. Equal ranks sort by collection ID. Semantic
and hybrid searches use only the active profile for vector ranking.
Overlapping hits from the same revision are collapsed. Each reference
carries the collection it came from.

Hosted search uses one tenant catalog generation for collection membership
and collection metadata throughout the request. Each collection still
contributes results from its own published generation.

```sh
cargo run --locked -p sanctum-cli -- search notes quartz --also reference --mode hybrid
```

`GetEmbeddingStatus` reports total, embedded, failed, and retrying chunks,
the earliest retry time, and a sample error without invoking Ollama. Pass
only the scope for collection coverage, or add a source ID and its current
revision ID for source coverage. The retrying count is a subset of the failed
count. A missing profile ID indicates embedding is disabled.

Each `TextHit` also reports the cache state of its chunk for the active
profile. `DISABLED` means there is no configured profile. `PENDING` means
there is no vector or recorded failure, `READY` means the vector is stored,
`RETRYING` means a failure has a scheduled retry, and `FAILED` means the
failure has no automatic retry. A lexical hit can be `READY` during a
query-provider outage because the document vector is still stored.

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

Missing files are reported and kept in search by default. A previously
imported path also counts as missing if it becomes a directory or
symbolic link, or if a parent is replaced by a symlink or regular file.
Pass `--prune-missing` on a directory import to delete these sources
when selected by the current filters and supersede their pending imports.
A scan with file or traversal errors skips missing-file reconciliation.

## Watched folders

```sh
cargo run --locked -p sanctum-cli -- watch notes ./library --include '**/*.md'
cargo run --locked -p sanctum-cli -- watches notes
cargo run --locked -p sanctum-cli -- unwatch notes WATCH_ID
```

`WatchFolder` persists a canonical directory, filters, and reconciliation
interval in the local queue. Native filesystem notifications schedule a
debounced scan. Notification overflow requests reconciliation. The server
also scans when a watch is added, after restart, and at the configured
interval (60 seconds by default). Periodic scans cover filesystems where
notifications are unavailable and edits the backend missed. Every scan
uses content hashes. Pausing the local queue prevents new scans. Scans
already in progress may finish after pause or removal.

Missing files are reported by default. Use `--prune-missing` to delete
selected sources whose files disappeared or stopped being regular files.
An unavailable root or an incomplete scan preserves sources and records the
error. Removing a watch
leaves existing source data and accepted jobs available. `ListWatchedFolders`
reports the most recent scan outcome and returns an `after_id` cursor for
pagination.

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

`WatchChanges` streams the same pages in cursor order. It sends an initial
page, waits for new events when caught up, and sends an empty heartbeat about
every 15 seconds while idle. Save `next_cursor` only after processing its
page. Use that cursor with the same scope after reconnecting. A page can
contain up to 200 events and a cursor applies to exactly one collection.

`GetChangeCursor` returns the collection's latest cursor, or zero for an
empty log. To recover from expiry, capture this cursor before rebuilding
the integration's view, then replay changes after it. Clients should
handle changes idempotently because the rescan can already include some
of those updates. A cursor ahead of the log returns `INVALID_ARGUMENT`.

Hosted deployments expose the same change methods. Registration by
itself emits no source change. Events become visible when the worker
publishes a searchable revision. The hosted stream checks for a newer
snapshot about every two seconds when caught up. Reconnect with the
saved cursor when the API connection or its deployment deadline ends.

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

`MeasureStorageUsage` streams the same measurement. Each `progress`
event reports cumulative bytes and files or objects scanned so far.
Fast scans can return only a `complete` event. The final event carries the
same scope, totals, and timestamps as `GetStorageUsage`. An error ends the
stream without a final total. Closing the stream stops enumeration at the
next local checkpoint or cancels the pending object listing.

```sh
cargo run --locked -p sanctum-cli -- usage notes
cargo run --locked -p sanctum-cli -- --tenant default usage
cargo run --locked -p sanctum-cli -- usage --cluster
```

## URL imports

```sh
cargo run --locked -p sanctum-cli -- import-url notes https://example.org/notes.md
cargo run --locked -p sanctum-cli -- job notes OPERATION_ID
```

`ImportUrl` fetches a complete HTTP(S) response on the server, streams it to
local staging within `max_source_bytes`, and submits it to the same durable
queue as uploads. It accepts UTF-8 text, common textual media types, and RTF.
HTML is indexed as UTF-8 source. `--retain-original` keeps fetched RTF beside
its extracted text. Real HTTP failures surface with the upstream status.
Transport errors and upstream 429 or 5xx responses return `UNAVAILABLE`.
An oversized or failed download never enters the ingestion queue.

The normalized requested URL, excluding its fragment, is the source's
external ID in the `url` namespace. Re-importing the same URL compares the
body's BLAKE3 digest. An unchanged body reuses the current revision or
pending operation. A changed body keeps the source ID and queues a new
revision. The response records the final URL, ETag, Last-Modified, and
Content-Type observed during the fetch when present. The accepted
operation and published revision retain the provenance of the fetch that
produced them. Each import performs a complete GET. URLs are stored as
provenance, so keep secrets out of their query strings.

Set `url_import_origins` in `SANCTUM_CONFIG_JSON` to restrict requests to
exact scheme, host, and port combinations. A null value permits that origin
without credentials. For an HTTPS origin, a string names a server environment
variable holding a bearer token. An object with `username` and
`password_env` uses HTTP Basic authentication, where the named environment
variable holds the password or token. The server looks up a credential
separately for each destination, including URL redirects. An empty object
disables remote imports. An omitted field allows HTTP(S) destinations for
the trusted local operator. Use network egress controls as well when an
origin's DNS or service is outside the operator's control.

```json
{
  "version": 1,
  "storage_root": "./local-data",
  "url_import_origins": {
    "https://example.org": null,
    "https://docs.example.org": "SANCTUM_DOCS_TOKEN",
    "https://git.example.org": {
      "username": "sanctum-reader",
      "password_env": "SANCTUM_GIT_TOKEN"
    }
  }
}
```

## Git imports

```sh
cargo run --locked -p sanctum-cli -- import-git notes --path /path/to/repo \
  --ref refs/heads/main --include 'docs/**'
cargo run --locked -p sanctum-cli -- import-git notes \
  --https-url https://github.com/ArcaneArts/Sanctum.git --include 'docs/**'
```

`ImportGit` requires a `git` executable on the server's PATH. It resolves
one commit from a local repository or fetches one ref into a temporary bare
repository over HTTPS. The default ref is `HEAD`. Use a full branch or tag ref,
such as `refs/heads/main` or `refs/tags/v1`. A local import can also use a full
commit ID. Local paths are resolved on the server.

The adapter enumerates committed regular files and streams raw blobs to
staging. It skips worktree changes, symbolic links, and submodule entries.
It reads Git LFS pointer files as ordinary committed text. Fetching uses the
configured HTTPS origin policy and credential from [URL imports](#url-imports).
For private GitHub repositories, configure the GitHub username and use the
name of a variable containing its personal access token for `password_env`.
[GitHub HTTPS authentication](https://docs.github.com/en/get-started/git-basics/about-remote-repositories).

The adapter bypasses the operator's Git configuration and credential helpers,
disables HTTPS redirects, and creates no checkout or hook directory. For a
Git host with a private CA, set `GIT_SSL_CAINFO` in the Sanctum server process
to its CA certificate bundle. The fetch still verifies the certificate and
hostname. [Git TLS configuration](https://git-scm.com/docs/git-config#Documentation/git-config.txt-httpsslCAInfo).

The import stream reports each selected path, its commit and blob IDs, and
the ingestion operation when one was accepted. A repository and ref define
an independent source namespace, with each path as its external ID. An
unchanged blob preserves the revision. An edit preserves the source ID and
queues a replacement. The accepted operation and committed revision record
the repository, ref, commit, path, and blob that produced that revision.

An import reports selected paths missing from the new commit. Use
`--prune-missing` to delete them and supersede their pending operations. Pruning
runs after the tree is read successfully and all selected blobs are accepted.
Includes and excludes use the same globs as local directory imports.

The local HTTPS integration check uses a temporary Git repository and
self-signed TLS fixture. It requires Python 3, Git, and OpenSSL.

```sh
cargo build --locked -p sanctum-server -p sanctum-cli
python3 scripts/check-git-https.py
```

### Structured metadata queries

`QueryMetadata` lists or counts entities without a text search. Set `tenant`
and `level`. For `SOURCE` or `CHUNK`, also set `collection`. A `COLLECTION`
query reads the tenant catalog. Queries include only current, non-deleted
source revisions and their chunks. Fields and filters can refer to an
entity's own level or an ancestor. For example, group chunks by a
source-level tag and filter them by a collection-level flag.

Use the same namespaced keys and literal path segments as search filters.
`list.sort_by` returns the selected JSON value with each target. The
ascending order is null, booleans, numbers, strings, arrays, and objects.
Numbers use numeric comparison and arrays and objects compare
lexicographically by their elements or sorted keys and values. Missing
values sort last in both directions. Equal values break ties by collection,
source, and chunk ordinal.

`count` returns the total number of matches. Set `count.group_by` for
counts per JSON value, ordered by descending count with ties in ascending
value order. An absent `value_json` identifies a missing value. The JSON
string `null` identifies an explicitly stored null. Numerically equivalent
values such as `1` and `1.0` share a group.

Pages default to 16 rows with a maximum of 32. Pass `next_offset` with the
same request to get the next page. Each page reads current committed data.
An integration that needs repeatable pages across concurrent edits can
query an exported database snapshot.

### Named metadata indexes

Use `CreateMetadataIndex`, `ListMetadataIndexes`, and `DropMetadataIndex`
to manage optional SQLite indexes. Each request selects a database with
`DatabaseTarget`. Omit `collection` for a tenant catalog index at
`COLLECTION` level, or set it for a `SOURCE` or `CHUNK` index in that
collection. For example, send this request to `CreateMetadataIndex`:

```json
{
  "target": { "tenant": "default", "collection": "notes" },
  "index": {
    "name": "review-status",
    "level": "METADATA_LEVEL_SOURCE",
    "namespace": "review",
    "key": "status"
  }
}
```

An index lets `QueryMetadata` and source or chunk search filters look up
top-level `EXISTS` and `EQUAL` matches by key. It narrows the metadata
rows considered and keeps the same JSON comparison rules. An index on a
sparse, frequently queried field is a good candidate. Nested paths and
other operators retain their existing query behavior. Every metadata key
can be stored and queried without creating an index.

Each database supports up to 32 named indexes. Names follow the tenant
and collection ID rules. Each level, namespace, and key combination can
have one index. Repeating the same create succeeds. A conflicting name
or field returns `ALREADY_EXISTS`, and dropping an unknown index returns
`NOT_FOUND`. Creation and removal commit through the database's normal
write path. Hosted changes publish a new generation, and an already
opened database session keeps its pinned snapshot.

## Server diagnostics

The server writes logs to stderr. Cloud Run roles emit one JSON object per
line. To collect the same events locally, start the server with:

```sh
SANCTUM_LOG_FORMAT=json \
RUST_LOG=info,sanctum_server=debug,sanctum_service=debug,sanctum_db=debug \
cargo run --locked -p sanctum-server 2>sanctum.jsonl
```

Supply your usual `SANCTUM_CONFIG_JSON` in the environment. Each event
places measurements in `fields`. Filter by `fields.event` to extract
timings and counts for an existing log pipeline:

| Event | Measurements |
| --- | --- |
| `local_job_claimed` | Queue age since acceptance and attempt number |
| `ingestion_stage` | Scoped operation age, stage duration, input bytes, and outcome |
| `upload_task_dispatched` | Tenant, collection, operation ID, stage, dispatch window, and manual retry number after a successful task handoff |
| `dispatch_reconcile` | Dispatch shard, number of successful task handoffs and failures, and whether the scan has another page |
| `embedding_request` | Profile ID, document/query purpose, input count, elapsed time, and outcome class |
| `vector_reuse` | Published chunk count and count covered by an existing vector at publication |
| `search` | Served search mode, elapsed time, hits, embedding coverage, and degradation reason when present |
| `hosted_input_transfer` | Direction, staged source bytes, and duration |
| `snapshot_publish`, `snapshot_download` | Snapshot size, actual transferred database bytes, and duration |
| `snapshot_cache` | Generation and whether it was already materialized at lookup |
| `collection_sqlite_handle` | Generation, whether the read reused an idle SQLite handle, and time to open or select that handle |
| `cleanup_pass` | Reclaimed entries, failures and further work when available |

Use `sanctum_service=trace` to include `source_range_read` events for
hosted excerpt reads. A hosted search allows up to four independent
range reads in flight and returns verified excerpts in ranked order.
`requested_bytes` sums the individual excerpt sizes,
`bytes` measures the combined transfer, and `excerpts` counts the excerpts
covered by that read. Measurements include work completed at the event's boundary,
so an upload that later fails can still produce a transfer event. Scoped
events use IDs and counters. Provider events omit response bodies and
credentials, and search events omit query and source text.

## Read-only SQL sessions and snapshots

`OpenDatabaseSession` accepts a tenant and optional collection. With only
the tenant, it snapshots `root.db`. With a collection, it snapshots that
collection's `meta.db`. The response includes a scoped generation ID,
expiry, exact file size, schema version, SQLite version, schema objects,
and required SQLite features. Collection databases use FTS5 with
contentless-delete support. Open a downloaded copy with SQLite 3.43 or later
and FTS5 enabled. [SQLite FTS5 reference](https://sqlite.org/fts5.html#external_content_and_contentless_tables).

The filesystem-backed server uses SQLite's online backup API. The hosted
server uses an immutable published generation. Later writes leave both
types of sessions pinned to their original bytes. `QueryDatabase` accepts a single SELECT
with typed positional parameters. It supports common scalar, aggregate,
and JSON functions. It restricts statements to the selected database and
rejects writes, attachment, PRAGMA, and extension loading. Result pages
contain at most 100 rows and roughly 1 MiB of values. Set a stable
`ORDER BY` and pass `next_offset` with the same statement and parameters
for subsequent pages. SQL execution has a five-second budget and responds
to gRPC cancellation.

`DownloadDatabase` streams the same SQLite file in frames of at most 256 KiB.
Keep the download once the RPC succeeds and its byte count matches the
advertised size. If server shutdown interrupts the stream, it returns
`UNAVAILABLE`. If the snapshot ends before the advertised size, it returns
`DATA_LOSS`. Discard any partial copy and retry from the beginning. A valid
hosted session can be retried on another instance. Each session expires after
five minutes and is tied to its original scope. Close it early
with `CloseDatabaseSession`. On a filesystem-backed server, up to four snapshot
copies may exist in the process, including copies held by active queries or
downloads. A hosted session can move between API processes. Its response
includes `snapshot_generation`, and its scoped session handle refers to that
generation until expiry or close. A stream already materializing or reading its
snapshot can finish after its handle expires or closes.
