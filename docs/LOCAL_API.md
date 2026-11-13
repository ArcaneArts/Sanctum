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
`retryable` and preserves the operation and revision IDs. Local maintenance
removes completed, canceled, and permanently failed job records 45 days
after they finish. Pending and retryable jobs remain available. A cursor
whose job has expired must restart from the newest page. Published source
bytes and search results follow the independent revision lifecycle.
`sanctum-cli jobs [COLLECTION]` lists local activity newest first. Pass
`--before OPERATION_ID` with the cursor shown for the next page.

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
`INVALID_TEXT`, or `EXTRACTION_PENDING` for an RTF upload awaiting canonical
text. A match is provisional until its source is published. Closing the
gRPC stream stops the scan. Published text uses the ordinary search path.

```sh
cargo run --locked -p sanctum-cli -- scan-pending notes OPERATION_ID '=value' --byte-limit 65536
```

## Chunk configuration

Set `chunking.window_bytes` and `chunking.overlap_bytes` in
`SANCTUM_CONFIG_JSON` to change the streaming UTF-8 window. The
defaults are 4096 and 400 bytes. Windows can be 256 to 16384 bytes.
Overlap can be zero through half the window. Boundaries follow UTF-8
code points and prefer nearby newlines.

Accepted uploads retain their chunk settings through retries and
worker restarts. Existing published revisions keep their byte ranges.
A replacement uses the settings effective at its acceptance. Revision
inspection reports the algorithm identifier with custom byte settings
when applicable. `GetRuntimeInfo` reports the current defaults.

## Runtime information

`GetRuntimeInfo` reports the serving state and version of the API process,
its storage backend and role, and effective limits and capabilities.
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

For hosted deletion, pass `--request-id` to replay an ambiguous response.
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

Missing files are reported and kept in search by default. Pass
`--prune-missing` on a directory import to delete missing sources selected
by the current filters and supersede their pending imports. A scan with
file or traversal errors skips missing-file reconciliation.

## Watched folders

```sh
cargo run --locked -p sanctum-cli -- watch notes ./library --include '**/*.md'
cargo run --locked -p sanctum-cli -- watches notes
cargo run --locked -p sanctum-cli -- unwatch notes WATCH_ID
```

`WatchFolder` persists a canonical directory, filters, and reconciliation
interval in the local queue. The server scans when a watch is added, after
restart, and at the configured interval (60 seconds by default). Periodic
reconciliation coalesces edits between scans and uses content hashes.
Pausing the local queue prevents new scans. Scans already in progress may
finish after pause or removal.

Missing files are reported by default. Use `--prune-missing` to delete
selected sources whose files disappeared. An unavailable root or an
incomplete scan preserves sources and records the error. Removing a watch
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
variable holding a bearer token. The server looks up a credential separately
for each destination, including redirects. An empty object disables URL
imports. An omitted field allows HTTP(S) destinations for the trusted local
operator. Use network egress controls as well when an origin's DNS or service
is outside the operator's control.

```json
{
  "version": 1,
  "storage_root": "./local-data",
  "url_import_origins": {
    "https://example.org": null,
    "https://docs.example.org": "SANCTUM_DOCS_TOKEN"
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
Git's configuration and credential helpers from the operator's home directory
are bypassed, HTTPS redirects are disabled, and the adapter creates no checkout
or hook directory.

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
Use the advertised byte count to detect interrupted downloads. Each session
expires after five minutes and is tied to its original scope. Close it early
with `CloseDatabaseSession`. On a filesystem-backed server, up to four snapshot
copies may exist in the process, including copies held by active queries or
downloads. A hosted session can move between API processes. Its response
includes `snapshot_generation`, and its scoped session handle refers to that
generation until expiry or close. A stream already materializing or reading its
snapshot can finish after its handle expires or closes.
