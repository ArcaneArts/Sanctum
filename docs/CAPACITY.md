# Capacity measurements

## 200 MiB text ingestion

The benchmark generates exactly 209,715,200 bytes of deterministic UTF-8
text from numbered synthetic observations. It streams the source through
the release CLI and gRPC, polls for lexical publication, searches for a
unique marker near the end, and compares the MCP fetch byte for byte with
the original file. It samples server process RSS every 200 ms from upload
through publication. Collection usage comes from the storage API and
includes source and index files or objects. The benchmark runs without
embeddings to isolate upload, chunking, and lexical indexing.

```sh
cargo build --release --locked -p sanctum-server -p sanctum-cli
python3 scripts/benchmark-ingestion.py
```

On macOS or Linux, pass `--mib` to change the input size. To run against
MinIO, supply a [standalone S3 configuration](STORAGE.md#hosted-server-configuration)
with a dedicated empty prefix in `SANCTUM_CONFIG_JSON` and run
`python3 scripts/benchmark-ingestion.py --hosted`. The script leaves hosted
objects in that prefix for inspection.

| Backend | Upload to acceptance | Acceptance to text ready | Total | Sampled peak server RSS | Warm MCP fetch | Stored collection bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Local filesystem | 0.279 s | 5.739 s | 6.018 s | 29,392,896 | 1.798 ms | 269,590,528 |
| MinIO | 2.950 s | 6.180 s | 9.130 s | 46,415,872 | 12.108 ms | 269,886,586 |

Each backend was measured once on the Apple M4 Max setup below. Both
returned chunk 58,199 with the same 2,631-byte range from offset
209,712,569 through EOF. The filesystem had 29 RSS samples and MinIO
had 45. RSS excludes the client, object store, and on-disk files.
The warm fetch followed a search in the same process. The synthetic
vocabulary is small, so index sizes and timings depend on the workload.
Vector work, GB-scale collections, and Cloud Run memory remain separate
measurements.

### Estimated token budget

A second release filesystem run used the same 200 MiB fixture with
`chunking.max_tokens` set to 1,000 alongside the 4,096-byte window and
400-byte overlap. It reached text readiness in 7.061 s with a sampled
peak RSS of 29,114,368 bytes. It emitted 97,949 chunks and stored
287,895,552 bytes for the collection. The final MCP excerpt matched
all 1,404 bytes at the end of the input. The two filesystem timings
are individual runs, and both used lexical indexing without embeddings.
[Chunk settings](LOCAL_API.md#chunk-configuration).

### 200 MiB RTF extraction

Run the same benchmark with `--format rtf` to upload 209,715,200 bytes of
deterministic RTF. The fixture contains a Windows-1252 hex escape,
paragraph controls, a Unicode surrogate pair, and a hidden destination.
The script writes the expected UTF-8 to a separate fixture file. It checks
that the hidden marker is absent from lexical results and compares the
search and fetch excerpts with the exact expected bytes at the returned
canonical offsets.

```sh
python3 scripts/benchmark-ingestion.py --format rtf
python3 scripts/benchmark-ingestion.py --format rtf --hosted
```

Use the standalone MinIO configuration described above for the hosted
command. Both runs extracted 205,681,584 bytes of UTF-8. MCP returned chunk
57,080 with the same 991-byte range from offset 205,680,593 through EOF.

| Backend | Upload to acceptance | Acceptance to text ready | Total | Sampled peak server RSS | Warm MCP fetch | Stored collection bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Local filesystem | 0.224 s | 6.641 s | 6.865 s | 29,294,592 | 1.313 ms | 263,537,584 |
| MinIO | 2.992 s | 10.723 s | 13.715 s | 46,006,272 | 3.323 ms | 263,858,234 |

These are sequential release runs on the Apple M4 Max setup below. Peak
server RSS uses 33 filesystem samples and 67 MinIO samples. It excludes
the client, MinIO, and files on disk. Original retention and embeddings
were disabled. The totals include the storage observed immediately after
publication and retrieval.

### Local uploads with acceptance digests

The release build with acceptance digests ran both 200 MiB filesystem
benchmarks sequentially on Apple M4 Max with request IDs and embeddings
disabled. MCP returned the same exact end-of-file ranges described above.
Original retention was disabled for the RTF run.

| Format | Upload to acceptance | Acceptance to text ready | Total | Sampled peak server RSS |
| --- | ---: | ---: | ---: | ---: |
| UTF-8 | 0.325 s | 4.662 s | 4.987 s | 30,261,248 bytes (25 samples) |
| RTF | 0.336 s | 6.977 s | 7.313 s | 30,916,608 bytes (36 samples) |

## SQLite snapshot transfer and publication

The MinIO benchmark publishes synthetic SQLite databases with about 1 GiB
and 2 GiB of page data using the production snapshot writer. For each
size it measures three cold materializations, three warm resolutions,
SQLite open plus a point query, and a single-row update that republishes
the database. Cold materialization includes the manifest read, object
download, integrity verification, and local file write. Warm resolution
includes a fresh manifest request and reuses the verified file.

Use a dedicated prefix in a test bucket. The command leaves two published
snapshots per size in the supplied prefix for inspection.

```sh
export SANCTUM_TEST_S3_ENDPOINT=http://127.0.0.1:9000
export SANCTUM_TEST_S3_BUCKET=sanctum-test
export SANCTUM_BENCHMARK_PREFIX="capacity-$(uuidgen | tr '[:upper:]' '[:lower:]')"
cargo test --release --locked -p sanctum-db --test snapshot_capacity \
  -- --ignored --nocapture
```

The benchmark accepts comma-separated sizes in MiB through
`SANCTUM_BENCHMARK_MIB`. Each cold sample uses a fresh in-process snapshot
cache. The storage host and OS page caches remain as observed. The
synthetic payload exercises SQLite file transfer and publication. Search
quality, vector loading, provider reuse, and multi-tenant contention
require their own workload measurements.

### Test environment

The test ran with a release build on an Apple M4 Max with 128 GiB of
unified memory. The MinIO development image ran in Docker Desktop and
was reached through a loopback port. The test uses one active writer
and one reader at a time. These timings describe that local setup.

### Results

| Published snapshot | Initial publication | Cold materialization | Warm resolution | SQLite open and point query | One-row update |
| --- | ---: | --- | --- | --- | ---: |
| 1,075,167,232 bytes | 33.57 s | 17.38, 8.57, 5.76 s | 2.40, 1.47, 1.25 ms | 185, 223, 174 µs | 22.37 s |
| 2,150,313,984 bytes | 34.37 s | 10.20, 10.98, 10.33 s | 1.90, 1.53, 2.16 ms | 216, 228, 236 µs | 51.72 s |

The cold samples straddled the 10-second target at 1 GiB and exceeded
it at 2 GiB. The 2 GiB database plus SQLite page overhead exceeded the
default 2 GiB snapshot cache budget by 2,830,336 bytes. The benchmark
configured enough space to retain it for the warm samples.

Each one-row update uploaded a full replacement snapshot of the same
byte length. Peak benchmark-process RSS was 40,583,168 bytes over the
205.66-second run. RSS excludes the local SQLite files and MinIO
process. Cloud Run accounts for its writable filesystem in instance
memory. [Cloud Run container filesystem](https://docs.cloud.google.com/run/docs/container-contract#file_system_access).

The one-row result establishes the cost of full snapshot publication at
the target size. The multi-GiB source workload below measures real indexing and publication.
Embedding batches still need separate measurements.

### Two simultaneous cold snapshot reads

Run the concurrent benchmark with the same MinIO environment and a fresh
prefix. `SANCTUM_CONCURRENT_SNAPSHOT_MIB` sets the size of each database
and defaults to 1024. This command leaves one snapshot per collection in
the supplied prefix.

```sh
cargo test --release --locked -p sanctum-db --test snapshot_capacity \
  minio_concurrent_snapshot_capacity -- --ignored --exact --nocapture
```

Both collections publish a synthetic SQLite payload with a distinct
marker. Three samples start with an empty snapshot cache and issue both
reads concurrently. Each sample then repeats both reads against the same
cache and checks that they share the verified files from the cold reads.
The test times opening SQLite and reading the marker separately.

The last pass sets a budget one byte below the combined database sizes.
It keeps a reader open for each collection, reloads the evicted snapshot,
and measures the temporary file lengths. The held copies can consume
space beyond the configured retention budget. The synthetic data measures
snapshot I/O. Query scoring and source range retrieval are measured below.

| Measurement | Two 1 GiB snapshots | Two 2 GiB snapshots |
| --- | ---: | ---: |
| Database length per collection | 1,075,167,232 bytes | 2,150,313,984 bytes |
| Cache budget for both | 2,150,334,464 bytes | 4,300,627,968 bytes |
| Cold resolution, six samples | 6.09–6.71 s | 12.85–20.04 s |
| Warm resolution, six samples | 1.12–1.82 ms | 1.26–1.96 ms |
| SQLite open and marker lookup | 182–257 µs | 182–264 µs |
| Reload after eviction | 3.93 s | 7.11 s |
| Three materialized files held by readers | 3,225,501,696 bytes | 6,450,941,952 bytes |
| Peak sampled benchmark process RSS | 37,961,728 bytes | 40,042,496 bytes |

Both runs used the Apple M4 Max and loopback MinIO. RSS was sampled every
200 ms and excludes temporary files and MinIO. At either size the default
2 GiB cache budget was too small to retain both databases. The two 2 GiB
snapshots exceeded the ten-second target on every cold sample. The same
cache budget cannot serve as a Cloud Run memory limit, where the retained
copies and in-flight files also use instance memory. Live Cloud Run sizing
and ingress timing remain open.

### Concurrent gRPC search on 1 GiB FTS snapshots

The [deployed snapshot benchmark](CLOUD_RUN.md#measure-gib-scale-snapshot-reads)
published twenty 200 MiB text sources per collection through the release
API and MinIO. The two SQLite databases were 1,131,315,200 and
1,131,708,416 bytes. The seed ran on macOS with page deltas enabled and
embeddings disabled. Each read run used a fresh API with a 5 GiB snapshot
cache, one concurrent query per collection, and three repeated passes.
The two cold searches ran against real FTS indexes. Every timed result and
the subsequent fetch matched the expected UTF-8 bytes and published
generation.

| Measure | macOS API to loopback MinIO | Linux ARM64 API to MinIO over Docker bridge |
| --- | ---: | ---: |
| First gRPC search, per collection | 9.27–10.19 s | 5.96–6.23 s |
| Download and verify collection snapshot | 9.23–10.17 s | 5.93–6.21 s |
| Open read-only SQLite connection | 0.127–0.141 ms | 0.074–0.295 ms |
| Repeated gRPC searches | 4.36–10.91 ms | 9.26–50.93 ms |

Each cold API downloaded 1,257,944,248 and 1,258,443,024 bytes for
the two collections, including their retained page deltas. The Linux run
reused both SQLite handles on each repeat. The macOS column contains two
API runs. The Linux column contains one run with two CPUs, an 8 GiB
cgroup limit, and `/tmp` mounted as tmpfs. The Linux cgroup peak was
2,590,826,496 bytes. The largest observed `shmem` sample was
2,498,883,584 bytes across 46 samples, including memory-backed files
during page replay. The timings come from local Docker and MinIO. Live
Cloud Run measurements at 1 GiB and 2 GiB still need the authorized
deployment.

### Hosted excerpt reads with injected latency

Two macOS debug builds queried the same warm SQLite generation through
MinIO. A loopback proxy delayed each source range request by 30 ms. The
generated Python gRPC client made seven measured searches per build.
Each search returned twelve hits from separate UTF-8 sources and checked
their order, ranges, and excerpt bytes against the serial build.

| Range scheduling | Median | Measured range | Peak in flight | Bytes per search |
| --- | ---: | ---: | ---: | ---: |
| Serial | 457.5 ms | 446.6–539.7 ms | 1 | 336 |
| Bounded concurrency | 152.5 ms | 130.2–165.1 ms | 4 | 336 |

Both builds made twelve range requests per search. The measurements isolate
warm retrieval with an artificial delay and small excerpts. Cold snapshot
reads are measured separately on the GiB-scale fixtures.

### Concurrent gRPC search on 2 GiB FTS snapshots

The same benchmark published 39 sources of 200 MiB per collection, with
page deltas enabled and embeddings disabled. The resulting SQLite
snapshots were 2,201,829,376 and 2,200,682,496 bytes. Each cold read
transferred 2,384,690,304 and 2,383,227,352 bytes respectively, including
the page deltas. The seed finished before any timed search began.

Each run started a fresh Linux ARM64 API container from the repository
image with two CPUs, an 8 GiB cgroup limit, a 6 GiB `/tmp` tmpfs, and
a 5 GiB snapshot cache. MinIO ran on the local Docker bridge.
There was one cold run against a single collection and two against both
collections. Each cold pass was followed by three repeat passes. The
generated Python client verified the snapshot generation, source
provenance, and exact UTF-8 bytes over gRPC search and fetch.

| Measurement | One active collection | Two active collections |
| --- | ---: | ---: |
| Cold search per collection | 4.53 s | 5.96–6.40 s |
| Download and verify per collection | 4.52 s | 5.95–6.35 s |
| Open read-only SQLite connection | 0.076 ms | 0.059–0.097 ms |
| Repeat search | 7.64–9.13 ms | 5.11–49.46 ms |
| Peak cgroup memory | 2.29 GiB | 4.55 GiB |

Every repeat reused its SQLite connection. The largest sampled
memory-backed file footprint was 2.18 GiB for one collection and
4.44 GiB for two, measured using the cgroup's `shmem` counter.

With the default 2 GiB cache, neither snapshot fit. Both repeat passes
downloaded and reconstructed both snapshots again. Those searches took
6.21–6.26 s and cgroup peak memory was 4.52 GiB for that run. The smaller
cache saved retention between requests, while simultaneous downloads
still needed nearly the same peak memory. These measurements cover the
local API container after publication. Cloud Run still needs its own
network, memory, and publication measurements.

### Concurrent reads of one oversized SQLite snapshot

The release benchmark publishes 2,048 MiB of synthetic SQLite payload and
starts two reads of the same collection with the default 2 GiB cache budget.
The resulting snapshot is 2,150,313,984 bytes. Both readers verify the marker
in the same materialized file. The benchmark releases both readers, checks
that the temporary file was removed, then reads the collection again.

```sh
SANCTUM_CONCURRENT_SNAPSHOT_MIB=2048 \
  cargo test --release --locked -p sanctum-db --test snapshot_capacity \
  minio_shared_oversized_snapshot_capacity -- --ignored --exact --nocapture
```

With loopback MinIO on the Apple M4 Max, the two concurrent reads completed
in 11.93 s and used one 2,150,313,984-byte temporary file. A later request
materialized a fresh copy in 10.26 s. These are single-sample snapshot
resolution timings. Cloud Run ingress and instance memory remain unmeasured
for this workload. [Snapshot cache behavior](STORAGE.md#hosted-snapshot-reuse).

## Exact vector ranking in SQLite

The release comparison uses one SQLite database for chunk identity, metadata,
and normalized float32 vectors. A fixture creates 262,144 synthetic vectors
at each dimension, with 64 chunks per source. It includes deleted sources and
chunks associated with another profile. A nested JSON array-membership filter
selects about 10% of sources. Both implementations apply current revision,
profile, and metadata conditions before choosing the top ten.

The production `VectorIndex` uses Rust to score existing SQLite vector BLOBs.
The test compares that implementation with the cosine function from
[sqlite-vec v0.1.9](https://github.com/asg017/sqlite-vec/releases/tag/v0.1.9)
using the same BLOBs and candidate SQL. A fixed-schema `vec0` table would need
its own design for arbitrary source and chunk filters, revision eligibility,
and content-hash reuse. [sqlite-vec KNN query modes](https://alexgarcia.xyz/sqlite-vec/features/knn.html).

```sh
cargo test --release --locked -p sanctum-db-sqlite compare_sqlite_vec \
  -- --ignored --nocapture
```

`SANCTUM_VECTOR_BENCH_ROWS` overrides the fixture size. Each case uses a warm-up
and five measured queries per implementation on separate read-only connections.
Rust ran first for each query. The working SQLite files were on the Apple M4 Max
host, and the operating system page cache was left as observed.

| Dimensions | SQLite file bytes | Filter | Rust median | sqlite-vec median | Matched top-ten IDs |
| --- | ---: | --- | ---: | ---: | ---: |
| 768 | 1,275,162,624 | None | 2,269 ms | 2,531 ms | 60/60 |
| 768 | 1,275,162,624 | Source JSON | 370 ms | 379 ms | 60/60 |
| 1,536 | 2,348,904,448 | None | 4,200 ms | 4,328 ms | 60/60 |
| 1,536 | 2,348,904,448 | Source JSON | 496 ms | 511 ms | 60/60 |

All 240 returned neighbors matched as sets and scores agreed within 0.000002.
Unfiltered 768-dimension Rust samples ranged from 2.22 to 4.62 seconds. The
fixture isolates ranking and SQLite I/O. It excludes real model recall,
lexical indexing, provider requests, source-range reads, object transfer, and
concurrent tenants.

The scalar extension did not improve median latency in these cases. Sanctum
keeps the Rust scorer and its existing storage format. The measured cost of
unfiltered exact scans at this size motivates testing candidate indexes or
vector layouts against a representative corpus with the same filter and recall
requirements.

### Query plan and read-only memory mapping

The original sqlite-vec comparison ran `ANALYZE` on its fixtures.
Sanctum's normal collection writer leaves SQLite planner statistics
unset. This follow-up uses the same 262,144-vector fixture and compares
SQLite's chosen join order with a scan starting from the active
profile's vector rows. Both use the same Rust scoring function, current
revision checks, and source metadata filter. The test alternates their
order and verifies the same ordered top ten and scores. [SQLite join order](https://sqlite.org/optoverview.html#manual_control_of_query_plans_using_cross_join).

```sh
cargo test --release --locked -p sanctum-db-sqlite compare_vector_scan_order \
  -- --ignored --nocapture
SANCTUM_VECTOR_BENCH_SKIP_ANALYZE=1 \
  cargo test --release --locked -p sanctum-db-sqlite compare_vector_scan_order \
  -- --ignored --nocapture
SANCTUM_VECTOR_BENCH_SKIP_ANALYZE=1 SANCTUM_VECTOR_BENCH_MMAP_BYTES=2147483648 \
  cargo test --release --locked -p sanctum-db-sqlite compare_vector_scan_order \
  -- --ignored --nocapture
```

Median elapsed time in milliseconds from five measured queries per
case, after one warm-up, on the same Apple M4 Max:

| Dimensions | Planner statistics | Memory map | Filter | Automatic join | Vectors first |
| --- | --- | --- | --- | ---: | ---: |
| 768 | Analyzed | Off | None | 2,462.58 | 521.74 |
| 768 | Analyzed | Off | Source JSON | 324.57 | 350.34 |
| 1,536 | Analyzed | Off | None | 4,502.57 | 841.23 |
| 1,536 | Analyzed | Off | Source JSON | 524.47 | 481.99 |
| 768 | Unset | Off | None | 498.92 | 494.61 |
| 768 | Unset | Off | Source JSON | 364.30 | 360.91 |
| 1,536 | Unset | Off | None | 837.13 | 832.49 |
| 1,536 | Unset | Off | Source JSON | 496.04 | 491.63 |
| 768 | Unset | On | None | 273.22 | 270.28 |
| 768 | Unset | On | Source JSON | 219.09 | 213.56 |
| 1,536 | Unset | On | None | 456.82 | 447.88 |
| 1,536 | Unset | On | Source JSON | 273.67 | 272.43 |

SQLite capped the requested mapping at 2,147,418,112 bytes. It covered
the 1.19 GiB fixture and part of the 2.19 GiB fixture. Both join orders
returned the same ordered hits and scores in all 72 comparisons.
Forcing vector order recovered most of the unfiltered slowdown from
`ANALYZE`. Without planner statistics, the two orders performed
similarly. The default query keeps SQLite's automatic planning.

Memory mapping lowered warm scoring latency on these closed read-only
files. The server continues to use ordinary SQLite reads. A hosted
rollout needs measurements of instance memory and concurrent snapshot
access, and the filesystem server also opens mutable databases.
[SQLite memory-mapped I/O](https://sqlite.org/mmap.html).

## Concurrent collections among 1,000 tenants

The release benchmark creates one collection each for 1,000 tenant IDs in
MinIO. Four tenants then run concurrently through one standalone Sanctum API
and its object worker. Each active tenant uploads a roughly 1 MiB source,
searches it nine times, and replaces it twice. The benchmark checks that
replacement preserves the source ID, removes the old search result, and
keeps each tenant's results isolated. The other 996 collections are empty
and remain idle during the active workload. The server uses lexical search
with embeddings disabled.

Supply a [standalone S3 configuration](STORAGE.md#hosted-server-configuration)
with a dedicated empty prefix. The script leaves the benchmark data in that
prefix for inspection.

```sh
cargo build --release --locked -p sanctum-server -p sanctum-cli
python3 scripts/benchmark-tenants.py
```

The Apple M4 Max and loopback MinIO setup above created the catalogs and
collections in 30.80 seconds with eight concurrent clients. The four active
workflows finished in 2.47 seconds. End-to-end query timings include spawning
the release CLI and a loopback gRPC call. Replacement publication includes
polling every 100 ms. The first search followed ingestion on the same server.

| Operation | Samples | p50 | p95 |
| --- | ---: | ---: | ---: |
| First search after publication | 4 | 24.70 ms | 55.30 ms |
| Repeated search | 32 | 18.32 ms | 54.88 ms |
| Replacement accepted | 8 | 55.72 ms | 128.77 ms |
| Replacement text ready | 8 | 658.74 ms | 860.10 ms |

The cluster usage API listed 4,052 objects totaling 209,418,168 bytes in
539.34 ms. An independent paginated MinIO listing matched both totals.
The benchmark covers one process, small active databases, and four active
writers submitting to a serial worker. It provides no estimate for many
simultaneously active tenants, large vector queries, or Cloud Run ingress.

### 10,000 mostly idle tenants

The same benchmark can extend catalog setup and usage measurement to
10,000 tenant IDs. This run selected eight active collections, each with
the same source size, search count, and replacement workload as above.
The other 9,992 collections were empty.

```sh
python3 scripts/benchmark-tenants.py --tenants 10000 --active 8 --setup-concurrency 8
```

The Apple M4 Max and loopback MinIO setup created the 10,000 catalogs
and collections in 165.68 s. The eight active workflows finished in
3.17 s. Each query still includes release CLI startup and loopback
gRPC latency.

| Operation | Samples | p50 | p95 |
| --- | ---: | ---: | ---: |
| First search after publication | 8 | 12.30 ms | 21.28 ms |
| Repeated search | 64 | 12.66 ms | 21.90 ms |
| Replacement accepted | 16 | 42.73 ms | 66.94 ms |
| Replacement text ready | 16 | 873.08 ms | 1,304.38 ms |

Cluster usage enumerated 40,100 objects totaling 1,995,800,329 bytes
in 1.68 s. An independent paginated MinIO listing matched both totals.
The measurements cover one release API and its serial object worker,
with lexical search and eight simultaneous active collections. They
leave Cloud Run concurrency, cold starts, and vector search at this
tenant count unmeasured.

## Small edit in a multi-GiB collection

The release benchmark publishes twelve 200 MiB sources to one MinIO-backed
collection. The files use independently seeded synthetic observations and
unique search terms near EOF. After restarting the API, it measures the first
search and three repeats, changes eight bytes near EOF in the final source,
replaces that source, and verifies the resulting MCP range byte for byte. The
current source data is 2,516,582,400 bytes (2.34 GiB). Embeddings are disabled.

```sh
cargo build --release --locked -p sanctum-server -p sanctum-cli
python3 scripts/benchmark-large-collection.py
```

Supply a [standalone S3 configuration](STORAGE.md#hosted-server-configuration)
with a dedicated empty prefix. `--sources` and `--mib` control the fixture.
The script leaves the published data and snapshot generations in that prefix.

| Measure | Apple M4 Max with loopback MinIO |
| --- | ---: |
| First / twelfth source from upload to text ready | 9.38 / 58.83 s |
| First search after process restart | 4,199.15 ms |
| Repeated searches | 23.43, 17.32, 34.07 ms |
| Eight-byte edit from upload to acceptance / text ready | 5.15 / 69.73 s |
| Canonical bytes returned by MCP fetch | 3,557 |
| SQLite snapshot before replacement | 685,527,040 bytes |
| SQLite snapshot after replacement | 743,645,184 bytes |
| Additional object storage immediately after replacement | 1,638,888,451 bytes |

A separate MinIO listing matched the reported 80 objects and 12,432,584,704
bytes after replacement. It found 27 SQLite generations totaling
9,706,274,816 bytes. The eight-byte edit used the ordinary whole-source upload
and published two full SQLite snapshots for registration and lexical indexing.
Both previous snapshots and prior source data were still within reader grace.
The figures describe the storage retained during that window. They do not
estimate steady-state storage after maintenance or cloud ingress latency.

This workload exposes write amplification before vectors are added. The next
publication optimization should reduce full-size writes for small changes while
preserving ordered registration and conditional publication. A durable mutation
journal remains a candidate and would require crash, replay, and compaction
measurements at the same collection size.

### Reusing verified snapshots during publication

A second release run used the same fixture and loopback MinIO with receipt
lookups and writers sharing the verified snapshot cache. The twelfth
source reached text readiness in 30.83 s, and the eight-byte replacement
in 29.56 s, compared with 58.83 s and 69.73 s in the baseline run. Cold
search took 3.27 s and the three repeated searches took 11.56, 12.15, and
14.57 ms. These are single runs and include varying upload latency.

The replacement still published two full snapshots. The final snapshot was
745,369,600 bytes, and the 27 retained generations occupied 9,728,929,792
bytes. This change saves repeated downloads within an instance. It leaves
full-size writes and temporary storage for private writer copies in the
publication path.

## Retrieval relevance and embedding reuse

The [retrieval benchmark](../scripts/benchmark-retrieval.py) streams the
8,674 documents in the BEIR ArguAna release through the filesystem
server's generated gRPC client. Each source contains its title, when
present, followed by its argument text. The server indexes UTF-8 windows
of up to 4,096 bytes with 400 bytes of overlap. The configured Ollama
profile uses EmbeddingGemma 2 with 768 dimensions and the model's search
query and untitled document prefixes. [BEIR release and archive checksum](https://github.com/beir-cellar/beir#-available-datasets),
[ArguAna dataset](https://huggingface.co/datasets/BeIR/arguana),
[EmbeddingGemma 2 model card](https://ai.google.dev/gemma/docs/embeddinggemma/model_card_2).

The evaluator uses all 1,406 test queries and one judged counterargument
per query. It requests up to 30 chunks per search, keeps the first hit per
document, removes identical query and document IDs, and scores the first
ten remaining documents. Six queries exceed the server's 4,096-byte limit
and are trimmed at a UTF-8 and word boundary. Five judged documents are
absent from the archive corpus and count as misses. The benchmark measures
the Sanctum API, including excerpt retrieval and the Ollama request for
semantic and hybrid modes. [BEIR evaluation code](https://github.com/beir-cellar/beir/blob/main/beir/retrieval/evaluation.py).

Download and extract ArguAna from the BEIR table. Run an Ollama server
with the chosen EmbeddingGemma model installed and use its digest for
`--revision`. The benchmark uses disposable local storage. A loopback
proxy counts requests and inputs and forwards each request to Ollama.

```sh
cargo build --release --locked -p sanctum-server
python3 -m venv /tmp/sanctum-retrieval-venv
. /tmp/sanctum-retrieval-venv/bin/activate
python -m pip install grpcio-tools==1.73.0
generated="$(mktemp -d)"
python -m grpc_tools.protoc -I proto \
  --python_out="$generated" --grpc_python_out="$generated" \
  proto/sanctum/v1/sanctum.proto
PYTHONPATH="$generated" python scripts/benchmark-retrieval.py /path/to/arguana \
  --revision eaff2253e2ba --output /tmp/sanctum-retrieval.json
```

### Results

The release filesystem server ran on the Apple M4 Max host with Ollama
0.40.16 and the NVFP4 `embeddinggemma-2:740m` build, digest
`eaff2253e2ba`. Eight ingestion workers published all 8,674 documents in
419.29 s. Embeddings reached 100% coverage at 422.95 s. Ollama processed
8,647 document inputs in 674 requests for 8,695 current chunks. The
collection's SQLite file was 54,317,056 bytes after clean shutdown,
including the FTS index, vectors, and metadata. This run included retry
handling for transient SQLite contention observed in a 16-worker trial.

| Search mode | Recall@10 | nDCG@10 | Queries with a result | gRPC p50 | gRPC p95 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Lexical | 0.00071 | 0.00071 | 63/1,406 | 125.55 ms | 235.29 ms |
| Semantic | 0.91038 | 0.63994 | 1,406/1,406 | 193.03 ms | 305.82 ms |
| Hybrid | 0.91038 | 0.63994 | 1,406/1,406 | 208.04 ms | 344.84 ms |

Lexical search requires every query term to match in a chunk. These
paragraph-length argument queries rarely meet that condition. Hybrid
and semantic returned the same aggregate scores on this workload.
The latency figures include reading excerpts and collection-wide
embedding coverage for each response. The two vector search modes
generated 2,812 query inputs in total. These measurements describe one
dataset and model configuration.

A separate collection held a 41,000-byte source with 11 distinct chunks.
An eight-byte replacement inside one chunk preserved the source ID,
published a new revision, and returned to full embedding coverage.
Ollama received 11 document inputs on initial ingestion and one on
replacement, avoiding ten input embeddings. Each took one batched
request. Time from upload start to complete embeddings was 1.69 s
initially and 1.18 s after the edit, including the polling interval.

### Concurrent hosted retrieval with real embeddings

The hosted benchmark runs one release API and standalone object worker
with MinIO. It publishes two collections with the same text window and
Ollama profile as the local ArguAna run. Each collection takes a
different slice of the corpus, ensures its selected queries have their
judged documents present, and then fills the remaining document slots
in archive order. The subsets can share documents.

Use the generated Python client and an isolated MinIO prefix as above:

```sh
PYTHONPATH="$generated" python scripts/benchmark-hosted-vectors.py /path/to/arguana \
  --revision eaff2253e2ba --documents 512 --queries 40
```

After both collections reach full coverage, a fresh API process serves
the first concurrent semantic pass. The same API then runs semantic
searches serially and concurrently, followed by concurrent hybrid
search. The timed interval includes the provider query, SQLite ranking,
and source-range fetches. Each returned excerpt is verified against the
source bytes. Each pass measures recall on the same fixed query subset.

The loopback MinIO run used 512 documents and 40 queries per collection.
The collections shared 40 documents. They had 514 and 512 chunks at full
coverage and published databases of 4,304,896 and 4,284,416 bytes. The first
API requests took 230.62 and 185.06 ms. Downloading and replaying each
collection's snapshot chain took 56.47 and 59.63 ms and transferred
4,501,952 and 4,510,448 bytes respectively. Both requests also used the
tenant catalog. Ollama was already warm when the API restarted.

| Pass | Queries | p50 (ms) | p95 (ms) | Recall@10 |
| --- | ---: | ---: | ---: | ---: |
| Concurrent semantic, starting with a fresh API | 80 | 105.03 | 229.48 | 0.98750 |
| Warm semantic, one query at a time | 80 | 104.13 | 138.14 | 0.98750 |
| Warm semantic, two active collections | 80 | 127.96 | 187.43 | 0.98750 |
| Warm hybrid, two active collections | 80 | 130.69 | 197.08 | 0.98750 |

Only the first query per collection starts with a cold collection snapshot.
All four passes retrieved the judged document for 79 of 80 queries after
removing exact query-ID matches. The query ID list, joined with newline
separators, had SHA-256
`af57e5456f8f7bb9d0283cad2252e04c586e679d4d4b3a440eaac61a9e344bbf`.
The subset limits the number of competing documents. Use the full ArguAna
run for retrieval quality across the complete dataset. Cloud Run memory and
latency for simultaneous GiB-scale collections remain unmeasured.

## SQLite page changes during a source replacement

Use the [delta measurement script](../scripts/measure-snapshot-delta.py)
with two closed SQLite backups from the same collection. The script
compares aligned pages, writes changed pages in order, and reconstructs
the second snapshot from a private copy of the first. It checks the
result byte for byte. The reported replay time includes copying the base
database and applying the delta. It excludes the final comparison,
object transfer, and SQLite queries.

```sh
python3 scripts/measure-snapshot-delta.py /path/to/before.db /path/to/after.db
```

The experiment records complete SQLite pages and their page numbers.
A result can grow or shrink, and an unchanged pair needs only the
16-byte header. The script measures a possible publication format.
The hosted server continues to publish full snapshots.

The MinIO rerun used the same 12-source, 2.34 GiB collection described
above. The edit took 30.00 s to reach text readiness and returned the
exact 3,557-byte MCP range. The final three object versions were checked
against the applied-command receipts to identify the base, registration,
and indexing generations.

| Transition | Full snapshot bytes | Changed 4 KiB pages | Delta bytes | Encode | Copy and replay |
| --- | ---: | ---: | ---: | ---: | ---: |
| Register the replacement | 687,362,048 | 3 | 12,328 | 0.098 s | 0.112 s |
| Publish its lexical index | 745,467,904 | 35,413 | 145,334,968 | 0.392 s | 0.273 s |

Both reconstructions matched the actual published SQLite backup byte for
byte. The two raw delta files totaled 145,347,296 bytes, or 10.14% of the
1,432,829,952 bytes of new SQLite snapshot data. The measurement concerns
database publication. The upload still sent the whole 200 MiB source.
The encode and replay figures are one local run with the OS cache as observed.
The first six source insertions in the run also took 10.82–30.44 s apiece,
so the hosted edit timing varies across runs.

Registration has the smallest change, while indexing rebuilds the
replacement's lexical state and changes substantially more pages.
A bounded page-delta chain is a candidate for reducing both writes.
A production implementation needs the same conditional HEAD publication,
digest verification, bounded replay, and retirement grace for every
generation used as a base. It also needs a full-snapshot path when the
change or accumulated chain approaches the size of the next backup.

### Publishing the deltas on MinIO

With `hosted_snapshot_page_deltas` enabled, the release server ran the
same 12 × 200 MiB fixture and eight-byte edit. A fresh process read the
current database before the replacement. A separate MinIO listing
confirmed the object lengths, and MCP returned the exact 3,557-byte
range from the new revision.

| Measurement | Full snapshots | Page deltas enabled |
| --- | ---: | ---: |
| Twelfth source to text ready | 28.24 s | 19.58 s |
| Eight-byte edit to text ready | 30.00 s | 20.38 s |
| Registration database upload | 687,362,048 bytes | 12,328 bytes |
| Indexing database upload | 745,467,904 bytes | 145,339,072 bytes |
| Cold search after restart | 2.68 s | 3.71 s |
| Three warm searches | 10.88–11.18 ms | 12.98–15.65 ms |
| Database objects retained after the edit | 9,728,167,936 bytes | 3,161,696,896 bytes |

These are single runs on loopback MinIO with the same cache settings.
The resulting SQLite backups were 745,467,904 and 745,963,520 bytes.
The delta-enabled run retained eight full backups and 19 deltas. Its
final HEAD required one 630,095,872-byte base and four deltas for cold
replay. The source upload still transferred the complete 200 MiB file.
The runs cover lexical indexing. Live Cloud Run memory and latency
measurements remain open.

## Partial lexical matches for prose queries

Hybrid tries partial-term FTS5 when both the all-term search and
semantic ranking have zero eligible hits in a collection. This includes
queries without an available embedding provider. Explicit lexical
requests continue to require all terms.

Run the same ArguAna benchmark with `--lexical-fallback` to disable the
provider and measure this path. The release filesystem server returned
results for 662 of 1,406 queries, with 0.31437 Recall@10 and 0.19465
nDCG@10. The original strict lexical run returned results for 63 queries
and reached 0.00071 on both measures. Query IDs matching a document ID
are excluded from the rankings. Some prose queries match only their own
document in the strict pass, so fallback is not triggered for those.
The two benchmarks used separate SQLite databases, with the same
8,695 current chunks in each.

The experiment also tested fusing partial matches with live Ollama
results when the all-term pass was empty. That variant reached 0.88549
Recall@10 and 0.61493 nDCG@10. The same run's semantic mode reached
0.91038 and 0.63994. The shipped rule uses partial terms only when
semantic ranking has no eligible hits, keeping the original ranking
path for collections with semantic results.

## Local ingestion with 16 workers

The release filesystem server ingested all 8,674 ArguAna sources with
16 concurrent workers and real Ollama. The generated client counted
zero SQLite-busy retries. Lexical publication finished in 270.94 s,
and all 8,695 chunks had embeddings at 464.96 s. The earlier 16-worker
trial hit SQLite contention during acceptance. The writer-lock run
used three queries as a smoke check and did not rescore the test set.
The provider processed 8,647 document inputs across 541 requests.
