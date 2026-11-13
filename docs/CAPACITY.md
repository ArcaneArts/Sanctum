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
the target size. The next workload should measure realistic source and
embedding batches before choosing a mutation journal or database split.

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
