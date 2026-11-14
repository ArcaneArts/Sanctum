use std::{path::Path, time::Instant};

use rusqlite::{
    auto_extension::{RawAutoExtension, register_auto_extension},
    params,
};
use serde_json::json;
use uuid::Uuid;

use super::*;
use crate::{
    initialize_collection,
    metadata_filter::{Filter, Level, Operator, PathSegment, Predicate},
};

const ACTIVE: &str = "capacity-v1";
const DIMENSIONS: usize = 768;
const TOP_K: u32 = 10;

#[test]
fn clearing_read_query_state_drops_vector_and_filter_functions() {
    let file = tempfile::NamedTempFile::new().unwrap();
    fixture(file.path(), 128, DIMENSIONS, false);
    let db = CollectionDb::open_read_only(file.path()).unwrap();
    let filter = Filter::Test(Predicate {
        level: Level::Source,
        namespace: "benchmark".into(),
        key: "sample".into(),
        path: vec![PathSegment::Property("groups".into())],
        operator: Operator::Contains,
        operand: Some(json!("selected")),
    });
    let first = vector(1, DIMENSIONS);
    let hits = db
        .search_vector(ACTIVE, first.clone(), TOP_K, Some(&filter), &[])
        .unwrap();
    assert!(!hits.is_empty());
    assert_eq!(Arc::strong_count(&first), 2);
    db.clear_read_query_state().unwrap();
    assert_eq!(Arc::strong_count(&first), 1);
    assert!(
        db.0.prepare("SELECT sanctum_metadata_matches(0, 'null')")
            .is_err()
    );
    let next = db
        .search_vector(ACTIVE, vector(90, DIMENSIONS), TOP_K, None, &[])
        .unwrap();
    assert_ne!(hits[0].chunk.reference, next[0].chunk.reference);
}

fn vector(seed: u64, dimensions: usize) -> Arc<[f32]> {
    let mut rng = seed.wrapping_add(1);
    let mut values: Vec<f32> = (0..dimensions)
        .map(|_| {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            ((rng >> 40) as f32 / 8_388_608.0) - 1.0
        })
        .collect();
    let norm = values.iter().map(|value| value * value).sum::<f32>().sqrt();
    values.iter_mut().for_each(|value| *value /= norm);
    values.into()
}

fn fixture(path: &Path, rows: u32, dimensions: usize, analyze: bool) {
    let mut connection = Connection::open(path).unwrap();
    let tx = connection.transaction().unwrap();
    initialize_collection(&tx).unwrap();
    {
        let mut insert_source = tx.prepare("INSERT INTO sources (id, name, current_revision, deleted) VALUES (?1, 'sample', ?2, ?3)").unwrap();
        let mut insert_revision = tx.prepare("INSERT INTO revisions (source_id,id,content_hash,bytes,chunk_count,chunker) VALUES (?1,?2,'synthetic',65536,64,'utf8-overlap-v1')").unwrap();
        let mut insert_chunk = tx.prepare("INSERT INTO chunks (source_id,revision_id,ordinal,start_byte,end_byte,content_hash) VALUES (?1,?2,?3,?4,?5,?6)").unwrap();
        let mut insert_embedding = tx
            .prepare("INSERT INTO embedding_cache (profile,input_hash,vector) VALUES (?1,?2,?3)")
            .unwrap();
        let mut insert_meta = tx.prepare("INSERT INTO revision_metadata (source,revision,ordinal,namespace,key,value) VALUES (?1,?2,-1,'benchmark','sample',?3)").unwrap();
        let mut source = String::new();
        let mut revision = String::new();
        for row in 0..rows {
            if row % 64 == 0 {
                source = Uuid::from_u128(u128::from(row) + 1).to_string();
                revision = Uuid::from_u128(u128::from(row) + 2).to_string();
                insert_source
                    .execute(params![source, revision, row / 64 % 101 == 100])
                    .unwrap();
                insert_revision.execute(params![source, revision]).unwrap();
                let value = json!({"groups": [if row / 64 % 10 == 0 {"selected"} else {"other"}]})
                    .to_string();
                insert_meta
                    .execute(params![source, revision, value])
                    .unwrap();
            }
            let hash = format!("{row:08x}");
            let ordinal = row % 64;
            insert_chunk
                .execute(params![
                    source,
                    revision,
                    ordinal,
                    ordinal * 1024,
                    (ordinal + 1) * 1024,
                    hash
                ])
                .unwrap();
            let bytes = vector(u64::from(row), dimensions)
                .iter()
                .flat_map(|value| value.to_le_bytes())
                .collect::<Vec<_>>();
            insert_embedding
                .execute(params![
                    if row % 103 == 102 {
                        "other-profile"
                    } else {
                        ACTIVE
                    },
                    hash,
                    bytes
                ])
                .unwrap();
        }
    }
    tx.commit().unwrap();
    if analyze {
        connection.execute_batch("ANALYZE").unwrap();
    }
}

struct SqliteVec(Connection);

impl VectorIndex for SqliteVec {
    fn search_vector(
        &self,
        profile: &str,
        query: Arc<[f32]>,
        limit: u32,
        filter: Option<&Filter>,
        metadata: &[metadata::Entry],
    ) -> Result<Vec<ScoredChunk>> {
        let query = query
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect::<Vec<_>>();
        search_candidates(
            &self.0,
            "1.0 - vec_distance_cosine(e.vector, ?2)",
            vec![profile.to_owned().into(), query.into()],
            limit,
            filter,
            metadata,
        )
    }
}

fn filter() -> Filter {
    Filter::Test(Predicate {
        level: Level::Source,
        namespace: "benchmark".into(),
        key: "sample".into(),
        path: vec![PathSegment::Property("groups".into())],
        operator: Operator::Contains,
        operand: Some(json!("selected")),
    })
}

fn compare(path: &Path, dimensions: usize, samples: usize) {
    let rust = CollectionDb::open_read_only(path).unwrap();
    // The upstream binding declares only the C symbol. Cast it to SQLite's extension ABI
    // as documented by sqlite-vec. Register it only in the isolated benchmark process.
    let entry = unsafe {
        std::mem::transmute::<*const (), RawAutoExtension>(
            sqlite_vec::sqlite3_vec_init as *const (),
        )
    };
    unsafe { register_auto_extension(entry) }.unwrap();
    let extension = SqliteVec(
        Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap(),
    );
    assert!(rusqlite::auto_extension::cancel_auto_extension(entry));
    let version: String = extension
        .0
        .query_row("SELECT vec_version()", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, "v0.1.9");

    let filter = filter();
    for (label, filter) in [
        ("unfiltered", None),
        ("source JSON array contains", Some(&filter)),
    ] {
        let mut rust_times = Vec::new();
        let mut extension_times = Vec::new();
        let mut equal = 0;
        for sample in 0..=samples {
            let query = vector(64 * sample as u64, dimensions);
            let start = Instant::now();
            let rust_hits = rust
                .search_vector(ACTIVE, query.clone(), TOP_K, filter, &[])
                .unwrap();
            let rust_elapsed = start.elapsed();
            let start = Instant::now();
            let extension_hits = extension
                .search_vector(ACTIVE, query, TOP_K, filter, &[])
                .unwrap();
            let extension_elapsed = start.elapsed();
            assert_eq!(rust_hits.len(), TOP_K as usize);
            assert_eq!(extension_hits.len(), rust_hits.len());
            for hit in &rust_hits {
                let peer = extension_hits
                    .iter()
                    .find(|other| other.chunk.reference == hit.chunk.reference)
                    .expect("missing exact top-k neighbor");
                assert!(
                    (hit.score - peer.score).abs() < 2e-6,
                    "scores {} and {}",
                    hit.score,
                    peer.score
                );
                equal += 1;
            }
            if sample > 0 {
                rust_times.push(rust_elapsed.as_secs_f64() * 1000.0);
                extension_times.push(extension_elapsed.as_secs_f64() * 1000.0);
            }
        }
        println!(
            "{label}: rust_ms={rust_times:?}, sqlite_vec_ms={extension_times:?}, matching_neighbors={equal}"
        );
    }
}

#[test]
#[ignore = "opt-in comparison with a large SQLite fixture"]
fn compare_vector_scan_order() {
    let rows: u32 = std::env::var("SANCTUM_VECTOR_BENCH_ROWS")
        .map_or(Ok(262_144), |value| value.parse())
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    assert!(rows >= 1024);
    let analyze = std::env::var_os("SANCTUM_VECTOR_BENCH_SKIP_ANALYZE").is_none();
    let predicate = filter();
    let mmap = std::env::var("SANCTUM_VECTOR_BENCH_MMAP_BYTES")
        .map_or(Ok(0i64), |value| value.parse())
        .unwrap();
    for dimensions in [DIMENSIONS, 1536] {
        let path = directory.path().join("vector.db");
        fixture(&path, rows, dimensions, analyze);
        let db = CollectionDb::open_read_only(&path).unwrap();
        db.0.pragma_update(None, "mmap_size", mmap).unwrap();
        let mapped: i64 =
            db.0.pragma_query_value(None, "mmap_size", |row| row.get(0))
                .unwrap();
        println!(
            "rows={rows}, dimensions={dimensions}, analyzed={analyze}, mmap={mapped}, db_bytes={}",
            path.metadata().unwrap().len()
        );
        for (label, filter) in [("unfiltered", None), ("filtered", Some(&predicate))] {
            let mut samples = [Vec::new(), Vec::new()];
            // CROSS JOIN keeps the embedding table in the outer loop.
            let plans = [
                ("automatic", None),
                ("vector_first", Some(
                    "embedding_cache e CROSS JOIN chunks c ON c.content_hash = e.input_hash
                     CROSS JOIN sources s ON s.id = c.source_id AND s.current_revision = c.revision_id",
                )),
            ];
            for sample in 0..=5 {
                register_dot_product(&db.0, vector(64 * sample as u64, dimensions)).unwrap();
                let mut results = [Vec::new(), Vec::new()];
                for order in 0..plans.len() {
                    let index = (sample + order) % plans.len();
                    let start = Instant::now();
                    let arguments = vec![ACTIVE.to_owned().into()];
                    let ranked = if let Some(tables) = plans[index].1 {
                        search_with_join_order(
                            &db.0,
                            "sanctum_cosine(e.vector)",
                            tables,
                            arguments,
                            TOP_K,
                            filter,
                            &[],
                        )
                    } else {
                        search_candidates(
                            &db.0,
                            "sanctum_cosine(e.vector)",
                            arguments,
                            TOP_K,
                            filter,
                            &[],
                        )
                    };
                    results[index] = ranked
                        .unwrap()
                        .into_iter()
                        .map(|hit| (hit.chunk.reference, hit.score))
                        .collect();
                    if sample != 0 {
                        samples[index].push(start.elapsed().as_secs_f64() * 1000.0);
                    }
                }
                assert_eq!(results[0].len(), TOP_K as usize);
                assert_eq!(results[0], results[1]);
            }
            for ((plan, _), times) in plans.iter().zip(samples) {
                println!("{label} {plan} ms={times:?}");
            }
        }
        drop(db);
        std::fs::remove_file(path).unwrap();
    }
}

#[test]
#[ignore = "opt-in vector comparison with a large SQLite fixture"]
fn compare_sqlite_vec() {
    let rows: u32 = std::env::var("SANCTUM_VECTOR_BENCH_ROWS")
        .map_or(Ok(262_144), |value| value.parse())
        .unwrap();
    assert!(rows >= 1024);
    let directory = tempfile::tempdir().unwrap();
    for dimensions in [DIMENSIONS, 1536] {
        let path = directory.path().join(format!("vector-{dimensions}.db"));
        let start = Instant::now();
        fixture(&path, rows, dimensions, true);
        println!(
            "rows={rows}, dimensions={dimensions}, db_bytes={}, setup_seconds={:.3}",
            path.metadata().unwrap().len(),
            start.elapsed().as_secs_f64()
        );
        compare(&path, dimensions, 5);
        std::fs::remove_file(path).unwrap();
    }
}
