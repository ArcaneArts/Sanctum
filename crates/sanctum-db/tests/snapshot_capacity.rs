use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use sanctum_core::Scope;
use sanctum_db::{Command, Generation, Materialized, Publication, SnapshotCache, Unit};
use sanctum_storage::{Key, Storage};
use sanctum_storage_s3::{S3Config, S3Storage};
use uuid::Uuid;

#[tokio::test]
#[ignore = "publishes multi-GiB snapshots to a dedicated MinIO benchmark prefix"]
async fn minio_snapshot_capacity() {
    let (store, prefix) = storage();
    let sizes = std::env::var("SANCTUM_BENCHMARK_MIB").unwrap_or_else(|_| "1024,2048".into());
    let publication = Publication::new(store.clone(), Duration::from_secs(300)).unwrap();
    for size in sizes.split(',').map(|size| size.parse::<u32>().unwrap()) {
        let scope = Scope {
            tenant: "benchmark".parse().unwrap(),
            collection: format!("snapshot-{size}").parse().unwrap(),
        };
        let unit = Unit::collection(Some(&prefix), &scope).unwrap();
        let start = Instant::now();
        let generation = publish_fixture(&publication, &unit, size, 0).await;
        let publish_ms = start.elapsed().as_millis();

        let mut samples = Vec::new();
        let mut warm = Vec::new();
        let mut sqlite = Vec::new();
        // Budget for one snapshot so the full nominal size can be warmed.
        for _ in 0..3 {
            let cache = SnapshotCache::new(generation.bytes);
            let start = Instant::now();
            let local = cache.read(&publication, &unit).await.unwrap().unwrap();
            samples.push(start.elapsed().as_millis());
            let start = Instant::now();
            assert_marker(&local, 0);
            sqlite.push(start.elapsed().as_micros());
            let start = Instant::now();
            let reused = cache.read(&publication, &unit).await.unwrap().unwrap();
            assert!(Arc::ptr_eq(&local, &reused));
            warm.push(start.elapsed().as_micros());
        }

        let start = Instant::now();
        let mut lease = publication.acquire(&unit).await.unwrap();
        let (changed, _) = lease
            .apply(
                vec![Command {
                    operation: Uuid::new_v4(),
                    payload: (),
                }],
                |tx, ()| {
                    tx.execute("UPDATE marker SET version = version + 1", [])
                        .map_err(sql)?;
                    Ok("updated".into())
                },
            )
            .await
            .unwrap();
        lease.release().await.unwrap();
        let update_ms = start.elapsed().as_millis();
        assert_ne!(generation.id, changed.id);
        let data_key = Key::parse(format!(
            "{prefix}/{}/collections/{}/snapshots/{}/meta.db",
            scope.tenant, scope.collection, changed.id
        ))
        .unwrap();
        assert_eq!(store.head(&data_key).await.unwrap().length, changed.bytes);
        println!(
            "capacity requested_mib={size} snapshot_bytes={} published_ms={publish_ms} cold_materialize_ms={samples:?} warm_resolve_us={warm:?} sqlite_open_and_point_query_us={sqlite:?} update_snapshot_bytes={} update_ms={update_ms}",
            generation.bytes, changed.bytes
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "publishes two multi-GiB fixtures to a dedicated MinIO benchmark prefix"]
async fn minio_concurrent_snapshot_capacity() {
    let (store, prefix) = storage();
    let size = std::env::var("SANCTUM_CONCURRENT_SNAPSHOT_MIB")
        .map(|size| size.parse::<u32>().unwrap())
        .unwrap_or(1024);
    let publication = Publication::new(store, Duration::from_secs(300)).unwrap();
    let units = ["snapshot-a", "snapshot-b"].map(|name| {
        let scope = Scope {
            tenant: "benchmark".parse().unwrap(),
            collection: name.parse().unwrap(),
        };
        Unit::collection(Some(&prefix), &scope).unwrap()
    });
    let left = publish_fixture(&publication, &units[0], size, 1).await;
    let right = publish_fixture(&publication, &units[1], size, 2).await;
    let bytes = [left.bytes, right.bytes];
    let combined = bytes.iter().sum::<u64>();
    let mut cold = Vec::new();
    let mut warm = Vec::new();
    let mut sqlite = Vec::new();
    for _ in 0..3 {
        let cache = SnapshotCache::new(combined);
        let (a, b) = tokio::join!(
            time_read(&publication, &cache, &units[0], 1),
            time_read(&publication, &cache, &units[1], 2),
        );
        cold.push([a.1 / 1000, b.1 / 1000]);
        sqlite.push([a.2, b.2]);
        assert_eq!(a.0.generation.id, left.id);
        assert_eq!(b.0.generation.id, right.id);
        let (reused_a, reused_b) = tokio::join!(
            time_read(&publication, &cache, &units[0], 1),
            time_read(&publication, &cache, &units[1], 2),
        );
        assert!(Arc::ptr_eq(&a.0, &reused_a.0));
        assert!(Arc::ptr_eq(&b.0, &reused_b.0));
        warm.push([reused_a.1, reused_b.1]);
    }

    let smaller = SnapshotCache::new(combined - 1);
    let (a, _, _) = time_read(&publication, &smaller, &units[0], 1).await;
    let (b, _, _) = time_read(&publication, &smaller, &units[1], 2).await;
    assert_marker(&a, 1);
    assert_marker(&b, 2);
    let (reused, reuse_us, _) = time_read(&publication, &smaller, &units[0], 1).await;
    assert!(Arc::ptr_eq(&a, &reused));
    let retained_reader_bytes: u64 = [&a, &b]
        .iter()
        .map(|file| file.path().metadata().unwrap().len())
        .sum();
    assert_eq!(retained_reader_bytes, combined);
    println!(
        "concurrent requested_mib_each={size} snapshot_bytes={bytes:?} both_fit_budget={combined} cold_ms={cold:?} warm_us={warm:?} sqlite_open_and_point_query_us={sqlite:?} smaller_budget={} reuse_after_eviction_us={reuse_us} files_held_by_readers_bytes={retained_reader_bytes}",
        combined - 1,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "publishes a multi-GiB fixture to a dedicated MinIO benchmark prefix"]
async fn minio_shared_oversized_snapshot_capacity() {
    let (store, prefix) = storage();
    let size = std::env::var("SANCTUM_CONCURRENT_SNAPSHOT_MIB")
        .map(|size| size.parse::<u32>().unwrap())
        .unwrap_or(2048);
    let publication = Publication::new(store, Duration::from_secs(300)).unwrap();
    let scope = Scope {
        tenant: "benchmark".parse().unwrap(),
        collection: "shared-snapshot".parse().unwrap(),
    };
    let unit = Unit::collection(Some(&prefix), &scope).unwrap();
    let generation = publish_fixture(&publication, &unit, size, 1).await;
    let budget = generation
        .bytes
        .saturating_sub(1)
        .min(2 * 1024 * 1024 * 1024);
    let cache = SnapshotCache::new(budget);
    let start = Instant::now();
    let (first, second) = tokio::join!(
        time_read(&publication, &cache, &unit, 1),
        time_read(&publication, &cache, &unit, 1),
    );
    let shared_ms = start.elapsed().as_millis();
    assert_eq!(first.0.generation, generation);
    assert!(Arc::ptr_eq(&first.0, &second.0));
    let path = first.0.path().to_owned();
    let active_bytes = path.metadata().unwrap().len();
    assert_eq!(active_bytes, generation.bytes);
    let (overlapping, overlapping_us, _) = time_read(&publication, &cache, &unit, 1).await;
    assert!(Arc::ptr_eq(&first.0, &overlapping));
    drop(first);
    drop(second);
    drop(overlapping);
    assert!(!path.exists());
    let (next, repeat_us, _) = time_read(&publication, &cache, &unit, 1).await;
    assert_eq!(next.generation, generation);
    assert_ne!(next.path(), path);
    println!(
        "shared_oversized requested_mib={size} snapshot_bytes={} budget={budget} active_bytes={active_bytes} shared_ms={shared_ms} overlap_after_download_us={overlapping_us} repeat_ms={}",
        generation.bytes,
        repeat_us / 1000,
    );
}

async fn time_read(
    publication: &Publication,
    cache: &SnapshotCache,
    unit: &Unit,
    expected_marker: u32,
) -> (Arc<Materialized>, u128, u128) {
    let start = Instant::now();
    let file = cache.read(publication, unit).await.unwrap().unwrap();
    let elapsed = start.elapsed().as_micros();
    let start = Instant::now();
    assert_marker(&file, expected_marker);
    (file, elapsed, start.elapsed().as_micros())
}

fn assert_marker(file: &Materialized, expected: u32) {
    let database = rusqlite::Connection::open_with_flags(
        file.path(),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let marker: u32 = database
        .query_row("SELECT version FROM marker", [], |row| row.get(0))
        .unwrap();
    assert_eq!(marker, expected);
}

fn storage() -> (Arc<S3Storage>, Key) {
    let endpoint = std::env::var("SANCTUM_TEST_S3_ENDPOINT").unwrap();
    let bucket = std::env::var("SANCTUM_TEST_S3_BUCKET").unwrap();
    let prefix = Key::parse(std::env::var("SANCTUM_BENCHMARK_PREFIX").unwrap()).unwrap();
    let store = S3Storage::new(S3Config {
        bucket,
        region: "us-east-1".into(),
        endpoint: Some(endpoint),
        allow_http: true,
        virtual_hosted_style: false,
    })
    .unwrap();
    (Arc::new(store), prefix)
}

async fn publish_fixture(
    publication: &Publication,
    unit: &Unit,
    size: u32,
    marker: u32,
) -> Generation {
    let mut lease = publication.acquire(unit).await.unwrap();
    let (generation, _) = lease
        .apply(
            vec![Command {
                operation: Uuid::new_v4(),
                payload: size,
            }],
            move |tx, size| {
                tx.execute_batch(
                    "CREATE TABLE payload (id INTEGER PRIMARY KEY, body BLOB NOT NULL);
                    CREATE TABLE marker (version INTEGER NOT NULL);",
                )
                .map_err(sql)?;
                tx.execute("INSERT INTO marker VALUES (?1)", [marker])
                    .map_err(sql)?;
                let mut insert = tx
                    .prepare("INSERT INTO payload(body) VALUES (zeroblob(1048576))")
                    .map_err(sql)?;
                for _ in 0..size {
                    insert.execute([]).map_err(sql)?;
                }
                Ok("created".into())
            },
        )
        .await
        .unwrap();
    lease.release().await.unwrap();
    generation
}

fn sql(error: rusqlite::Error) -> sanctum_core::Error {
    sanctum_core::Error::Internal(Box::new(error))
}
