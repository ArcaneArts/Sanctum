use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use sanctum_core::Scope;
use sanctum_db::{Command, Publication, SnapshotCache, Unit};
use sanctum_storage::{Key, Storage};
use sanctum_storage_s3::{S3Config, S3Storage};
use uuid::Uuid;

#[tokio::test]
#[ignore = "publishes multi-GiB snapshots to a dedicated MinIO benchmark prefix"]
async fn minio_snapshot_capacity() {
    let endpoint = std::env::var("SANCTUM_TEST_S3_ENDPOINT").unwrap();
    let bucket = std::env::var("SANCTUM_TEST_S3_BUCKET").unwrap();
    let prefix = Key::parse(std::env::var("SANCTUM_BENCHMARK_PREFIX").unwrap()).unwrap();
    let sizes = std::env::var("SANCTUM_BENCHMARK_MIB").unwrap_or_else(|_| "1024,2048".into());
    let store = Arc::new(
        S3Storage::new(S3Config {
            bucket,
            region: "us-east-1".into(),
            endpoint: Some(endpoint),
            allow_http: true,
            virtual_hosted_style: false,
        })
        .unwrap(),
    );
    let publication = Publication::new(store.clone(), Duration::from_secs(300)).unwrap();
    for size in sizes.split(',').map(|size| size.parse::<u32>().unwrap()) {
        let scope = Scope {
            tenant: "benchmark".parse().unwrap(),
            collection: format!("snapshot-{size}").parse().unwrap(),
        };
        let unit = Unit::collection(Some(&prefix), &scope).unwrap();
        let start = Instant::now();
        let mut lease = publication.acquire(&unit).await.unwrap();
        let (generation, _) = lease
            .apply(
                vec![Command {
                    operation: Uuid::new_v4(),
                    payload: size,
                }],
                |tx, size| {
                    tx.execute_batch(
                        "CREATE TABLE payload (id INTEGER PRIMARY KEY, body BLOB NOT NULL);
                    CREATE TABLE marker (version INTEGER NOT NULL); INSERT INTO marker VALUES (0);",
                    )
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
            let db = rusqlite::Connection::open_with_flags(
                local.path(),
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )
            .unwrap();
            let version: u32 = db
                .query_row("SELECT version FROM marker", [], |row| row.get(0))
                .unwrap();
            assert_eq!(version, 0);
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

fn sql(error: rusqlite::Error) -> sanctum_core::Error {
    sanctum_core::Error::Internal(Box::new(error))
}
