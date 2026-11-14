use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rusqlite::{Connection, Transaction, params};
use sanctum_core::{Error, Scope};
use sanctum_db::{Command, Publication, Unit};
use sanctum_storage::{Key, Storage};
use tokio::{io::AsyncReadExt, sync::oneshot};
use uuid::Uuid;

const LOCAL_TAKEOVER_LEASE: Duration = Duration::from_millis(650);

#[tokio::test]
async fn filesystem_publication_contract() {
    let directory = tempfile::tempdir().unwrap();
    let storage = sanctum_storage_fs::FileStorage::open(directory.path()).unwrap();
    verify(
        Arc::new(storage),
        "publication",
        Some(directory.path()),
        LOCAL_TAKEOVER_LEASE,
    )
    .await;
}

#[tokio::test]
#[ignore = "requires a live MinIO bucket and S3 credentials"]
async fn minio_publication_contract() {
    let storage = minio_storage();
    let prefix = format!("publication-{}", Uuid::new_v4());
    verify(storage, &prefix, None, LOCAL_TAKEOVER_LEASE).await;
}

#[tokio::test]
#[ignore = "requires an authorized GCS test bucket and Application Default Credentials"]
async fn gcs_publication_contract() {
    let bucket = std::env::var("SANCTUM_GCS_BUCKET").expect("SANCTUM_GCS_BUCKET");
    let storage = sanctum_storage_gcs::GcsStorage::new(bucket).unwrap();
    let prefix = format!("publication-{}", Uuid::new_v4());
    verify(Arc::new(storage), &prefix, None, Duration::from_secs(5)).await;
}

fn minio_storage() -> Arc<dyn Storage> {
    use sanctum_storage_s3::{S3Config, S3Storage};
    Arc::new(
        S3Storage::new(S3Config {
            bucket: std::env::var("SANCTUM_MINIO_BUCKET").expect("SANCTUM_MINIO_BUCKET"),
            region: "us-east-1".into(),
            endpoint: Some(
                std::env::var("SANCTUM_MINIO_ENDPOINT").expect("SANCTUM_MINIO_ENDPOINT"),
            ),
            allow_http: true,
            virtual_hosted_style: false,
        })
        .unwrap(),
    )
}

#[tokio::test]
async fn filesystem_active_writer_renews_through_slow_io() {
    let directory = tempfile::tempdir().unwrap();
    let storage = sanctum_storage_fs::FileStorage::open(directory.path()).unwrap();
    verify_active_writer(Arc::new(storage), "slow-publication").await;
}

#[tokio::test]
#[ignore = "requires a live MinIO bucket and S3 credentials"]
async fn minio_active_writer_renews_through_slow_io() {
    let prefix = format!("slow-publication-{}", Uuid::new_v4());
    verify_active_writer(minio_storage(), &prefix).await;
}

async fn verify_active_writer(storage: Arc<dyn Storage>, prefix: &str) {
    let prefix = Key::parse(prefix).unwrap();
    let unit = Unit::catalog(Some(&prefix), &"default".parse().unwrap()).unwrap();
    let ttl = Duration::from_millis(700);
    let slow = Duration::from_millis(1050);
    let store = Arc::new(PublicationStorage::new(storage.clone()));
    let writer = Publication::new(store.clone(), ttl).unwrap();
    let reader = Publication::new(storage.clone(), Duration::from_secs(15)).unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let mut first = reader.acquire(&unit).await.unwrap();
    let (initial, _) = first
        .apply(vec![command(Uuid::new_v4(), 7)], apply_value)
        .await
        .unwrap();
    first.release().await.unwrap();
    let mut lease = writer.acquire(&unit).await.unwrap();
    store.transfer_delay_ms.store(
        slow.as_millis() as usize,
        std::sync::atomic::Ordering::Relaxed,
    );
    let (finish, final_upload) = tokio::sync::oneshot::channel();
    *store.upload_hold.lock().unwrap() = Some(final_upload);
    let writing = tokio::spawn(async move {
        let result = lease
            .apply(vec![command(Uuid::new_v4(), 5)], move |tx, value| {
                std::thread::sleep(slow);
                apply_value(tx, value)
            })
            .await;
        lease.release().await.unwrap();
        result
    });
    // Each of the download, SQLite, and upload phases exceeds the lease.
    for _ in 0..3 {
        tokio::time::sleep(ttl + Duration::from_millis(30)).await;
        assert_eq!(
            reader.generation(&unit).await.unwrap(),
            Some(initial.clone())
        );
        assert!(matches!(reader.acquire(&unit).await, Err(Error::Busy(_))));
    }
    finish.send(()).unwrap();
    let (published, receipts) = writing.await.unwrap().unwrap();
    assert_eq!(receipts[0].outcome, "12");
    assert_eq!(reader.generation(&unit).await.unwrap(), Some(published));
    assert_eq!(
        value(
            reader
                .read(&unit, scratch.path())
                .await
                .unwrap()
                .unwrap()
                .path()
        ),
        12
    );
    delete_prefix(storage.as_ref(), &prefix).await;
}

#[tokio::test]
async fn filesystem_snapshot_preparation_observes_cancellation() {
    let directory = tempfile::tempdir().unwrap();
    let storage = sanctum_storage_fs::FileStorage::open(directory.path()).unwrap();
    verify_canceled_preparation(Arc::new(storage), "canceled-preparation").await;
}

#[tokio::test]
#[ignore = "requires a live MinIO bucket and S3 credentials"]
async fn minio_snapshot_preparation_observes_cancellation() {
    let prefix = format!("canceled-preparation-{}", Uuid::new_v4());
    verify_canceled_preparation(minio_storage(), &prefix).await;
}

async fn verify_canceled_preparation(storage: Arc<dyn Storage>, prefix: &str) {
    let prefix = Key::parse(prefix).unwrap();
    let unit = Unit::catalog(Some(&prefix), &"default".parse().unwrap()).unwrap();
    let store = Arc::new(PublicationStorage::new(storage.clone()));
    let ttl = LOCAL_TAKEOVER_LEASE;
    let short = Publication::new(store.clone(), ttl).unwrap();
    let long = Publication::new(storage.clone(), Duration::from_secs(15)).unwrap();

    for (lose_lease, expected) in [(false, 7), (true, 14)] {
        let initial = long.generation(&unit).await.unwrap();
        let mut lease = short.acquire(&unit).await.unwrap();
        let operation = Uuid::new_v4();
        let (started, started_rx) = oneshot::channel();
        let (resume, resume_rx) = std::sync::mpsc::channel();
        let (stopped, stopped_rx) = oneshot::channel();
        let mut started = Some(started);
        let mut stopped = Some(stopped);
        let mut prepare = Box::pin(lease.apply_cancellable_stage(
            "index",
            vec![command(operation, 100)],
            move |tx, payload, checkpoint| {
                let outcome = apply_value(tx, payload)?;
                started.take().unwrap().send(()).unwrap();
                resume_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                let canceled = checkpoint();
                let interrupted = tx.query_row(
                    "WITH RECURSIVE seq(i) AS (
                        VALUES(0) UNION ALL SELECT i + 1 FROM seq WHERE i < 10000000
                    ) SELECT max(i) FROM seq",
                    [],
                    |row| row.get::<_, i64>(0),
                );
                stopped
                    .take()
                    .unwrap()
                    .send((
                        matches!(canceled, Err(Error::Busy(_))),
                        interrupted
                            .err()
                            .and_then(|error| error.sqlite_error_code()),
                    ))
                    .unwrap();
                Ok(outcome)
            },
        ));
        tokio::select! {
            result = &mut prepare => panic!("preparation finished early: {result:?}"),
            started = started_rx => started.unwrap(),
        }
        if lose_lease {
            let (_hold, reply) = oneshot::channel();
            *store.control_reply_hold.lock().unwrap() = Some(reply);
            let result = tokio::time::timeout(ttl * 3, &mut prepare).await.unwrap();
            assert!(matches!(result, Err(Error::StaleRevision(_))));
        }
        drop(prepare);
        resume.send(()).unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), stopped_rx)
                .await
                .unwrap()
                .unwrap(),
            (true, Some(rusqlite::ErrorCode::OperationInterrupted)),
        );
        if lose_lease {
            assert!(matches!(
                lease.release().await,
                Err(Error::StaleRevision(_))
            ));
            // The renewal reached storage before its reply was lost.
            tokio::time::sleep(ttl).await;
        } else {
            lease.release().await.unwrap();
        }

        assert_eq!(long.generation(&unit).await.unwrap(), initial);
        assert_eq!(
            long.stage_receipt(&unit, operation, "index").await.unwrap(),
            None
        );
        let mut retry = long.acquire(&unit).await.unwrap();
        let (published, receipts) = retry
            .apply_stage("index", vec![command(operation, 7)], apply_value)
            .await
            .unwrap();
        retry.release().await.unwrap();
        assert_eq!(receipts[0].outcome, expected.to_string());
        assert_eq!(long.generation(&unit).await.unwrap(), Some(published));
    }
    delete_prefix(storage.as_ref(), &prefix).await;
}

#[tokio::test]
async fn filesystem_reclamation_renews_through_slow_io() {
    let directory = tempfile::tempdir().unwrap();
    let storage = sanctum_storage_fs::FileStorage::open(directory.path()).unwrap();
    verify_reclamation_lease(Arc::new(storage), "slow-reclamation").await;
}

#[tokio::test]
#[ignore = "requires a live MinIO bucket and S3 credentials"]
async fn minio_reclamation_renews_through_slow_io() {
    let prefix = format!("slow-reclamation-{}", Uuid::new_v4());
    verify_reclamation_lease(minio_storage(), &prefix).await;
}

async fn verify_reclamation_lease(storage: Arc<dyn Storage>, prefix: &str) {
    let prefix = Key::parse(prefix).unwrap();
    let unit = Unit::catalog(Some(&prefix), &"default".parse().unwrap()).unwrap();
    let grace = Duration::from_secs(300);
    let ttl = Duration::from_millis(500);
    let store = Arc::new(PublicationStorage::new(storage.clone()));
    let sweep = Publication::new(store.clone(), ttl).unwrap();
    let other = Publication::new(storage.clone(), Duration::from_secs(15)).unwrap();
    let mut writer = other.acquire(&unit).await.unwrap();
    let (current, _) = writer
        .apply(vec![command(Uuid::new_v4(), 7)], apply_value)
        .await
        .unwrap();
    writer.release().await.unwrap();
    let orphan = Key::parse(format!(
        "{prefix}/default/catalog/snapshots/{}/root.db",
        Uuid::new_v4()
    ))
    .unwrap();
    storage
        .put_control(
            &orphan,
            bytes::Bytes::from_static(b"orphan"),
            sanctum_storage::Condition::Create,
        )
        .await
        .unwrap();
    store.cleanup_delay_ms.store(800, Ordering::Relaxed);
    let (listing, listed) = oneshot::channel();
    *store.list_started.lock().unwrap() = Some(listing);
    let interrupted = tokio::spawn({
        let sweep = sweep.clone();
        let unit = unit.clone();
        async move { sweep.reclaim(&unit, grace, None).await }
    });
    tokio::time::timeout(Duration::from_secs(5), listed)
        .await
        .unwrap()
        .unwrap();
    let (finish, response) = oneshot::channel();
    *store.control_reply_hold.lock().unwrap() = Some(response);
    assert!(matches!(
        interrupted.await.unwrap(),
        Err(Error::StaleRevision(_))
    ));
    assert!(finish.send(()).is_err());
    let marker = Key::parse(format!(
        "{}/retired.json",
        orphan.as_str().strip_suffix("/root.db").unwrap()
    ))
    .unwrap();
    assert!(matches!(
        storage.head(&marker).await,
        Err(Error::NotFound(_))
    ));
    // The renewal may have committed before its response was lost.
    tokio::time::sleep(ttl).await;

    let (listing, listed) = oneshot::channel();
    *store.list_started.lock().unwrap() = Some(listing);
    let active = tokio::spawn({
        let sweep = sweep.clone();
        let unit = unit.clone();
        async move { sweep.reclaim(&unit, grace, None).await }
    });
    tokio::time::timeout(Duration::from_secs(5), listed)
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(ttl + Duration::from_millis(50)).await;
    assert!(matches!(other.acquire(&unit).await, Err(Error::Busy(_))));
    assert_eq!(active.await.unwrap().unwrap().deleted, 0);
    storage.head(&marker).await.unwrap();
    assert_eq!(other.generation(&unit).await.unwrap(), Some(current));

    other.fence_deletion(&unit).await.unwrap();
    let head = Key::parse(format!("{prefix}/default/catalog/HEAD.json")).unwrap();
    let record = storage.get(&head).await.unwrap();
    let mut bytes = Vec::new();
    record
        .reader
        .take(1024 * 1024)
        .read_to_end(&mut bytes)
        .await
        .unwrap();
    let mut manifest: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    manifest["deleted_at_ms"] = serde_json::json!(
        (SystemTime::now() - grace - Duration::from_secs(1))
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );
    storage
        .put_control(
            &head,
            serde_json::to_vec(&manifest).unwrap().into(),
            sanctum_storage::Condition::Match(record.object.version),
        )
        .await
        .unwrap();
    let page = sweep.reclaim_retired(&unit, grace, None).await.unwrap();
    assert_eq!(page.deleted, 3);
    assert!(page.next_cursor.is_none());
    let remaining = storage.list(Some(&prefix), None, 10).await.unwrap();
    assert_eq!(remaining.objects.len(), 1);
    assert_eq!(remaining.objects[0].key, head);
    assert!(other.is_retired(&unit).await.unwrap());
    assert!(other.generation(&unit).await.unwrap().is_none());
    delete_prefix(storage.as_ref(), &prefix).await;
}

#[tokio::test]
async fn late_renewal_response_requires_reacquiring_the_lease() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(PublicationStorage::new(Arc::new(
        sanctum_storage_fs::FileStorage::open(directory.path()).unwrap(),
    )));
    let unit = Unit::catalog(None, &"default".parse().unwrap()).unwrap();
    let ttl = Duration::from_millis(250);
    let publication = Publication::new(store.clone(), ttl).unwrap();
    let mut lease = publication.acquire(&unit).await.unwrap();
    let (finish, late_reply) = tokio::sync::oneshot::channel();
    *store.control_reply_hold.lock().unwrap() = Some(late_reply);
    let operation = Uuid::new_v4();
    assert!(matches!(
        lease.apply(vec![command(operation, 4)], apply_value).await,
        Err(Error::StaleRevision(_))
    ));
    assert!(finish.send(()).is_err());
    assert!(matches!(lease.renew().await, Err(Error::StaleRevision(_))));
    assert!(publication.generation(&unit).await.unwrap().is_none());

    // The timed-out renewal may have committed. Let its ownership expire.
    tokio::time::sleep(ttl).await;
    let next = Publication::new(store, Duration::from_secs(15)).unwrap();
    let mut winner = next.acquire(&unit).await.unwrap();
    let (_, receipts) = winner
        .apply(vec![command(operation, 4)], apply_value)
        .await
        .unwrap();
    assert_eq!(receipts[0].outcome, "4");
    winner.release().await.unwrap();
}

#[tokio::test]
async fn filesystem_late_publication_response_recovers_the_committed_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(sanctum_storage_fs::FileStorage::open(dir.path()).unwrap());
    verify_late_publication(storage, "late-publication").await;
}

#[tokio::test]
#[ignore = "requires a live MinIO bucket and S3 credentials"]
async fn minio_late_publication_response_recovers_the_committed_receipt() {
    let prefix = format!("late-publication-{}", Uuid::new_v4());
    verify_late_publication(minio_storage(), &prefix).await;
}

async fn verify_late_publication(storage: Arc<dyn Storage>, prefix: &str) {
    let prefix = Key::parse(prefix).unwrap();
    let unit = Unit::catalog(Some(&prefix), &"default".parse().unwrap()).unwrap();
    let store = Arc::new(PublicationStorage::new(storage.clone()));
    let ttl = Duration::from_millis(900);
    let publication = Publication::new(store.clone(), ttl).unwrap();
    let mut writer = publication.acquire(&unit).await.unwrap();
    let (finish, late_reply) = oneshot::channel();
    *store.publication_reply_hold.lock().unwrap() = Some(late_reply);
    let operation = Uuid::new_v4();
    let result = tokio::time::timeout(
        ttl * 3,
        writer.apply(vec![command(operation, 7)], apply_value),
    )
    .await
    .expect("publication must return even when storage withholds its reply");
    assert!(matches!(result, Err(Error::StaleRevision(_))));
    assert!(finish.send(()).is_err());
    assert!(matches!(writer.renew().await, Err(Error::StaleRevision(_))));

    let next = Publication::new(storage.clone(), Duration::from_secs(15)).unwrap();
    let generation = next.generation(&unit).await.unwrap().unwrap();
    assert_eq!(
        next.receipt(&unit, operation).await.unwrap().as_deref(),
        Some("7")
    );
    let mut retry = next.acquire(&unit).await.unwrap();
    let (replayed, receipts) = retry
        .apply(vec![command(operation, 999)], |_, _| {
            panic!("the committed command must not run again")
        })
        .await
        .unwrap();
    assert_eq!(replayed, generation);
    assert_eq!(receipts[0].outcome, "7");
    retry.release().await.unwrap();
    delete_prefix(storage.as_ref(), &prefix).await;
}

#[tokio::test]
async fn a_timed_out_acquisition_and_release_are_reconciled_by_the_next_writer() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(PublicationStorage::new(Arc::new(
        sanctum_storage_fs::FileStorage::open(dir.path()).unwrap(),
    )));
    let unit = Unit::catalog(None, &"default".parse().unwrap()).unwrap();
    let ttl = Duration::from_millis(300);
    let publication = Publication::new(store.clone(), ttl).unwrap();

    let (finish, acquisition_reply) = oneshot::channel();
    *store.control_reply_hold.lock().unwrap() = Some(acquisition_reply);
    let result = tokio::time::timeout(ttl * 3, publication.acquire(&unit))
        .await
        .expect("acquisition must stop at its deadline");
    assert!(matches!(result, Err(Error::Busy(_))));
    assert!(finish.send(()).is_err());
    let writer = publication.acquire(&unit).await.unwrap();
    assert_eq!(writer.epoch(), 2);

    let (finish, release_reply) = oneshot::channel();
    *store.control_reply_hold.lock().unwrap() = Some(release_reply);
    let result = tokio::time::timeout(ttl * 3, writer.release())
        .await
        .expect("release must stop at its deadline");
    assert!(matches!(result, Err(Error::StaleRevision(_))));
    assert!(finish.send(()).is_err());
    let writer = publication.acquire(&unit).await.unwrap();
    assert_eq!(writer.epoch(), 3);
    writer.release().await.unwrap();
}

#[tokio::test]
async fn filesystem_snapshot_delta_contract() {
    let directory = tempfile::tempdir().unwrap();
    let storage = sanctum_storage_fs::FileStorage::open(directory.path()).unwrap();
    verify_deltas(
        Arc::new(storage),
        "delta-publication",
        Some(directory.path()),
    )
    .await;
}

#[tokio::test]
#[ignore = "requires a live MinIO bucket and S3 credentials"]
async fn minio_snapshot_delta_contract() {
    let prefix = format!("delta-publication-{}", Uuid::new_v4());
    verify_deltas(minio_storage(), &prefix, None).await;
}

#[tokio::test]
#[ignore = "requires an authorized GCS test bucket and Application Default Credentials"]
async fn gcs_snapshot_delta_contract() {
    let bucket = std::env::var("SANCTUM_GCS_BUCKET").expect("SANCTUM_GCS_BUCKET");
    let storage = sanctum_storage_gcs::GcsStorage::new(bucket).unwrap();
    let prefix = format!("delta-publication-{}", Uuid::new_v4());
    verify_deltas(Arc::new(storage), &prefix, None).await;
}

async fn verify_deltas(
    storage: Arc<dyn Storage>,
    prefix: &str,
    local_root: Option<&std::path::Path>,
) {
    let prefix = Key::parse(prefix).unwrap();
    let unit = Unit::catalog(Some(&prefix), &"default".parse().unwrap()).unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let reader = Publication::new(storage.clone(), Duration::from_secs(30)).unwrap();
    let writer = reader
        .clone()
        .with_snapshot_cache(Arc::new(sanctum_db::SnapshotCache::new(1024 * 1024)))
        .with_page_deltas(true);
    let mut lease = writer.acquire(&unit).await.unwrap();
    let (base, _) = lease
        .apply(vec![command(Uuid::new_v4(), 0)], |tx, _| {
            tx.execute_batch(
                "CREATE TABLE padding (body BLOB); INSERT INTO padding VALUES (zeroblob(524288));",
            )
            .map_err(|error| Error::Internal(Box::new(error)))?;
            apply_value(tx, 0)
        })
        .await
        .unwrap();
    lease.release().await.unwrap();

    let key =
        |id, file| Key::parse(format!("{prefix}/default/catalog/snapshots/{id}/{file}")).unwrap();
    assert_eq!(
        storage.head(&key(base.id, "root.db")).await.unwrap().length,
        base.bytes
    );
    let sessions = sanctum_db::SnapshotCache::new(0);
    let mut chain = Vec::new();
    for step in 1..=5 {
        let operation = Uuid::new_v4();
        let mut lease = writer.acquire(&unit).await.unwrap();
        let (generation, _) = lease
            .apply(vec![command(operation, 1)], apply_value)
            .await
            .unwrap();
        let (replayed, receipts) = lease
            .apply(vec![command(operation, 1000)], apply_value)
            .await
            .unwrap();
        assert_eq!(replayed, generation);
        assert_eq!(receipts[0].outcome, step.to_string());
        lease.release().await.unwrap();
        if step < 5 {
            let patch = storage
                .head(&key(generation.id, "pages.delta"))
                .await
                .unwrap();
            assert!(patch.length < generation.bytes / 2);
            chain.push(generation.clone());
        } else {
            assert_eq!(
                storage
                    .head(&key(generation.id, "root.db"))
                    .await
                    .unwrap()
                    .length,
                generation.bytes
            );
        }
        let cold = reader.read(&unit, scratch.path()).await.unwrap().unwrap();
        assert_eq!(cold.generation, generation);
        assert_eq!(value(cold.path()), step);

        let cleanup = reader
            .reclaim(&unit, Duration::from_secs(300), None)
            .await
            .unwrap();
        assert_eq!(cleanup.deleted, 0);
        let base_marker = key(base.id, "retired.json");
        if step < 5 {
            assert!(matches!(
                storage.head(&base_marker).await,
                Err(Error::NotFound(_))
            ));
        } else {
            assert!(storage.head(&base_marker).await.is_ok());
        }
    }

    // A session opened before compaction can still replay its own generation.
    let oldest_delta = sessions
        .read_generation(&reader, &unit, chain[0].clone())
        .await
        .unwrap();
    assert_eq!(value(oldest_delta.path()), 1);
    assert_eq!(
        value(
            sessions
                .read_generation(&reader, &unit, chain[3].clone())
                .await
                .unwrap()
                .path()
        ),
        4
    );

    if let Some(root) = local_root {
        let grace = Duration::from_secs(300);
        for id in std::iter::once(base.id).chain(chain.iter().map(|item| item.id)) {
            let marker = key(id, "retired.json");
            std::fs::OpenOptions::new()
                .write(true)
                .open(root.join(marker.as_str()))
                .unwrap()
                .set_modified(std::time::SystemTime::now() - grace - Duration::from_secs(1))
                .unwrap();
        }
        let cleanup = reader.reclaim(&unit, grace, None).await.unwrap();
        assert_eq!(cleanup.deleted, 5);
        assert!(matches!(
            storage.head(&key(base.id, "root.db")).await,
            Err(Error::NotFound(_))
        ));
        assert_eq!(value(oldest_delta.path()), 1);
        assert_eq!(
            value(
                reader
                    .read(&unit, scratch.path())
                    .await
                    .unwrap()
                    .unwrap()
                    .path()
            ),
            5
        );
    }
    delete_prefix(storage.as_ref(), &prefix).await;
}

#[tokio::test]
async fn recovering_from_a_lost_publish_response_reuses_its_receipt() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Arc::new(PublicationStorage::new(Arc::new(
        sanctum_storage_fs::FileStorage::open(directory.path()).unwrap(),
    )));
    storage
        .lose
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let unit = Unit::catalog(None, &"default".parse().unwrap()).unwrap();
    let cache = Arc::new(sanctum_db::SnapshotCache::new(1024 * 1024));
    let short = Publication::new(storage.clone(), Duration::from_millis(400))
        .unwrap()
        .with_snapshot_cache(cache.clone());
    let long = Publication::new(storage, Duration::from_secs(15))
        .unwrap()
        .with_snapshot_cache(cache);
    let mut writer = short.acquire(&unit).await.unwrap();
    let operation = Uuid::new_v4();
    let result = writer.apply(vec![command(operation, 7)], apply_value).await;
    assert!(matches!(result, Err(Error::Busy(_))));
    assert!(matches!(writer.renew().await, Err(Error::StaleRevision(_))));
    let scratch = tempfile::tempdir().unwrap();
    let committed = long.read(&unit, scratch.path()).await.unwrap().unwrap();
    assert_eq!(value(committed.path()), 7);
    tokio::time::sleep(Duration::from_millis(450)).await;
    let mut retry = long.acquire(&unit).await.unwrap();
    let (generation, receipt) = retry
        .apply(vec![command(operation, 999)], apply_value)
        .await
        .unwrap();
    assert_eq!(generation, committed.generation);
    assert_eq!(receipt[0].outcome, "7");
    retry.release().await.unwrap();
}

#[tokio::test]
async fn snapshot_cache_reuses_verified_files_and_preserves_active_readers() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Arc::new(sanctum_storage_fs::FileStorage::open(directory.path()).unwrap());
    let publication = Publication::new(storage, Duration::from_secs(15)).unwrap();
    let unit = Unit::catalog(None, &"first".parse().unwrap()).unwrap();
    let other = Unit::catalog(None, &"second".parse().unwrap()).unwrap();

    let mut writer = publication.acquire(&unit).await.unwrap();
    let (first, _) = writer
        .apply(vec![command(Uuid::new_v4(), 1)], apply_value)
        .await
        .unwrap();
    writer.release().await.unwrap();
    let cache = sanctum_db::SnapshotCache::new(first.bytes);
    let (one, same) = tokio::join!(
        cache.read(&publication, &unit),
        cache.read(&publication, &unit)
    );
    let one = one.unwrap().unwrap();
    let same = same.unwrap().unwrap();
    assert!(Arc::ptr_eq(&one, &same));
    assert_eq!(value(one.path()), 1);

    let mut writer = publication.acquire(&unit).await.unwrap();
    let (second, _) = writer
        .apply(vec![command(Uuid::new_v4(), 2)], apply_value)
        .await
        .unwrap();
    writer.release().await.unwrap();
    let two = cache.read(&publication, &unit).await.unwrap().unwrap();
    assert_eq!(two.generation, second);
    assert_eq!(value(two.path()), 3);
    assert_eq!(value(one.path()), 1);

    let mut writer = publication.acquire(&other).await.unwrap();
    writer
        .apply(vec![command(Uuid::new_v4(), 9)], apply_value)
        .await
        .unwrap();
    writer.release().await.unwrap();
    let other = cache.read(&publication, &other).await.unwrap().unwrap();
    assert_eq!(value(other.path()), 9);
    let again = cache.read(&publication, &unit).await.unwrap().unwrap();
    assert_eq!(value(again.path()), 3);
    assert!(Arc::ptr_eq(&two, &again));
    let retained = again.path().to_owned();
    drop((two, again));
    assert!(retained.exists());

    let disabled = sanctum_db::SnapshotCache::new(0);
    let a = disabled.read(&publication, &unit).await.unwrap().unwrap();
    let b = disabled.read(&publication, &unit).await.unwrap().unwrap();
    assert!(Arc::ptr_eq(&a, &b));
    let path = a.path().to_owned();
    drop(a);
    drop(b);
    assert!(!path.exists());
    let reopened = disabled.read(&publication, &unit).await.unwrap().unwrap();
    assert_ne!(path, reopened.path());
}

#[tokio::test]
async fn oversized_snapshot_reads_share_the_file_until_the_last_reader_releases_it() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Arc::new(PublicationStorage::new(Arc::new(
        sanctum_storage_fs::FileStorage::open(directory.path()).unwrap(),
    )));
    let publication = Publication::new(storage.clone(), Duration::from_secs(15)).unwrap();
    let unit = Unit::catalog(None, &"default".parse().unwrap()).unwrap();
    let mut writer = publication.acquire(&unit).await.unwrap();
    let (generation, _) = writer
        .apply(vec![command(Uuid::new_v4(), 7)], apply_value)
        .await
        .unwrap();
    writer.release().await.unwrap();
    storage.transfer_delay_ms.store(50, Ordering::Relaxed);
    let cache = sanctum_db::SnapshotCache::new(generation.bytes - 1);
    let (first, second) = tokio::join!(
        cache.read_generation(&publication, &unit, generation.clone()),
        cache.read_generation(&publication, &unit, generation.clone()),
    );
    let first = first.unwrap();
    let second = second.unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(value(first.path()), 7);
    assert_eq!(storage.snapshot_gets.load(Ordering::Relaxed), 1);

    let next = cache
        .read_generation(&publication, &unit, generation.clone())
        .await
        .unwrap();
    assert_eq!(value(next.path()), 7);
    assert!(Arc::ptr_eq(&first, &next));
    assert_eq!(storage.snapshot_gets.load(Ordering::Relaxed), 1);
    let path = next.path().to_owned();
    drop((first, second, next));
    assert!(!path.exists());

    let next = cache
        .read_generation(&publication, &unit, generation)
        .await
        .unwrap();
    assert_ne!(path, next.path());
    assert_eq!(value(next.path()), 7);
    assert_eq!(storage.snapshot_gets.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn an_evicted_transfer_can_be_joined_and_a_canceled_leader_can_be_replaced() {
    use std::{
        future::{Future, poll_fn},
        task::Poll,
    };

    let directory = tempfile::tempdir().unwrap();
    let storage = Arc::new(PublicationStorage::new(Arc::new(
        sanctum_storage_fs::FileStorage::open(directory.path()).unwrap(),
    )));
    let publication = Publication::new(storage.clone(), Duration::from_secs(15)).unwrap();
    let units =
        ["first", "second"].map(|tenant| Unit::catalog(None, &tenant.parse().unwrap()).unwrap());
    let mut generations = Vec::new();
    for (index, unit) in units.iter().enumerate() {
        let mut writer = publication.acquire(unit).await.unwrap();
        let (generation, _) = writer
            .apply(vec![command(Uuid::new_v4(), index as i64 + 1)], apply_value)
            .await
            .unwrap();
        writer.release().await.unwrap();
        generations.push(generation);
    }
    assert_eq!(generations[0].bytes, generations[1].bytes);
    let cache = sanctum_db::SnapshotCache::new(generations[0].bytes);
    let read = |index: usize| {
        cache.read_generation(&publication, &units[index], generations[index].clone())
    };
    storage.transfer_delay_ms.store(3_000, Ordering::Relaxed);
    let mut first = Box::pin(read(0));
    let mut other = Box::pin(read(1));
    let mut follower = Box::pin(read(0));
    poll_fn(|cx| {
        // Poll all three while storage is stalled. The second lookup
        // evicts the first before the follower attempts to join it.
        assert!(first.as_mut().poll(cx).is_pending());
        assert!(other.as_mut().poll(cx).is_pending());
        assert!(follower.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(storage.snapshot_gets.load(Ordering::Relaxed), 2);
    drop(first);
    drop(other);
    storage.transfer_delay_ms.store(0, Ordering::Relaxed);
    let data = follower.await.unwrap();
    assert_eq!(value(data.path()), 1);
    assert_eq!(storage.snapshot_gets.load(Ordering::Relaxed), 3);
    let retained = read(0).await.unwrap();
    assert!(Arc::ptr_eq(&data, &retained));
    assert_eq!(storage.snapshot_gets.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn publication_reuses_immutable_snapshots_for_receipts_and_subsequent_writes() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Arc::new(PublicationStorage::new(Arc::new(
        sanctum_storage_fs::FileStorage::open(directory.path()).unwrap(),
    )));
    let unit = Unit::catalog(None, &"default".parse().unwrap()).unwrap();
    let cache = Arc::new(sanctum_db::SnapshotCache::new(1024 * 1024));
    let publication = Publication::new(storage.clone(), Duration::from_secs(15))
        .unwrap()
        .with_snapshot_cache(cache.clone());
    let first = Uuid::new_v4();
    let mut writer = publication.acquire(&unit).await.unwrap();
    let (generation, _) = writer
        .apply(vec![command(first, 5)], apply_value)
        .await
        .unwrap();
    writer.release().await.unwrap();
    let old = cache.read(&publication, &unit).await.unwrap().unwrap();
    assert_eq!(old.generation, generation);
    assert_eq!(
        publication.receipt(&unit, first).await.unwrap().as_deref(),
        Some("5")
    );
    let gets = || {
        storage
            .snapshot_gets
            .load(std::sync::atomic::Ordering::Relaxed)
    };
    assert_eq!(gets(), 0);

    let second = Uuid::new_v4();
    let mut writer = publication.acquire(&unit).await.unwrap();
    writer
        .apply(vec![command(second, 7)], apply_value)
        .await
        .unwrap();
    writer.release().await.unwrap();
    assert_eq!(value(old.path()), 5);
    assert_eq!(
        publication.receipt(&unit, second).await.unwrap().as_deref(),
        Some("12")
    );
    let current = cache.read(&publication, &unit).await.unwrap().unwrap();
    assert_eq!(value(current.path()), 12);
    assert_eq!(gets(), 0);

    let separate = Publication::new(storage.clone(), Duration::from_secs(15)).unwrap();
    let mut writer = separate.acquire(&unit).await.unwrap();
    writer
        .apply(vec![command(Uuid::new_v4(), 1)], apply_value)
        .await
        .unwrap();
    writer.release().await.unwrap();
    assert_eq!(gets(), 1);
    let fresh = cache.read(&publication, &unit).await.unwrap().unwrap();
    assert_eq!(value(fresh.path()), 13);
    assert_eq!(value(current.path()), 12);
    assert_eq!(gets(), 2);
}

struct PublicationStorage {
    inner: Arc<dyn Storage>,
    transfer_delay_ms: std::sync::atomic::AtomicUsize,
    upload_hold: std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    control_reply_hold: std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    publication_reply_hold: Mutex<Option<oneshot::Receiver<()>>>,
    lose: std::sync::atomic::AtomicBool,
    snapshot_gets: std::sync::atomic::AtomicUsize,
    fail_delta: std::sync::atomic::AtomicBool,
    fail_only_delta_head: std::sync::atomic::AtomicBool,
    cleanup_delay_ms: AtomicU64,
    list_started: Mutex<Option<oneshot::Sender<()>>>,
}

impl PublicationStorage {
    fn new(inner: Arc<dyn Storage>) -> Self {
        Self {
            inner,
            transfer_delay_ms: Default::default(),
            upload_hold: Default::default(),
            control_reply_hold: Default::default(),
            publication_reply_hold: Default::default(),
            lose: Default::default(),
            snapshot_gets: Default::default(),
            fail_delta: Default::default(),
            fail_only_delta_head: Default::default(),
            cleanup_delay_ms: Default::default(),
            list_started: Default::default(),
        }
    }

    async fn delay_transfer(&self) {
        let delay = self
            .transfer_delay_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        if delay != 0 {
            tokio::time::sleep(Duration::from_millis(delay as u64)).await;
        }
    }

    async fn delay_cleanup(&self) {
        let delay = self.cleanup_delay_ms.load(Ordering::Relaxed);
        if delay != 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
    }
}

#[async_trait::async_trait]
impl Storage for PublicationStorage {
    async fn head(
        &self,
        key: &sanctum_storage::Key,
    ) -> sanctum_core::Result<sanctum_storage::Object> {
        self.inner.head(key).await
    }

    async fn get(
        &self,
        key: &sanctum_storage::Key,
    ) -> sanctum_core::Result<sanctum_storage::Download> {
        if key.as_str().contains("/snapshots/") {
            self.snapshot_gets
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.delay_transfer().await;
        }
        self.inner.get(key).await
    }

    async fn read_range(
        &self,
        key: &sanctum_storage::Key,
        range: sanctum_core::ByteRange,
    ) -> sanctum_core::Result<bytes::Bytes> {
        self.inner.read_range(key, range).await
    }

    async fn put_stream(
        &self,
        key: &sanctum_storage::Key,
        reader: sanctum_storage::Reader,
        length: u64,
    ) -> sanctum_core::Result<sanctum_storage::Version> {
        self.delay_transfer().await;
        let hold = self.upload_hold.lock().unwrap().take();
        if let Some(hold) = hold {
            hold.await.unwrap();
        }
        let version = self.inner.put_stream(key, reader, length).await?;
        if key.as_str().ends_with("/pages.delta")
            && self
                .fail_delta
                .swap(false, std::sync::atomic::Ordering::Relaxed)
        {
            return Err(Error::Busy("delta upload response lost".into()));
        }
        Ok(version)
    }

    async fn put_control(
        &self,
        key: &sanctum_storage::Key,
        bytes: bytes::Bytes,
        condition: sanctum_storage::Condition,
    ) -> sanctum_core::Result<sanctum_storage::Version> {
        let generation = if key.as_str().ends_with("/HEAD.json") {
            Some(serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()["generation"].clone())
        } else {
            None
        };
        let published = match &generation {
            Some(generation) if generation["chain"].is_object() => {
                let previous = self.inner.get(key).await?;
                use tokio::io::AsyncReadExt;
                let mut body = Vec::new();
                previous
                    .reader
                    .take(1024 * 1024)
                    .read_to_end(&mut body)
                    .await?;
                serde_json::from_slice::<serde_json::Value>(&body).unwrap()["generation"]["id"]
                    != generation["id"]
            }
            Some(generation) => {
                generation.is_object()
                    && !self
                        .fail_only_delta_head
                        .load(std::sync::atomic::Ordering::Relaxed)
            }
            None => false,
        };
        let version = self.inner.put_control(key, bytes, condition).await?;
        let hold = self.control_reply_hold.lock().unwrap().take();
        if let Some(hold) = hold {
            hold.await.unwrap();
        }
        if published {
            let hold = self.publication_reply_hold.lock().unwrap().take();
            if let Some(hold) = hold {
                hold.await.unwrap();
            }
        }
        if published && self.lose.swap(false, std::sync::atomic::Ordering::Relaxed) {
            return Err(Error::Busy("publication response lost".into()));
        }
        Ok(version)
    }

    async fn list(
        &self,
        prefix: Option<&sanctum_storage::Key>,
        cursor: Option<&str>,
        limit: u32,
    ) -> sanctum_core::Result<sanctum_storage::ListPage> {
        if let Some(started) = self.list_started.lock().unwrap().take() {
            started.send(()).unwrap();
        }
        self.delay_cleanup().await;
        self.inner.list(prefix, cursor, limit).await
    }

    async fn delete(&self, key: &sanctum_storage::Key) -> sanctum_core::Result<()> {
        self.delay_cleanup().await;
        self.inner.delete(key).await
    }
}

async fn verify(
    storage: Arc<dyn Storage>,
    prefix: &str,
    local_root: Option<&std::path::Path>,
    takeover_lease: Duration,
) {
    let prefix = Key::parse(prefix).unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let scope = Scope {
        tenant: "default".parse().unwrap(),
        collection: "catalog".parse().unwrap(),
    };
    let unit = Unit::collection(Some(&prefix), &scope).unwrap();
    let catalog = Unit::catalog(Some(&prefix), &scope.tenant).unwrap();
    let short = Publication::new(storage.clone(), takeover_lease).unwrap();
    let long = Publication::new(storage.clone(), Duration::from_secs(15)).unwrap();

    assert!(long.read(&unit, scratch.path()).await.unwrap().is_none());
    let first_id = Uuid::new_v4();
    let mut initial = long.acquire(&unit).await.unwrap();
    assert_eq!(initial.epoch(), 1);
    let (first, receipt) = initial
        .apply(vec![command(first_id, 7)], apply_value)
        .await
        .unwrap();
    assert_eq!(receipt[0].outcome, "7");
    assert!(matches!(long.acquire(&unit).await, Err(Error::Busy(_))));
    initial.renew().await.unwrap();
    initial.release().await.unwrap();

    let read = long.read(&unit, scratch.path()).await.unwrap().unwrap();
    assert_eq!(read.generation, first);
    assert_eq!(value(read.path()), 7);
    let cache = sanctum_db::SnapshotCache::new(first.bytes);
    let cached = cache.read(&long, &unit).await.unwrap().unwrap();
    let warm = cache.read(&long, &unit).await.unwrap().unwrap();
    assert!(Arc::ptr_eq(&cached, &warm));
    assert!(long.read(&catalog, scratch.path()).await.unwrap().is_none());

    let mut abandoned = short.acquire(&unit).await.unwrap();
    assert_eq!(abandoned.epoch(), 2);
    // Block before publication, as if an instance paused after preparing a command.
    let stale_id = Uuid::new_v4();
    let (entered, prepare_started) = tokio::sync::oneshot::channel();
    let (resume, prepared) = std::sync::mpsc::channel();
    let mut entered = Some(entered);
    let stale = abandoned.apply(vec![command(stale_id, 100)], move |tx, value| {
        entered.take().unwrap().send(()).unwrap();
        prepared.recv_timeout(Duration::from_secs(15)).unwrap();
        apply_value(tx, value)
    });
    tokio::pin!(stale);
    tokio::select! {
        result = &mut stale => panic!("preparation finished early: {result:?}"),
        started = prepare_started => started.unwrap(),
    }
    // Stop polling the owner, including its renewal timer, before taking over.
    tokio::time::sleep(takeover_lease + Duration::from_millis(50)).await;
    let mut winner = long.acquire(&unit).await.unwrap();
    assert_eq!(winner.epoch(), 3);
    let next_id = Uuid::new_v4();
    let (second, receipts) = winner
        .apply(
            vec![
                command(first_id, 999),
                command(next_id, 4),
                command(next_id, 999),
            ],
            apply_value,
        )
        .await
        .unwrap();
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| receipt.outcome.as_str())
            .collect::<Vec<_>>(),
        ["7", "11", "11"]
    );
    resume.send(()).unwrap();
    assert!(matches!(stale.await, Err(Error::StaleRevision(_))));
    let (duplicate, receipts) = winner
        .apply(vec![command(next_id, 500)], apply_value)
        .await
        .unwrap();
    assert_eq!(duplicate, second);
    assert_eq!(receipts[0].outcome, "11");
    winner.release().await.unwrap();

    let current = long.read(&unit, scratch.path()).await.unwrap().unwrap();
    assert_eq!(current.generation, second);
    assert_eq!(value(current.path()), 11);
    assert_eq!(value(read.path()), 7);
    assert_eq!(
        long.reclaim(&unit, Duration::from_secs(300), None)
            .await
            .unwrap()
            .deleted,
        0
    );

    let retired = format!(
        "{prefix}/default/collections/catalog/snapshots/{}/retired.json",
        first.id
    );
    let retired_key = Key::parse(&retired).unwrap();
    assert!(storage.head(&retired_key).await.is_ok());
    if let Some(root) = local_root {
        std::fs::OpenOptions::new()
            .write(true)
            .open(root.join(&retired))
            .unwrap()
            .set_modified(std::time::SystemTime::now() - Duration::from_secs(301))
            .unwrap();
        let cleaned = long
            .reclaim(&unit, Duration::from_secs(300), None)
            .await
            .unwrap();
        assert_eq!(cleaned.deleted, 1);
        assert!(matches!(
            storage.head(&retired_key).await,
            Err(Error::NotFound(_))
        ));
        assert_eq!(value(read.path()), 7);
    }

    // An interrupted command cannot advance the visible generation.
    let mut writer = long.acquire(&unit).await.unwrap();
    let failed = writer
        .apply(vec![command(Uuid::new_v4(), 25)], |tx, value| {
            apply_value(tx, value)?;
            Err(Error::Busy("interrupted command".into()))
        })
        .await;
    assert!(matches!(failed, Err(Error::Busy(_))));
    writer.release().await.unwrap();
    assert_eq!(
        long.read(&unit, scratch.path())
            .await
            .unwrap()
            .unwrap()
            .generation,
        second
    );

    delete_prefix(storage.as_ref(), &prefix).await;
}

async fn delete_prefix(storage: &dyn Storage, prefix: &Key) {
    let mut cursor = None;
    loop {
        let page = storage
            .list(Some(prefix), cursor.as_deref(), 10)
            .await
            .unwrap();
        for object in page.objects {
            storage.delete(&object.key).await.unwrap();
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
}

fn command(operation: Uuid, payload: i64) -> Command<i64> {
    Command { operation, payload }
}

fn apply_value(tx: &Transaction<'_>, delta: i64) -> sanctum_core::Result<String> {
    let execute = || -> rusqlite::Result<i64> {
        tx.execute_batch(
            "CREATE TABLE IF NOT EXISTS data(id INTEGER PRIMARY KEY, value INTEGER NOT NULL);",
        )?;
        tx.query_row(
            "INSERT INTO data VALUES (1, ?1)
             ON CONFLICT(id) DO UPDATE SET value = value + ?1 RETURNING value",
            params![delta],
            |row| row.get(0),
        )
    };
    execute()
        .map(|value| value.to_string())
        .map_err(|error| Error::Internal(Box::new(error)))
}

fn value(path: &std::path::Path) -> i64 {
    Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .unwrap()
        .query_row("SELECT value FROM data", [], |row| row.get(0))
        .unwrap()
}

#[tokio::test]
async fn unpublished_delta_and_lost_manifest_response_preserve_commit_state() {
    use std::sync::atomic::Ordering;

    let root = tempfile::tempdir().unwrap();
    let storage = Arc::new(PublicationStorage::new(Arc::new(
        sanctum_storage_fs::FileStorage::open(root.path()).unwrap(),
    )));
    let unit = Unit::catalog(None, &"default".parse().unwrap()).unwrap();
    let writer = Publication::new(storage.clone(), Duration::from_millis(500))
        .unwrap()
        .with_page_deltas(true);
    let scratch = tempfile::tempdir().unwrap();
    let mut lease = writer.acquire(&unit).await.unwrap();
    let (base, _) = lease
        .apply(vec![command(Uuid::new_v4(), 2)], |tx, initial| {
            tx.execute_batch(
                "CREATE TABLE padding (body BLOB); INSERT INTO padding VALUES (zeroblob(262144));",
            )
            .map_err(|error| Error::Internal(Box::new(error)))?;
            apply_value(tx, initial)
        })
        .await
        .unwrap();
    let operation = Uuid::new_v4();
    storage.fail_delta.store(true, Ordering::Relaxed);
    assert!(matches!(
        lease.apply(vec![command(operation, 3)], apply_value).await,
        Err(Error::Busy(_))
    ));
    assert_eq!(writer.generation(&unit).await.unwrap(), Some(base));
    assert_eq!(writer.receipt(&unit, operation).await.unwrap(), None);
    lease.release().await.unwrap();
    let mut lease = writer.acquire(&unit).await.unwrap();
    storage.fail_only_delta_head.store(true, Ordering::Relaxed);
    storage.lose.store(true, Ordering::Relaxed);
    let result = lease.apply(vec![command(operation, 3)], apply_value).await;
    assert!(matches!(result, Err(Error::Busy(_))));
    assert!(matches!(
        lease.release().await,
        Err(Error::StaleRevision(_))
    ));
    let cold = writer.read(&unit, scratch.path()).await.unwrap().unwrap();
    assert_eq!(value(cold.path()), 5);
    assert_eq!(
        writer.receipt(&unit, operation).await.unwrap().as_deref(),
        Some("5")
    );
    tokio::time::sleep(Duration::from_millis(550)).await;
    let mut lease = writer.acquire(&unit).await.unwrap();
    let (unchanged, _) = lease
        .apply(vec![command(operation, 99)], apply_value)
        .await
        .unwrap();
    assert_eq!(unchanged, cold.generation);
    lease.release().await.unwrap();
    let key = Key::parse(format!(
        "default/catalog/snapshots/{}/pages.delta",
        unchanged.id
    ))
    .unwrap();
    let path = root.path().join(key.as_str());
    let mut bytes = std::fs::read(&path).unwrap();
    let last = bytes.last_mut().unwrap();
    *last ^= 1;
    std::fs::write(path, bytes).unwrap();
    assert!(matches!(
        writer.read(&unit, scratch.path()).await,
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::InvalidData
    ));
}
