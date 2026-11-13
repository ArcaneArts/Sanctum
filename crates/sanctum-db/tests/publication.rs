use std::{sync::Arc, time::Duration};

use rusqlite::{Connection, Transaction, params};
use sanctum_core::{Error, Scope};
use sanctum_db::{Command, Publication, Unit};
use sanctum_storage::Storage;
use uuid::Uuid;

#[tokio::test]
async fn filesystem_publication_contract() {
    let directory = tempfile::tempdir().unwrap();
    let storage = sanctum_storage_fs::FileStorage::open(directory.path()).unwrap();
    verify(Arc::new(storage), "publication", Some(directory.path())).await;
}

#[tokio::test]
#[ignore = "requires a live MinIO bucket and S3 credentials"]
async fn minio_publication_contract() {
    use sanctum_storage_s3::{S3Config, S3Storage};
    let storage = S3Storage::new(S3Config {
        bucket: std::env::var("SANCTUM_MINIO_BUCKET").expect("SANCTUM_MINIO_BUCKET"),
        region: "us-east-1".into(),
        endpoint: Some(std::env::var("SANCTUM_MINIO_ENDPOINT").expect("SANCTUM_MINIO_ENDPOINT")),
        allow_http: true,
        virtual_hosted_style: false,
    })
    .unwrap();
    let prefix = format!("publication-{}", Uuid::new_v4());
    verify(Arc::new(storage), &prefix, None).await;
}

#[tokio::test]
async fn recovering_from_a_lost_publish_response_reuses_its_receipt() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Arc::new(LosePublishResponse {
        inner: sanctum_storage_fs::FileStorage::open(directory.path()).unwrap(),
        lose: std::sync::atomic::AtomicBool::new(true),
    });
    let unit = Unit::catalog(None, &"default".parse().unwrap()).unwrap();
    let short = Publication::new(storage.clone(), Duration::from_millis(400)).unwrap();
    let long = Publication::new(storage, Duration::from_secs(15)).unwrap();
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

struct LosePublishResponse {
    inner: sanctum_storage_fs::FileStorage,
    lose: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl Storage for LosePublishResponse {
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
        self.inner.put_stream(key, reader, length).await
    }

    async fn put_control(
        &self,
        key: &sanctum_storage::Key,
        bytes: bytes::Bytes,
        condition: sanctum_storage::Condition,
    ) -> sanctum_core::Result<sanctum_storage::Version> {
        let published = key.as_str().ends_with("/HEAD.json")
            && serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()["generation"]
                .is_object();
        let version = self.inner.put_control(key, bytes, condition).await?;
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
        self.inner.list(prefix, cursor, limit).await
    }

    async fn delete(&self, key: &sanctum_storage::Key) -> sanctum_core::Result<()> {
        self.inner.delete(key).await
    }
}

async fn verify(storage: Arc<dyn Storage>, prefix: &str, local_root: Option<&std::path::Path>) {
    use sanctum_storage::Key;
    let prefix = Key::parse(prefix).unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let scope = Scope {
        tenant: "default".parse().unwrap(),
        collection: "catalog".parse().unwrap(),
    };
    let unit = Unit::collection(Some(&prefix), &scope).unwrap();
    let catalog = Unit::catalog(Some(&prefix), &scope.tenant).unwrap();
    let short = Publication::new(storage.clone(), Duration::from_millis(650)).unwrap();
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
    assert!(long.read(&catalog, scratch.path()).await.unwrap().is_none());

    let mut abandoned = short.acquire(&unit).await.unwrap();
    assert_eq!(abandoned.epoch(), 2);
    // Block before publication, as if an instance paused after preparing a command.
    let stale_id = Uuid::new_v4();
    let (entered, prepare_started) = tokio::sync::oneshot::channel();
    let (resume, prepared) = std::sync::mpsc::channel();
    let stale = tokio::spawn(async move {
        let mut entered = Some(entered);
        abandoned
            .apply(vec![command(stale_id, 100)], move |tx, value| {
                entered.take().unwrap().send(()).unwrap();
                prepared.recv_timeout(Duration::from_secs(15)).unwrap();
                apply_value(tx, value)
            })
            .await
    });
    prepare_started.await.unwrap();
    tokio::time::sleep(Duration::from_millis(700)).await;
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
    assert!(matches!(stale.await.unwrap(), Err(Error::StaleRevision(_))));
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

    let mut cursor = None;
    loop {
        let page = storage
            .list(Some(&prefix), cursor.as_deref(), 10)
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
