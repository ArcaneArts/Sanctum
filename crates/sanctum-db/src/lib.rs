use std::{
    future::Future,
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use rusqlite::{Connection, Transaction, TransactionBehavior};
use sanctum_core::{Error, Result, Scope, ScopeId};
use sanctum_storage::{Condition, Key, MAX_CONTROL_BYTES, Storage, Version};
use serde::{Deserialize, Serialize};
use tempfile::{NamedTempFile, TempDir};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

mod cache;
mod generation;
mod page_delta;
pub use cache::SnapshotCache;
pub use generation::Generation;
use generation::{DeltaChain, DeltaFile};

const FORMAT: u32 = 1;
const DELTA_FORMAT: u32 = 2;
const MAX_BATCH: usize = 64;
const RECEIPT_RETENTION: Duration = Duration::from_secs(45 * 24 * 3600);

#[derive(Clone)]
pub struct Unit {
    prefix: Key,
    database: &'static str,
}

impl Unit {
    pub fn catalog(prefix: Option<&Key>, tenant: &ScopeId) -> Result<Self> {
        Self::new(prefix, &format!("{tenant}/catalog"), "root.db")
    }

    pub fn collection(prefix: Option<&Key>, scope: &Scope) -> Result<Self> {
        Ok(Self {
            prefix: collection_prefix(prefix, scope)?,
            database: "meta.db",
        })
    }

    fn new(prefix: Option<&Key>, unit: &str, database: &'static str) -> Result<Self> {
        Ok(Self {
            prefix: scoped_key(prefix, unit)?,
            database,
        })
    }

    fn head(&self) -> Result<Key> {
        Key::parse(format!("{}/HEAD.json", self.prefix))
    }

    pub fn snapshot_session(&self, id: Uuid) -> Result<Key> {
        Key::parse(format!("{}/snapshots/sessions/{id}.json", self.prefix))
    }

    fn snapshot(&self, generation: Uuid) -> Result<Key> {
        Key::parse(format!(
            "{}/snapshots/{generation}/{}",
            self.prefix, self.database
        ))
    }

    fn delta(&self, generation: Uuid) -> Result<Key> {
        Key::parse(format!(
            "{}/snapshots/{generation}/pages.delta",
            self.prefix
        ))
    }
}

pub fn collection_prefix(prefix: Option<&Key>, scope: &Scope) -> Result<Key> {
    scoped_key(
        prefix,
        &format!("{}/collections/{}", scope.tenant, scope.collection),
    )
}

pub fn tenant_prefix(prefix: Option<&Key>, tenant: &ScopeId) -> Result<Key> {
    scoped_key(prefix, tenant.as_str())
}

fn scoped_key(prefix: Option<&Key>, path: &str) -> Result<Key> {
    Key::parse(prefix.map_or_else(|| path.to_owned(), |prefix| format!("{prefix}/{path}")))
}

#[derive(Clone, Serialize, Deserialize)]
struct Owner {
    id: Uuid,
    expires_at_ms: u64,
}

#[derive(Clone, Serialize, Deserialize)]
struct Head {
    format: u32,
    epoch: u64,
    update: Uuid,
    generation: Option<Generation>,
    owner: Option<Owner>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    deleted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    deleted_at_ms: Option<u64>,
}

#[derive(Clone)]
pub struct Publication {
    storage: Arc<dyn Storage>,
    ttl: Duration,
    snapshots: Option<Arc<SnapshotCache>>,
    page_deltas: bool,
}

pub struct Lease {
    publication: Publication,
    unit: Unit,
    head: Head,
    version: Version,
    deadline: Instant,
    lost: bool,
}

pub struct Materialized {
    pub generation: Generation,
    collection_reader: Mutex<Option<sanctum_db_sqlite::CollectionDb>>,
    file: NamedTempFile,
}

impl Materialized {
    fn new(generation: Generation, file: NamedTempFile) -> Self {
        Self {
            generation,
            collection_reader: Mutex::new(None),
            file,
        }
    }

    pub fn path(&self) -> &Path {
        self.file.path()
    }

    pub fn reopen(&self) -> Result<std::fs::File> {
        Ok(self.file.reopen()?)
    }

    /// Borrows an exclusive read-only SQLite handle for this snapshot. At most
    /// one idle handle stays with the file. Concurrent readers can open another
    /// connection while the first is in use.
    pub fn read_collection<T>(
        &self,
        read: impl FnOnce(&sanctum_db_sqlite::CollectionDb) -> Result<T>,
    ) -> Result<T> {
        let cached = self.idle_collection_reader().take();
        let reused = cached.is_some();
        let start = Instant::now();
        let database = match cached {
            Some(database) => database,
            None => sanctum_db_sqlite::CollectionDb::open_read_only(self.path())?,
        };
        let duration_ms = start.elapsed().as_secs_f64() * 1_000.0;
        tracing::debug!(
            event = "collection_sqlite_handle",
            generation = %self.generation.id,
            reused,
            duration_ms,
            "opened or reused a read-only SQLite handle"
        );
        let result = read(&database);
        if result.is_ok() && database.clear_read_query_state().is_ok() {
            let mut idle = self.idle_collection_reader();
            if idle.is_none() {
                *idle = Some(database);
            }
        }
        result
    }

    fn idle_collection_reader(
        &self,
    ) -> std::sync::MutexGuard<'_, Option<sanctum_db_sqlite::CollectionDb>> {
        self.collection_reader
            .lock()
            .expect("idle SQLite handle mutex was poisoned")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Receipt {
    pub operation: Uuid,
    pub outcome: String,
}

pub struct CleanupPage {
    pub deleted: usize,
    pub next_cursor: Option<String>,
}

pub struct Command<T> {
    pub operation: Uuid,
    pub payload: T,
}

impl Publication {
    pub fn new(storage: Arc<dyn Storage>, ttl: Duration) -> Result<Self> {
        if !(Duration::from_millis(100)..=Duration::from_secs(3600)).contains(&ttl) {
            return Err(Error::Invalid(
                "database lease must last from 100 milliseconds to one hour".into(),
            ));
        }
        Ok(Self {
            storage,
            ttl,
            snapshots: None,
            page_deltas: false,
        })
    }

    pub fn with_snapshot_cache(mut self, snapshots: Arc<SnapshotCache>) -> Self {
        self.snapshots = Some(snapshots);
        self
    }

    pub fn with_page_deltas(mut self, enabled: bool) -> Self {
        self.page_deltas = enabled;
        self
    }

    pub async fn acquire(&self, unit: &Unit) -> Result<Lease> {
        self.acquire_unit(unit, true, false).await
    }

    pub async fn acquire_existing(&self, unit: &Unit) -> Result<Lease> {
        self.acquire_unit(unit, false, false).await
    }

    pub async fn fence_deletion(&self, unit: &Unit) -> Result<()> {
        if self
            .read_head(unit)
            .await?
            .is_some_and(|(head, _)| head.deleted_at_ms.is_some())
        {
            return Ok(());
        }
        let mut lease = self.acquire_unit(unit, false, true).await?;
        if lease.head.deleted_at_ms.is_some() {
            return lease.release().await;
        }
        let mut head = lease.head.clone();
        head.deleted = true;
        head.deleted_at_ms = Some(unix_ms()?);
        head.owner = None;
        lease.write(head).await
    }

    async fn acquire_unit(&self, unit: &Unit, create: bool, allow_deleted: bool) -> Result<Lease> {
        let key = unit.head()?;
        let (mut head, condition) = match self.read_head(unit).await? {
            Some((head, version)) => (head, Condition::Match(version)),
            None if !create => {
                return Err(Error::NotFound("database manifest was not found".into()));
            }
            None => (
                Head {
                    format: FORMAT,
                    epoch: 0,
                    update: Uuid::nil(),
                    generation: None,
                    owner: None,
                    deleted: false,
                    deleted_at_ms: None,
                },
                Condition::Create,
            ),
        };
        if head.deleted && !allow_deleted {
            return Err(Error::NotFound("database unit was deleted".into()));
        }
        let now = unix_ms()?;
        if head
            .owner
            .as_ref()
            .is_some_and(|owner| owner.expires_at_ms > now)
        {
            return Err(Error::Busy(
                "database unit is owned by another writer".into(),
            ));
        }
        head.epoch = head
            .epoch
            .checked_add(1)
            .ok_or_else(|| Error::Internal("database epoch exhausted".into()))?;
        head.update = Uuid::new_v4();
        head.owner = Some(Owner {
            id: Uuid::new_v4(),
            expires_at_ms: now + self.ttl.as_millis() as u64,
        });
        let deadline = Instant::now() + self.ttl;
        let write = self.storage.put_control(&key, encode(&head)?, condition);
        let version = tokio::time::timeout_at(deadline.into(), write)
            .await
            .map_err(|_| Error::Busy("database writer acquisition timed out".into()))?
            .map_err(|error| match error {
                Error::Conflict(_) => {
                    Error::Busy("another writer acquired the database unit".into())
                }
                error => error,
            })?;
        if Instant::now() >= deadline {
            return Err(Error::Busy("database writer acquisition timed out".into()));
        }
        Ok(Lease {
            publication: self.clone(),
            unit: unit.clone(),
            head,
            version,
            deadline,
            lost: false,
        })
    }

    /// Mark and reclaim one page of abandoned or retired snapshots.
    /// The owner fence serializes cleanup with new publication.
    pub async fn reclaim(
        &self,
        unit: &Unit,
        grace: Duration,
        cursor: Option<&str>,
    ) -> Result<CleanupPage> {
        if grace < Duration::from_secs(300) {
            return Err(Error::Invalid(
                "snapshot cleanup requires at least five minutes of grace".into(),
            ));
        }
        let mut lease = self.acquire_unit(unit, false, true).await?;
        let result = lease.reclaim(grace, cursor).await;
        let release = lease.release().await;
        result.and_then(|page| release.map(|()| page))
    }

    pub async fn reclaim_retired(
        &self,
        unit: &Unit,
        grace: Duration,
        cursor: Option<&str>,
    ) -> Result<CleanupPage> {
        if grace < Duration::from_secs(300) {
            return Err(Error::Invalid(
                "collection cleanup requires at least five minutes of grace".into(),
            ));
        }
        let mut lease = self.acquire_unit(unit, false, true).await?;
        let result = lease.reclaim_retired(grace, cursor).await;
        let release = lease.release().await;
        result.and_then(|page| release.map(|()| page))
    }

    pub async fn receipt(&self, unit: &Unit, operation: Uuid) -> Result<Option<String>> {
        self.stage_receipt(unit, operation, "").await
    }

    pub async fn stage_receipt(
        &self,
        unit: &Unit,
        operation: Uuid,
        stage: &str,
    ) -> Result<Option<String>> {
        let id = receipt_id(operation, stage);
        let database = match &self.snapshots {
            Some(snapshots) => snapshots.read(self, unit).await?,
            None => self.read(unit, &std::env::temp_dir()).await?.map(Arc::new),
        };
        let Some(database) = database else {
            return Ok(None);
        };
        tokio::task::spawn_blocking(move || {
            let connection = Connection::open_with_flags(
                database.path(),
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )
            .map_err(sql_error)?;
            read_receipt(&connection, &id)
        })
        .await
        .map_err(|error| Error::Internal(Box::new(error)))?
    }

    pub async fn generation(&self, unit: &Unit) -> Result<Option<Generation>> {
        Ok(self
            .read_head(unit)
            .await?
            .and_then(|(head, _)| if head.deleted { None } else { head.generation }))
    }

    pub async fn is_retired(&self, unit: &Unit) -> Result<bool> {
        Ok(self
            .read_head(unit)
            .await?
            .is_some_and(|(head, _)| head.deleted))
    }

    pub async fn read(&self, unit: &Unit, directory: &Path) -> Result<Option<Materialized>> {
        let Some((head, _)) = self.read_head(unit).await? else {
            return Ok(None);
        };
        match (head.deleted, head.generation) {
            (false, Some(generation)) => self
                .materialize(unit, generation, directory)
                .await
                .map(Some),
            _ => Ok(None),
        }
    }

    async fn read_head(&self, unit: &Unit) -> Result<Option<(Head, Version)>> {
        let key = unit.head()?;
        let response = match self.storage.get(&key).await {
            Ok(response) => response,
            Err(Error::NotFound(_)) => return Ok(None),
            Err(error) => return Err(error),
        };
        if response.object.length > MAX_CONTROL_BYTES as u64 {
            return Err(corrupt("database manifest is too large"));
        }
        let mut bytes = Vec::new();
        response
            .reader
            .take(MAX_CONTROL_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .await?;
        if bytes.len() as u64 != response.object.length {
            return Err(corrupt(
                "database manifest length differs from its metadata",
            ));
        }
        let head: Head =
            serde_json::from_slice(&bytes).map_err(|error| Error::Internal(Box::new(error)))?;
        if !matches!(head.format, FORMAT | DELTA_FORMAT) {
            return Err(corrupt("unsupported database manifest version"));
        }
        if let Some(generation) = &head.generation
            && let Some(chain) = &generation.chain
        {
            if head.format != DELTA_FORMAT {
                return Err(corrupt(
                    "snapshot deltas require database manifest format 2",
                ));
            }
            chain.validate(generation)?;
        }
        Ok(Some((head, response.object.version)))
    }

    async fn materialize(
        &self,
        unit: &Unit,
        generation: Generation,
        directory: &Path,
    ) -> Result<Materialized> {
        let start = Instant::now();
        let mut downloaded_bytes = generation.bytes;
        let file = if let Some(chain) = &generation.chain {
            // Database sessions carry their own generation descriptor.
            chain.validate(&generation)?;
            let base = &chain.base;
            downloaded_bytes = base.bytes;
            let database = self
                .download_checked(
                    &unit.snapshot(base.id)?,
                    base.bytes,
                    &base.blake3,
                    directory,
                )
                .await?;
            for delta in &chain.deltas {
                let patch = self
                    .download_checked(
                        &unit.delta(delta.id)?,
                        delta.bytes,
                        &delta.blake3,
                        directory,
                    )
                    .await?;
                downloaded_bytes = downloaded_bytes.saturating_add(delta.bytes);
                let mut writer = database.reopen()?;
                let bytes = delta.result_bytes;
                tokio::task::spawn_blocking(move || {
                    page_delta::apply(&mut writer, patch.reopen()?, bytes)
                })
                .await
                .map_err(|error| Error::Internal(Box::new(error)))??;
            }
            let mut contents = database.reopen()?;
            let expected = generation.blake3.clone();
            let actual = tokio::task::spawn_blocking(move || hash_file(&mut contents))
                .await
                .map_err(|error| Error::Internal(Box::new(error)))??;
            if actual != expected {
                return Err(corrupt(
                    "reconstructed SQLite backup does not match its manifest",
                ));
            }
            database
        } else {
            self.download_checked(
                &unit.snapshot(generation.id)?,
                generation.bytes,
                &generation.blake3,
                directory,
            )
            .await?
        };
        tracing::debug!(
            event = "snapshot_download",
            unit = %unit.prefix,
            generation = %generation.id,
            snapshot_bytes = generation.bytes,
            downloaded_bytes,
            duration_ms = start.elapsed().as_secs_f64() * 1_000.0,
            "materialized a database snapshot"
        );
        Ok(Materialized::new(generation, file))
    }

    async fn download_checked(
        &self,
        key: &Key,
        length: u64,
        digest: &str,
        directory: &Path,
    ) -> Result<NamedTempFile> {
        let response = self.storage.get(key).await?;
        if response.object.length != length {
            return Err(corrupt(
                "database snapshot length differs from its manifest",
            ));
        }
        let file = NamedTempFile::new_in(directory)?;
        let mut output = tokio::fs::File::from_std(file.reopen()?);
        let mut input = response.reader;
        let mut hasher = blake3::Hasher::new();
        let mut buffer = vec![0; 256 * 1024];
        let mut read = 0u64;
        loop {
            let bytes = input.read(&mut buffer).await?;
            if bytes == 0 {
                break;
            }
            read += bytes as u64;
            if read > length {
                return Err(corrupt(
                    "database snapshot body exceeds its manifest length",
                ));
            }
            hasher.update(&buffer[..bytes]);
            output.write_all(&buffer[..bytes]).await?;
        }
        if read != length || hasher.finalize().to_hex().as_str() != digest {
            return Err(corrupt("database snapshot does not match its manifest"));
        }
        output.flush().await?;
        Ok(file)
    }
}

impl Lease {
    pub fn epoch(&self) -> u64 {
        self.head.epoch
    }

    pub async fn renew(&mut self) -> Result<()> {
        self.ensure_live()?;
        let mut head = self.head.clone();
        head.owner
            .as_mut()
            .expect("live lease has an owner")
            .expires_at_ms = unix_ms()? + self.publication.ttl.as_millis() as u64;
        let deadline = Instant::now() + self.publication.ttl;
        self.write(head).await?;
        self.deadline = deadline;
        self.ensure_live()
    }

    // Only this task mutates HEAD. Pending I/O and blocking SQLite preparation
    // can outlast the original lease, so interleave renewals while they run.
    async fn while_renewing<T>(&mut self, work: impl Future<Output = Result<T>>) -> Result<T> {
        tokio::pin!(work);
        loop {
            self.ensure_live()?;
            let renew_at = self.deadline - self.publication.ttl / 2;
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(renew_at.into()) => self.renew().await?,
                result = &mut work => {
                    self.ensure_live()?;
                    return result;
                }
            }
        }
    }

    pub async fn release(mut self) -> Result<()> {
        let mut head = self.head.clone();
        head.owner = None;
        self.write(head).await
    }

    /// Applies a deterministic batch without external side effects while the database
    /// is open. A repeat operation receives its stored outcome from the same transaction.
    pub async fn apply<T, F>(
        &mut self,
        commands: Vec<Command<T>>,
        apply: F,
    ) -> Result<(Generation, Vec<Receipt>)>
    where
        T: Send + 'static,
        F: FnMut(&Transaction<'_>, T) -> Result<String> + Send + 'static,
    {
        self.apply_stage("", commands, apply).await
    }

    /// Stage names distinguish several idempotent commands from the same operation.
    pub async fn apply_stage<T, F>(
        &mut self,
        stage: &'static str,
        commands: Vec<Command<T>>,
        apply: F,
    ) -> Result<(Generation, Vec<Receipt>)>
    where
        T: Send + 'static,
        F: FnMut(&Transaction<'_>, T) -> Result<String> + Send + 'static,
    {
        self.ensure_live()?;
        if commands.is_empty() || commands.len() > MAX_BATCH {
            return Err(Error::Invalid(
                "database command batch must contain 1 to 64 commands".into(),
            ));
        }
        let start = Instant::now();
        self.renew().await?;
        let publication = self.publication.clone();
        let unit = self.unit.clone();
        let current = self.head.generation.clone();
        let prepared = self
            .while_renewing(async move {
                let directory = tempfile::tempdir()?;
                let (source, shared) = match (current, &publication.snapshots) {
                    (Some(generation), Some(snapshots)) if snapshots.retains(&generation) => (
                        Some(
                            snapshots
                                .read_generation(&publication, &unit, generation)
                                .await?,
                        ),
                        true,
                    ),
                    (Some(generation), _) => (
                        Some(Arc::new(
                            publication
                                .materialize(&unit, generation, directory.path())
                                .await?,
                        )),
                        false,
                    ),
                    (None, _) => (None, false),
                };
                let delta_bytes = publication
                    .page_deltas
                    .then(|| source.as_ref()?.generation.chain_bytes())
                    .flatten();
                tokio::task::spawn_blocking(move || {
                    apply_snapshot(
                        directory,
                        source,
                        shared || delta_bytes.is_some(),
                        delta_bytes,
                        stage,
                        commands,
                        apply,
                    )
                })
                .await
                .map_err(|error| Error::Internal(Box::new(error)))?
            })
            .await?;
        let (staged, generation, receipts, delta) = match prepared {
            Prepared::Changed {
                file,
                generation,
                receipts,
                delta,
            } => (file, generation, receipts, delta),
            Prepared::Unchanged {
                generation,
                receipts,
            } => return Ok((generation, receipts)),
        };
        self.renew().await?;
        let (key, file) = match &delta {
            Some(delta) => (self.unit.delta(generation.id)?, delta.reopen()?),
            None => (self.unit.snapshot(generation.id)?, staged.reopen()?),
        };
        let uploaded_bytes = file.metadata()?.len();
        let storage = self.publication.storage.clone();
        self.while_renewing(async move {
            storage
                .put_stream(
                    &key,
                    Box::pin(tokio::fs::File::from_std(file)),
                    uploaded_bytes,
                )
                .await
        })
        .await?;
        let mut head = self.head.clone();
        head.format = if delta.is_some() {
            DELTA_FORMAT
        } else {
            FORMAT
        };
        head.generation = Some(generation.clone());
        self.write(head).await?;
        if let Some(snapshots) = &self.publication.snapshots {
            snapshots.remember(&self.unit, Materialized::new(generation.clone(), staged));
        }
        tracing::debug!(
            event = "snapshot_publish",
            unit = %self.unit.prefix,
            generation = %generation.id,
            stage,
            epoch = self.head.epoch,
            duration_ms = start.elapsed().as_secs_f64() * 1_000.0,
            snapshot_bytes = generation.bytes,
            uploaded_bytes,
            commands = receipts.len(),
            "published database snapshot"
        );
        Ok((generation, receipts))
    }

    async fn reclaim(&mut self, grace: Duration, cursor: Option<&str>) -> Result<CleanupPage> {
        let storage = self.publication.storage.clone();
        let prefix = Key::parse(format!("{}/snapshots", self.unit.prefix))?;
        let page = self
            .while_renewing(storage.list(Some(&prefix), cursor, 128))
            .await?;
        let mut deleted = 0;
        let prefix = format!("{prefix}/");
        for object in page.objects {
            let Some((generation, file)) = object
                .key
                .as_str()
                .strip_prefix(&prefix)
                .and_then(|suffix| suffix.split_once('/'))
            else {
                continue;
            };
            if generation == "sessions" && file.ends_with(".json") {
                // A session expires five minutes after its open request started.
                // Object age is a conservative bound even if the upload was delayed.
                if SystemTime::now()
                    .duration_since(object.modified_at)
                    .is_ok_and(|age| age >= grace)
                {
                    self.while_renewing(storage.delete(&object.key)).await?;
                }
                continue;
            }
            let Ok(generation) = generation.parse::<Uuid>() else {
                continue;
            };
            let is_marker = file == "retired.json";
            if !is_marker && file != self.unit.database && file != "pages.delta" {
                continue;
            }
            if self
                .head
                .generation
                .as_ref()
                .is_some_and(|current| current.uses(generation))
            {
                continue;
            }
            let marker = Key::parse(format!("{prefix}{generation}/retired.json"))?;
            if is_marker {
                if !self.retired_data_exists(generation).await? {
                    self.while_renewing(storage.delete(&marker)).await?;
                }
                continue;
            }
            match self.while_renewing(storage.head(&marker)).await {
                Err(Error::NotFound(_)) => {
                    // Start grace from the observation that the snapshot is no longer current.
                    match self
                        .while_renewing(storage.put_control(
                            &marker,
                            bytes::Bytes::from_static(b"{}"),
                            Condition::Create,
                        ))
                        .await
                    {
                        Ok(_) | Err(Error::Conflict(_)) => {}
                        Err(error) => return Err(error),
                    }
                }
                Ok(observation)
                    if SystemTime::now()
                        .duration_since(observation.modified_at)
                        .is_ok_and(|age| age >= grace) =>
                {
                    self.while_renewing(storage.delete(&object.key)).await?;
                    deleted += 1;
                    self.while_renewing(storage.delete(&marker)).await?;
                }
                Ok(_) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(CleanupPage {
            deleted,
            next_cursor: page.next_cursor,
        })
    }

    async fn retired_data_exists(&mut self, generation: Uuid) -> Result<bool> {
        let storage = self.publication.storage.clone();
        for key in [
            self.unit.snapshot(generation)?,
            self.unit.delta(generation)?,
        ] {
            match self.while_renewing(storage.head(&key)).await {
                Ok(_) => return Ok(true),
                Err(Error::NotFound(_)) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(false)
    }

    async fn reclaim_retired(
        &mut self,
        grace: Duration,
        cursor: Option<&str>,
    ) -> Result<CleanupPage> {
        let deleted_at = self
            .head
            .deleted_at_ms
            .ok_or_else(|| Error::Conflict("database is not retired".into()))?;
        if unix_ms()?.saturating_sub(deleted_at) < grace.as_millis() as u64 {
            return Ok(CleanupPage {
                deleted: 0,
                next_cursor: None,
            });
        }
        let storage = self.publication.storage.clone();
        let prefix = self.unit.prefix.clone();
        let page = self
            .while_renewing(storage.list(Some(&prefix), cursor, 128))
            .await?;
        let manifest = self.unit.head()?;
        let mut deleted = 0;
        for object in page.objects {
            if object.key == manifest {
                continue;
            }
            self.while_renewing(storage.delete(&object.key)).await?;
            deleted += 1;
        }
        if page.next_cursor.is_none() && self.head.generation.is_some() {
            let mut head = self.head.clone();
            head.generation = None;
            self.write(head).await?;
        }
        Ok(CleanupPage {
            deleted,
            next_cursor: page.next_cursor,
        })
    }

    fn ensure_live(&mut self) -> Result<()> {
        if self.lost || Instant::now() >= self.deadline {
            self.lost = true;
            return Err(Error::StaleRevision(
                "database writer lease expired or was lost".into(),
            ));
        }
        Ok(())
    }

    async fn write(&mut self, mut head: Head) -> Result<()> {
        head.update = Uuid::new_v4();
        let key = self.unit.head()?;
        let bytes = encode(&head)?;
        self.ensure_live()?;
        // Dropping a pending CAS can hide a successful HEAD update.
        self.lost = true;
        let write = self.publication.storage.put_control(
            &key,
            bytes,
            Condition::Match(self.version.clone()),
        );
        let result = tokio::time::timeout_at(self.deadline.into(), write).await;
        match result {
            Ok(Ok(version)) if Instant::now() < self.deadline => {
                self.version = version;
                self.head = head;
                self.lost = false;
                Ok(())
            }
            Ok(Err(Error::Conflict(_) | Error::NotFound(_))) => Err(Error::StaleRevision(
                "database writer lost ownership".into(),
            )),
            Ok(Err(error)) => Err(error),
            Ok(Ok(_)) | Err(_) => Err(Error::StaleRevision(
                "database manifest write exceeded its lease".into(),
            )),
        }
    }
}

enum Prepared {
    Changed {
        file: NamedTempFile,
        generation: Generation,
        receipts: Vec<Receipt>,
        delta: Option<NamedTempFile>,
    },
    Unchanged {
        generation: Generation,
        receipts: Vec<Receipt>,
    },
}

fn receipt_id(operation: Uuid, stage: &str) -> String {
    if stage.is_empty() {
        operation.to_string()
    } else {
        format!("{operation}:{stage}")
    }
}

/// Looks up a stage outcome inside the same transaction as a dependent command.
pub fn command_receipt(
    transaction: &Transaction<'_>,
    operation: Uuid,
    stage: &str,
) -> Result<Option<String>> {
    read_receipt(transaction, &receipt_id(operation, stage))
}

fn read_receipt(connection: &Connection, id: &str) -> Result<Option<String>> {
    use rusqlite::OptionalExtension;
    connection
        .query_row(
            "SELECT outcome FROM applied_commands WHERE id = ?1",
            [id],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)
}

fn apply_snapshot<T>(
    directory: TempDir,
    source: Option<Arc<Materialized>>,
    shared: bool,
    delta_bytes: Option<u64>,
    stage: &str,
    commands: Vec<Command<T>>,
    mut apply: impl FnMut(&Transaction<'_>, T) -> Result<String>,
) -> Result<Prepared> {
    use rusqlite::params;
    let working = directory.path().join("working.sqlite");
    let path = match source.as_deref() {
        Some(source) if shared => {
            // A cached file may still serve readers. Copy the closed, published
            // snapshot before opening it for a write transaction.
            std::fs::copy(source.path(), &working)?;
            working.as_path()
        }
        Some(source) => source.path(),
        None => working.as_path(),
    };
    let mut db = Connection::open(path).map_err(sql_error)?;
    db.pragma_update(None, "foreign_keys", true)
        .map_err(sql_error)?;
    let tx = db
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sql_error)?;
    tx.execute_batch(
        "CREATE TABLE IF NOT EXISTS applied_commands (
        id TEXT PRIMARY KEY NOT NULL, outcome TEXT NOT NULL, applied_at INTEGER NOT NULL
    );",
    )
    .map_err(sql_error)?;
    let mut receipts = Vec::with_capacity(commands.len());
    let mut changed = false;
    let now = (unix_ms()? / 1000) as i64;
    for command in commands {
        let id = receipt_id(command.operation, stage);
        let previous = read_receipt(&tx, &id)?;
        let outcome = match previous {
            Some(outcome) => outcome,
            None => {
                let outcome = apply(&tx, command.payload)?;
                changed = true;
                if outcome.len() > 64 * 1024 {
                    return Err(Error::Invalid(
                        "database command receipt exceeds 64 KiB".into(),
                    ));
                }
                tx.execute(
                    "INSERT INTO applied_commands (id, outcome, applied_at) VALUES (?1, ?2, ?3)",
                    params![id, outcome, now],
                )
                .map_err(sql_error)?;
                outcome
            }
        };
        receipts.push(Receipt {
            operation: command.operation,
            outcome,
        });
    }
    if !changed && let Some(source) = &source {
        return Ok(Prepared::Unchanged {
            generation: source.generation.clone(),
            receipts,
        });
    }
    tx.execute(
        "DELETE FROM applied_commands WHERE applied_at < ?1",
        [now - RECEIPT_RETENTION.as_secs() as i64],
    )
    .map_err(sql_error)?;
    tx.commit().map_err(sql_error)?;
    drop(db);
    let snapshot = NamedTempFile::new()?;
    sanctum_db_sqlite::snapshot::backup(path, snapshot.path(), || false)?;
    let mut reader = snapshot.reopen()?;
    let bytes = reader.metadata()?.len();
    let mut generation = Generation {
        id: Uuid::new_v4(),
        bytes,
        blake3: hash_file(&mut reader)?,
        chain: None,
    };
    let delta = match (delta_bytes, source) {
        (Some(chain_bytes), Some(before)) if bytes >= before.generation.bytes => {
            let limit = (bytes / 2).saturating_sub(chain_bytes);
            prepare_delta(&before, &snapshot, limit, &mut generation)?
        }
        _ => None,
    };
    Ok(Prepared::Changed {
        file: snapshot,
        generation,
        receipts,
        delta,
    })
}

fn prepare_delta(
    before: &Materialized,
    after: &NamedTempFile,
    limit: u64,
    generation: &mut Generation,
) -> Result<Option<NamedTempFile>> {
    let Some(patch) = page_delta::encode(before.path(), after.path(), limit)? else {
        return Ok(None);
    };
    let mut file = patch.reopen()?;
    let mut chain = before
        .generation
        .chain
        .clone()
        .unwrap_or_else(|| DeltaChain {
            base: before.generation.file(),
            deltas: Vec::new(),
        });
    chain.deltas.push(DeltaFile {
        id: generation.id,
        bytes: file.metadata()?.len(),
        blake3: hash_file(&mut file)?,
        result_bytes: generation.bytes,
    });
    generation.chain = Some(chain);
    Ok(Some(patch))
}

fn hash_file(file: &mut std::fs::File) -> Result<String> {
    let mut digest = blake3::Hasher::new();
    digest.update_reader(file)?;
    Ok(digest.finalize().to_hex().to_string())
}

fn encode(head: &Head) -> Result<bytes::Bytes> {
    serde_json::to_vec(head)
        .map(Into::into)
        .map_err(|error| Error::Internal(Box::new(error)))
}

fn unix_ms() -> Result<u64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| Error::Internal(Box::new(error)))?;
    u64::try_from(duration.as_millis()).map_err(|error| Error::Internal(Box::new(error)))
}

fn sql_error(error: rusqlite::Error) -> Error {
    Error::Internal(Box::new(error))
}

fn corrupt(message: &'static str) -> Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn materialized_snapshot_retains_one_idle_handle_without_locking_out_other_readers() {
        let file = NamedTempFile::new().unwrap();
        let mut connection = Connection::open(file.path()).unwrap();
        let transaction = connection.transaction().unwrap();
        sanctum_db_sqlite::initialize_collection(&transaction).unwrap();
        transaction.commit().unwrap();
        drop(connection);
        let snapshot = Materialized::new(
            Generation {
                id: Uuid::new_v4(),
                bytes: file.path().metadata().unwrap().len(),
                blake3: hash_file(&mut file.reopen().unwrap()).unwrap(),
                chain: None,
            },
            file,
        );
        assert!(snapshot.idle_collection_reader().is_none());
        snapshot.read_collection(|db| db.change_cursor()).unwrap();
        assert!(snapshot.idle_collection_reader().is_some());
        snapshot
            .read_collection(|outer| {
                assert!(snapshot.idle_collection_reader().is_none());
                snapshot.read_collection(|inner| {
                    assert!(!std::ptr::eq(outer, inner));
                    assert_eq!(outer.change_cursor()?, inner.change_cursor()?);
                    Ok(())
                })
            })
            .unwrap();
        assert!(snapshot.idle_collection_reader().is_some());
        let error = snapshot.read_collection::<()>(|_| Err(Error::Busy("read failed".into())));
        assert!(matches!(error, Err(Error::Busy(_))));
        assert!(snapshot.idle_collection_reader().is_none());
    }
}
