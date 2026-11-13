use std::{
    path::Path,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use rusqlite::{Connection, Transaction, TransactionBehavior};
use sanctum_core::{Error, Result, Scope, ScopeId};
use sanctum_storage::{Condition, Key, MAX_CONTROL_BYTES, Storage, Version};
use serde::{Deserialize, Serialize};
use tempfile::{NamedTempFile, TempDir};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

const FORMAT: u32 = 1;
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
        Self::new(
            prefix,
            &format!("{}/collections/{}", scope.tenant, scope.collection),
            "meta.db",
        )
    }

    fn new(prefix: Option<&Key>, unit: &str, database: &'static str) -> Result<Self> {
        Ok(Self {
            prefix: Key::parse(
                prefix.map_or_else(|| unit.to_owned(), |prefix| format!("{prefix}/{unit}")),
            )?,
            database,
        })
    }

    fn head(&self) -> Result<Key> {
        Key::parse(format!("{}/HEAD.json", self.prefix))
    }

    fn snapshot(&self, generation: Uuid) -> Result<Key> {
        Key::parse(format!(
            "{}/snapshots/{generation}/{}",
            self.prefix, self.database
        ))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Generation {
    pub id: Uuid,
    pub bytes: u64,
    pub blake3: String,
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
}

#[derive(Clone)]
pub struct Publication {
    storage: Arc<dyn Storage>,
    ttl: Duration,
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
    file: NamedTempFile,
}

impl Materialized {
    pub fn path(&self) -> &Path {
        self.file.path()
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
        Ok(Self { storage, ttl })
    }

    pub async fn acquire(&self, unit: &Unit) -> Result<Lease> {
        let key = unit.head()?;
        let (mut head, condition) = match self.read_head(unit).await? {
            Some((head, version)) => (head, Condition::Match(version)),
            None => (
                Head {
                    format: FORMAT,
                    epoch: 0,
                    update: Uuid::nil(),
                    generation: None,
                    owner: None,
                },
                Condition::Create,
            ),
        };
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
        let version = self
            .storage
            .put_control(&key, encode(&head)?, condition)
            .await
            .map_err(|error| {
                if matches!(error, Error::Conflict(_)) {
                    Error::Busy("another writer acquired the database unit".into())
                } else {
                    error
                }
            })?;
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
        let mut lease = self.acquire(unit).await?;
        let result = lease.reclaim(grace, cursor).await;
        let release = lease.release().await;
        result.and_then(|page| release.map(|()| page))
    }

    pub async fn read(&self, unit: &Unit, directory: &Path) -> Result<Option<Materialized>> {
        let Some((head, _)) = self.read_head(unit).await? else {
            return Ok(None);
        };
        match head.generation {
            Some(generation) => self
                .materialize(unit, generation, directory)
                .await
                .map(Some),
            None => Ok(None),
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
        if head.format != FORMAT {
            return Err(corrupt("unsupported database manifest version"));
        }
        Ok(Some((head, response.object.version)))
    }

    async fn materialize(
        &self,
        unit: &Unit,
        generation: Generation,
        directory: &Path,
    ) -> Result<Materialized> {
        let response = self.storage.get(&unit.snapshot(generation.id)?).await?;
        if response.object.length != generation.bytes {
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
            if read > generation.bytes {
                return Err(corrupt(
                    "database snapshot body exceeds its manifest length",
                ));
            }
            hasher.update(&buffer[..bytes]);
            output.write_all(&buffer[..bytes]).await?;
        }
        if read != generation.bytes || hasher.finalize().to_hex().as_str() != generation.blake3 {
            return Err(corrupt("database snapshot does not match its manifest"));
        }
        output.flush().await?;
        Ok(Materialized { generation, file })
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
        Ok(())
    }

    pub async fn release(mut self) -> Result<()> {
        self.ensure_live()?;
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
        self.ensure_live()?;
        if commands.is_empty() || commands.len() > MAX_BATCH {
            return Err(Error::Invalid(
                "database command batch must contain 1 to 64 commands".into(),
            ));
        }
        self.renew().await?;
        let directory = tempfile::tempdir()?;
        let source = match self.head.generation.clone() {
            Some(generation) => Some(
                self.publication
                    .materialize(&self.unit, generation, directory.path())
                    .await?,
            ),
            None => None,
        };
        let prepared =
            tokio::task::spawn_blocking(move || apply_snapshot(directory, source, commands, apply))
                .await
                .map_err(|error| Error::Internal(Box::new(error)))??;
        self.ensure_live()?;
        let (staged, generation, receipts) = match prepared {
            Prepared::Changed {
                file,
                generation,
                receipts,
            } => (file, generation, receipts),
            Prepared::Unchanged {
                generation,
                receipts,
            } => return Ok((generation, receipts)),
        };
        self.renew().await?;
        let file = tokio::fs::File::from_std(staged.reopen()?);
        let key = self.unit.snapshot(generation.id)?;
        self.publication
            .storage
            .put_stream(&key, Box::pin(file), generation.bytes)
            .await?;
        self.ensure_live()?;
        let mut head = self.head.clone();
        head.generation = Some(generation.clone());
        self.write(head).await?;
        tracing::debug!(
            epoch = self.head.epoch,
            snapshot_bytes = generation.bytes,
            commands = receipts.len(),
            "published database snapshot"
        );
        Ok((generation, receipts))
    }

    async fn reclaim(&mut self, grace: Duration, cursor: Option<&str>) -> Result<CleanupPage> {
        let prefix = Key::parse(format!("{}/snapshots", self.unit.prefix))?;
        let page = self
            .publication
            .storage
            .list(Some(&prefix), cursor, 128)
            .await?;
        let current = self
            .head
            .generation
            .as_ref()
            .map(|generation| self.unit.snapshot(generation.id))
            .transpose()?;
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
            let Ok(generation) = generation.parse::<Uuid>() else {
                continue;
            };
            let is_marker = file == "retired.json";
            if !is_marker && file != self.unit.database {
                continue;
            }
            let data = self.unit.snapshot(generation)?;
            if current.as_ref() == Some(&data) {
                continue;
            }
            let marker = Key::parse(format!("{prefix}{generation}/retired.json"))?;
            self.ensure_live()?;
            if is_marker {
                match self.publication.storage.head(&data).await {
                    Err(Error::NotFound(_)) => {
                        self.ensure_live()?;
                        self.publication.storage.delete(&marker).await?;
                    }
                    Ok(_) => {}
                    Err(error) => return Err(error),
                }
                continue;
            }
            match self.publication.storage.head(&marker).await {
                Err(Error::NotFound(_)) => {
                    // Start grace from the observation that the snapshot is no longer current.
                    self.ensure_live()?;
                    match self
                        .publication
                        .storage
                        .put_control(&marker, bytes::Bytes::from_static(b"{}"), Condition::Create)
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
                    self.ensure_live()?;
                    self.publication.storage.delete(&data).await?;
                    deleted += 1;
                    self.publication.storage.delete(&marker).await?;
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
        let result = self
            .publication
            .storage
            .put_control(
                &self.unit.head()?,
                encode(&head)?,
                Condition::Match(self.version.clone()),
            )
            .await;
        match result {
            Ok(version) => {
                self.version = version;
                self.head = head;
                Ok(())
            }
            Err(error) => {
                // A network failure can hide a successful CAS. Stop using this lease.
                self.lost = true;
                if matches!(error, Error::Conflict(_) | Error::NotFound(_)) {
                    Err(Error::StaleRevision(
                        "database writer lost ownership".into(),
                    ))
                } else {
                    Err(error)
                }
            }
        }
    }
}

enum Prepared {
    Changed {
        file: NamedTempFile,
        generation: Generation,
        receipts: Vec<Receipt>,
    },
    Unchanged {
        generation: Generation,
        receipts: Vec<Receipt>,
    },
}

fn apply_snapshot<T>(
    directory: TempDir,
    source: Option<Materialized>,
    commands: Vec<Command<T>>,
    mut apply: impl FnMut(&Transaction<'_>, T) -> Result<String>,
) -> Result<Prepared> {
    use rusqlite::{OptionalExtension, params};
    let empty = directory.path().join("initial.sqlite");
    let path = source.as_ref().map_or(empty.as_path(), Materialized::path);
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
        let id = command.operation.to_string();
        let previous: Option<String> = tx
            .query_row(
                "SELECT outcome FROM applied_commands WHERE id = ?1",
                [&id],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?;
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
    let mut digest = blake3::Hasher::new();
    digest.update_reader(&mut reader)?;
    Ok(Prepared::Changed {
        file: snapshot,
        generation: Generation {
            id: Uuid::new_v4(),
            bytes,
            blake3: digest.finalize().to_hex().to_string(),
        },
        receipts,
    })
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
