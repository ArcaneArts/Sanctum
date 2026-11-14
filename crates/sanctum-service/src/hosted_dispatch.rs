use std::{sync::Arc, time::Duration};

use sanctum_core::{Error, Result, Scope};
use sanctum_db::Unit;
use sanctum_storage::{Condition, Key, MAX_CONTROL_BYTES, Object};
use serde::{Deserialize, Serialize};
use tokio::{io::AsyncReadExt, sync::Notify, task::JoinSet, time::timeout};
use uuid::Uuid;

use crate::HostedCatalog;

pub(crate) const DISPATCH_WINDOW_SECONDS: u64 = 6 * 3600;
pub const DISPATCH_SHARDS: u8 = 16;
const HINT_PAGE_SIZE: u32 = 16;
const REPAIR_CONCURRENCY: usize = 4;
const REPAIR_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);
const REPAIR_PAGE_TIMEOUT: Duration = Duration::from_secs(240);

#[derive(Default, Serialize, Deserialize)]
struct Cursor {
    next: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskStage {
    Register,
    Prepare,
    Index,
    Embed { step: u32 },
    Rebuild { step: u32 },
}

pub struct Task<'a> {
    pub scope: &'a Scope,
    pub operation: Uuid,
    pub stage: TaskStage,
    pub profile: Option<&'a str>,
    pub window: u64,
    pub retry: u32,
    pub not_before: Option<u64>,
}

#[async_trait::async_trait]
pub trait Dispatch: Send + Sync {
    async fn send(&self, task: Task<'_>) -> Result<()>;
}

pub struct ReconcilePage {
    pub dispatched: usize,
    pub failed: usize,
    pub next_cursor: Option<String>,
}

impl ReconcilePage {
    fn record(&mut self, scope: &Scope, operation: Uuid, result: Result<bool>) {
        match result {
            Ok(true) => self.dispatched += 1,
            Ok(false) => {}
            Err(error) => {
                self.failed += 1;
                tracing::warn!(%error, tenant = %scope.tenant, collection = %scope.collection,
                    %operation, "could not reconcile hosted dispatch");
            }
        }
    }
}

type RepairAttempt = (Scope, Uuid, Result<bool>);

async fn record_next_attempt(
    report: &mut ReconcilePage,
    running: &mut JoinSet<RepairAttempt>,
) -> Result<()> {
    if let Some(finished) = running.join_next().await {
        let (scope, operation, result) =
            finished.map_err(|error| Error::Internal(Box::new(error)))?;
        report.record(&scope, operation, result);
    }
    Ok(())
}

struct HintPage {
    hints: Vec<(Scope, Uuid, HintKind, Object)>,
    next_cursor: Option<String>,
}

#[derive(Clone, Copy)]
enum HintKind {
    Upload(Option<Uuid>),
    Rebuild,
}

pub(super) struct UploadHint {
    pub(super) attempt: Option<Uuid>,
    pub(super) object: Object,
}

impl HostedCatalog {
    pub fn with_dispatch(mut self, dispatcher: Arc<dyn Dispatch>) -> Self {
        self.dispatcher = Some(dispatcher);
        self
    }

    pub fn with_standalone_worker(mut self, wake: Arc<Notify>) -> Self {
        self.standalone_wake = Some(wake);
        self
    }

    pub async fn try_dispatch(&self, scope: &Scope, operation: Uuid) {
        if let Some(wake) = &self.standalone_wake {
            wake.notify_one();
        }
        if let Err(error) = self.dispatch_pending(scope, operation, None).await {
            tracing::warn!(%error, tenant = %scope.tenant, collection = %scope.collection, %operation,
                "hosted operation remains pending dispatch");
        }
    }

    /// Persists a continuation so a fresh scheduler invocation resumes the scan.
    pub async fn reconcile_shard(&self, shard: u8) -> Result<ReconcilePage> {
        self.reconcile_shard_with_timeouts(shard, REPAIR_PAGE_TIMEOUT, REPAIR_ATTEMPT_TIMEOUT)
            .await
    }

    async fn reconcile_shard_with_timeouts(
        &self,
        shard: u8,
        page_timeout: Duration,
        attempt_timeout: Duration,
    ) -> Result<ReconcilePage> {
        timeout(
            page_timeout,
            self.reconcile_shard_page(shard, attempt_timeout),
        )
        .await
        .map_err(|_| Error::Busy("dispatch repair page timed out".into()))?
    }

    async fn reconcile_shard_page(
        &self,
        shard: u8,
        attempt_timeout: Duration,
    ) -> Result<ReconcilePage> {
        if self.dispatcher.is_none() {
            return Err(Error::Invalid(
                "hosted task dispatch is not configured".into(),
            ));
        }
        let partition = self.dispatch_partition(shard)?;
        let key = Key::parse(format!("{partition}/cursor.json"))?;
        let (cursor, condition) = match self.storage.get(&key).await {
            Ok(download) => {
                if download.object.length > MAX_CONTROL_BYTES as u64 {
                    return Err(Error::Internal("dispatch cursor exceeds size limit".into()));
                }
                let mut body = Vec::new();
                download
                    .reader
                    .take(MAX_CONTROL_BYTES as u64 + 1)
                    .read_to_end(&mut body)
                    .await?;
                let cursor = serde_json::from_slice(&body)
                    .map_err(|error| Error::Internal(Box::new(error)))?;
                (cursor, Condition::Match(download.object.version))
            }
            Err(Error::NotFound(_)) => (Cursor::default(), Condition::Create),
            Err(error) => return Err(error),
        };
        let page = self
            .reconcile_dispatch(shard, cursor.next.as_deref(), attempt_timeout)
            .await?;
        let body = serde_json::to_vec(&Cursor {
            next: page.next_cursor.clone(),
        })
        .map_err(|error| Error::Internal(Box::new(error)))?;
        match self.storage.put_control(&key, body.into(), condition).await {
            Ok(_) | Err(Error::Conflict(_)) => Ok(page),
            Err(error) => Err(error),
        }
    }

    /// Replays dispatch hints. Expired uploads may need to settle a committed database stage.
    async fn reconcile_dispatch(
        &self,
        shard: u8,
        cursor: Option<&str>,
        attempt_timeout: Duration,
    ) -> Result<ReconcilePage> {
        let HintPage { hints, next_cursor } = self.hint_page(shard, cursor).await?;
        let mut report = ReconcilePage {
            dispatched: 0,
            failed: 0,
            next_cursor,
        };
        // Dropping the request aborts the in-flight attempts. Unconfirmed
        // handoffs remain discoverable under their original task identity.
        let mut running = JoinSet::new();
        for (scope, operation, kind, object) in hints {
            if running.len() == REPAIR_CONCURRENCY {
                record_next_attempt(&mut report, &mut running).await?;
            }
            let service = self.clone();
            running.spawn(async move {
                let attempt = async {
                    match kind {
                        HintKind::Rebuild => service.dispatch_rebuild(&scope, operation).await,
                        HintKind::Upload(attempt) => {
                            service
                                .dispatch_pending(
                                    &scope,
                                    operation,
                                    Some(UploadHint { attempt, object }),
                                )
                                .await
                        }
                    }
                };
                let result = timeout(attempt_timeout, attempt).await.unwrap_or_else(|_| {
                    Err(Error::Busy("dispatch repair attempt timed out".into()))
                });
                (scope, operation, result)
            });
        }
        while !running.is_empty() {
            record_next_attempt(&mut report, &mut running).await?;
        }
        Ok(report)
    }

    pub async fn work_standalone_shard(
        &self,
        shard: u8,
        cursor: Option<&str>,
        stopping: impl Fn() -> bool,
    ) -> Result<ReconcilePage> {
        let HintPage { hints, next_cursor } = self.hint_page(shard, cursor).await?;
        let mut report = ReconcilePage {
            dispatched: 0,
            failed: 0,
            next_cursor,
        };
        for (scope, operation, kind, object) in hints {
            if stopping() {
                break;
            }
            let result = match kind {
                HintKind::Rebuild => self.work_standalone_rebuild(&scope, operation).await,
                HintKind::Upload(attempt) => {
                    self.work_standalone_upload(&scope, operation, UploadHint { attempt, object })
                        .await
                }
            };
            report.record(&scope, operation, result);
        }
        Ok(report)
    }

    /// Clears the dispatch hint when a late upload task reaches a retired collection.
    pub async fn discard_retired_upload_task(
        &self,
        scope: &Scope,
        operation: Uuid,
    ) -> Result<bool> {
        self.clear_retired_dispatch(scope, operation, false).await
    }

    pub(crate) async fn clear_retired_dispatch(
        &self,
        scope: &Scope,
        operation: Uuid,
        rebuild: bool,
    ) -> Result<bool> {
        // A retired manifest is permanent. A missing envelope is inconclusive
        // because admission writes its dispatch hint before the envelope.
        let unit = Unit::collection(self.prefix.as_ref(), scope)?;
        if !self.publication.is_retired(&unit).await? {
            return Ok(false);
        }
        let kind = if rebuild {
            HintKind::Rebuild
        } else {
            HintKind::Upload(None)
        };
        self.delete_hint(&self.dispatch_hint(scope, operation, kind)?)
            .await?;
        Ok(true)
    }

    async fn hint_page(&self, shard: u8, cursor: Option<&str>) -> Result<HintPage> {
        let prefix = self.dispatch_partition(shard)?;
        let pending = Key::parse(format!("{prefix}/pending"))?;
        let page = self
            .storage
            .list(Some(&pending), cursor, HINT_PAGE_SIZE)
            .await?;
        if page.next_cursor.is_some() && page.next_cursor.as_deref() == cursor {
            return Err(Error::Internal(
                "dispatch list cursor did not advance".into(),
            ));
        }
        Ok(HintPage {
            hints: page
                .objects
                .into_iter()
                .filter_map(|entry| {
                    let (scope, operation, kind) = parse_hint(&pending, &entry.key)?;
                    Some((scope, operation, kind, entry))
                })
                .collect(),
            next_cursor: page.next_cursor,
        })
    }

    pub(crate) async fn record_dispatch_hint(
        &self,
        scope: &Scope,
        operation: Uuid,
        attempt: Uuid,
    ) -> Result<()> {
        self.write_hint(&self.dispatch_hint(scope, operation, HintKind::Upload(Some(attempt)))?)
            .await
    }

    pub(crate) async fn record_rebuild_hint(&self, scope: &Scope, operation: Uuid) -> Result<()> {
        self.write_hint(&self.dispatch_hint(scope, operation, HintKind::Rebuild)?)
            .await
    }

    async fn write_hint(&self, key: &Key) -> Result<()> {
        match self
            .storage
            .put_control(key, b"{}".as_slice().into(), Condition::Create)
            .await
        {
            Ok(_) | Err(Error::Conflict(_)) => Ok(()),
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn clear_dispatch_hint(
        &self,
        scope: &Scope,
        operation: Uuid,
        attempt: Uuid,
    ) -> Result<()> {
        self.clear_upload_attempt_hint(scope, operation, attempt)
            .await?;
        self.delete_hint(&self.dispatch_hint(scope, operation, HintKind::Upload(None))?)
            .await
    }

    pub(super) async fn clear_upload_attempt_hint(
        &self,
        scope: &Scope,
        operation: Uuid,
        attempt: Uuid,
    ) -> Result<()> {
        self.delete_hint(&self.dispatch_hint(scope, operation, HintKind::Upload(Some(attempt)))?)
            .await
    }

    pub(crate) async fn clear_rebuild_hint(&self, scope: &Scope, operation: Uuid) -> Result<()> {
        self.delete_hint(&self.dispatch_hint(scope, operation, HintKind::Rebuild)?)
            .await
    }

    pub(super) async fn delete_hint(&self, key: &Key) -> Result<()> {
        match self.storage.delete(key).await {
            Ok(()) | Err(Error::NotFound(_)) => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn dispatch_hint(&self, scope: &Scope, operation: Uuid, kind: HintKind) -> Result<Key> {
        let shard = operation.as_bytes()[0] % DISPATCH_SHARDS;
        let partition = self.dispatch_partition(shard)?;
        let name = match kind {
            HintKind::Rebuild => format!("rebuild-{operation}"),
            HintKind::Upload(Some(attempt)) => format!("{operation}.{attempt}"),
            HintKind::Upload(None) => operation.to_string(),
        };
        Key::parse(format!(
            "{partition}/pending/{}/{}/{name}",
            scope.tenant, scope.collection
        ))
    }

    fn dispatch_partition(&self, shard: u8) -> Result<Key> {
        if shard >= DISPATCH_SHARDS {
            return Err(Error::Invalid(
                "dispatch shard must be between 0 and 15".into(),
            ));
        }
        // Tenant IDs begin with a letter or digit.
        let path = format!("_dispatch/{shard:02x}");
        Key::parse(
            self.prefix
                .as_ref()
                .map_or_else(|| path.clone(), |prefix| format!("{prefix}/{path}")),
        )
    }
}

fn parse_hint(prefix: &Key, key: &Key) -> Option<(Scope, Uuid, HintKind)> {
    let path = key
        .as_str()
        .strip_prefix(prefix.as_str())?
        .strip_prefix('/')?;
    let (tenant, tail) = path.split_once('/')?;
    let (collection, operation) = tail.split_once('/')?;
    let scope = Scope {
        tenant: tenant.parse().ok()?,
        collection: collection.parse().ok()?,
    };
    let (id, kind) = match operation.strip_prefix("rebuild-") {
        Some(id) => (id, HintKind::Rebuild),
        None => match operation.split_once('.') {
            Some((id, attempt)) => (id, HintKind::Upload(Some(attempt.parse().ok()?))),
            None => (operation, HintKind::Upload(None)),
        },
    };
    Some((scope, id.parse().ok()?, kind))
}

#[cfg(test)]
mod tests {
    use std::{
        io::Write,
        sync::{
            Mutex,
            atomic::{AtomicBool, Ordering},
        },
    };

    use sanctum_storage::Storage;

    use super::*;

    struct DelayedDispatch {
        released: AtomicBool,
        first_started: Notify,
        delivered: Mutex<Vec<Uuid>>,
        active: tokio::sync::Semaphore,
    }

    #[async_trait::async_trait]
    impl Dispatch for DelayedDispatch {
        async fn send(&self, task: Task<'_>) -> Result<()> {
            let _permit = self
                .active
                .try_acquire()
                .expect("repair exceeds its concurrency");
            if task.operation == Uuid::from_u128(1) && !self.released.load(Ordering::Relaxed) {
                self.first_started.notify_one();
                std::future::pending::<()>().await;
            }
            self.delivered.lock().unwrap().push(task.operation);
            Ok(())
        }
    }

    #[tokio::test]
    async fn repair_advances_past_a_stalled_hint_and_recovers_it_on_the_next_pass() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Arc::new(sanctum_storage_fs::FileStorage::open(directory.path()).unwrap());
        let fixture = Arc::new(DelayedDispatch {
            active: tokio::sync::Semaphore::new(REPAIR_CONCURRENCY),
            released: AtomicBool::new(false),
            first_started: Notify::new(),
            delivered: Mutex::new(Vec::new()),
        });
        let fresh_catalog = || HostedCatalog::new(storage.clone(), None, 1024).unwrap();
        let api = fresh_catalog();
        let scope = Scope {
            tenant: "default".parse().unwrap(),
            collection: "notes".parse().unwrap(),
        };
        api.create_collection(&scope, None).await.unwrap();
        for id in 1..=u128::from(HINT_PAGE_SIZE) + 1 {
            let text = crate::TextUpload {
                name: "notes.txt".into(),
                external: None,
                replacement: None,
                retain_original: false,
                initial_metadata: Vec::new(),
                initial_chunk_metadata: Vec::new(),
            };
            let upload = api
                .stage_upload(scope.clone(), text, Some(Uuid::from_u128(id)))
                .await
                .unwrap();
            upload.reopen().unwrap().write_all(b"quartz").unwrap();
            upload.enqueue().await.unwrap();
        }
        let worker = || fresh_catalog().with_dispatch(fixture.clone());

        let timed_out = worker()
            .reconcile_shard_with_timeouts(0, Duration::from_secs(2), Duration::from_secs(10))
            .await;
        assert!(
            matches!(timed_out, Err(Error::Busy(message)) if message.contains("page timed out"))
        );
        timeout(Duration::ZERO, fixture.first_started.notified())
            .await
            .unwrap();
        let cursor = Key::parse("_dispatch/00/cursor.json").unwrap();
        assert!(matches!(
            storage.head(&cursor).await,
            Err(Error::NotFound(_))
        ));
        timeout(Duration::from_secs(1), async {
            let _all = fixture
                .active
                .acquire_many(REPAIR_CONCURRENCY as u32)
                .await
                .unwrap();
        })
        .await
        .unwrap();
        let previous = fixture.delivered.lock().unwrap().len();
        assert_eq!(previous, 15);

        let first = worker()
            .reconcile_shard_with_timeouts(0, REPAIR_PAGE_TIMEOUT, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!((first.dispatched, first.failed), (0, 1));
        assert!(first.next_cursor.is_some());
        timeout(Duration::ZERO, fixture.first_started.notified())
            .await
            .unwrap();
        assert_eq!(fixture.active.available_permits(), REPAIR_CONCURRENCY);
        assert_eq!(fixture.delivered.lock().unwrap().len(), 15);

        let second = worker().reconcile_shard(0).await.unwrap();
        assert_eq!((second.dispatched, second.failed), (1, 0));
        assert!(second.next_cursor.is_none());
        assert_eq!(
            fixture.delivered.lock().unwrap().last(),
            Some(&Uuid::from_u128(17))
        );

        fixture.released.store(true, Ordering::Relaxed);
        let recovered = worker().reconcile_shard(0).await.unwrap();
        assert_eq!((recovered.dispatched, recovered.failed), (1, 0));
        assert!(recovered.next_cursor.is_some());
        let mut delivered = fixture.delivered.lock().unwrap().clone();
        delivered.sort_unstable();
        assert_eq!(delivered, (1..=17).map(Uuid::from_u128).collect::<Vec<_>>());
    }
}
