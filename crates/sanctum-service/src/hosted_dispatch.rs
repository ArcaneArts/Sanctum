use std::sync::Arc;

use sanctum_core::{Error, Result, Scope};
use sanctum_storage::{Condition, Key, MAX_CONTROL_BYTES};
use serde::{Deserialize, Serialize};
use tokio::{io::AsyncReadExt, sync::Notify};
use uuid::Uuid;

use crate::HostedCatalog;

pub(crate) const DISPATCH_WINDOW_SECONDS: u64 = 6 * 3600;
pub const DISPATCH_SHARDS: u8 = 16;

#[derive(Default, Serialize, Deserialize)]
struct Cursor {
    next: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskStage {
    Register,
    Index,
    Embed { step: u32 },
    Rebuild { step: u32 },
}

pub struct Task<'a> {
    pub scope: &'a Scope,
    pub operation: Uuid,
    pub stage: TaskStage,
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

struct HintPage {
    hints: Vec<(Scope, Uuid, bool)>,
    next_cursor: Option<String>,
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
        if let Err(error) = self.dispatch_pending(scope, operation).await {
            tracing::warn!(%error, tenant = %scope.tenant, collection = %scope.collection, %operation,
                "hosted operation remains pending dispatch");
        }
    }

    /// Persists a continuation so a fresh scheduler invocation resumes the scan.
    pub async fn reconcile_shard(&self, shard: u8) -> Result<ReconcilePage> {
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
            .reconcile_dispatch(shard, cursor.next.as_deref())
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

    /// Replays dispatch hints without opening any tenant or collection database.
    async fn reconcile_dispatch(&self, shard: u8, cursor: Option<&str>) -> Result<ReconcilePage> {
        if self.dispatcher.is_none() {
            return Err(Error::Invalid(
                "hosted task dispatch is not configured".into(),
            ));
        }
        self.visit_hints(shard, cursor, false, || false).await
    }

    pub async fn work_standalone_shard(
        &self,
        shard: u8,
        cursor: Option<&str>,
        stopping: impl Fn() -> bool,
    ) -> Result<ReconcilePage> {
        self.visit_hints(shard, cursor, true, stopping).await
    }

    async fn visit_hints(
        &self,
        shard: u8,
        cursor: Option<&str>,
        standalone: bool,
        stopping: impl Fn() -> bool,
    ) -> Result<ReconcilePage> {
        let HintPage { hints, next_cursor } = self.hint_page(shard, cursor).await?;
        let mut dispatched = 0;
        let mut failed = 0;
        for (scope, operation, rebuild) in hints {
            if stopping() {
                break;
            }
            let result = match (standalone, rebuild) {
                (true, true) => self.work_standalone_rebuild(&scope, operation).await,
                (true, false) => self.work_standalone_upload(&scope, operation).await,
                (false, true) => self.dispatch_rebuild(&scope, operation).await,
                (false, false) => self.dispatch_pending(&scope, operation).await,
            };
            match result {
                Ok(true) => dispatched += 1,
                Ok(false) => {}
                Err(error) => {
                    failed += 1;
                    tracing::warn!(%error, tenant = %scope.tenant, collection = %scope.collection,
                        %operation, "could not reconcile hosted dispatch");
                }
            }
        }
        Ok(ReconcilePage {
            dispatched,
            failed,
            next_cursor,
        })
    }

    async fn hint_page(&self, shard: u8, cursor: Option<&str>) -> Result<HintPage> {
        let prefix = self.dispatch_partition(shard)?;
        let pending = Key::parse(format!("{prefix}/pending"))?;
        let page = self.storage.list(Some(&pending), cursor, 16).await?;
        if page.next_cursor.is_some() && page.next_cursor.as_deref() == cursor {
            return Err(Error::Internal(
                "dispatch list cursor did not advance".into(),
            ));
        }
        Ok(HintPage {
            hints: page
                .objects
                .into_iter()
                .filter_map(|entry| parse_hint(&pending, &entry.key))
                .collect(),
            next_cursor: page.next_cursor,
        })
    }

    pub(crate) async fn record_dispatch_hint(&self, scope: &Scope, operation: Uuid) -> Result<()> {
        self.record_hint(scope, operation, false).await
    }

    pub(crate) async fn record_hint(
        &self,
        scope: &Scope,
        operation: Uuid,
        rebuild: bool,
    ) -> Result<()> {
        let key = self.dispatch_hint(scope, operation, rebuild)?;
        match self
            .storage
            .put_control(&key, b"{}".as_slice().into(), Condition::Create)
            .await
        {
            Ok(_) | Err(Error::Conflict(_)) => Ok(()),
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn clear_dispatch_hint(&self, scope: &Scope, operation: Uuid) -> Result<()> {
        self.clear_hint(scope, operation, false).await
    }

    pub(crate) async fn clear_hint(
        &self,
        scope: &Scope,
        operation: Uuid,
        rebuild: bool,
    ) -> Result<()> {
        let key = self.dispatch_hint(scope, operation, rebuild)?;
        match self.storage.delete(&key).await {
            Ok(()) | Err(Error::NotFound(_)) => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn dispatch_hint(&self, scope: &Scope, operation: Uuid, rebuild: bool) -> Result<Key> {
        let shard = operation.as_bytes()[0] % DISPATCH_SHARDS;
        let partition = self.dispatch_partition(shard)?;
        let name = if rebuild {
            format!("rebuild-{operation}")
        } else {
            operation.to_string()
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

fn parse_hint(prefix: &Key, key: &Key) -> Option<(Scope, Uuid, bool)> {
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
    let (id, rebuild) = match operation.strip_prefix("rebuild-") {
        Some(id) => (id, true),
        None => (operation, false),
    };
    Some((scope, id.parse().ok()?, rebuild))
}
