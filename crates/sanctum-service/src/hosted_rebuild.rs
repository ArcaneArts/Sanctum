use std::time::Duration;

use futures::{StreamExt, stream};
use sanctum_core::{Error, Result, Scope};
use sanctum_storage::{Condition, Key, MAX_CONTROL_BYTES, Version};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::{io::AsyncReadExt, time::timeout};
use uuid::Uuid;

use crate::{
    HostedCatalog, Task, TaskStage,
    embedding::Coverage,
    hosted_dispatch::DISPATCH_WINDOW_SECONDS,
    hosted_ingestion::unix_seconds,
    maintenance_budget::{REQUEST_TIMEOUT, SweepBudget},
};

const DISCOVERY_PAGE: u32 = 256;
const DISCOVERY_BATCH: usize = 16;
const DISCOVERY_CONCURRENCY: usize = 4;
const DISCOVERY_RETRIES_PER_BATCH: usize = 4;
const DISCOVERY_RETRY_LIMIT: usize = 256;
const DISCOVERY_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Serialize, Deserialize)]
struct Discovery {
    #[serde(default)]
    checkpoint_version: u8,
    profile: Option<String>,
    run: Uuid,
    cursor: Option<String>,
    complete: bool,
    #[serde(default)]
    pending: Vec<String>,
    #[serde(default)]
    at_end: bool,
    #[serde(default)]
    retries: Vec<String>,
    #[serde(default)]
    rescan: bool,
}

#[derive(Serialize, Deserialize)]
struct Rebuild {
    run: Uuid,
    operation: Uuid,
    step: Option<u32>,
    not_before: Option<u64>,
    dispatched_window: Option<u64>,
}

pub struct RebuildPage {
    pub discovered: usize,
    pub deferred: usize,
    pub complete: bool,
}

impl HostedCatalog {
    /// Discover collections from their manifests. The worker resolves the active catalog
    /// and current collection snapshot only when it runs a batch.
    pub async fn discover_profile_rebuild(&self) -> Result<RebuildPage> {
        timeout(
            REQUEST_TIMEOUT,
            self.discover_profile_page(SweepBudget::with_item_timeout(DISCOVERY_ATTEMPT_TIMEOUT)),
        )
        .await
        .map_err(|_| Error::Busy("profile discovery checkpoint timed out".into()))?
    }

    async fn discover_profile_page(&self, budget: SweepBudget) -> Result<RebuildPage> {
        if self.dispatcher.is_none() && self.standalone_wake.is_none() {
            return Ok(RebuildPage {
                discovered: 0,
                deferred: 0,
                complete: true,
            });
        }
        let profile = self.embedding_profile();
        let key = self.discovery_key()?;
        let existing = self.read_rebuild_record::<Discovery>(&key).await?;
        let (mut discovery, version) = match existing {
            Some((discovery, version)) if discovery.profile.as_deref() == profile => {
                (discovery, version)
            }
            existing => {
                let condition =
                    existing.map_or(Condition::Create, |(_, version)| Condition::Match(version));
                let discovery = Discovery {
                    checkpoint_version: 1,
                    profile: profile.map(str::to_owned),
                    run: Uuid::new_v4(),
                    cursor: None,
                    complete: profile.is_none(),
                    pending: Vec::new(),
                    at_end: false,
                    retries: Vec::new(),
                    rescan: false,
                };
                let version = self
                    .write_rebuild_record(&key, &discovery, condition)
                    .await?;
                (discovery, version)
            }
        };
        if discovery.checkpoint_version == 0 {
            // An older worker can overwrite the new checkpoint during rollout.
            // Re-list with the same run ID to recover work it could not track.
            discovery.checkpoint_version = 1;
            discovery.cursor = None;
            discovery.complete = profile.is_none();
        } else if discovery.complete {
            return Ok(RebuildPage {
                discovered: 0,
                deferred: 0,
                complete: true,
            });
        }

        if !discovery.complete && discovery.pending.is_empty() && !discovery.at_end {
            let page = self
                .storage
                .list(
                    self.prefix.as_ref(),
                    discovery.cursor.as_deref(),
                    DISCOVERY_PAGE,
                )
                .await?;
            if page.next_cursor.is_some() && page.next_cursor == discovery.cursor {
                return Err(Error::Internal(
                    "profile discovery cursor did not advance".into(),
                ));
            }
            discovery.pending = page
                .objects
                .into_iter()
                .filter(|object| {
                    collection_manifest(self.prefix.as_ref(), &object.key).is_some()
                        && !discovery
                            .retries
                            .iter()
                            .any(|key| key == object.key.as_str())
                })
                .map(|object| object.key.to_string())
                .collect();
            discovery.cursor = page.next_cursor;
            discovery.at_end = discovery.cursor.is_none();
        }

        let reserved_for_new = discovery
            .pending
            .len()
            .min(DISCOVERY_BATCH - DISCOVERY_RETRIES_PER_BATCH);
        let retry_count = discovery
            .retries
            .len()
            .min(DISCOVERY_BATCH - reserved_for_new);
        let new_count = discovery.pending.len().min(DISCOVERY_BATCH - retry_count);
        let candidates = discovery
            .retries
            .drain(..retry_count)
            .chain(discovery.pending.drain(..new_count))
            .map(|path| {
                let manifest = Key::parse(&path)?;
                let scope = collection_manifest(self.prefix.as_ref(), &manifest)
                    .ok_or_else(|| Error::Internal("invalid profile discovery manifest".into()))?;
                Ok((path, scope))
            })
            .collect::<Result<Vec<_>>>()?;
        let run = discovery.run;
        let mut attempts = stream::iter(candidates)
            .map(|(path, scope)| {
                let budget = &budget;
                async move {
                    let result = budget.attempt(self.ensure_rebuild(&scope, run)).await;
                    (path, scope, result)
                }
            })
            .buffer_unordered(DISCOVERY_CONCURRENCY);
        let mut discovered = 0;
        let mut deferred = 0;
        while let Some((path, scope, result)) = attempts.next().await {
            match result {
                Ok(Some(())) => {
                    discovered += 1;
                    continue;
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(%error, tenant = %scope.tenant, collection = %scope.collection,
                    "profile rebuild discovery will retry the collection")
                }
            }
            deferred += 1;
            if discovery.retries.len() < DISCOVERY_RETRY_LIMIT {
                discovery.retries.push(path);
            } else {
                // Bound the control object. Entries past the limit are rediscovered
                // with the same run ID, so already-scheduled tasks stay reusable.
                discovery.rescan = true;
            }
        }
        if discovery.at_end && discovery.pending.is_empty() && discovery.rescan {
            discovery.at_end = false;
            discovery.rescan = false;
        }
        discovery.complete = discovery.complete
            || (discovery.at_end && discovery.pending.is_empty() && discovery.retries.is_empty());
        self.write_rebuild_record(&key, &discovery, Condition::Match(version))
            .await?;
        tracing::debug!(
            event = "embedding_profile_discovery",
            discovered,
            deferred,
            pending = discovery.pending.len(),
            complete = discovery.complete,
            "saved hosted profile discovery progress"
        );
        Ok(RebuildPage {
            discovered,
            deferred,
            complete: discovery.complete,
        })
    }

    pub(crate) async fn schedule_rebuild_on_access(
        &self,
        scope: &Scope,
        profile: Option<&str>,
        coverage: Coverage,
    ) {
        let Some(profile) = profile else {
            return;
        };
        if coverage.total <= coverage.embedded
            || (self.dispatcher.is_none() && self.standalone_wake.is_none())
        {
            return;
        }
        match tokio::time::timeout(
            std::time::Duration::from_secs(1),
            self.schedule_accessed_collection(scope, profile),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(%error, tenant = %scope.tenant, collection = %scope.collection,
                "profile rebuild will be retried by discovery")
            }
            Err(_) => tracing::debug!(tenant = %scope.tenant, collection = %scope.collection,
                "profile rebuild scheduling deferred to discovery"),
        }
    }

    async fn schedule_accessed_collection(&self, scope: &Scope, profile: &str) -> Result<()> {
        let Some((discovery, _)) = self
            .read_rebuild_record::<Discovery>(&self.discovery_key()?)
            .await?
        else {
            return Ok(());
        };
        // Only the worker selects the deployment's active profile. An older
        // API process can still serve lexical retrieval during a rollout.
        if discovery.complete || discovery.profile.as_deref() != Some(profile) {
            return Ok(());
        }
        self.ensure_rebuild(scope, discovery.run).await?;
        if let Some(wake) = &self.standalone_wake {
            wake.notify_one();
        }
        Ok(())
    }

    async fn ensure_rebuild(&self, scope: &Scope, run: Uuid) -> Result<()> {
        let key = self.rebuild_key(scope)?;
        let existing = self.read_rebuild_record::<Rebuild>(&key).await?;
        let condition = match existing {
            Some((record, _)) if record.run == run => return Ok(()),
            Some((_, version)) => Condition::Match(version),
            None => Condition::Create,
        };
        let current = self
            .read_rebuild_record::<Discovery>(&self.discovery_key()?)
            .await?;
        if current.is_none_or(|(current, _)| current.run != run) {
            return Err(Error::Busy(
                "embedding profile changed during discovery".into(),
            ));
        }
        let operation = scoped_id(run, scope, 0);
        let record = Rebuild {
            run,
            operation,
            step: Some(0),
            not_before: None,
            dispatched_window: None,
        };
        self.record_rebuild_hint(scope, operation).await?;
        match self.write_rebuild_record(&key, &record, condition).await {
            Ok(_) => Ok(()),
            Err(Error::Conflict(_)) => {
                let current = self.read_rebuild_record::<Rebuild>(&key).await?;
                if current.is_some_and(|(current, _)| current.run == run) {
                    Ok(())
                } else {
                    Err(Error::Busy(
                        "profile rebuild changed during scheduling".into(),
                    ))
                }
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn dispatch_rebuild(&self, scope: &Scope, operation: Uuid) -> Result<bool> {
        if self.clear_retired_dispatch(scope, operation, true).await? {
            return Ok(false);
        }
        let Some(dispatcher) = self
            .dispatcher
            .as_ref()
            .filter(|_| self.embedding.is_some())
        else {
            return Ok(false);
        };
        let Some((key, mut record, version)) = self.current_rebuild(scope, operation).await? else {
            self.clear_rebuild_hint(scope, operation).await?;
            return Ok(false);
        };
        let Some(step) = record.step else {
            self.clear_rebuild_hint(scope, operation).await?;
            return Ok(false);
        };
        let now = unix_seconds()?;
        let window = record.not_before.unwrap_or(now).max(now) / DISPATCH_WINDOW_SECONDS;
        if record
            .dispatched_window
            .is_some_and(|previous| previous >= window)
        {
            return Ok(false);
        }
        dispatcher
            .send(Task {
                scope,
                operation,
                stage: TaskStage::Rebuild { step },
                profile: self.embedding_profile(),
                window,
                retry: 0,
                not_before: record.not_before,
            })
            .await?;
        record.dispatched_window = Some(window);
        match self
            .write_rebuild_record(&key, &record, Condition::Match(version))
            .await
        {
            Ok(_) | Err(Error::Conflict(_)) => Ok(true),
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn work_standalone_rebuild(
        &self,
        scope: &Scope,
        operation: Uuid,
    ) -> Result<bool> {
        if self.clear_retired_dispatch(scope, operation, true).await? {
            return Ok(false);
        }
        if self.embedding.is_none() {
            return Ok(false);
        }
        let Some((_, record, _)) = self.current_rebuild(scope, operation).await? else {
            self.clear_rebuild_hint(scope, operation).await?;
            return Ok(false);
        };
        let Some(step) = record.step else {
            self.clear_rebuild_hint(scope, operation).await?;
            return Ok(false);
        };
        if let Some(not_before) = record.not_before
            && unix_seconds()? < not_before
        {
            return Ok(false);
        }
        self.run_profile_rebuild(scope, operation, step).await?;
        Ok(true)
    }

    pub async fn deliver_profile_rebuild(
        &self,
        scope: &Scope,
        operation: Uuid,
        step: u32,
        target_profile: Option<&str>,
    ) -> Result<()> {
        if self.clear_retired_dispatch(scope, operation, true).await? {
            return Ok(());
        }
        if target_profile.is_some() && target_profile != self.embedding_profile() {
            return Ok(());
        }
        self.run_profile_rebuild(scope, operation, step).await
    }

    pub async fn run_profile_rebuild(
        &self,
        scope: &Scope,
        operation: Uuid,
        step: u32,
    ) -> Result<()> {
        let Some((key, mut record, version)) = self.current_rebuild(scope, operation).await? else {
            return Ok(());
        };
        match record.step {
            Some(current) if current > step => return Ok(()),
            Some(current) if current < step => {
                return Err(Error::Busy(
                    "profile rebuild awaits its preceding batch".into(),
                ));
            }
            None => return Ok(()),
            Some(_) => {}
        }
        let now = unix_seconds()?;
        if record.not_before.is_some_and(|not_before| now < not_before) {
            return Err(Error::Busy("profile rebuild is scheduled for later".into()));
        }
        let count = match self
            .embed_collection_with_id(scope, scoped_id(record.run, scope, u64::from(step) + 1))
            .await
        {
            Ok(count) => Some(count),
            Err(Error::NotFound(_)) => None,
            Err(error) => return Err(error),
        };
        record.not_before = if count == Some(0) {
            self.embedding_status(scope, None).await?.next_retry
        } else {
            None
        };
        record.step = if count.unwrap_or(0) > 0 || record.not_before.is_some() {
            Some(
                step.checked_add(1)
                    .ok_or_else(|| Error::Internal("profile rebuild step exhausted".into()))?,
            )
        } else {
            None
        };
        record.dispatched_window = None;
        self.write_rebuild_record(&key, &record, Condition::Match(version))
            .await?;
        Ok(())
    }

    pub async fn try_dispatch_rebuild(&self, scope: &Scope, operation: Uuid) {
        if let Err(error) = self.dispatch_rebuild(scope, operation).await {
            tracing::warn!(%error, tenant = %scope.tenant, collection = %scope.collection, %operation,
                "profile rebuild remains pending dispatch");
        }
    }

    async fn current_rebuild(
        &self,
        scope: &Scope,
        operation: Uuid,
    ) -> Result<Option<(Key, Rebuild, Version)>> {
        let Some(provider) = &self.embedding else {
            return Ok(None);
        };
        let Some((discovery, _)) = self
            .read_rebuild_record::<Discovery>(&self.discovery_key()?)
            .await?
        else {
            return Ok(None);
        };
        if discovery.profile.as_deref() != Some(provider.profile_id()) {
            return Err(Error::Busy(
                "profile rebuild belongs to another worker configuration".into(),
            ));
        }
        if scoped_id(discovery.run, scope, 0) != operation {
            return Ok(None);
        }
        let key = self.rebuild_key(scope)?;
        let (record, version) = self
            .read_rebuild_record::<Rebuild>(&key)
            .await?
            .ok_or_else(|| Error::Busy("profile rebuild discovery is incomplete".into()))?;
        if record.run != discovery.run {
            return Err(Error::Busy(
                "profile rebuild discovery is incomplete".into(),
            ));
        }
        if record.operation != operation {
            return Err(Error::Internal("profile rebuild identity mismatch".into()));
        }
        Ok(Some((key, record, version)))
    }

    fn discovery_key(&self) -> Result<Key> {
        self.embedding_key("discovery.json")
    }

    fn rebuild_key(&self, scope: &Scope) -> Result<Key> {
        self.embedding_key(&format!("{}/{}.json", scope.tenant, scope.collection))
    }

    fn embedding_key(&self, suffix: &str) -> Result<Key> {
        let path = format!("_embedding/{suffix}");
        Key::parse(
            self.prefix
                .as_ref()
                .map_or_else(|| path.clone(), |prefix| format!("{prefix}/{path}")),
        )
    }

    async fn read_rebuild_record<T: DeserializeOwned>(
        &self,
        key: &Key,
    ) -> Result<Option<(T, Version)>> {
        let download = match self.storage.get(key).await {
            Ok(download) => download,
            Err(Error::NotFound(_)) => return Ok(None),
            Err(error) => return Err(error),
        };
        let size = download.object.length;
        if size > MAX_CONTROL_BYTES as u64 {
            return Err(Error::Internal(
                "profile rebuild record exceeds size limit".into(),
            ));
        }
        let mut body = Vec::new();
        download
            .reader
            .take(size + 1)
            .read_to_end(&mut body)
            .await?;
        if body.len() as u64 != size {
            return Err(Error::Internal(
                "profile rebuild record length mismatch".into(),
            ));
        }
        let record =
            serde_json::from_slice(&body).map_err(|error| Error::Internal(Box::new(error)))?;
        Ok(Some((record, download.object.version)))
    }

    async fn write_rebuild_record<T: Serialize>(
        &self,
        key: &Key,
        record: &T,
        condition: Condition,
    ) -> Result<Version> {
        let body = serde_json::to_vec(record).map_err(|error| Error::Internal(Box::new(error)))?;
        self.storage.put_control(key, body.into(), condition).await
    }
}

fn scoped_id(run: Uuid, scope: &Scope, step: u64) -> Uuid {
    let mut hash = blake3::Hasher::new();
    hash.update(run.as_bytes());
    hash.update(scope.tenant.as_str().as_bytes());
    hash.update(b"/");
    hash.update(scope.collection.as_str().as_bytes());
    hash.update(&step.to_le_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    Uuid::from_bytes(bytes)
}

pub(crate) fn collection_manifest(prefix: Option<&Key>, key: &Key) -> Option<Scope> {
    let path = match prefix {
        Some(prefix) => key
            .as_str()
            .strip_prefix(prefix.as_str())?
            .strip_prefix('/')?,
        None => key.as_str(),
    };
    let (tenant, path) = path.split_once("/collections/")?;
    let collection = path.strip_suffix("/HEAD.json")?;
    Some(Scope {
        tenant: tenant.parse().ok()?,
        collection: collection.parse().ok()?,
    })
}
