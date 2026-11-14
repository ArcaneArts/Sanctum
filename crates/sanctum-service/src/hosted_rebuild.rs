use sanctum_core::{Error, Result, Scope};
use sanctum_storage::{Condition, Key, MAX_CONTROL_BYTES, Version};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::io::AsyncReadExt;
use uuid::Uuid;

use crate::{
    HostedCatalog, Task, TaskStage, embedding::Coverage, hosted_dispatch::DISPATCH_WINDOW_SECONDS,
    hosted_ingestion::unix_seconds,
};

const DISCOVERY_PAGE: u32 = 256;

#[derive(Serialize, Deserialize)]
struct Discovery {
    profile: Option<String>,
    run: Uuid,
    cursor: Option<String>,
    complete: bool,
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
    pub complete: bool,
}

impl HostedCatalog {
    /// Discover collections from their manifests. The worker resolves the active catalog
    /// and current collection snapshot only when it runs a batch.
    pub async fn discover_profile_rebuild(&self) -> Result<RebuildPage> {
        if self.dispatcher.is_none() && self.standalone_wake.is_none() {
            return Ok(RebuildPage {
                discovered: 0,
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
                    profile: profile.map(str::to_owned),
                    run: Uuid::new_v4(),
                    cursor: None,
                    complete: profile.is_none(),
                };
                let version = self
                    .write_rebuild_record(&key, &discovery, condition)
                    .await?;
                (discovery, version)
            }
        };
        if discovery.complete {
            return Ok(RebuildPage {
                discovered: 0,
                complete: true,
            });
        }

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
        let mut discovered = 0;
        for object in page.objects {
            if let Some(scope) = collection_manifest(self.prefix.as_ref(), &object.key) {
                self.ensure_rebuild(&scope, discovery.run).await?;
                discovered += 1;
            }
        }
        discovery.complete = page.next_cursor.is_none();
        discovery.cursor = page.next_cursor;
        self.write_rebuild_record(&key, &discovery, Condition::Match(version))
            .await?;
        Ok(RebuildPage {
            discovered,
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
