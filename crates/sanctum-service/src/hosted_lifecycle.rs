use std::{collections::VecDeque, time::Duration};

use futures::{StreamExt, stream};
use sanctum_core::{Error, Result, Scope};
use sanctum_db::{Command, Unit, collection_prefix};
use sanctum_storage::{Condition, Key, MAX_CONTROL_BYTES};
use serde::{Deserialize, Serialize};
use tokio::{
    io::AsyncReadExt,
    time::{Instant, timeout, timeout_at},
};
use uuid::Uuid;

use crate::{HostedCatalog, db::hosted, hosted_rebuild::collection_manifest};

#[derive(Serialize, Deserialize)]
struct Deletion {
    source: Uuid,
    expected: Option<Uuid>,
    revision: Uuid,
}

#[derive(Default, Serialize, Deserialize)]
struct SweepCursor {
    next: Option<String>,
    #[serde(default)]
    pending: VecDeque<String>,
}

pub struct SourceSweep {
    pub deleted: usize,
    pub deferred_objects: usize,
    pub collections: usize,
    pub failed: usize,
    pub more: bool,
}

#[derive(Default)]
struct ReclaimedObjects {
    deleted: Vec<String>,
    deferred: Vec<String>,
}

const DELETE_TIMEOUT: Duration = Duration::from_secs(10);
const CONCURRENT_DELETES: usize = 4;
const DISCOVERY_LIMIT: u32 = 32;
const COLLECTION_LIMIT: usize = 8;
const COLLECTION_TIMEOUT: Duration = Duration::from_secs(180);
const SWEEP_WORK_TIMEOUT: Duration = Duration::from_secs(210);
const SWEEP_TIMEOUT: Duration = Duration::from_secs(240);

impl HostedCatalog {
    pub async fn delete_source(
        &self,
        scope: &Scope,
        source: Uuid,
        expected: Option<Uuid>,
        request_id: Option<Uuid>,
    ) -> Result<Uuid> {
        if request_id.is_some_and(|id| id.is_nil()) {
            return Err(Error::Invalid("request ID must be a nonzero UUID".into()));
        }
        self.require_collection(scope).await?;
        let unit = Unit::collection(self.prefix.as_ref(), scope)?;
        let mut writer = self.publication.acquire(&unit).await?;
        let result = writer
            .apply_stage(
                "delete-source",
                vec![Command {
                    operation: request_id.unwrap_or_else(Uuid::new_v4),
                    payload: (source, expected),
                }],
                |tx, (source, expected)| {
                    let revision = hosted::delete_source(tx, source, expected)?;
                    serde_json::to_string(&Deletion {
                        source,
                        expected,
                        revision,
                    })
                    .map_err(|error| Error::Internal(Box::new(error)))
                },
            )
            .await;
        let released = writer.release().await;
        let (_, receipts) = result?;
        released?;
        let deletion: Deletion = serde_json::from_str(&receipts[0].outcome)
            .map_err(|error| Error::Internal(Box::new(error)))?;
        if deletion.source != source || deletion.expected != expected {
            return Err(Error::Conflict(
                "request ID already applies to a different source deletion".into(),
            ));
        }
        Ok(deletion.revision)
    }

    pub async fn sweep_hosted_sources(&self) -> Result<SourceSweep> {
        timeout(
            SWEEP_TIMEOUT,
            self.sweep_source_page(Instant::now() + SWEEP_WORK_TIMEOUT, COLLECTION_TIMEOUT),
        )
        .await
        .map_err(|_| Error::Busy("hosted source sweep checkpoint timed out".into()))?
    }

    async fn sweep_source_page(
        &self,
        work_deadline: Instant,
        collection_timeout: Duration,
    ) -> Result<SourceSweep> {
        let path = "_maintenance/source-sweep.json";
        let key = Key::parse(
            self.prefix
                .as_ref()
                .map_or_else(|| path.into(), |prefix| format!("{prefix}/{path}")),
        )?;
        let (mut cursor, condition) = match self.storage.get(&key).await {
            Ok(download) => {
                let size = download.object.length;
                if size > MAX_CONTROL_BYTES as u64 {
                    return Err(Error::Internal(
                        "source sweep cursor exceeds size limit".into(),
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
                        "source sweep cursor length mismatch".into(),
                    ));
                }
                let cursor = serde_json::from_slice(&body)
                    .map_err(|error| Error::Internal(Box::new(error)))?;
                (cursor, Condition::Match(download.object.version))
            }
            Err(Error::NotFound(_)) => (SweepCursor::default(), Condition::Create),
            Err(error) => return Err(error),
        };
        if cursor.pending.is_empty() {
            let page = self
                .storage
                .list(
                    self.prefix.as_ref(),
                    cursor.next.as_deref(),
                    DISCOVERY_LIMIT,
                )
                .await?;
            if page.next_cursor.is_some() && page.next_cursor == cursor.next {
                return Err(Error::Internal(
                    "source sweep cursor did not advance".into(),
                ));
            }
            cursor.pending = page
                .objects
                .into_iter()
                .filter(|object| collection_manifest(self.prefix.as_ref(), &object.key).is_some())
                .map(|object| object.key.to_string())
                .collect();
            cursor.next = page.next_cursor;
        }
        let mut report = SourceSweep {
            deleted: 0,
            deferred_objects: 0,
            collections: 0,
            failed: 0,
            more: false,
        };
        for _ in 0..COLLECTION_LIMIT.min(cursor.pending.len()) {
            if Instant::now() >= work_deadline {
                break;
            }
            let manifest = cursor
                .pending
                .front()
                .expect("work is bounded by the queue");
            let scope = collection_manifest(self.prefix.as_ref(), &Key::parse(manifest)?)
                .ok_or_else(|| {
                    Error::Internal("invalid collection manifest in source sweep cursor".into())
                })?;
            let attempt = timeout(collection_timeout, self.reclaim_source_batch(&scope));
            let Ok(result) = timeout_at(work_deadline, attempt).await else {
                // Give the unfinished collection a full attempt in the next invocation.
                break;
            };
            cursor.pending.pop_front();
            match result
                .unwrap_or_else(|_| Err(Error::Busy("source cleanup attempt timed out".into())))
            {
                Ok((deleted, deferred)) => {
                    report.collections += 1;
                    report.deleted += deleted;
                    report.deferred_objects += deferred;
                    report.failed += usize::from(deferred > 0);
                }
                Err(Error::NotFound(_)) => {}
                Err(error) => {
                    report.failed += 1;
                    tracing::warn!(%error, tenant = %scope.tenant, collection = %scope.collection, "hosted source sweep will retry on its next pass");
                }
            }
        }
        report.more = !cursor.pending.is_empty() || cursor.next.is_some();
        let body = serde_json::to_vec(&cursor).map_err(|error| Error::Internal(Box::new(error)))?;
        self.storage
            .put_control(&key, body.into(), condition)
            .await?;
        tracing::debug!(
            event = "cleanup_pass",
            kind = "hosted_source",
            deleted_objects = report.deleted,
            deferred_objects = report.deferred_objects,
            collections = report.collections,
            failed = report.failed,
            more = report.more,
            "completed a bounded cleanup pass"
        );
        Ok(report)
    }

    /// Reclaims a bounded set of objects after their locators and reader grace expire.
    pub async fn reclaim_hosted_sources(&self, scope: &Scope) -> Result<usize> {
        self.reclaim_source_batch(scope)
            .await
            .map(|(deleted, _)| deleted)
    }

    async fn reclaim_source_batch(&self, scope: &Scope) -> Result<(usize, usize)> {
        let profile = self
            .embedding
            .as_ref()
            .map(|provider| provider.profile_id().to_owned());
        let selected = profile.clone();
        let ((retire, objects), _) = self
            .with_collection(scope, move |db| {
                Ok((
                    db.has_hosted_retirement_work(selected.as_deref())?,
                    db.due_hosted_objects()?,
                ))
            })
            .await?;
        let prefix = format!("{}/", collection_prefix(self.prefix.as_ref(), scope)?);
        let mut deletions = stream::iter(objects)
            .map(|object| {
                let prefix = &prefix;
                async move {
                    let result = self.delete_retired_object(prefix, &object).await;
                    (object, result)
                }
            })
            .buffer_unordered(CONCURRENT_DELETES);
        let mut settled = ReclaimedObjects::default();
        while let Some((object, result)) = deletions.next().await {
            match result {
                Ok(()) | Err(Error::NotFound(_)) => settled.deleted.push(object),
                Err(error) => {
                    tracing::warn!(%error, tenant = %scope.tenant, collection = %scope.collection,
                        "retired source object deletion will be retried");
                    settled.deferred.push(object);
                }
            }
        }
        let counts = (settled.deleted.len(), settled.deferred.len());
        if retire || counts != (0, 0) {
            let unit = Unit::collection(self.prefix.as_ref(), scope)?;
            let mut writer = self.publication.acquire(&unit).await?;
            let result = writer
                .apply_stage(
                    "retire-sources",
                    vec![Command {
                        operation: Uuid::new_v4(),
                        payload: settled,
                    }],
                    move |tx, settled| {
                        if retire {
                            hosted::retire_revisions(tx, profile.as_deref())?;
                        }
                        hosted::settle_object_deletions(tx, &settled.deleted, &settled.deferred)?;
                        Ok("reclaimed".into())
                    },
                )
                .await;
            let released = writer.release().await;
            result?;
            released?;
        }
        Ok(counts)
    }

    async fn delete_retired_object(&self, prefix: &str, object: &str) -> Result<()> {
        if !object.starts_with(prefix) {
            return Err(Error::Internal(
                "retired object key escapes its collection".into(),
            ));
        }
        let key = Key::parse(object)?;
        tokio::time::timeout(DELETE_TIMEOUT, self.storage.delete(&key))
            .await
            .map_err(|_| Error::Busy("retired source object deletion timed out".into()))?
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[tokio::test]
    async fn source_sweep_checkpoints_discovery_when_its_work_budget_is_spent() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Arc::new(sanctum_storage_fs::FileStorage::open(directory.path()).unwrap());
        let catalog = || HostedCatalog::new(storage.clone(), None, 1024).unwrap();
        let service = catalog();
        let scope = Scope {
            tenant: "default".parse().unwrap(),
            collection: "notes".parse().unwrap(),
        };
        service.create_collection(&scope, None).await.unwrap();
        let paused = service
            .sweep_source_page(Instant::now(), COLLECTION_TIMEOUT)
            .await
            .unwrap();
        assert_eq!(paused.collections, 0);
        assert_eq!(paused.failed, 0);
        assert!(paused.more);
        drop(service);

        let resumed = catalog().sweep_hosted_sources().await.unwrap();
        assert_eq!(resumed.collections, 1);
        assert_eq!(resumed.failed, 0);
        assert!(!resumed.more);
    }
}
