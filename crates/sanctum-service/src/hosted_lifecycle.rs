use std::time::Duration;

use futures::{StreamExt, stream};
use sanctum_core::{Error, Result, Scope};
use sanctum_db::{Command, Unit, collection_prefix};
use sanctum_storage::{Condition, Key, MAX_CONTROL_BYTES};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
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
        let path = "_maintenance/source-sweep.json";
        let key = Key::parse(
            self.prefix
                .as_ref()
                .map_or_else(|| path.into(), |prefix| format!("{prefix}/{path}")),
        )?;
        let (cursor, condition) = match self.storage.get(&key).await {
            Ok(download) => {
                if download.object.length > MAX_CONTROL_BYTES as u64 {
                    return Err(Error::Internal(
                        "source sweep cursor exceeds size limit".into(),
                    ));
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
            Err(Error::NotFound(_)) => (SweepCursor::default(), Condition::Create),
            Err(error) => return Err(error),
        };
        let page = self
            .storage
            .list(self.prefix.as_ref(), cursor.next.as_deref(), 32)
            .await?;
        if page.next_cursor.is_some() && page.next_cursor == cursor.next {
            return Err(Error::Internal(
                "source sweep cursor did not advance".into(),
            ));
        }
        let mut report = SourceSweep {
            deleted: 0,
            deferred_objects: 0,
            collections: 0,
            failed: 0,
            more: page.next_cursor.is_some(),
        };
        for object in page.objects {
            let Some(scope) = collection_manifest(self.prefix.as_ref(), &object.key) else {
                continue;
            };
            match self.reclaim_source_batch(&scope).await {
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
        let body = serde_json::to_vec(&SweepCursor {
            next: page.next_cursor,
        })
        .map_err(|error| Error::Internal(Box::new(error)))?;
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
