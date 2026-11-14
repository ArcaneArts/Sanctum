use std::{collections::VecDeque, time::Duration};

use sanctum_core::{Error, Result};
use sanctum_db::Unit;
use sanctum_storage::{Condition, Key, MAX_CONTROL_BYTES};
use serde::{Deserialize, Serialize};
use tokio::{io::AsyncReadExt, time::timeout};

use crate::{
    HostedCatalog,
    hosted_collection_creation::creation_key_scope,
    hosted_rebuild::collection_manifest,
    maintenance_budget::{REQUEST_TIMEOUT, SweepBudget},
};

const GRACE: Duration = Duration::from_secs(900);
const UNIT_BUDGET: usize = 8;

#[derive(Default, Serialize, Deserialize)]
struct Cursor {
    next: Option<String>,
    pending: VecDeque<UnitCursor>,
}

#[derive(Serialize, Deserialize)]
struct UnitCursor {
    #[serde(rename = "manifest")]
    key: String,
    next: Option<String>,
}

pub struct SnapshotSweep {
    pub deleted: usize,
    pub units: usize,
    pub recovered_creations: usize,
    pub failed: usize,
    pub more: bool,
}

impl HostedCatalog {
    /// Scans a bounded page of manifests and pending creations.
    pub async fn sweep_hosted_snapshots(&self) -> Result<SnapshotSweep> {
        timeout(
            REQUEST_TIMEOUT,
            self.sweep_snapshot_page(SweepBudget::start()),
        )
        .await
        .map_err(|_| Error::Busy("hosted snapshot sweep checkpoint timed out".into()))?
    }

    async fn sweep_snapshot_page(&self, budget: SweepBudget) -> Result<SnapshotSweep> {
        let path = "_maintenance/snapshot-sweep.json";
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
                        "snapshot sweep cursor exceeds size limit".into(),
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
                        "snapshot sweep cursor length mismatch".into(),
                    ));
                }
                let cursor = serde_json::from_slice(&body)
                    .map_err(|error| Error::Internal(Box::new(error)))?;
                (cursor, Condition::Match(download.object.version))
            }
            Err(Error::NotFound(_)) => (Cursor::default(), Condition::Create),
            Err(error) => return Err(error),
        };
        if cursor.pending.is_empty() {
            let page = self
                .storage
                .list(self.prefix.as_ref(), cursor.next.as_deref(), 128)
                .await?;
            if page.next_cursor.is_some() && page.next_cursor == cursor.next {
                return Err(Error::Internal(
                    "snapshot sweep cursor did not advance".into(),
                ));
            }
            for item in page.objects {
                if unit_from_manifest(self.prefix.as_ref(), &item.key)?.is_some()
                    || creation_key_scope(self.prefix.as_ref(), &item.key).is_some()
                {
                    cursor.pending.push_back(UnitCursor {
                        key: item.key.to_string(),
                        next: None,
                    });
                }
            }
            cursor.next = page.next_cursor;
        }

        let mut report = SnapshotSweep {
            deleted: 0,
            units: 0,
            recovered_creations: 0,
            failed: 0,
            more: false,
        };
        for _ in 0..UNIT_BUDGET.min(cursor.pending.len()) {
            let pending = cursor
                .pending
                .front()
                .expect("budget is bounded by the queue");
            let manifest = Key::parse(&pending.key)?;
            if let Some((tenant, operation)) = creation_key_scope(self.prefix.as_ref(), &manifest) {
                let result = budget
                    .attempt(self.recover_collection_creation(tenant, operation, &manifest))
                    .await;
                match result {
                    Ok(None) => break,
                    Ok(Some(recovered)) => report.recovered_creations += usize::from(recovered),
                    Err(error) => {
                        report.failed += 1;
                        tracing::warn!(%error, key = %manifest, "collection creation remains pending");
                    }
                }
                cursor.pending.pop_front();
                continue;
            }
            let Some(unit) = unit_from_manifest(self.prefix.as_ref(), &manifest)? else {
                cursor.pending.pop_front();
                continue;
            };
            let Some(result) = budget
                .attempt(self.sweep_snapshot_unit(&manifest, &unit, pending.next.as_deref()))
                .await
                .transpose()
            else {
                break;
            };
            let mut pending = cursor
                .pending
                .pop_front()
                .expect("attempt used the front of the queue");
            match result {
                Ok(page) => {
                    report.units += 1;
                    report.deleted += page.deleted;
                    if page.next_cursor.is_some() {
                        if page.next_cursor == pending.next {
                            return Err(Error::Internal(
                                "snapshot list cursor did not advance".into(),
                            ));
                        }
                        pending.next = page.next_cursor;
                        cursor.pending.push_back(pending);
                    }
                }
                Err(Error::NotFound(_)) => {}
                Err(error) => {
                    report.failed += 1;
                    tracing::warn!(%error, %manifest, "snapshot sweep will revisit this unit on the next pass");
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
            kind = "hosted_snapshot",
            deleted_objects = report.deleted,
            units = report.units,
            recovered_creations = report.recovered_creations,
            failed = report.failed,
            more = report.more,
            "completed a bounded cleanup pass"
        );
        Ok(report)
    }

    async fn sweep_snapshot_unit(
        &self,
        manifest: &Key,
        unit: &Unit,
        cursor: Option<&str>,
    ) -> Result<sanctum_db::CleanupPage> {
        if let Some(scope) = collection_manifest(self.prefix.as_ref(), manifest)
            && self.collection_retired(&scope).await?
        {
            self.publication.fence_deletion(unit).await?;
            return self.publication.reclaim_retired(unit, GRACE, cursor).await;
        }
        self.publication.reclaim(unit, GRACE, cursor).await
    }
}

fn unit_from_manifest(prefix: Option<&Key>, key: &Key) -> Result<Option<Unit>> {
    if let Some(scope) = collection_manifest(prefix, key) {
        return Unit::collection(prefix, &scope).map(Some);
    }
    let path = match prefix {
        Some(prefix) => key
            .as_str()
            .strip_prefix(prefix.as_str())
            .and_then(|suffix| suffix.strip_prefix('/')),
        None => Some(key.as_str()),
    };
    let Some(tenant) = path
        .and_then(|path| path.strip_suffix("/catalog/HEAD.json"))
        .and_then(|tenant| tenant.parse().ok())
    else {
        return Ok(None);
    };
    Unit::catalog(prefix, &tenant).map(Some)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use sanctum_core::Scope;

    use super::*;

    #[tokio::test]
    async fn snapshot_sweep_resumes_discovered_units_after_the_work_deadline() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Arc::new(sanctum_storage_fs::FileStorage::open(directory.path()).unwrap());
        let catalog = || HostedCatalog::new(storage.clone(), None, 1024).unwrap();
        let first = catalog();
        first
            .create_collection(
                &Scope {
                    tenant: "default".parse().unwrap(),
                    collection: "notes".parse().unwrap(),
                },
                None,
            )
            .await
            .unwrap();
        let paused = first
            .sweep_snapshot_page(SweepBudget::exhausted())
            .await
            .unwrap();
        assert_eq!((paused.units, paused.failed), (0, 0));
        assert!(paused.more);
        drop(first);
        let resumed = catalog().sweep_hosted_snapshots().await.unwrap();
        assert_eq!((resumed.units, resumed.failed), (2, 0));
        assert!(!resumed.more);
    }
}
