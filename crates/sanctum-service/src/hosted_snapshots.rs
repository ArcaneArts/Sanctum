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
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    retired: bool,
}

pub struct SnapshotSweep {
    pub deleted: usize,
    pub failed_deletions: usize,
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
                        retired: false,
                    });
                }
            }
            cursor.next = page.next_cursor;
        }

        let mut report = SnapshotSweep {
            deleted: 0,
            failed_deletions: 0,
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
                .attempt(self.sweep_snapshot_unit(&manifest, &unit, pending))
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
                Ok((page, retired)) => {
                    report.units += 1;
                    report.deleted += page.deleted;
                    report.failed_deletions += page.failed_deletions;
                    if page.next_cursor.is_some() {
                        if pending.retired == retired && page.next_cursor == pending.next {
                            return Err(Error::Internal(
                                "snapshot list cursor did not advance".into(),
                            ));
                        }
                        pending.next = page.next_cursor;
                        pending.retired = retired;
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
            failed_deletions = report.failed_deletions,
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
        pending: &UnitCursor,
    ) -> Result<(sanctum_db::CleanupPage, bool)> {
        let retired = match collection_manifest(self.prefix.as_ref(), manifest) {
            Some(scope) => self.collection_retired(&scope).await?,
            None => false,
        };
        // Active and retired sweeps list different prefixes.
        let cursor = pending
            .next
            .as_deref()
            .filter(|_| pending.retired == retired);
        let page = if retired {
            self.publication.fence_deletion(unit).await?;
            self.publication
                .reclaim_retired(unit, GRACE, cursor)
                .await?
        } else {
            self.publication.reclaim(unit, GRACE, cursor).await?
        };
        Ok((page, retired))
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
    use std::{
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    use sanctum_core::Scope;
    use sanctum_storage::Storage;

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

    #[tokio::test]
    async fn retired_collection_restarts_listing_from_the_collection_prefix() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Arc::new(sanctum_storage_fs::FileStorage::open(directory.path()).unwrap());
        let catalog = || HostedCatalog::new(storage.clone(), None, 1024).unwrap();
        let scope = Scope {
            tenant: "default".parse().unwrap(),
            collection: "notes".parse().unwrap(),
        };
        let prefix = Key::parse("default/collections/notes").unwrap();
        let head = Key::parse(format!("{prefix}/HEAD.json")).unwrap();
        let input = Key::parse(format!("{prefix}/operations/pending/input")).unwrap();
        let service = catalog();
        service.create_collection(&scope, None).await.unwrap();
        storage
            .put_control(&input, b"source".as_slice().into(), Condition::Create)
            .await
            .unwrap();
        // Save a continuation inside the snapshot prefix before retiring the collection.
        for n in 0..70 {
            let object = Key::parse(format!("{prefix}/snapshots/test/{n:02}")).unwrap();
            storage
                .put_control(&object, b"snapshot".as_slice().into(), Condition::Create)
                .await
                .unwrap();
        }
        let active = service.sweep_hosted_snapshots().await.unwrap();
        assert_eq!((active.units, active.deleted, active.failed), (2, 0, 0));
        assert!(active.more);

        service.delete_collection(&scope, None).await.unwrap();
        let mut download = storage.get(&head).await.unwrap();
        let mut body = Vec::new();
        download.reader.read_to_end(&mut body).await.unwrap();
        let mut retired: serde_json::Value = serde_json::from_slice(&body).unwrap();
        retired["deleted_at_ms"] = serde_json::json!(
            (SystemTime::now() - GRACE - Duration::from_secs(1))
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );
        storage
            .put_control(
                &head,
                serde_json::to_vec(&retired).unwrap().into(),
                Condition::Match(download.object.version),
            )
            .await
            .unwrap();
        drop(service);

        let first = catalog().sweep_hosted_snapshots().await.unwrap();
        assert!(matches!(
            storage.head(&input).await,
            Err(Error::NotFound(_))
        ));
        assert_eq!((first.units, first.deleted, first.failed), (1, 63, 0));
        assert!(first.more);
        let second = catalog().sweep_hosted_snapshots().await.unwrap();
        assert_eq!((second.units, second.deleted, second.failed), (1, 9, 0));
        assert!(!second.more);
        let remaining = storage.list(Some(&prefix), None, 1).await.unwrap();
        assert_eq!(remaining.objects[0].key, head);
        assert!(remaining.next_cursor.is_none());
    }
}
