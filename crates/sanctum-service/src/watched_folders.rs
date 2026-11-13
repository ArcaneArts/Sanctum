use std::{path::Path, time::Duration};

use sanctum_core::{Error, Result, Scope};
use sanctum_db_sqlite::watched_folders::{Counts, Folder};
use tokio::sync::watch;
use uuid::Uuid;

use crate::{ImportOptions, ImportState, Service};

pub struct WatchedFolder {
    pub id: Uuid,
    pub scope: Scope,
    pub path: String,
    pub options: ImportOptions,
    pub interval: u32,
    pub scanned: Option<u64>,
    pub counts: Counts,
    pub error: Option<String>,
}

impl TryFrom<Folder> for WatchedFolder {
    type Error = Error;

    fn try_from(folder: Folder) -> Result<Self> {
        let options = serde_json::from_str(&folder.options_json)
            .map_err(|error| Error::Internal(Box::new(error)))?;
        Ok(Self {
            id: folder.id,
            scope: folder.scope,
            path: folder.path,
            options,
            interval: folder.interval,
            scanned: folder.scanned,
            counts: folder.counts,
            error: folder.error,
        })
    }
}

impl Service {
    pub fn watch_folder(
        &self,
        scope: &Scope,
        path: &Path,
        options: &ImportOptions,
        interval: u32,
    ) -> Result<WatchedFolder> {
        if !(1..=86_400).contains(&interval) {
            return Err(Error::Invalid(
                "watch interval must be between 1 and 86400 seconds".into(),
            ));
        }
        if options.include.len() + options.exclude.len() > 64
            || options
                .include
                .iter()
                .chain(&options.exclude)
                .any(|pattern| pattern.len() > 1024)
        {
            return Err(Error::Invalid(
                "watch filters allow 64 patterns of at most 1024 bytes".into(),
            ));
        }
        let _reader = self
            .retrieval
            .read()
            .map_err(|_| Error::Busy("retrieval lock is unavailable".into()))?;
        let path = self.validate_watch(scope, path, options)?;
        let path = path
            .to_str()
            .filter(|value| value.len() <= 4096)
            .ok_or_else(|| {
                Error::Invalid("watch path must be UTF-8 and at most 4096 bytes".into())
            })?;
        let json =
            serde_json::to_string(options).map_err(|error| Error::Internal(Box::new(error)))?;
        self.queue
            .add_folder(scope, path, &json, interval)?
            .try_into()
    }

    pub fn watched_folders(
        &self,
        scope: &Scope,
        after: Option<Uuid>,
        limit: u32,
    ) -> Result<(Vec<WatchedFolder>, Option<Uuid>)> {
        if !(1..=25).contains(&limit) {
            return Err(Error::Invalid(
                "watch page limit must be between 1 and 25".into(),
            ));
        }
        self.require_collection(scope)?;
        let after = after.map(|id| id.to_string()).unwrap_or_default();
        let mut folders = self.queue.folders(scope, &after, limit + 1)?;
        let next = if folders.len() > limit as usize {
            folders.pop();
            folders.last().map(|folder| folder.id)
        } else {
            None
        };
        let folders = folders
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_>>()?;
        Ok((folders, next))
    }

    pub fn unwatch_folder(&self, scope: &Scope, id: Uuid) -> Result<()> {
        self.queue.remove_folder(scope, id)
    }

    fn scan_next_folder(&self, stop: &watch::Receiver<bool>) -> Result<bool> {
        if *stop.borrow() {
            return Ok(false);
        }
        let Some(folder) = self.queue.next_folder()? else {
            return Ok(false);
        };
        let folder = WatchedFolder::try_from(folder)?;
        let mut counts = Counts::default();
        let mut error = None;
        let scan = self.import_local_until(
            &folder.scope,
            Path::new(&folder.path),
            &folder.options,
            || *stop.borrow(),
            |event| {
                match event.state {
                    ImportState::Queued => counts.queued += 1,
                    ImportState::Missing => counts.missing += 1,
                    ImportState::Deleted => counts.deleted += 1,
                    ImportState::Failed => {
                        counts.failed += 1;
                        if error.is_none() {
                            error = event
                                .message
                                .map(|reason| format!("{}: {reason}", event.path));
                        }
                    }
                    ImportState::Pending | ImportState::Unchanged => {}
                }
                !*stop.borrow()
            },
        );
        if *stop.borrow() {
            return Ok(false);
        }
        if let Err(failure) = scan {
            counts.failed += 1;
            error = Some(failure.to_string());
        }
        let error: Option<String> = error.map(|reason| reason.chars().take(512).collect());
        self.queue
            .scanned_folder(folder.id, counts, error.as_deref())?;
        Ok(true)
    }
}

pub async fn run_watched_folders(service: Service, mut stop: watch::Receiver<bool>) {
    while !*stop.borrow_and_update() {
        let watcher = service.clone();
        let stopped = stop.clone();
        match tokio::task::spawn_blocking(move || watcher.scan_next_folder(&stopped)).await {
            Ok(Ok(true)) => continue,
            Ok(Ok(false)) => {}
            Ok(Err(error)) => tracing::warn!(%error, "watched folder scan failed"),
            Err(error) => tracing::warn!(%error, "watched folder worker failed"),
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            changed = stop.changed() => if changed.is_err() { return; }
        }
    }
}
