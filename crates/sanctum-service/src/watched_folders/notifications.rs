use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, TrySendError},
    },
    time::{Duration, Instant},
};

use notify::{
    Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher,
    event::{AccessKind, AccessMode, MetadataKind, ModifyKind},
};
use sanctum_core::Result;
use sanctum_db_sqlite::queue::LocalQueue;
use uuid::Uuid;

const REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const RETRY_INTERVAL: Duration = Duration::from_secs(15);
const DEBOUNCE: Duration = Duration::from_millis(400);
const MAX_DELAY: Duration = Duration::from_secs(2);
const EVENT_CAPACITY: usize = 256;

pub(super) struct Notifications {
    watcher: RecommendedWatcher,
    events: Receiver<notify::Result<Event>>,
    overflow: Arc<AtomicBool>,
    folders: HashMap<Uuid, PathBuf>,
    roots: HashMap<PathBuf, Option<Instant>>,
    dirty: HashMap<Uuid, (Instant, Instant)>,
    refresh_after: Instant,
}

impl Notifications {
    pub(super) fn open(managed_storage: PathBuf) -> Option<Self> {
        let (sender, events) = mpsc::sync_channel(EVENT_CAPACITY);
        let overflow = Arc::new(AtomicBool::new(false));
        let dropped = overflow.clone();
        let watcher = RecommendedWatcher::new(
            move |event: notify::Result<Event>| {
                if event.as_ref().is_ok_and(|event| {
                    !event.need_rescan()
                        && (matches!(event.kind, EventKind::Access(kind)
                            if kind != AccessKind::Close(AccessMode::Write))
                            || matches!(
                                event.kind,
                                EventKind::Modify(ModifyKind::Metadata(MetadataKind::AccessTime))
                            )
                            || (!event.paths.is_empty()
                                && event
                                    .paths
                                    .iter()
                                    .all(|path| path.starts_with(&managed_storage))))
                }) {
                    return;
                }
                if let Err(TrySendError::Full(_)) = sender.try_send(event) {
                    dropped.store(true, Ordering::Relaxed);
                }
            },
            Config::default().with_follow_symlinks(false),
        );
        match watcher {
            Ok(watcher) => Some(Self {
                watcher,
                events,
                overflow,
                folders: HashMap::new(),
                roots: HashMap::new(),
                dirty: HashMap::new(),
                refresh_after: Instant::now(),
            }),
            Err(error) => {
                tracing::warn!(%error, "native folder notifications unavailable; using periodic reconciliation");
                None
            }
        }
    }

    pub(super) fn schedule(&mut self, queue: &LocalQueue) -> Result<()> {
        let now = Instant::now();
        if now >= self.refresh_after {
            self.refresh(queue, now)?;
        }
        let mut lost = self.overflow.swap(false, Ordering::Relaxed);
        let mut subscription_error = false;
        // Limit work per pass even when the event producer never goes idle.
        for _ in 0..EVENT_CAPACITY {
            let Ok(event) = self.events.try_recv() else {
                break;
            };
            match event {
                Ok(event) if !event.need_rescan() => self.changed(event, now),
                Ok(_) => lost = true,
                Err(error) => {
                    tracing::debug!(%error, "native folder notification error");
                    lost = true;
                    subscription_error = true;
                }
            }
        }
        if subscription_error {
            self.invalidate_roots(now, &[]);
        }
        if lost {
            for id in self.folders.keys().copied() {
                mark(&mut self.dirty, id, now);
            }
        }
        let due: Vec<_> = self
            .dirty
            .iter()
            .filter(|(_, (first, last))| {
                now.duration_since(*last) >= DEBOUNCE || now.duration_since(*first) >= MAX_DELAY
            })
            .map(|(&id, _)| id)
            .collect();
        if !due.is_empty() {
            queue.request_folder_scans(&due)?;
            for id in due {
                self.dirty.remove(&id);
            }
        }
        Ok(())
    }

    pub(super) fn cover_folder(&mut self, queue: &LocalQueue, id: Uuid) -> Result<()> {
        if !self.folders.contains_key(&id) {
            self.refresh(queue, Instant::now())?;
        }
        Ok(())
    }

    fn refresh(&mut self, queue: &LocalQueue, now: Instant) -> Result<()> {
        let folders: HashMap<_, _> = queue
            .folder_paths()?
            .into_iter()
            .map(|(id, path)| (id, PathBuf::from(path)))
            .collect();
        let paths: HashSet<_> = folders.values().map(PathBuf::as_path).collect();
        // Share a recursive subscription across watches of the same or nested roots.
        let outer: HashSet<_> = paths
            .iter()
            .copied()
            .filter(|path| {
                !path
                    .ancestors()
                    .skip(1)
                    .any(|ancestor| paths.contains(ancestor))
            })
            .collect();
        self.roots.retain(|root, retry| {
            if outer.contains(root.as_path()) {
                return true;
            }
            if retry.is_none() {
                let _ = self.watcher.unwatch(root);
            }
            false
        });
        for root in outer {
            let retry = self.roots.entry(root.to_owned()).or_insert(Some(now));
            if retry.is_some_and(|retry| now >= retry) {
                *retry = match subscribe(&mut self.watcher, root) {
                    Ok(()) => {
                        tracing::debug!(path = %root.display(), "watching folder notifications");
                        for (&id, path) in &folders {
                            if path.starts_with(root) && self.folders.contains_key(&id) {
                                // Close the gap between the previous scan and subscription.
                                mark(&mut self.dirty, id, now);
                            }
                        }
                        None
                    }
                    Err(error) => {
                        tracing::debug!(%error, path = %root.display(), "could not subscribe to folder notifications");
                        Some(now + RETRY_INTERVAL)
                    }
                };
            }
        }
        self.dirty.retain(|id, _| folders.contains_key(id));
        self.folders = folders;
        self.refresh_after = now + REFRESH_INTERVAL;
        Ok(())
    }

    fn changed(&mut self, event: Event, now: Instant) {
        if matches!(
            event.kind,
            EventKind::Remove(_) | EventKind::Modify(ModifyKind::Name(_))
        ) {
            self.invalidate_roots(now, &event.paths);
        }
        for (&id, folder) in &self.folders {
            if event.paths.is_empty() || event.paths.iter().any(|path| affects(path, folder)) {
                mark(&mut self.dirty, id, now);
            }
        }
    }

    fn invalidate_roots(&mut self, now: Instant, paths: &[PathBuf]) {
        for (root, retry) in &mut self.roots {
            if retry.is_none()
                && (paths.is_empty() || paths.iter().any(|path| root.starts_with(path)))
            {
                let _ = self.watcher.unwatch(root);
                *retry = Some(now);
                self.refresh_after = now;
            }
        }
    }
}

fn subscribe(watcher: &mut RecommendedWatcher, root: &Path) -> notify::Result<()> {
    // A formerly canonical directory can be replaced while the server is running.
    if !root.symlink_metadata()?.is_dir() {
        return Err(notify::Error::generic(
            "watch root is no longer a directory",
        ));
    }
    watcher.watch(root, RecursiveMode::Recursive)
}

fn affects(event_path: &Path, folder: &Path) -> bool {
    event_path.starts_with(folder) || folder.starts_with(event_path)
}

fn mark(dirty: &mut HashMap<Uuid, (Instant, Instant)>, id: Uuid, now: Instant) {
    dirty
        .entry(id)
        .and_modify(|(_, last)| *last = now)
        .or_insert((now, now));
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify::event::{DataChange, Flag};
    use sanctum_core::Scope;
    use sanctum_db_sqlite::watched_folders::Counts;

    #[test]
    fn overflow_and_rescan_events_schedule_reconciliation_for_every_watch() {
        let state = tempfile::tempdir().unwrap();
        let files = tempfile::tempdir().unwrap();
        let first = files.path().join("first");
        let second = files.path().join("second");
        std::fs::create_dir(&first).unwrap();
        std::fs::create_dir(&second).unwrap();
        let queue = LocalQueue::open(state.path().join("runtime.db")).unwrap();
        let scope = Scope {
            tenant: "default".parse().unwrap(),
            collection: "notes".parse().unwrap(),
        };
        let first = queue
            .add_folder(&scope, first.to_str().unwrap(), "{}", 3600)
            .unwrap();
        let second = queue
            .add_folder(&scope, second.to_str().unwrap(), "{}", 3600)
            .unwrap();
        let mut notifications = Notifications::open(state.path().to_owned()).unwrap();
        notifications.schedule(&queue).unwrap();
        let (events, notifications_rx) = mpsc::sync_channel(1);
        notifications.events = notifications_rx;
        let checked = |queue: &LocalQueue, ids: &[Uuid]| {
            for id in ids {
                queue.scanned_folder(*id, Counts::default(), None).unwrap();
            }
        };
        let aged_events = |notifications: &mut Notifications| {
            for (_, last) in notifications.dirty.values_mut() {
                *last -= DEBOUNCE;
            }
        };
        let ids = [first.id, second.id];
        checked(&queue, &ids);
        events
            .send(Ok(Event::new(EventKind::Modify(ModifyKind::Data(
                DataChange::Content,
            )))
            .add_path(Path::new(&first.path).join("notes.txt"))))
            .unwrap();
        notifications.schedule(&queue).unwrap();
        assert!(queue.next_folder().unwrap().is_none());
        aged_events(&mut notifications);
        notifications.schedule(&queue).unwrap();
        assert_eq!(queue.next_folder().unwrap().unwrap().id, first.id);
        checked(&queue, &[first.id]);
        assert!(queue.next_folder().unwrap().is_none());

        for overflow in [true, false] {
            if overflow {
                notifications.overflow.store(true, Ordering::Relaxed);
            } else {
                events
                    .send(Ok(Event::new(EventKind::Other).set_flag(Flag::Rescan)))
                    .unwrap();
            }
            notifications.schedule(&queue).unwrap();
            assert_eq!(notifications.dirty.len(), 2);
            aged_events(&mut notifications);
            notifications.schedule(&queue).unwrap();
            let found = queue.next_folder().unwrap().unwrap().id;
            checked(&queue, &[found]);
            let remaining = queue.next_folder().unwrap().unwrap().id;
            assert!(ids.contains(&found) && ids.contains(&remaining) && found != remaining);
            checked(&queue, &[remaining]);
            assert!(queue.next_folder().unwrap().is_none());
        }
    }
}
