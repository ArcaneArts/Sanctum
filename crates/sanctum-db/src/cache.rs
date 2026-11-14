use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, Weak},
};

use sanctum_core::{Error, Result};
use sanctum_storage::Key;
use tokio::sync::OnceCell;

use crate::{Generation, Materialized, Publication, Unit};

const MAX_CACHED_SNAPSHOTS: usize = 16;

pub struct SnapshotCache {
    max_bytes: u64,
    state: Mutex<CacheState>,
}

type DownloadSlot = OnceCell<Arc<Materialized>>;

#[derive(Default)]
struct CacheState {
    retained: VecDeque<Entry<Arc<DownloadSlot>>>,
    active: Vec<Entry<ActiveSnapshot>>,
}

#[derive(Default)]
struct ActiveSnapshot {
    download: Weak<DownloadSlot>,
    file: Weak<Materialized>,
}

struct Entry<Slot> {
    unit: Key,
    generation: Generation,
    slot: Slot,
}

impl<Slot> Entry<Slot> {
    fn matches(&self, unit: &Unit, generation: &Generation) -> bool {
        self.unit == unit.prefix && self.generation == *generation
    }
}

impl SnapshotCache {
    pub fn new(max_bytes: u64) -> Self {
        Self {
            max_bytes,
            state: Mutex::default(),
        }
    }

    /// Resolves HEAD on every call. The cache only reuses immutable, verified files.
    pub async fn read(
        &self,
        publication: &Publication,
        unit: &Unit,
    ) -> Result<Option<Arc<Materialized>>> {
        let Some(generation) = publication.generation(unit).await? else {
            return Ok(None);
        };
        self.read_generation(publication, unit, generation)
            .await
            .map(Some)
    }

    pub async fn read_generation(
        &self,
        publication: &Publication,
        unit: &Unit,
        generation: Generation,
    ) -> Result<Arc<Materialized>> {
        let slot = self.slot(unit, &generation);
        let ready = slot.initialized();
        let mut downloaded = false;
        let file = slot
            .get_or_try_init(|| async {
                downloaded = true;
                let file = Arc::new(
                    publication
                        .materialize(unit, generation, &std::env::temp_dir())
                        .await?,
                );
                self.track_file(unit, &file);
                Ok::<_, Error>(file)
            })
            .await?;
        tracing::debug!(
            event = "snapshot_cache",
            unit = %unit.prefix,
            generation = %file.generation.id,
            snapshot_bytes = file.generation.bytes,
            cached = ready,
            shared_transfer = !ready && !downloaded,
            "resolved hosted snapshot"
        );
        Ok(file.clone())
    }

    pub(crate) fn retains(&self, generation: &Generation) -> bool {
        self.max_bytes != 0 && generation.bytes <= self.max_bytes
    }

    pub(crate) fn remember(&self, unit: &Unit, snapshot: Materialized) {
        if !self.retains(&snapshot.generation) {
            return;
        }
        let mut state = self
            .state
            .lock()
            .expect("snapshot cache mutex was poisoned");
        let slot = state.slot(self.max_bytes, unit, &snapshot.generation);
        let file = Arc::new(snapshot);
        if slot.set(file.clone()).is_ok() {
            state.track_file(unit, &file);
        }
    }

    fn track_file(&self, unit: &Unit, file: &Arc<Materialized>) {
        self.state
            .lock()
            .expect("snapshot cache mutex was poisoned")
            .track_file(unit, file);
    }

    fn slot(&self, unit: &Unit, generation: &Generation) -> Arc<DownloadSlot> {
        let mut state = self
            .state
            .lock()
            .expect("snapshot cache mutex was poisoned");
        state.slot(self.max_bytes, unit, generation)
    }
}

impl CacheState {
    fn slot(&mut self, max_bytes: u64, unit: &Unit, generation: &Generation) -> Arc<DownloadSlot> {
        self.active.retain(|entry| {
            entry.slot.download.strong_count() > 0 || entry.slot.file.strong_count() > 0
        });
        if let Some(index) = self
            .retained
            .iter()
            .position(|entry| entry.matches(unit, generation))
        {
            let entry = self
                .retained
                .remove(index)
                .expect("entry was found under the same lock");
            let slot = entry.slot.clone();
            self.retained.push_back(entry);
            return slot;
        }
        let active = if let Some(index) = self
            .active
            .iter()
            .position(|entry| entry.matches(unit, generation))
        {
            &mut self.active[index].slot
        } else {
            self.active.push(Entry {
                unit: unit.prefix.clone(),
                generation: generation.clone(),
                slot: ActiveSnapshot::default(),
            });
            &mut self
                .active
                .last_mut()
                .expect("entry was just inserted")
                .slot
        };
        // Waiters own the slot while it downloads. Once it is gone, readers
        // can still share the verified file without retaining it in the LRU.
        let slot = active.download.upgrade().unwrap_or_else(|| {
            let slot = Arc::new(OnceCell::new_with(active.file.upgrade()));
            active.download = Arc::downgrade(&slot);
            slot
        });
        if max_bytes != 0 && generation.bytes <= max_bytes {
            let entries = &mut self.retained;
            let mut occupied: u64 = entries.iter().map(|entry| entry.generation.bytes).sum();
            while occupied > max_bytes - generation.bytes || entries.len() >= MAX_CACHED_SNAPSHOTS {
                let retired = entries
                    .pop_front()
                    .expect("capacity can only be exhausted by retained entries");
                occupied -= retired.generation.bytes;
            }
            entries.push_back(Entry {
                unit: unit.prefix.clone(),
                generation: generation.clone(),
                slot: slot.clone(),
            });
        }
        slot
    }

    fn track_file(&mut self, unit: &Unit, file: &Arc<Materialized>) {
        let entry = self
            .active
            .iter_mut()
            .find(|entry| entry.matches(unit, &file.generation))
            .expect("the caller still owns the download slot");
        entry.slot.file = Arc::downgrade(file);
    }
}
