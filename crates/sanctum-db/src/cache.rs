use std::{
    collections::VecDeque,
    sync::{Arc, Weak},
};

use sanctum_core::Result;
use sanctum_storage::Key;
use tokio::sync::{Mutex, OnceCell};

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
    active: Vec<Entry<Weak<DownloadSlot>>>,
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
        let slot = self.slot(unit, &generation).await;
        let ready = slot.initialized();
        let mut downloaded = false;
        let file = slot
            .get_or_try_init(|| async {
                downloaded = true;
                publication
                    .materialize(unit, generation, &std::env::temp_dir())
                    .await
                    .map(Arc::new)
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

    pub(crate) async fn remember(&self, unit: &Unit, snapshot: Materialized) {
        if !self.retains(&snapshot.generation) {
            return;
        }
        let slot = self.slot(unit, &snapshot.generation).await;
        let _ = slot.set(Arc::new(snapshot));
    }

    async fn slot(&self, unit: &Unit, generation: &Generation) -> Arc<DownloadSlot> {
        let mut state = self.state.lock().await;
        state.active.retain(|entry| entry.slot.strong_count() > 0);
        if let Some(index) = state
            .retained
            .iter()
            .position(|entry| entry.matches(unit, generation))
        {
            let entry = state
                .retained
                .remove(index)
                .expect("entry was found under the same lock");
            let slot = entry.slot.clone();
            state.retained.push_back(entry);
            return slot;
        }
        let slot = state
            .active
            .iter()
            .filter(|entry| entry.matches(unit, generation))
            .find_map(|entry| entry.slot.upgrade())
            .unwrap_or_else(|| {
                let slot = Arc::new(OnceCell::new());
                // An active transfer can outlive LRU eviction or exceed the
                // byte budget. Waiters own the slot in both cases.
                state.active.push(Entry {
                    unit: unit.prefix.clone(),
                    generation: generation.clone(),
                    slot: Arc::downgrade(&slot),
                });
                slot
            });
        if self.retains(generation) {
            let entries = &mut state.retained;
            let mut occupied: u64 = entries.iter().map(|entry| entry.generation.bytes).sum();
            while occupied > self.max_bytes - generation.bytes
                || entries.len() >= MAX_CACHED_SNAPSHOTS
            {
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
}
