use std::{collections::VecDeque, sync::Arc};

use sanctum_core::Result;
use sanctum_storage::Key;
use tokio::sync::{Mutex, OnceCell};

use crate::{Generation, Materialized, Publication, Unit};

const MAX_CACHED_SNAPSHOTS: usize = 16;

pub struct SnapshotCache {
    max_bytes: u64,
    entries: Mutex<VecDeque<Entry>>,
}

struct Entry {
    unit: Key,
    generation: Generation,
    file: Arc<OnceCell<Arc<Materialized>>>,
}

impl SnapshotCache {
    pub fn new(max_bytes: u64) -> Self {
        Self {
            max_bytes,
            entries: Mutex::new(VecDeque::new()),
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
        if self.max_bytes == 0 || generation.bytes > self.max_bytes {
            let file = publication
                .materialize(unit, generation, &std::env::temp_dir())
                .await?;
            return Ok(Arc::new(file));
        }
        let slot = self.slot(unit, &generation).await;
        let ready = slot.initialized();
        let file = slot
            .get_or_try_init(|| async {
                publication
                    .materialize(unit, generation, &std::env::temp_dir())
                    .await
                    .map(Arc::new)
            })
            .await?;
        tracing::debug!(
            generation = %file.generation.id,
            snapshot_bytes = file.generation.bytes,
            cached = ready,
            "resolved hosted snapshot"
        );
        Ok(file.clone())
    }

    async fn slot(&self, unit: &Unit, generation: &Generation) -> Arc<OnceCell<Arc<Materialized>>> {
        let mut entries = self.entries.lock().await;
        if let Some(index) = entries
            .iter()
            .position(|entry| entry.unit == unit.prefix && entry.generation == *generation)
        {
            let entry = entries
                .remove(index)
                .expect("entry was found under the same lock");
            let slot = entry.file.clone();
            entries.push_back(entry);
            return slot;
        }

        let mut occupied: u64 = entries.iter().map(|entry| entry.generation.bytes).sum();
        while occupied > self.max_bytes - generation.bytes || entries.len() >= MAX_CACHED_SNAPSHOTS
        {
            let retired = entries
                .pop_front()
                .expect("capacity can only be exhausted by retained entries");
            occupied -= retired.generation.bytes;
        }
        let slot = Arc::new(OnceCell::new());
        entries.push_back(Entry {
            unit: unit.prefix.clone(),
            generation: generation.clone(),
            file: slot.clone(),
        });
        slot
    }
}
