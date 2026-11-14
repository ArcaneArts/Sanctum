use sanctum_core::Result;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{corrupt, page_delta};

const MAX_DELTAS: usize = 4;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Generation {
    pub id: Uuid,
    pub bytes: u64,
    pub blake3: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) chain: Option<DeltaChain>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct SnapshotFile {
    pub(crate) id: Uuid,
    pub(crate) bytes: u64,
    pub(crate) blake3: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct DeltaFile {
    pub(crate) id: Uuid,
    pub(crate) bytes: u64,
    pub(crate) blake3: String,
    pub(crate) result_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct DeltaChain {
    pub(crate) base: SnapshotFile,
    pub(crate) deltas: Vec<DeltaFile>,
}

impl Generation {
    pub(crate) fn file(&self) -> SnapshotFile {
        SnapshotFile {
            id: self.id,
            bytes: self.bytes,
            blake3: self.blake3.clone(),
        }
    }

    pub(crate) fn uses(&self, id: Uuid) -> bool {
        self.id == id
            || self.chain.as_ref().is_some_and(|chain| {
                chain.base.id == id || chain.deltas.iter().any(|delta| delta.id == id)
            })
    }

    pub(crate) fn chain_bytes(&self) -> Option<u64> {
        let Some(chain) = &self.chain else {
            return Some(0);
        };
        if chain.deltas.len() >= MAX_DELTAS {
            return None;
        }
        chain
            .deltas
            .iter()
            .try_fold(0u64, |sum, delta| sum.checked_add(delta.bytes))
    }
}

impl DeltaChain {
    pub(crate) fn validate(&self, generation: &Generation) -> Result<()> {
        if self.deltas.len() > MAX_DELTAS
            || self.base.bytes == 0
            || self.deltas.last().is_none_or(|last| {
                last.id != generation.id || last.result_bytes != generation.bytes
            })
        {
            return Err(corrupt("invalid SQLite snapshot delta chain"));
        }
        let mut ids = std::collections::HashSet::from([self.base.id]);
        let mut result_bytes = self.base.bytes;
        let mut remaining = generation.bytes / 2;
        for delta in &self.deltas {
            if delta.bytes < page_delta::HEADER_BYTES
                || delta.bytes > remaining
                || delta.result_bytes < result_bytes
                || !ids.insert(delta.id)
            {
                return Err(corrupt("invalid SQLite snapshot delta chain"));
            }
            remaining -= delta.bytes;
            result_bytes = delta.result_bytes;
        }
        Ok(())
    }
}
