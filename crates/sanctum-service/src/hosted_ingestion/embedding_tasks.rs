use sanctum_core::{Error, Result, Scope};
use uuid::Uuid;

use super::{EmbeddingStep, HostedCatalog, REGISTRATION_WINDOW, Stage, unix_seconds};

impl HostedCatalog {
    pub async fn embed_upload(&self, scope: &Scope, operation: Uuid, step: u32) -> Result<()> {
        let prefix = self.operation_prefix(scope, operation)?;
        let (mut record, version) = self.require_operation(&prefix, operation).await?;
        let current = match &record.stage {
            Stage::TextReady {
                embedding: Some(current),
                ..
            } if current.step == step => *current,
            Stage::TextReady {
                embedding: Some(current),
                ..
            } if current.step < step => {
                return Err(Error::Busy(
                    "embedding task awaits its preceding batch".into(),
                ));
            }
            Stage::PendingRegistration | Stage::PendingIndexing { .. } => {
                return Err(Error::Busy(
                    "embedding task awaits lexical publication".into(),
                ));
            }
            _ => return Ok(()),
        };
        let now = unix_seconds()?;
        if current.not_before.is_some_and(|earliest| now < earliest) {
            return Err(Error::Busy("embedding task is scheduled for later".into()));
        }
        let next = if let Some(provider) = &self.embedding
            && now.saturating_sub(record.created_at) < REGISTRATION_WINDOW.as_secs()
        {
            let batch = embedding_batch_id(operation, step, provider.profile_id());
            let count = self.embed_collection_with_id(scope, batch).await?;
            let not_before = if count == 0 {
                self.embedding_status(scope, None).await?.next_retry
            } else {
                None
            };
            if count == 0 && not_before.is_none() {
                None
            } else {
                let next_step = step
                    .checked_add(1)
                    .ok_or_else(|| Error::Internal("hosted embedding step exhausted".into()))?;
                Some(EmbeddingStep {
                    step: next_step,
                    not_before,
                })
            }
        } else {
            None
        };
        if let Stage::TextReady { embedding, .. } = &mut record.stage {
            *embedding = next;
        }
        record.dispatched_at = None;
        self.checkpoint(&prefix, record, version, |stage| {
            matches!(stage, Stage::TextReady { embedding: Some(current), .. } if current.step == step)
        }).await?;
        Ok(())
    }
}

fn embedding_batch_id(operation: Uuid, step: u32, profile: &str) -> Uuid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(operation.as_bytes());
    hasher.update(&step.to_le_bytes());
    hasher.update(profile.as_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    Uuid::from_bytes(bytes)
}
