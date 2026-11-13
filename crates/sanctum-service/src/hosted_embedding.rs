use sanctum_core::{Error, Result, Scope};
use sanctum_db::{Command, Unit};
use sanctum_db_sqlite::embedding::{EmbeddingStatus, apply_hosted_embeddings};
use sanctum_embedding::EmbedError;
use uuid::Uuid;

use crate::{
    HostedCatalog,
    embedding::{Batch, embed_batch},
    hosted_retrieval::locate,
};

impl HostedCatalog {
    pub async fn embedding_status(
        &self,
        scope: &Scope,
        source_revision: Option<(Uuid, Uuid)>,
    ) -> Result<EmbeddingStatus> {
        let profile = self
            .embedding
            .as_ref()
            .map(|provider| provider.profile_id().to_owned());
        self.with_collection(scope, move |db| {
            db.embedding_status(profile.as_deref(), source_revision)
        })
        .await
        .map(|(status, _)| status)
    }

    /// Runs one bounded provider batch for the current configured profile.
    pub async fn embed_collection(&self, scope: &Scope) -> Result<usize> {
        self.embed_collection_batch(scope, Uuid::new_v4(), false)
            .await
    }

    pub(crate) async fn embed_collection_with_id(
        &self,
        scope: &Scope,
        batch_id: Uuid,
    ) -> Result<usize> {
        self.embed_collection_batch(scope, batch_id, true).await
    }

    async fn embed_collection_batch(
        &self,
        scope: &Scope,
        batch_id: Uuid,
        replay: bool,
    ) -> Result<usize> {
        let provider = self
            .embedding
            .as_ref()
            .ok_or_else(|| Error::Invalid("hosted embeddings are disabled".into()))?;
        let profile = provider.profile_id().to_owned();
        let unit = Unit::collection(self.prefix.as_ref(), scope)?;
        if replay
            && let Some(receipt) = self
                .publication
                .stage_receipt(&unit, batch_id, "embed")
                .await?
        {
            return parse_count(&receipt);
        }
        let limit = provider.max_batch_size().clamp(1, 16);
        let selected = profile.clone();
        let (chunks, generation) = self
            .with_collection(scope, move |db| {
                db.pending_embeddings(&selected, limit)?
                    .into_iter()
                    .map(|chunk| locate(db, chunk))
                    .collect::<Result<Vec<_>>>()
            })
            .await?;
        if chunks.is_empty() {
            return Ok(0);
        }
        let mut batch = Batch {
            hashes: Vec::with_capacity(chunks.len()),
            texts: Vec::with_capacity(chunks.len()),
        };
        for (chunk, key) in chunks {
            let hit = self.read_hit(scope, chunk, key, generation).await?;
            batch.hashes.push(hit.chunk.hash);
            batch.texts.push(hit.excerpt);
        }
        let count = batch.hashes.len();
        let outcomes = embed_batch(provider.as_ref(), batch).await;
        let mut writer = self.publication.acquire(&unit).await?;
        let saved = writer
            .apply_stage(
                "embed",
                vec![Command {
                    operation: batch_id,
                    payload: outcomes,
                }],
                move |tx, outcomes| {
                    apply_hosted_embeddings(tx, &profile, |writer| {
                        for outcome in outcomes {
                            match outcome.result {
                                Ok(vectors) => {
                                    for (hash, vector) in outcome.hashes.iter().zip(vectors) {
                                        writer.store_embedding(&profile, hash, &vector)?;
                                    }
                                }
                                Err(error) => {
                                    let retry = matches!(error, EmbedError::Transient(_));
                                    let message = error.to_string();
                                    for hash in outcome.hashes {
                                        writer
                                            .embedding_failure(&profile, &hash, &message, retry)?;
                                    }
                                }
                            }
                        }
                        Ok(())
                    })?;
                    Ok(count.to_string())
                },
            )
            .await;
        let released = writer.release().await;
        let (_, receipts) = saved?;
        released?;
        parse_count(&receipts[0].outcome)
    }
}

fn parse_count(receipt: &str) -> Result<usize> {
    receipt
        .parse()
        .map_err(|error| Error::Internal(Box::new(error)))
}
