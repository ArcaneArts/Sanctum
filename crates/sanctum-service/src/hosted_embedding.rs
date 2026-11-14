use std::time::Duration;

use sanctum_core::{Error, Result, Scope};
use sanctum_db::{Command, Unit};
use sanctum_db_sqlite::embedding::{
    EmbeddingRetry, EmbeddingStatus, PendingEmbedding, apply_hosted_embeddings,
};
use sanctum_embedding::EmbedError;
use tokio::time::{Instant, timeout_at};
use uuid::Uuid;

use crate::{
    Chunk, HostedCatalog,
    embedding::{Batch, BatchInput, BatchOutcome, embed_batch},
    hosted_retrieval::locate,
};

const PUBLICATION_INPUT_LIMIT: u32 = 16;
const PROVIDER_WINDOW: Duration = Duration::from_secs(120);

impl HostedCatalog {
    pub async fn embedding_status(
        &self,
        scope: &Scope,
        source_revision: Option<(Uuid, Uuid)>,
    ) -> Result<EmbeddingStatus> {
        let profile = self.embedding_profile().map(str::to_owned);
        self.with_collection(scope, move |db| {
            db.embedding_status(profile.as_deref(), source_revision)
        })
        .await
        .map(|(status, _)| status)
    }

    /// Runs one bounded publication of provider results for the configured profile.
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
        let selected = profile.clone();
        let (chunks, _) = self
            .with_collection(scope, move |db| {
                db.pending_embeddings(&selected, PUBLICATION_INPUT_LIMIT)?
                    .into_iter()
                    .map(|pending| locate_pending(db, pending))
                    .collect::<Result<Vec<_>>>()
            })
            .await?;
        if chunks.is_empty() {
            return Ok(0);
        }
        let outcomes = self
            .prepare_embedding_publication(
                scope,
                provider.as_ref(),
                chunks,
                Instant::now() + PROVIDER_WINDOW,
            )
            .await?;
        let count: usize = outcomes.iter().map(|outcome| outcome.inputs.len()).sum();
        if count == 0 {
            return Err(Error::Busy("hosted embedding preparation timed out".into()));
        }
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
                                    for (input, vector) in outcome.inputs.iter().zip(vectors) {
                                        writer.store_embedding(&profile, &input.hash, &vector)?;
                                    }
                                }
                                Err(error) => {
                                    let retry = matches!(error, EmbedError::Transient(_));
                                    let message = error.to_string();
                                    for input in outcome.inputs {
                                        writer.embedding_failure(
                                            &profile,
                                            &input.hash,
                                            input.retry,
                                            &message,
                                            retry,
                                        )?;
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

    async fn prepare_embedding_publication(
        &self,
        scope: &Scope,
        provider: &dyn sanctum_embedding::Provider,
        chunks: Vec<(Chunk, String, Option<EmbeddingRetry>)>,
        deadline: Instant,
    ) -> Result<Vec<BatchOutcome>> {
        let request_limit = provider.max_batch_size().clamp(1, PUBLICATION_INPUT_LIMIT) as usize;
        let mut outcomes = Vec::new();
        let mut chunks = chunks.into_iter();
        while chunks.len() != 0 && Instant::now() < deadline {
            let next = timeout_at(deadline, async {
                let mut batch = Batch {
                    inputs: Vec::with_capacity(request_limit),
                    texts: Vec::with_capacity(request_limit),
                };
                for (chunk, key, retry) in chunks.by_ref().take(request_limit) {
                    batch
                        .texts
                        .push(self.read_excerpt(scope, &chunk, key).await?);
                    batch.inputs.push(BatchInput {
                        hash: chunk.hash,
                        retry,
                    });
                }
                Ok::<_, Error>(embed_batch(scope, provider, batch).await)
            })
            .await;
            match next {
                Ok(Ok(prepared)) => {
                    let unavailable = prepared
                        .iter()
                        .all(|batch| matches!(batch.result, Err(EmbedError::Transient(_))));
                    outcomes.extend(prepared);
                    if unavailable {
                        break;
                    }
                }
                Ok(Err(error)) if outcomes.is_empty() => return Err(error),
                Ok(Err(error)) => {
                    tracing::warn!(
                        %error, tenant = %scope.tenant, collection = %scope.collection,
                        "publishing prepared embeddings after a source read failed"
                    );
                    break;
                }
                Err(_) => break,
            }
        }
        Ok(outcomes)
    }
}

fn locate_pending(
    database: &sanctum_db_sqlite::CollectionDb,
    PendingEmbedding { chunk, retry }: PendingEmbedding,
) -> Result<(Chunk, String, Option<EmbeddingRetry>)> {
    let (chunk, key) = locate(database, chunk)?;
    Ok((chunk, key, retry))
}

fn parse_count(receipt: &str) -> Result<usize> {
    receipt
        .parse()
        .map_err(|error| Error::Internal(Box::new(error)))
}

#[cfg(test)]
mod tests {
    use std::{
        io::Write,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use super::*;
    use sanctum_embedding::{Provider, Purpose};

    struct StalledProvider(AtomicUsize);

    #[async_trait::async_trait]
    impl Provider for StalledProvider {
        fn profile_id(&self) -> &str {
            "stalled-provider"
        }

        fn dimensions(&self) -> usize {
            2
        }

        fn max_batch_size(&self) -> u32 {
            1
        }

        async fn embed(
            &self,
            _: Purpose,
            texts: &[String],
        ) -> std::result::Result<Vec<Vec<f32>>, EmbedError> {
            assert_eq!(texts.len(), 1);
            if self.0.fetch_add(1, Ordering::Relaxed) == 0 {
                return Ok(vec![vec![1.0, 0.0]]);
            }
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn hosted_preparation_returns_finished_requests_when_the_next_request_times_out() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Arc::new(sanctum_storage_fs::FileStorage::open(directory.path()).unwrap());
        let service = HostedCatalog::new(storage, None, 1024).unwrap();
        let scope = Scope {
            tenant: "default".parse().unwrap(),
            collection: "notes".parse().unwrap(),
        };
        service.create_collection(&scope, None).await.unwrap();
        for text in [b"cobalt seam".as_slice(), b"quartz vein".as_slice()] {
            let upload = service
                .stage_upload(
                    scope.clone(),
                    crate::TextUpload {
                        name: "notes.txt".into(),
                        external: None,
                        replacement: None,
                        retain_original: false,
                        initial_metadata: Vec::new(),
                        initial_chunk_metadata: Vec::new(),
                    },
                    None,
                )
                .await
                .unwrap();
            upload.reopen().unwrap().write_all(text).unwrap();
            let job = upload.enqueue().await.unwrap();
            service
                .register_upload(&scope, job.operation)
                .await
                .unwrap();
            service.index_upload(&scope, job.operation).await.unwrap();
        }
        let (chunks, _) = service
            .with_collection(&scope, |db| {
                db.pending_embeddings("stalled-provider", 16)?
                    .into_iter()
                    .map(|pending| locate_pending(db, pending))
                    .collect::<Result<Vec<_>>>()
            })
            .await
            .unwrap();
        let expected = chunks[0].0.hash.clone();
        let provider = StalledProvider(AtomicUsize::new(0));
        let prepared = service
            .prepare_embedding_publication(
                &scope,
                &provider,
                chunks,
                Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert_eq!(provider.0.load(Ordering::Relaxed), 2);
        assert_eq!(prepared.len(), 1);
        assert_eq!(prepared[0].inputs.len(), 1);
        assert_eq!(prepared[0].inputs[0].hash, expected);
        assert!(prepared[0].result.is_ok());
    }
}
