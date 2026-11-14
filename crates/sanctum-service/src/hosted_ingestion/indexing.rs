use std::{
    io::{self, BufReader, BufWriter, Write},
    time::Instant,
};

use sanctum_chunk::index_utf8_with_settings;
use sanctum_core::{ChunkSettings, Error, Result, Scope, is_rtf};
use sanctum_db::{Command, Unit, collection_prefix};
use sanctum_db_sqlite::hosted::{
    Registration, RevisionObjects, is_registered, publish_revision, published_revision,
    release_canceled_intent, release_intent,
};
use sanctum_extract::{RTF_EXTRACTOR, rtf_to_utf8};
use sanctum_storage::Key;
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

use super::{EmbeddingStep, HostedCatalog, HostedJob, Operation, Stage, unix_seconds};
use crate::{ChunkRef, Revision, RevisionContent, telemetry};

pub(super) const INDEX_STAGE: &str = "index";

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Outcome {
    Ready(Box<RevisionContent>),
    Canceled(Option<Registration>),
    Failed { code: String, message: String },
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Preparation {
    Ready(Box<Canonical>),
    Invalid(String),
}

impl Preparation {
    pub(super) fn status(&self) -> super::HostedPreparation {
        match self {
            Self::Ready(canonical) => super::HostedPreparation::Ready {
                canonical_bytes: canonical.content.bytes,
                chunks: canonical.content.chunks,
            },
            Self::Invalid(message) => super::HostedPreparation::InvalidSource(message.clone()),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub(super) struct Canonical {
    content: RevisionContent,
    extracted: Option<Uuid>,
}

struct Prepared {
    file: NamedTempFile,
    revision: Revision,
    objects: RevisionObjects,
}

impl HostedCatalog {
    pub async fn prepare_upload(&self, scope: &Scope, operation: Uuid) -> Result<HostedJob> {
        let prefix = self.operation_prefix(scope, operation)?;
        let (mut record, version) = self.require_operation(&prefix, operation).await?;
        let registration = match &record.stage {
            Stage::PendingRegistration => {
                return Err(Error::Busy("upload awaits registration".into()));
            }
            Stage::PendingIndexing {
                registration,
                preparation: None,
            } => registration,
            _ => return Ok(record.job()),
        };
        let start = Instant::now();
        if unix_seconds()? >= record.expires_at() {
            return self.index_upload(scope, operation).await;
        }
        let prepared = match self
            .prepare_source(scope, &prefix, &record, registration)
            .await
        {
            Ok((_, canonical)) => Preparation::Ready(Box::new(canonical)),
            Err(Error::Invalid(message)) => Preparation::Invalid(message),
            Err(Error::NotFound(_)) => {
                let unit = Unit::collection(self.prefix.as_ref(), scope)?;
                if self
                    .publication
                    .stage_receipt(&unit, operation, INDEX_STAGE)
                    .await?
                    .is_some()
                {
                    return self.index_upload(scope, operation).await;
                }
                return Err(Error::Busy("registered input is unavailable".into()));
            }
            Err(error) => return Err(error),
        };
        let Stage::PendingIndexing { preparation, .. } = &mut record.stage else {
            unreachable!("stage was checked above");
        };
        *preparation = Some(prepared);
        record.dispatched_at = None;
        let job = self
            .checkpoint(&prefix, record, version, |stage| {
                matches!(
                    stage,
                    Stage::PendingIndexing {
                        preparation: None,
                        ..
                    }
                )
            })
            .await?;
        job.log_stage(scope, "prepare", start);
        Ok(job)
    }

    pub async fn index_upload(&self, scope: &Scope, operation: Uuid) -> Result<HostedJob> {
        let prefix = self.operation_prefix(scope, operation)?;
        let (mut record, version) = self.require_operation(&prefix, operation).await?;
        match &record.stage {
            Stage::PendingRegistration => {
                return Err(Error::Busy("upload awaits registration".into()));
            }
            Stage::PendingIndexing { .. } => {}
            _ => return Ok(record.job()),
        }
        let start = Instant::now();
        let unit = Unit::collection(self.prefix.as_ref(), scope)?;
        let receipt = self
            .publication
            .stage_receipt(&unit, operation, INDEX_STAGE)
            .await?;
        let outcome = match receipt {
            Some(receipt) => decode(&receipt)?,
            None => match self.index_registered(scope, &unit, &prefix, &record).await {
                Ok(outcome) => outcome,
                Err(Error::NotFound(_)) => {
                    // A competing index delivery may have committed and removed its RTF input.
                    let receipt = self
                        .publication
                        .stage_receipt(&unit, operation, INDEX_STAGE)
                        .await?
                        .ok_or_else(|| Error::Busy("registered input is unavailable".into()))?;
                    decode(&receipt)?
                }
                Err(error) => return Err(error),
            },
        };
        if let Outcome::Canceled(registration) = outcome {
            let job = self
                .checkpoint_canceled(scope, &prefix, record, version, registration)
                .await?;
            job.log_stage(scope, "index", start);
            return Ok(job);
        }
        let schedule_embedding = self.embedding.is_some() && unix_seconds()? < record.expires_at();
        let Stage::PendingIndexing { registration, .. } = record.stage else {
            unreachable!("stage was checked above");
        };
        record.dispatched_at = None;
        record.stage = match outcome {
            Outcome::Ready(content) => {
                if schedule_embedding {
                    self.record_dispatch_hint(scope, operation, record.attempt)
                        .await?;
                }
                Stage::TextReady {
                    registration,
                    content,
                    embedding: schedule_embedding.then_some(EmbeddingStep {
                        step: 0,
                        not_before: None,
                    }),
                }
            }
            Outcome::Canceled(_) => unreachable!("cancellation was handled above"),
            Outcome::Failed { code, message } => Stage::Failed {
                code,
                message,
                registration: Some(registration),
            },
        };
        let discard_input = match &record.stage {
            Stage::Failed { .. } => {
                !self
                    .expired_input_is_referenced(scope, &prefix, &record)
                    .await?
            }
            Stage::TextReady { .. } => {
                is_rtf(&record.upload.name) && !record.upload.retain_original
            }
            _ => false,
        };
        if discard_input {
            self.discard_input(&prefix, &record).await?;
        }
        let job = self
            .checkpoint(&prefix, record, version, |stage| {
                matches!(stage, Stage::PendingIndexing { .. })
            })
            .await?;
        job.log_stage(scope, "index", start);
        Ok(job)
    }

    async fn index_registered(
        &self,
        scope: &Scope,
        unit: &Unit,
        operation_prefix: &Key,
        record: &Operation,
    ) -> Result<Outcome> {
        let Stage::PendingIndexing {
            registration,
            preparation,
        } = &record.stage
        else {
            unreachable!("stage was checked above");
        };
        let expires_at = record.expires_at();
        let replacement = record.upload.replacement.is_some();
        let prepared = if unix_seconds()? >= expires_at {
            Err(Outcome::Failed {
                code: "expired".into(),
                message: "indexing window expired".into(),
            })
        } else {
            match self
                .prepare_for_index(
                    scope,
                    operation_prefix,
                    record,
                    registration,
                    preparation.as_ref(),
                )
                .await
            {
                Ok(prepared) => Ok(prepared),
                Err(Error::Invalid(message)) => Err(Outcome::Failed {
                    code: "invalid_source".into(),
                    message,
                }),
                Err(error) => return Err(error),
            }
        };
        let profile = self
            .embedding
            .as_ref()
            .map(|provider| provider.profile_id().to_owned());
        let measure_reuse = tracing::enabled!(tracing::Level::DEBUG) && profile.is_some();
        let (metrics_tx, metrics_rx) = std::sync::mpsc::channel();
        let operation = record.operation;
        let revision = record.revision;
        let chunking = record.chunking;
        let initial_metadata = record.upload.initial_metadata.clone();
        let initial_chunk_metadata = record.upload.initial_chunk_metadata.clone();
        let registration = Registration {
            source: registration.source,
            external: registration.external.clone(),
            update_sequence: registration.update_sequence,
        };
        let mut writer = self.publication.acquire(unit).await?;
        let result = writer
            .apply_stage(
                INDEX_STAGE,
                vec![Command {
                    operation,
                    payload: prepared,
                }],
                move |tx, prepared| {
                    let outcome = if unix_seconds()? >= expires_at {
                        // A committed revision can outlive its publication receipt.
                        if let Some(content) =
                            published_revision(tx, registration.source, revision)?
                        {
                            Outcome::Ready(Box::new(content))
                        } else {
                            release_canceled_intent(tx, operation, revision, registration.source)?;
                            Outcome::Failed {
                                code: "expired".into(),
                                message: "indexing window expired".into(),
                            }
                        }
                    } else if !is_registered(tx, operation, revision, &registration)? {
                        Outcome::Failed {
                            code: "stale_revision".into(),
                            message: "a newer operation superseded this revision".into(),
                        }
                    } else {
                        match prepared {
                            Err(outcome) => {
                                release_intent(tx, registration.source)?;
                                outcome
                            }
                            Ok(prepared) => {
                                let revision_writer = publish_revision(
                                    tx,
                                    &prepared.revision,
                                    &prepared.objects,
                                    replacement,
                                    profile.as_deref(),
                                    (&initial_metadata, &initial_chunk_metadata),
                                    |writer| {
                                        let mut ordinal = 0;
                                        let index = index_utf8_with_settings(
                                            BufReader::new(prepared.file.reopen()?),
                                            chunking,
                                            |range, text| {
                                                let reference = ChunkRef {
                                                    source: registration.source,
                                                    revision,
                                                    ordinal,
                                                };
                                                writer.add_chunk(
                                                    &reference,
                                                    range,
                                                    blake3::hash(text.as_bytes()).to_hex().as_str(),
                                                    text,
                                                )?;
                                                ordinal += 1;
                                                Ok(())
                                            },
                                        )?;
                                        let expected = &prepared.revision.content;
                                        if index.hash != expected.hash
                                            || index.bytes != expected.bytes
                                            || index.chunks != expected.chunks
                                        {
                                            return Err(invalid_input());
                                        }
                                        Ok(())
                                    },
                                )?;
                                if measure_reuse
                                    && let Some(profile) = &profile
                                    && let Ok(reused) = revision_writer.cached_revision_chunks(
                                        registration.source,
                                        revision,
                                        profile,
                                    )
                                {
                                    let _ =
                                        metrics_tx.send((prepared.revision.content.chunks, reused));
                                }
                                Outcome::Ready(Box::new(prepared.revision.content))
                            }
                        }
                    };
                    serde_json::to_string(&outcome)
                        .map_err(|error| Error::Internal(Box::new(error)))
                },
            )
            .await;
        let released = writer.release().await;
        let (_, receipts) = result?;
        released?;
        if let Ok((chunks, reused)) = metrics_rx.try_recv() {
            telemetry::vector_reuse(scope, operation, chunks, reused);
        }
        decode(&receipts[0].outcome)
    }

    async fn prepare_source(
        &self,
        scope: &Scope,
        prefix: &Key,
        record: &Operation,
        registration: &Registration,
    ) -> Result<(NamedTempFile, Canonical)> {
        let input = record.input_key(prefix)?;
        let staged = self
            .download_source(scope, record.operation, &input, record.bytes)
            .await?;
        let is_rtf = is_rtf(&record.upload.name);
        let expected = record.blake3.clone();
        let chunking = record.chunking;
        let (file, mut content) =
            tokio::task::spawn_blocking(move || prepare_text(staged, is_rtf, expected, chunking))
                .await
                .map_err(|error| Error::Internal(Box::new(error)))??;
        crate::db::metadata::validate_metadata_ordinals(
            &record.upload.initial_chunk_metadata,
            content.chunks,
        )?;
        content.retain_original = record.upload.retain_original;
        let extracted = if is_rtf {
            let attempt = Uuid::new_v4();
            let key = self.extracted_key(scope, record, registration, attempt)?;
            let reader = tokio::fs::File::from_std(file.reopen()?);
            self.storage
                .put_stream(&key, Box::pin(reader), content.bytes)
                .await?;
            Some(attempt)
        } else {
            None
        };
        Ok((file, Canonical { content, extracted }))
    }

    async fn prepare_for_index(
        &self,
        scope: &Scope,
        prefix: &Key,
        record: &Operation,
        registration: &Registration,
        preparation: Option<&Preparation>,
    ) -> Result<Prepared> {
        let (local, content, extracted) = match preparation {
            Some(Preparation::Ready(canonical)) => {
                (None, canonical.content.clone(), canonical.extracted)
            }
            Some(Preparation::Invalid(message)) => return Err(Error::Invalid(message.clone())),
            None => {
                // Index deliveries queued before the preparation stage still work.
                let (file, canonical) = self
                    .prepare_source(scope, prefix, record, registration)
                    .await?;
                (Some(file), canonical.content, canonical.extracted)
            }
        };
        let input = record.input_key(prefix)?;
        let (text, original) = match extracted {
            Some(attempt) => (
                self.extracted_key(scope, record, registration, attempt)?,
                record.upload.retain_original.then(|| input.to_string()),
            ),
            None => (input, None),
        };
        let file = match local {
            Some(file) => file,
            None => {
                self.download_source(scope, record.operation, &text, content.bytes)
                    .await?
            }
        };
        Ok(Prepared {
            file,
            revision: Revision {
                source: registration.source,
                revision: record.revision,
                name: record.upload.name.clone(),
                external: registration.external.clone(),
                content,
            },
            objects: RevisionObjects {
                text_key: text.to_string(),
                original_key: original,
            },
        })
    }

    fn extracted_key(
        &self,
        scope: &Scope,
        record: &Operation,
        registration: &Registration,
        attempt: Uuid,
    ) -> Result<Key> {
        let collection = collection_prefix(self.prefix.as_ref(), scope)?;
        Key::parse(format!(
            "{collection}/sources/{}/{}/objects/{attempt}/text.utf8",
            registration.source, record.revision,
        ))
    }

    async fn download_source(
        &self,
        scope: &Scope,
        operation: Uuid,
        key: &Key,
        bytes: u64,
    ) -> Result<NamedTempFile> {
        let download_started = Instant::now();
        let input = self.storage.get(key).await?;
        if input.object.length != bytes {
            return Err(invalid_input());
        }
        let staged = NamedTempFile::new()?;
        let mut output = tokio::fs::File::from_std(staged.reopen()?);
        let copied =
            tokio::io::copy(&mut input.reader.take(bytes.saturating_add(1)), &mut output).await?;
        output.flush().await?;
        drop(output);
        if copied != bytes {
            return Err(invalid_input());
        }
        tracing::debug!(
            event = "hosted_input_transfer",
            tenant = %scope.tenant,
            collection = %scope.collection,
            %operation,
            direction = "download",
            bytes = copied,
            duration_ms = download_started.elapsed().as_secs_f64() * 1_000.0,
            "downloaded a staged source"
        );
        Ok(staged)
    }
}

fn prepare_text(
    input: NamedTempFile,
    is_rtf: bool,
    original_hash: String,
    chunking: ChunkSettings,
) -> Result<(NamedTempFile, RevisionContent)> {
    let (file, encoding) = if is_rtf {
        let mut hash = blake3::Hasher::new();
        hash.update_reader(input.reopen()?)?;
        if hash.finalize().to_hex().as_str() != original_hash {
            return Err(invalid_input());
        }
        let canonical = NamedTempFile::new()?;
        let mut writer = BufWriter::new(canonical.reopen()?);
        let encoding = rtf_to_utf8(BufReader::new(input.reopen()?), &mut writer)?;
        writer.flush()?;
        (canonical, encoding)
    } else {
        (input, "UTF-8")
    };
    let index = index_utf8_with_settings(BufReader::new(file.reopen()?), chunking, |_, _| Ok(()))?;
    if !is_rtf && index.hash != original_hash {
        return Err(invalid_input());
    }
    Ok((
        file,
        RevisionContent {
            bytes: index.bytes,
            chunks: index.chunks,
            chunker: chunking.identifier(),
            hash: index.hash,
            original_hash,
            encoding: encoding.into(),
            extractor: is_rtf.then(|| RTF_EXTRACTOR.into()),
            retain_original: false,
            url: None,
            git: None,
        },
    ))
}

fn invalid_input() -> Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "stored upload differs from its accepted bytes",
    )
    .into()
}

pub(super) fn decode(receipt: &str) -> Result<Outcome> {
    serde_json::from_str(receipt).map_err(|error| Error::Internal(Box::new(error)))
}
