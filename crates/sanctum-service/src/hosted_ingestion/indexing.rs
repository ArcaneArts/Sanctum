use std::io::{self, BufReader, BufWriter, Write};

use sanctum_chunk::index_utf8_with_settings;
use sanctum_core::{ChunkSettings, Error, Result, Scope, is_rtf};
use sanctum_db::{Command, Unit, collection_prefix};
use sanctum_db_sqlite::hosted::{
    Registration, RevisionObjects, is_registered, publish_revision, release_intent,
};
use sanctum_extract::{RTF_EXTRACTOR, rtf_to_utf8};
use sanctum_storage::Key;
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

use super::{
    EmbeddingStep, HostedCatalog, HostedJob, Operation, REGISTRATION_WINDOW, Stage, unix_seconds,
};
use crate::{ChunkRef, Revision, RevisionContent};

pub(super) const INDEX_STAGE: &str = "index";

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Outcome {
    Ready(Box<RevisionContent>),
    Canceled(Option<Registration>),
    Failed { code: String, message: String },
}

struct Prepared {
    file: NamedTempFile,
    revision: Revision,
    objects: RevisionObjects,
}

impl HostedCatalog {
    pub async fn index_upload(&self, scope: &Scope, operation: Uuid) -> Result<HostedJob> {
        let prefix = self.operation_prefix(scope, operation)?;
        let (mut record, version) = self.require_operation(&prefix, operation).await?;
        let registration = match &record.stage {
            Stage::PendingRegistration => {
                return Err(Error::Busy("upload awaits registration".into()));
            }
            Stage::PendingIndexing { registration } => registration,
            _ => return Ok(record.job()),
        };
        let unit = Unit::collection(self.prefix.as_ref(), scope)?;
        let receipt = self
            .publication
            .stage_receipt(&unit, operation, INDEX_STAGE)
            .await?;
        let outcome = match receipt {
            Some(receipt) => decode(&receipt)?,
            None => match self
                .index_registered(scope, &unit, &prefix, &record, registration)
                .await
            {
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
            return self
                .checkpoint_canceled(scope, &prefix, record, version, registration)
                .await;
        }
        let Stage::PendingIndexing { registration } = record.stage else {
            unreachable!("stage was checked above");
        };
        record.dispatched_at = None;
        record.stage = match outcome {
            Outcome::Ready(content) => {
                if self.embedding.is_some() {
                    self.record_dispatch_hint(scope, operation).await?;
                }
                Stage::TextReady {
                    registration,
                    content,
                    embedding: self.embedding.as_ref().map(|_| EmbeddingStep {
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
        let discard_input = matches!(&record.stage, Stage::Failed { .. })
            || (is_rtf(&record.upload.name) && !record.upload.retain_original);
        if discard_input {
            self.discard_input(&prefix, &record).await?;
        }
        self.checkpoint(&prefix, record, version, |stage| {
            matches!(stage, Stage::PendingIndexing { .. })
        })
        .await
    }

    async fn index_registered(
        &self,
        scope: &Scope,
        unit: &Unit,
        operation_prefix: &Key,
        record: &Operation,
        registration: &Registration,
    ) -> Result<Outcome> {
        let expires_at = record
            .created_at
            .saturating_add(REGISTRATION_WINDOW.as_secs());
        let replacement = record.upload.replacement.is_some();
        let prepared = if unix_seconds()? >= expires_at {
            Err(Outcome::Failed {
                code: "expired".into(),
                message: "indexing window expired".into(),
            })
        } else {
            match self
                .prepare_revision(scope, operation_prefix, record, registration)
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
                    let outcome = if !is_registered(tx, operation, revision, &registration)? {
                        Outcome::Failed {
                            code: "stale_revision".into(),
                            message: "a newer operation superseded this revision".into(),
                        }
                    } else if unix_seconds()? >= expires_at {
                        release_intent(tx, registration.source)?;
                        Outcome::Failed {
                            code: "expired".into(),
                            message: "indexing window expired".into(),
                        }
                    } else {
                        match prepared {
                            Err(outcome) => {
                                release_intent(tx, registration.source)?;
                                outcome
                            }
                            Ok(prepared) => {
                                publish_revision(
                                    tx,
                                    &prepared.revision,
                                    &prepared.objects,
                                    replacement,
                                    profile.as_deref(),
                                    (&initial_metadata, &initial_chunk_metadata),
                                    |writer| {
                                        let mut ordinal = 0;
                                        index_utf8_with_settings(
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
                                        Ok(())
                                    },
                                )?;
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
        decode(&receipts[0].outcome)
    }

    async fn prepare_revision(
        &self,
        scope: &Scope,
        prefix: &Key,
        record: &Operation,
        registration: &Registration,
    ) -> Result<Prepared> {
        let input_key = record.input_key(prefix)?;
        let input = self.storage.get(&input_key).await?;
        if input.object.length != record.bytes {
            return Err(invalid_input());
        }
        let staged = NamedTempFile::new()?;
        let mut output = tokio::fs::File::from_std(staged.reopen()?);
        let copied = tokio::io::copy(
            &mut input.reader.take(record.bytes.saturating_add(1)),
            &mut output,
        )
        .await?;
        output.flush().await?;
        drop(output);
        if copied != record.bytes {
            return Err(invalid_input());
        }
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
        let mut objects = RevisionObjects {
            text_key: input_key.to_string(),
            original_key: None,
        };
        if is_rtf {
            let collection = collection_prefix(self.prefix.as_ref(), scope)?;
            let key = Key::parse(format!(
                "{collection}/sources/{}/{}/objects/{}/text.utf8",
                registration.source,
                record.revision,
                Uuid::new_v4(),
            ))?;
            let reader = tokio::fs::File::from_std(file.reopen()?);
            self.storage
                .put_stream(&key, Box::pin(reader), content.bytes)
                .await?;
            objects.text_key = key.to_string();
            objects.original_key = record.upload.retain_original.then(|| input_key.to_string());
        }
        Ok(Prepared {
            file,
            revision: Revision {
                source: registration.source,
                revision: record.revision,
                name: record.upload.name.clone(),
                external: registration.external.clone(),
                content,
            },
            objects,
        })
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
