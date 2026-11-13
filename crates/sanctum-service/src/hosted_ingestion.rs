use std::{
    io,
    time::{Duration, SystemTime},
};

use sanctum_core::{Error, ExternalSource, Result, Scope};
use sanctum_db::{Command, Unit, collection_prefix};
use sanctum_db_sqlite::hosted::{Registration, RevisionIntent, Target, register_revision};
use sanctum_storage::{Condition, Key, MAX_CONTROL_BYTES, Version};
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;
use tokio::io::AsyncReadExt;
use uuid::Uuid;

use crate::{HostedCatalog, RevisionContent, TextUpload};

mod indexing;

const FORMAT: u8 = 1;
// Expire unregistered work before the publication executor can reclaim its receipt.
const REGISTRATION_WINDOW: Duration = Duration::from_secs(30 * 24 * 3600);

pub struct HostedUpload {
    service: HostedCatalog,
    scope: Scope,
    upload: TextUpload,
    operation: Uuid,
    file: NamedTempFile,
}

pub struct HostedJob {
    pub operation: Uuid,
    pub source: Option<Uuid>,
    pub revision: Uuid,
    pub name: String,
    pub external: Option<ExternalSource>,
    pub bytes: u64,
    pub created_at: u64,
    pub update_sequence: u64,
    pub state: HostedJobState,
    pub content: Option<RevisionContent>,
}

pub enum HostedJobState {
    Accepted,
    Registered,
    TextReady,
    Failed { code: String, message: String },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "stage", rename_all = "snake_case")]
enum Stage {
    PendingRegistration,
    PendingIndexing {
        registration: Registration,
    },
    TextReady {
        registration: Registration,
        content: Box<RevisionContent>,
    },
    Failed {
        code: String,
        message: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        registration: Option<Registration>,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RegistrationOutcome {
    Registered(Registration),
    Failed { code: String, message: String },
}

#[derive(Serialize, Deserialize)]
struct Operation {
    format: u8,
    operation: Uuid,
    source: Option<Uuid>,
    revision: Uuid,
    upload: TextUpload,
    bytes: u64,
    blake3: String,
    attempt: Uuid,
    created_at: u64,
    #[serde(flatten)]
    stage: Stage,
}

impl HostedCatalog {
    pub fn max_source_bytes(&self) -> u64 {
        self.max_source_bytes
    }

    pub async fn stage_upload(
        &self,
        scope: Scope,
        upload: TextUpload,
        operation: Option<Uuid>,
    ) -> Result<HostedUpload> {
        upload.validate()?;
        if operation.is_some_and(|id| id.is_nil()) {
            return Err(Error::Invalid("request ID must be a nonzero UUID".into()));
        }
        self.require_collection(&scope).await?;
        Ok(HostedUpload {
            service: self.clone(),
            scope,
            upload,
            operation: operation.unwrap_or_else(Uuid::new_v4),
            file: NamedTempFile::new()?,
        })
    }

    pub async fn ingestion_job(&self, scope: &Scope, operation: Uuid) -> Result<HostedJob> {
        let prefix = self.operation_prefix(scope, operation)?;
        let (record, _) = self.require_operation(&prefix, operation).await?;
        Ok(record.job())
    }

    /// Registers revision intent and checkpoints the next stage for a durable submission.
    pub async fn register_upload(&self, scope: &Scope, operation: Uuid) -> Result<HostedJob> {
        let prefix = self.operation_prefix(scope, operation)?;
        let (mut record, version) = self.require_operation(&prefix, operation).await?;
        if !matches!(record.stage, Stage::PendingRegistration) {
            return Ok(record.job());
        }
        let unit = Unit::collection(self.prefix.as_ref(), scope)?;
        let intent = record.revision_intent()?;
        let expires_at = record
            .created_at
            .saturating_add(REGISTRATION_WINDOW.as_secs());
        let mut writer = self.publication.acquire(&unit).await?;
        let result = writer
            .apply(
                vec![Command {
                    operation,
                    payload: intent,
                }],
                move |tx, intent| {
                    let outcome = if unix_seconds()? >= expires_at {
                        RegistrationOutcome::Failed {
                            code: "expired".into(),
                            message: "registration window expired".into(),
                        }
                    } else {
                        RegistrationOutcome::from_registration(register_revision(tx, intent))?
                    };
                    serde_json::to_string(&outcome)
                        .map_err(|error| Error::Internal(Box::new(error)))
                },
            )
            .await;
        let released = writer.release().await;
        let (_, receipts) = result?;
        released?;
        let outcome: RegistrationOutcome = serde_json::from_str(&receipts[0].outcome)
            .map_err(|error| Error::Internal(Box::new(error)))?;
        record.stage = match outcome {
            RegistrationOutcome::Registered(registration) => {
                Stage::PendingIndexing { registration }
            }
            RegistrationOutcome::Failed { code, message } => Stage::Failed {
                code,
                message,
                registration: None,
            },
        };
        self.checkpoint(&prefix, record, version, |stage| {
            matches!(stage, Stage::PendingRegistration)
        })
        .await
    }

    async fn checkpoint(
        &self,
        prefix: &Key,
        record: Operation,
        version: Version,
        still_pending: impl FnOnce(&Stage) -> bool,
    ) -> Result<HostedJob> {
        let envelope = envelope_key(prefix)?;
        let body = serde_json::to_vec(&record).map_err(|error| Error::Internal(Box::new(error)))?;
        match self
            .storage
            .put_control(&envelope, body.into(), Condition::Match(version))
            .await
        {
            Ok(_) => Ok(record.job()),
            Err(Error::Conflict(_)) => {
                let (current, _) = self.require_operation(prefix, record.operation).await?;
                if still_pending(&current.stage) {
                    return Err(Error::Busy(
                        "ingestion envelope changed during checkpoint".into(),
                    ));
                }
                Ok(current.job())
            }
            Err(error) => Err(error),
        }
    }

    fn operation_prefix(&self, scope: &Scope, operation: Uuid) -> Result<Key> {
        let collection = collection_prefix(self.prefix.as_ref(), scope)?;
        Key::parse(format!("{collection}/operations/{operation}"))
    }

    async fn require_operation(
        &self,
        prefix: &Key,
        operation: Uuid,
    ) -> Result<(Operation, Version)> {
        self.read_operation(prefix, operation)
            .await?
            .ok_or_else(|| Error::NotFound("ingestion operation was not found".into()))
    }

    async fn read_operation(
        &self,
        prefix: &Key,
        operation: Uuid,
    ) -> Result<Option<(Operation, Version)>> {
        let key = envelope_key(prefix)?;
        let download = match self.storage.get(&key).await {
            Ok(download) => download,
            Err(Error::NotFound(_)) => return Ok(None),
            Err(error) => return Err(error),
        };
        if download.object.length > MAX_CONTROL_BYTES as u64 {
            return Err(Error::Internal("ingestion envelope is too large".into()));
        }
        let mut body = Vec::new();
        download
            .reader
            .take(MAX_CONTROL_BYTES as u64 + 1)
            .read_to_end(&mut body)
            .await?;
        if body.len() as u64 != download.object.length {
            return Err(Error::Internal("ingestion envelope length mismatch".into()));
        }
        let record: Operation =
            serde_json::from_slice(&body).map_err(|error| Error::Internal(Box::new(error)))?;
        if record.format != FORMAT || record.operation != operation {
            return Err(Error::Internal(
                "ingestion envelope identity mismatch".into(),
            ));
        }
        Ok(Some((record, download.object.version)))
    }
}

impl HostedUpload {
    pub fn reopen(&self) -> Result<std::fs::File> {
        Ok(self.file.reopen()?)
    }

    pub async fn enqueue(self) -> Result<HostedJob> {
        let mut input = self.file.reopen()?;
        let (bytes, blake3) = tokio::task::spawn_blocking(move || {
            let mut hash = blake3::Hasher::new();
            let bytes = io::copy(&mut input, &mut hash)?;
            Ok::<_, io::Error>((bytes, hash.finalize().to_hex().to_string()))
        })
        .await
        .map_err(|error| Error::Internal(Box::new(error)))??;
        if bytes > self.service.max_source_bytes() {
            return Err(Error::Invalid(
                "source exceeds the configured upload limit".into(),
            ));
        }

        let prefix = self.service.operation_prefix(&self.scope, self.operation)?;
        let source = match &self.upload.replacement {
            Some(replacement) => replacement.source,
            None => Some(Uuid::new_v4()),
        };
        let candidate = Operation {
            format: FORMAT,
            operation: self.operation,
            source,
            revision: Uuid::new_v4(),
            upload: self.upload,
            bytes,
            blake3,
            attempt: Uuid::new_v4(),
            created_at: unix_seconds()?,
            stage: Stage::PendingRegistration,
        };
        if let Some((saved, _)) = self.service.read_operation(&prefix, self.operation).await? {
            return saved.replay(&candidate);
        }
        let key = envelope_key(&prefix)?;
        let input = candidate.input_key(&prefix)?;
        let body =
            serde_json::to_vec(&candidate).map_err(|error| Error::Internal(Box::new(error)))?;
        let store = self.service.storage.as_ref();
        let reader = tokio::fs::File::from_std(self.file.reopen()?);
        store.put_stream(&input, Box::pin(reader), bytes).await?;
        match store
            .put_control(&key, body.into(), Condition::Create)
            .await
        {
            Ok(_) => Ok(candidate.job()),
            Err(Error::Conflict(_)) => {
                let (saved, _) = self
                    .service
                    .read_operation(&prefix, self.operation)
                    .await?
                    .ok_or_else(|| {
                        Error::Busy("ingestion envelope changed during acceptance".into())
                    })?;
                if saved.attempt != candidate.attempt
                    && let Err(error) = store.delete(&input).await
                {
                    tracing::warn!(%error, operation = %self.operation, "could not delete losing upload attempt");
                }
                saved.replay(&candidate)
            }
            // The conditional write may have committed. Preserve its input for replay.
            Err(error) => Err(error),
        }
    }
}

fn envelope_key(prefix: &Key) -> Result<Key> {
    Key::parse(format!("{prefix}/operation.json"))
}

impl Operation {
    fn input_key(&self, prefix: &Key) -> Result<Key> {
        Key::parse(format!("{prefix}/inputs/{}", self.attempt))
    }

    fn revision_intent(&self) -> Result<RevisionIntent> {
        let target = match &self.upload.replacement {
            Some(replacement) => Target::Replace {
                source: replacement.source,
                expected_revision: replacement.expected_revision,
            },
            None => Target::Create(
                self.source
                    .ok_or_else(|| Error::Internal("creation has no source ID".into()))?,
            ),
        };
        Ok(RevisionIntent {
            operation: self.operation,
            revision: self.revision,
            target,
            external: self.upload.external.clone(),
        })
    }

    fn job(self) -> HostedJob {
        let (state, registration, content) = match self.stage {
            Stage::PendingRegistration => (HostedJobState::Accepted, None, None),
            Stage::PendingIndexing { registration } => {
                (HostedJobState::Registered, Some(registration), None)
            }
            Stage::TextReady {
                registration,
                content,
            } => (
                HostedJobState::TextReady,
                Some(registration),
                Some(*content),
            ),
            Stage::Failed {
                code,
                message,
                registration,
            } => (HostedJobState::Failed { code, message }, registration, None),
        };
        let (source, external, update_sequence) = match registration {
            Some(registration) => (
                Some(registration.source),
                registration.external,
                registration.update_sequence,
            ),
            None => (self.source, self.upload.external, 0),
        };
        HostedJob {
            operation: self.operation,
            source,
            revision: self.revision,
            name: self.upload.name,
            external,
            bytes: self.bytes,
            created_at: self.created_at,
            update_sequence,
            state,
            content,
        }
    }

    fn replay(self, candidate: &Self) -> Result<HostedJob> {
        if self.upload != candidate.upload
            || self.bytes != candidate.bytes
            || self.blake3 != candidate.blake3
        {
            return Err(Error::Conflict(
                "request ID already applies to a different upload".into(),
            ));
        }
        Ok(self.job())
    }
}

fn unix_seconds() -> Result<u64> {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .map_err(|error| Error::Internal(Box::new(error)))
}

impl RegistrationOutcome {
    fn from_registration(result: Result<Registration>) -> Result<Self> {
        let (code, message) = match result {
            Ok(registration) => return Ok(Self::Registered(registration)),
            Err(Error::NotFound(message)) => ("not_found", message),
            Err(Error::StaleRevision(message)) => ("stale_revision", message),
            Err(Error::Conflict(message)) => ("conflict", message),
            Err(error) => return Err(error),
        };
        Ok(Self::Failed {
            code: code.into(),
            message,
        })
    }
}
