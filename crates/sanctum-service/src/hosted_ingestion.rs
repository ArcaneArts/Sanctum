use std::{
    io,
    time::{Duration, Instant, SystemTime},
};

use sanctum_core::{ChunkSettings, Error, ExternalSource, Result, Scope, is_rtf};
use sanctum_db::{Command, Unit, collection_prefix, command_receipt};
use sanctum_db_sqlite::hosted::{
    Registration, RevisionIntent, Target, register_revision, release_canceled_intent,
};
use sanctum_storage::{Condition, Key, MAX_CONTROL_BYTES, Version};
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;
use tokio::io::AsyncReadExt;
use uuid::Uuid;

use crate::{
    HostedCatalog, RevisionContent, TaskStage, TextUpload, hosted_dispatch::UploadHint, telemetry,
};

mod activity;
mod cancellation;
mod embedding_tasks;
mod indexing;
mod orphan_cleanup;

pub use activity::HostedActivityPage;
pub use orphan_cleanup::UploadSweep;

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
    pub processed_bytes: u64,
    pub created_at: u64,
    pub update_sequence: u64,
    pub state: HostedJobState,
    pub content: Option<RevisionContent>,
    pub embedding_work: Option<HostedEmbeddingWork>,
    pub preparation: Option<HostedPreparation>,
}

impl HostedJob {
    fn log_stage(&self, scope: &Scope, stage: &'static str, start: Instant) {
        let outcome = match self.state {
            HostedJobState::Accepted => "accepted",
            HostedJobState::Registered => "registered",
            HostedJobState::TextReady => "text_ready",
            HostedJobState::Canceled => "canceled",
            HostedJobState::Failed { .. } => "failed",
        };
        telemetry::ingestion_stage(
            scope,
            self.operation,
            stage,
            self.created_at,
            self.bytes,
            start.elapsed(),
            outcome,
        );
    }
}

pub struct HostedEmbeddingWork {
    pub next_batch: u32,
    pub not_before: Option<u64>,
}

pub enum HostedPreparation {
    Ready { canonical_bytes: u64, chunks: u64 },
    InvalidSource(String),
}

pub enum HostedJobState {
    Accepted,
    Registered,
    TextReady,
    Canceled,
    Failed { code: String, message: String },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "stage", rename_all = "snake_case")]
enum Stage {
    PendingRegistration,
    PendingIndexing {
        registration: Registration,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        preparation: Option<indexing::Preparation>,
    },
    TextReady {
        registration: Registration,
        content: Box<RevisionContent>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        embedding: Option<EmbeddingStep>,
    },
    Canceled {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        registration: Option<Registration>,
    },
    Failed {
        code: String,
        message: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        registration: Option<Registration>,
    },
}

#[derive(Clone, Copy, Serialize, Deserialize)]
struct EmbeddingStep {
    step: u32,
    not_before: Option<u64>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RegistrationOutcome {
    Registered(Registration),
    Canceled,
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
    #[serde(default)]
    chunking: ChunkSettings,
    created_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    dispatched_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    dispatched_embedding_profile: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    dispatched_preparation: bool,
    #[serde(default, skip_serializing_if = "is_zero")]
    dispatch_retry: u32,
    #[serde(flatten)]
    stage: Stage,
}

fn is_zero(value: &u32) -> bool {
    *value == 0
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum Envelope {
    Accepted(Box<Operation>),
    Fence(AcceptanceFence),
    Expired(ExpiredOperation),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AcceptanceFence {
    operation: Uuid,
    fence_id: Uuid,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExpiredOperation {
    operation: Uuid,
    expired_at: u64,
    retained_attempt: Option<Uuid>,
}

impl HostedCatalog {
    pub fn max_source_bytes(&self) -> u64 {
        self.max_source_bytes
    }

    pub async fn stage_upload(
        &self,
        scope: Scope,
        mut upload: TextUpload,
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
        self.require_collection(scope).await?;
        let prefix = self.operation_prefix(scope, operation)?;
        let (record, _) = self.require_operation(&prefix, operation).await?;
        Ok(record.job())
    }

    pub async fn pending_text(&self, scope: &Scope, operation: Uuid) -> Result<crate::PendingText> {
        use crate::pending_scan::ScanSource;
        self.require_collection(scope).await?;
        let prefix = self.operation_prefix(scope, operation)?;
        let (record, _) = self.require_operation(&prefix, operation).await?;
        if !matches!(
            record.stage,
            Stage::PendingRegistration | Stage::PendingIndexing { .. }
        ) {
            return Err(Error::Conflict(
                "literal scan requires pending text ingestion".into(),
            ));
        }
        let (input, total_bytes) = if sanctum_core::is_rtf(&record.upload.name) {
            match &record.stage {
                Stage::PendingIndexing {
                    registration,
                    preparation: Some(indexing::Preparation::Ready(canonical)),
                } => {
                    let (key, bytes) =
                        self.prepared_rtf_text(scope, &record, registration, canonical)?;
                    (
                        ScanSource::Hosted {
                            storage: self.storage.clone(),
                            key,
                        },
                        bytes,
                    )
                }
                _ => (ScanSource::RtfOriginal, record.bytes),
            }
        } else {
            (
                ScanSource::Hosted {
                    storage: self.storage.clone(),
                    key: record.input_key(&prefix)?,
                },
                record.bytes,
            )
        };
        let job = record.job();
        Ok(crate::PendingText {
            source: job.source,
            revision: job.revision,
            name: job.name,
            external: job.external,
            total_bytes,
            input,
        })
    }

    /// Registers revision intent and checkpoints the next stage for a durable submission.
    pub async fn register_upload(&self, scope: &Scope, operation: Uuid) -> Result<HostedJob> {
        let prefix = self.operation_prefix(scope, operation)?;
        let (mut record, version) = self.require_operation(&prefix, operation).await?;
        if !matches!(record.stage, Stage::PendingRegistration) {
            return Ok(record.job());
        }
        let start = Instant::now();
        let unit = Unit::collection(self.prefix.as_ref(), scope)?;
        let intent = record.revision_intent()?;
        let expires_at = record.expires_at();
        let mut writer = self.publication.acquire(&unit).await?;
        let result = writer
            .apply(
                vec![Command {
                    operation,
                    payload: intent,
                }],
                move |tx, intent| {
                    let canceled = command_receipt(tx, operation, indexing::INDEX_STAGE)?;
                    let outcome = if cancellation::index_was_canceled(canceled.as_deref())? {
                        RegistrationOutcome::Canceled
                    } else if unix_seconds()? >= expires_at {
                        if let Target::Create(source) = intent.target {
                            release_canceled_intent(tx, operation, intent.revision, source)?;
                        }
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
        let outcome = decode_registration(&receipts[0].outcome)?;
        record.dispatched_at = None;
        record.stage = match outcome {
            RegistrationOutcome::Registered(registration) => Stage::PendingIndexing {
                registration,
                preparation: None,
            },
            RegistrationOutcome::Canceled => {
                let job = self
                    .checkpoint_canceled(scope, &prefix, record, version, None)
                    .await?;
                job.log_stage(scope, "register", start);
                return Ok(job);
            }
            RegistrationOutcome::Failed { code, message } => Stage::Failed {
                code,
                message,
                registration: None,
            },
        };
        let record = self
            .checkpoint(&prefix, record, version, |stage| {
                matches!(stage, Stage::PendingRegistration)
            })
            .await?;
        self.try_discard_completed_input(scope, &prefix, &record)
            .await;
        let job = record.job();
        job.log_stage(scope, "register", start);
        Ok(job)
    }

    async fn checkpoint(
        &self,
        prefix: &Key,
        record: Operation,
        version: Version,
        still_pending: impl FnOnce(&Stage) -> bool,
    ) -> Result<Operation> {
        let envelope = envelope_key(prefix)?;
        let body = serde_json::to_vec(&record).map_err(|error| Error::Internal(Box::new(error)))?;
        match self
            .storage
            .put_control(&envelope, body.into(), Condition::Match(version))
            .await
        {
            Ok(_) => Ok(record),
            Err(Error::Conflict(_)) => {
                let (current, _) = self.require_operation(prefix, record.operation).await?;
                if still_pending(&current.stage) {
                    return Err(Error::Busy(
                        "ingestion envelope changed during checkpoint".into(),
                    ));
                }
                Ok(current)
            }
            Err(error) => Err(error),
        }
    }

    /// Requests another delivery of the same pending work without changing its revision.
    pub async fn retry_upload(&self, scope: &Scope, operation: Uuid) -> Result<HostedJob> {
        self.require_collection(scope).await?;
        let prefix = self.operation_prefix(scope, operation)?;
        let (mut record, version) = self.require_operation(&prefix, operation).await?;
        if !record.is_pending() {
            return Err(Error::Conflict("operation has no retryable work".into()));
        }
        if unix_seconds()? >= record.expires_at() {
            return Err(Error::Conflict("the upload retry window expired".into()));
        }
        record.dispatch_retry = record
            .dispatch_retry
            .checked_add(1)
            .ok_or_else(|| Error::Conflict("operation retry counter exhausted".into()))?;
        record.dispatched_at = None;
        // Write the hint first so a stop after the checkpoint cannot strand the request.
        self.record_dispatch_hint(scope, operation, record.attempt)
            .await?;
        let key = envelope_key(&prefix)?;
        let body = serde_json::to_vec(&record).map_err(|error| Error::Internal(Box::new(error)))?;
        match self
            .storage
            .put_control(&key, body.into(), Condition::Match(version))
            .await
        {
            Ok(_) => {
                self.try_dispatch(scope, operation).await;
                Ok(record.job())
            }
            Err(Error::Conflict(_)) => {
                let (current, _) = self.require_operation(&prefix, operation).await?;
                if current.dispatch_retry >= record.dispatch_retry {
                    Ok(current.job())
                } else {
                    Err(Error::Busy("ingestion stage changed during retry".into()))
                }
            }
            Err(error) => Err(error),
        }
    }

    pub(super) async fn dispatch_pending(
        &self,
        scope: &Scope,
        operation: Uuid,
        hint: Option<UploadHint>,
    ) -> Result<bool> {
        let Some(dispatcher) = self.dispatcher.as_ref() else {
            return Ok(false);
        };
        if self.clear_retired_dispatch(scope, operation, false).await? {
            if let Some(hint) = hint {
                self.delete_hint(&hint.object.key).await?;
            }
            return Ok(false);
        }
        let prefix = self.operation_prefix(scope, operation)?;
        let Some((mut record, version)) = self
            .read_dispatched_upload(&prefix, operation, hint)
            .await?
        else {
            return Ok(false);
        };
        let Some((stage, not_before)) = self.pending_upload_task(scope, operation, &record).await?
        else {
            return Ok(false);
        };
        let now = unix_seconds()?;
        let window =
            not_before.unwrap_or(now).max(now) / crate::hosted_dispatch::DISPATCH_WINDOW_SECONDS;
        let profile = match stage {
            TaskStage::Embed { .. } => match self.embedding_profile() {
                Some(profile) => Some(profile),
                // Keep the durable hint for a worker with embeddings enabled.
                None => return Ok(false),
            },
            _ => None,
        };
        let preparing = stage == TaskStage::Prepare;
        if record.dispatched_preparation == preparing
            && record.dispatched_embedding_profile.as_deref() == profile
            && record.dispatched_at.is_some_and(|dispatched| {
                dispatched / crate::hosted_dispatch::DISPATCH_WINDOW_SECONDS >= window
            })
        {
            return Ok(false);
        }
        dispatcher
            .send(crate::Task {
                scope,
                operation,
                stage,
                profile,
                window,
                retry: record.dispatch_retry,
                not_before,
            })
            .await?;
        tracing::debug!(
            event = "upload_task_dispatched",
            tenant = %scope.tenant,
            collection = %scope.collection,
            %operation,
            ?stage,
            window,
            retry = record.dispatch_retry,
            "handed off hosted upload task"
        );
        record.dispatched_at = Some(not_before.unwrap_or(now).max(now));
        record.dispatched_embedding_profile = profile.map(str::to_owned);
        record.dispatched_preparation = preparing;
        let key = envelope_key(&prefix)?;
        let body = serde_json::to_vec(&record).map_err(|error| Error::Internal(Box::new(error)))?;
        match self
            .storage
            .put_control(&key, body.into(), Condition::Match(version))
            .await
        {
            Ok(_) | Err(Error::Conflict(_)) => Ok(true),
            Err(error) => Err(error),
        }
    }

    pub(super) async fn work_standalone_upload(
        &self,
        scope: &Scope,
        operation: Uuid,
        hint: UploadHint,
    ) -> Result<bool> {
        if self.clear_retired_dispatch(scope, operation, false).await? {
            self.delete_hint(&hint.object.key).await?;
            return Ok(false);
        }
        let prefix = self.operation_prefix(scope, operation)?;
        let Some((record, _)) = self
            .read_dispatched_upload(&prefix, operation, Some(hint))
            .await?
        else {
            return Ok(false);
        };
        let Some((stage, not_before)) = self.pending_upload_task(scope, operation, &record).await?
        else {
            return Ok(false);
        };
        if let Some(not_before) = not_before
            && unix_seconds()? < not_before
        {
            return Ok(false);
        }
        match stage {
            TaskStage::Register => {
                self.register_upload(scope, operation).await?;
            }
            TaskStage::Index => {
                self.index_upload(scope, operation).await?;
            }
            TaskStage::Prepare => {
                self.prepare_upload(scope, operation).await?;
            }
            TaskStage::Embed { step } => self.embed_upload(scope, operation, step).await?,
            TaskStage::Rebuild { .. } => unreachable!("rebuilds use separate records"),
        }
        Ok(true)
    }

    async fn pending_upload_task(
        &self,
        scope: &Scope,
        operation: Uuid,
        record: &Operation,
    ) -> Result<Option<(TaskStage, Option<u64>)>> {
        if record.is_pending() && unix_seconds()? >= record.expires_at() {
            self.settle_expired_upload(scope, record).await?;
            return Ok(None);
        }
        let task = match record.stage {
            Stage::PendingRegistration => (TaskStage::Register, None),
            Stage::PendingIndexing {
                preparation: None, ..
            } if self.dispatcher.is_some() => (TaskStage::Prepare, None),
            Stage::PendingIndexing { .. } => (TaskStage::Index, None),
            Stage::TextReady {
                embedding: Some(embedding),
                ..
            } => (
                TaskStage::Embed {
                    step: embedding.step,
                },
                embedding.not_before,
            ),
            Stage::TextReady {
                embedding: None, ..
            }
            | Stage::Canceled { .. }
            | Stage::Failed { .. } => {
                self.clear_dispatch_hint(scope, operation, record.attempt)
                    .await?;
                return Ok(None);
            }
        };
        Ok(Some(task))
    }

    async fn discard_input(&self, prefix: &Key, record: &Operation) -> Result<()> {
        match self.storage.delete(&record.input_key(prefix)?).await {
            Ok(()) | Err(Error::NotFound(_)) => Ok(()),
            Err(error) => Err(error),
        }
    }

    async fn try_discard_completed_input(&self, scope: &Scope, prefix: &Key, record: &Operation) {
        let cleanup = async {
            if self.can_discard_input(scope, prefix, record).await? {
                self.discard_input(prefix, record).await?;
            }
            Ok::<(), Error>(())
        };
        let result = tokio::time::timeout(Duration::from_secs(10), cleanup)
            .await
            .unwrap_or_else(|_| Err(Error::Busy("upload input cleanup timed out".into())));
        if let Err(error) = result {
            tracing::warn!(%error, tenant = %scope.tenant, collection = %scope.collection,
                operation = %record.operation, "upload sweep will retry staged input cleanup");
        }
    }

    async fn can_discard_input(
        &self,
        scope: &Scope,
        prefix: &Key,
        record: &Operation,
    ) -> Result<bool> {
        match record.stage {
            Stage::Canceled { .. } => Ok(true),
            Stage::Failed { .. } => Ok(!self
                .expired_input_is_referenced(scope, prefix, record)
                .await?),
            Stage::TextReady { .. } => {
                Ok(is_rtf(&record.upload.name) && !record.upload.retain_original)
            }
            _ => Ok(false),
        }
    }

    async fn expired_input_is_referenced(
        &self,
        scope: &Scope,
        prefix: &Key,
        record: &Operation,
    ) -> Result<bool> {
        let Stage::Failed {
            code,
            registration: Some(registration),
            ..
        } = &record.stage
        else {
            return Ok(false);
        };
        if code != "expired" {
            return Ok(false);
        }
        // The revision and receipt can both have aged out while the source
        // sweep still holds the input for readers of the previous generation.
        let key = record.input_key(prefix)?.to_string();
        let source = registration.source;
        let revision = record.revision;
        let (referenced, _) = self
            .with_collection(scope, move |db| {
                db.has_hosted_revision_object(source, revision, &key)
            })
            .await?;
        Ok(referenced)
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
        Ok(match self.read_envelope(prefix, operation).await? {
            Some((Envelope::Accepted(record), version)) => Some((*record, version)),
            _ => None,
        })
    }

    async fn read_dispatched_upload(
        &self,
        prefix: &Key,
        operation: Uuid,
        hint: Option<UploadHint>,
    ) -> Result<Option<(Operation, Version)>> {
        let envelope = self.read_envelope(prefix, operation).await?;
        match envelope {
            Some((Envelope::Accepted(record), version)) => {
                if let Some(hint) = hint
                    && hint
                        .attempt
                        .is_some_and(|attempt| attempt != record.attempt)
                {
                    self.delete_hint(&hint.object.key).await?;
                    return Ok(None);
                }
                Ok(Some((*record, version)))
            }
            Some((Envelope::Expired(_), _)) => {
                if let Some(hint) = hint {
                    self.delete_hint(&hint.object.key).await?;
                }
                Ok(None)
            }
            unaccepted => {
                if let Some(hint) = hint
                    && hint.attempt.is_some()
                    && orphan_cleanup::admission_grace_elapsed(&hint.object)
                    && self
                        .fence_unaccepted_upload(prefix, operation, unaccepted)
                        .await?
                {
                    self.delete_hint(&hint.object.key).await?;
                }
                Ok(None)
            }
        }
    }

    async fn read_envelope(
        &self,
        prefix: &Key,
        operation: Uuid,
    ) -> Result<Option<(Envelope, Version)>> {
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
        let envelope: Envelope =
            serde_json::from_slice(&body).map_err(|error| Error::Internal(Box::new(error)))?;
        let valid = match &envelope {
            Envelope::Accepted(record) => record.format == FORMAT && record.operation == operation,
            Envelope::Fence(fence) => fence.operation == operation,
            Envelope::Expired(record) => record.operation == operation,
        };
        if !valid {
            return Err(Error::Internal(
                "ingestion envelope identity mismatch".into(),
            ));
        }
        Ok(Some((envelope, download.object.version)))
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
            chunking: self.service.chunking,
            created_at: unix_seconds()?,
            dispatched_at: None,
            dispatched_embedding_profile: None,
            dispatched_preparation: false,
            dispatch_retry: 0,
            stage: Stage::PendingRegistration,
        };
        let condition = match self.service.read_envelope(&prefix, self.operation).await? {
            Some((Envelope::Accepted(saved), _)) => {
                let attempt = saved.attempt;
                let job = saved.replay(&candidate)?;
                if (self.service.dispatcher.is_some() || self.service.standalone_wake.is_some())
                    && matches!(
                        job.state,
                        HostedJobState::Accepted | HostedJobState::Registered
                    )
                {
                    self.service
                        .record_dispatch_hint(&self.scope, self.operation, attempt)
                        .await?;
                    self.service.try_dispatch(&self.scope, self.operation).await;
                }
                return Ok(job);
            }
            Some((Envelope::Expired(_), _)) => {
                return Err(Error::Conflict(
                    "upload request ID has expired; use a new request ID".into(),
                ));
            }
            Some((Envelope::Fence(_), version)) => Condition::Match(version),
            None => Condition::Create,
        };
        self.service.require_collection(&self.scope).await?;
        let key = envelope_key(&prefix)?;
        let input = candidate.input_key(&prefix)?;
        let body =
            serde_json::to_vec(&candidate).map_err(|error| Error::Internal(Box::new(error)))?;
        let store = self.service.storage.as_ref();
        let reader = tokio::fs::File::from_std(self.file.reopen()?);
        let upload_started = Instant::now();
        store.put_stream(&input, Box::pin(reader), bytes).await?;
        tracing::debug!(
            event = "hosted_input_transfer",
            tenant = %self.scope.tenant,
            collection = %self.scope.collection,
            operation = %self.operation,
            direction = "upload",
            bytes,
            duration_ms = upload_started.elapsed().as_secs_f64() * 1_000.0,
            "uploaded a staged source"
        );
        if let Err(error) = self.service.require_collection(&self.scope).await {
            if matches!(error, Error::NotFound(_))
                && let Err(cleanup) = store.delete(&input).await
            {
                tracing::warn!(%cleanup, %input, "could not discard input for a deleted collection");
            }
            return Err(error);
        }
        self.service
            .record_dispatch_hint(&self.scope, self.operation, candidate.attempt)
            .await?;
        let job = match store.put_control(&key, body.into(), condition).await {
            Ok(_) => Ok(candidate.job()),
            Err(Error::Conflict(_)) => {
                let (saved, _) = self
                    .service
                    .read_operation(&prefix, self.operation)
                    .await?
                    .ok_or_else(|| {
                        Error::Busy("ingestion envelope changed during acceptance".into())
                    })?;
                if saved.attempt != candidate.attempt {
                    if let Err(error) = store.delete(&input).await {
                        tracing::warn!(%error, operation = %self.operation, "could not delete losing upload attempt");
                    }
                    if let Err(error) = self
                        .service
                        .clear_upload_attempt_hint(&self.scope, self.operation, candidate.attempt)
                        .await
                    {
                        tracing::warn!(%error, operation = %self.operation, "could not remove losing upload hint");
                    }
                }
                saved.replay(&candidate)
            }
            // The conditional write may have committed. Preserve its input for replay.
            Err(error) => Err(error),
        }?;
        self.service.try_dispatch(&self.scope, self.operation).await;
        Ok(job)
    }
}

fn envelope_key(prefix: &Key) -> Result<Key> {
    Key::parse(format!("{prefix}/operation.json"))
}

impl Operation {
    fn is_pending(&self) -> bool {
        matches!(
            self.stage,
            Stage::PendingRegistration
                | Stage::PendingIndexing { .. }
                | Stage::TextReady {
                    embedding: Some(_),
                    ..
                }
        )
    }

    fn expires_at(&self) -> u64 {
        self.created_at
            .saturating_add(REGISTRATION_WINDOW.as_secs())
    }

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
        let preparation = match &self.stage {
            Stage::PendingIndexing {
                preparation: Some(preparation),
                ..
            } => Some(preparation.status()),
            _ => None,
        };
        let processed_bytes = if matches!(self.stage, Stage::TextReady { .. })
            || matches!(preparation, Some(HostedPreparation::Ready { .. }))
        {
            self.bytes
        } else {
            0
        };
        let embedding_work = match &self.stage {
            Stage::TextReady {
                embedding: Some(step),
                ..
            } => Some(HostedEmbeddingWork {
                next_batch: step.step,
                not_before: step.not_before,
            }),
            _ => None,
        };
        let (state, registration, content) = match self.stage {
            Stage::PendingRegistration => (HostedJobState::Accepted, None, None),
            Stage::PendingIndexing { registration, .. } => {
                (HostedJobState::Registered, Some(registration), None)
            }
            Stage::TextReady {
                registration,
                content,
                ..
            } => (
                HostedJobState::TextReady,
                Some(registration),
                Some(*content),
            ),
            Stage::Canceled { registration } => (HostedJobState::Canceled, registration, None),
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
            processed_bytes,
            created_at: self.created_at,
            update_sequence,
            state,
            content,
            embedding_work,
            preparation,
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

pub(crate) fn unix_seconds() -> Result<u64> {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .map_err(|error| Error::Internal(Box::new(error)))
}

fn decode_registration(receipt: &str) -> Result<RegistrationOutcome> {
    serde_json::from_str(receipt).map_err(|error| Error::Internal(Box::new(error)))
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
