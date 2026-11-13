use std::{io, time::SystemTime};

use sanctum_core::{Error, ExternalSource, Result, Scope};
use sanctum_db::collection_prefix;
use sanctum_storage::{Condition, Key, MAX_CONTROL_BYTES};
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;
use tokio::io::AsyncReadExt;
use uuid::Uuid;

use crate::{HostedCatalog, TextUpload};

const FORMAT: u8 = 1;

pub struct HostedUpload {
    service: HostedCatalog,
    scope: Scope,
    upload: TextUpload,
    operation: Uuid,
    file: NamedTempFile,
}

pub struct AcceptedUpload {
    pub operation: Uuid,
    pub source: Option<Uuid>,
    pub revision: Uuid,
    pub name: String,
    pub external: Option<ExternalSource>,
    pub bytes: u64,
    pub created_at: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Stage {
    PendingRegistration,
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

    pub async fn ingestion_job(&self, scope: &Scope, operation: Uuid) -> Result<AcceptedUpload> {
        let prefix = self.operation_prefix(scope, operation)?;
        self.read_operation(&prefix, operation)
            .await?
            .map(Operation::accepted)
            .ok_or_else(|| Error::NotFound("ingestion operation was not found".into()))
    }

    fn operation_prefix(&self, scope: &Scope, operation: Uuid) -> Result<Key> {
        let collection = collection_prefix(self.prefix.as_ref(), scope)?;
        Key::parse(format!("{collection}/operations/{operation}"))
    }

    async fn read_operation(&self, prefix: &Key, operation: Uuid) -> Result<Option<Operation>> {
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
        Ok(Some(record))
    }
}

impl HostedUpload {
    pub fn reopen(&self) -> Result<std::fs::File> {
        Ok(self.file.reopen()?)
    }

    pub async fn enqueue(self) -> Result<AcceptedUpload> {
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
            created_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map_err(|error| Error::Internal(Box::new(error)))?
                .as_secs(),
            stage: Stage::PendingRegistration,
        };
        if let Some(saved) = self.service.read_operation(&prefix, self.operation).await? {
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
            Ok(_) => Ok(candidate.accepted()),
            Err(Error::Conflict(_)) => {
                let saved = self
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

    fn accepted(self) -> AcceptedUpload {
        AcceptedUpload {
            operation: self.operation,
            source: self.source,
            revision: self.revision,
            name: self.upload.name,
            external: self.upload.external,
            bytes: self.bytes,
            created_at: self.created_at,
        }
    }

    fn replay(self, candidate: &Self) -> Result<AcceptedUpload> {
        if self.upload != candidate.upload
            || self.bytes != candidate.bytes
            || self.blake3 != candidate.blake3
        {
            return Err(Error::Conflict(
                "request ID already applies to a different upload".into(),
            ));
        }
        Ok(self.accepted())
    }
}
