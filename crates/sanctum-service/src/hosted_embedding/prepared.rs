use std::collections::HashSet;

use base64::{Engine, engine::general_purpose::STANDARD};
use sanctum_core::{Error, Result, Scope};
use sanctum_db::collection_prefix;
use sanctum_embedding::{EmbedError, Provider};
use sanctum_storage::{Condition, Key, MAX_CONTROL_BYTES};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use uuid::Uuid;

use super::{BatchInput, BatchOutcome, HostedCatalog, PUBLICATION_INPUT_LIMIT};

const ERROR_BYTES: usize = 1024;

#[derive(Deserialize, Serialize)]
struct PreparedEmbedding {
    format: u8,
    key: String,
    profile: String,
    outcomes: Vec<PreparedOutcome>,
}

#[derive(Deserialize, Serialize)]
struct PreparedOutcome {
    inputs: Vec<BatchInput>,
    result: PreparedResult,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum PreparedResult {
    // Base64 of IEEE 754 little-endian floats keeps 16 × 8192 values
    // comfortably below the control object size limit.
    Vectors(Vec<String>),
    Transient(String),
    Permanent(String),
    Input(String),
}

impl HostedCatalog {
    pub(super) fn prepared_embedding_key(&self, scope: &Scope, id: Uuid) -> Result<Key> {
        let collection = collection_prefix(self.prefix.as_ref(), scope)?;
        Key::parse(format!("{collection}/prepared-embeddings/{id}.json"))
    }

    pub(super) async fn read_prepared_embeddings(
        &self,
        key: &Key,
        provider: &dyn Provider,
    ) -> Result<Option<Vec<BatchOutcome>>> {
        let download = match self.storage.get(key).await {
            Ok(download) => download,
            Err(Error::NotFound(_)) => return Ok(None),
            Err(error) => return Err(error),
        };
        if download.object.length > MAX_CONTROL_BYTES as u64 {
            return Err(invalid_batch());
        }
        let mut body = Vec::new();
        download
            .reader
            .take(MAX_CONTROL_BYTES as u64 + 1)
            .read_to_end(&mut body)
            .await?;
        if body.len() as u64 != download.object.length {
            return Err(invalid_batch());
        }
        let batch: PreparedEmbedding =
            serde_json::from_slice(&body).map_err(|_| invalid_batch())?;
        if batch.format != 1 || batch.key != key.as_str() || batch.profile != provider.profile_id()
        {
            return Err(invalid_batch());
        }
        let mut seen = HashSet::new();
        for group in &batch.outcomes {
            if group.inputs.is_empty()
                || group.inputs.iter().any(|input| {
                    input.hash.len() != 64
                        || !input.hash.bytes().all(|b| b.is_ascii_hexdigit())
                        || !seen.insert(input.hash.as_str())
                })
            {
                return Err(invalid_batch());
            }
        }
        if !(1..=PUBLICATION_INPUT_LIMIT as usize).contains(&seen.len()) {
            return Err(invalid_batch());
        }
        batch
            .outcomes
            .into_iter()
            .map(|outcome| {
                let result = match outcome.result {
                    PreparedResult::Vectors(vectors) => {
                        if vectors.len() != outcome.inputs.len() {
                            return Err(invalid_batch());
                        }
                        Ok(vectors
                            .into_iter()
                            .map(|vector| decode_vector(&vector, provider.dimensions()))
                            .collect::<Result<_>>()?)
                    }
                    PreparedResult::Transient(message) => Err(EmbedError::Transient(message)),
                    PreparedResult::Permanent(message) => Err(EmbedError::Permanent(message)),
                    PreparedResult::Input(message) => Err(EmbedError::Input(message)),
                };
                Ok(BatchOutcome {
                    inputs: outcome.inputs,
                    result,
                })
            })
            .collect::<Result<_>>()
            .map(Some)
    }

    pub(super) async fn stage_prepared_embeddings(
        &self,
        key: &Key,
        provider: &dyn Provider,
        mut outcomes: Vec<BatchOutcome>,
    ) -> Result<Vec<BatchOutcome>> {
        for outcome in &mut outcomes {
            if let Err(error) = &mut outcome.result {
                let message = match error {
                    EmbedError::Transient(message)
                    | EmbedError::Permanent(message)
                    | EmbedError::Input(message) => message,
                };
                message.truncate(message.floor_char_boundary(ERROR_BYTES.min(message.len())));
            }
        }
        let batch = PreparedEmbedding {
            format: 1,
            key: key.to_string(),
            profile: provider.profile_id().to_owned(),
            outcomes: outcomes.iter().map(PreparedOutcome::from).collect(),
        };
        let body = serde_json::to_vec(&batch).map_err(|error| Error::Internal(Box::new(error)))?;
        match self
            .storage
            .put_control(key, body.into(), Condition::Create)
            .await
        {
            Ok(_) => Ok(outcomes),
            Err(Error::Conflict(_)) => self
                .read_prepared_embeddings(key, provider)
                .await?
                .ok_or_else(|| {
                    Error::Busy("prepared embedding batch changed during publication".into())
                }),
            Err(error) => Err(error),
        }
    }

    pub(super) async fn discard_prepared_embeddings(&self, key: &Key) {
        if let Err(error) = self.storage.delete(key).await
            && !matches!(error, Error::NotFound(_))
        {
            tracing::warn!(%error, %key, "could not remove prepared embedding batch");
        }
    }
}

impl From<&BatchOutcome> for PreparedOutcome {
    fn from(outcome: &BatchOutcome) -> Self {
        let result = match &outcome.result {
            Ok(vectors) => PreparedResult::Vectors(
                vectors
                    .iter()
                    .map(|vector| {
                        let bytes: Vec<u8> = vector
                            .iter()
                            .flat_map(|value| value.to_le_bytes())
                            .collect();
                        STANDARD.encode(bytes)
                    })
                    .collect(),
            ),
            Err(EmbedError::Transient(message)) => PreparedResult::Transient(message.clone()),
            Err(EmbedError::Permanent(message)) => PreparedResult::Permanent(message.clone()),
            Err(EmbedError::Input(message)) => PreparedResult::Input(message.clone()),
        };
        Self {
            inputs: outcome.inputs.clone(),
            result,
        }
    }
}

fn decode_vector(encoded: &str, dimensions: usize) -> Result<Vec<f32>> {
    let bytes = STANDARD.decode(encoded).map_err(|_| invalid_batch())?;
    if bytes.len() != dimensions * 4 {
        return Err(invalid_batch());
    }
    let vector: Vec<f32> = bytes
        .chunks_exact(4)
        .map(|bytes| f32::from_le_bytes(bytes.try_into().expect("four vector bytes")))
        .collect();
    if vector.iter().any(|value| !value.is_finite()) || vector.iter().all(|value| *value == 0.0) {
        return Err(invalid_batch());
    }
    Ok(vector)
}

fn invalid_batch() -> Error {
    Error::Internal("invalid prepared embedding batch".into())
}
