use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub model: String,
    pub revision: String,
    pub dimensions: usize,
    #[serde(default)]
    pub document_prefix: String,
    #[serde(default)]
    pub query_prefix: String,
}

impl Profile {
    pub fn validate(&self) -> Result<(), EmbedError> {
        if self.model.trim().is_empty()
            || self.revision.trim().is_empty()
            || self.model.len() > 256
            || self.revision.len() > 256
            || !(1..=8192).contains(&self.dimensions)
            || self.document_prefix.len() > 4096
            || self.query_prefix.len() > 4096
        {
            return Err(EmbedError::Permanent(
                "invalid embedding profile model, revision, dimensions, or input prefix".into(),
            ));
        }
        Ok(())
    }

    pub fn id(&self, provider: &str, endpoint: &str) -> String {
        let mut hash = blake3::Hasher::new();
        for part in [
            "sanctum-embedding-v1/cosine/raw-utf8",
            provider,
            endpoint,
            &self.model,
            &self.revision,
            &self.dimensions.to_string(),
            &self.document_prefix,
            &self.query_prefix,
        ] {
            hash.update(&(part.len() as u64).to_le_bytes());
            hash.update(part.as_bytes());
        }
        hash.finalize().to_hex().to_string()
    }

    pub fn input(&self, purpose: Purpose, text: &str) -> String {
        let prefix = match purpose {
            Purpose::Document => &self.document_prefix,
            Purpose::Query => &self.query_prefix,
        };
        format!("{prefix}{text}")
    }
}

#[derive(Clone, Copy)]
pub enum Purpose {
    Document,
    Query,
}

#[derive(Debug, thiserror::Error)]
pub enum EmbedError {
    #[error("{0}")]
    Transient(String),
    #[error("{0}")]
    Permanent(String),
    #[error("{0}")]
    Input(String),
}

#[async_trait::async_trait]
pub trait Provider: Send + Sync {
    fn profile_id(&self) -> &str;
    fn dimensions(&self) -> usize;

    fn max_batch_size(&self) -> u32 {
        16
    }

    async fn embed(&self, purpose: Purpose, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError>;
}

pub async fn embed(
    provider: &dyn Provider,
    purpose: Purpose,
    texts: &[String],
) -> Result<Vec<Vec<f32>>, EmbedError> {
    let mut vectors = provider.embed(purpose, texts).await?;
    if vectors.len() != texts.len() {
        return Err(EmbedError::Permanent(
            "embedding provider returned the wrong number of vectors".into(),
        ));
    }
    for vector in &mut vectors {
        normalize(vector, provider.dimensions())?;
    }
    Ok(vectors)
}

fn normalize(vector: &mut [f32], dimensions: usize) -> Result<(), EmbedError> {
    let norm = vector
        .iter()
        .map(|&value| f64::from(value).powi(2))
        .sum::<f64>()
        .sqrt();
    if vector.len() != dimensions || !norm.is_finite() || norm == 0.0 {
        return Err(EmbedError::Permanent(
            "embedding provider returned an invalid vector".into(),
        ));
    }
    for value in vector {
        *value = (f64::from(*value) / norm) as f32;
    }
    Ok(())
}
