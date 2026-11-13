use std::time::Duration;

use reqwest::{Client, Url};
use sanctum_embedding::{EmbedError, Profile, Provider, Purpose};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OllamaConfig {
    pub endpoint: String,
    pub profile: Profile,
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u64,
}

fn default_timeout() -> u64 {
    30
}

pub struct Ollama {
    endpoint: Url,
    client: Client,
    profile: Profile,
    id: String,
}

impl Ollama {
    pub fn new(config: OllamaConfig) -> Result<Self, EmbedError> {
        config.profile.validate()?;
        if !(1..=300).contains(&config.timeout_seconds) {
            return Err(EmbedError::Permanent(
                "embedding timeout must be between 1 and 300 seconds".into(),
            ));
        }
        let mut endpoint = Url::parse(&config.endpoint)
            .map_err(|_| EmbedError::Permanent("invalid Ollama endpoint".into()))?;
        if !matches!(endpoint.scheme(), "http" | "https")
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(EmbedError::Permanent(
                "Ollama requires an HTTP(S) base URL without credentials, query, or fragment"
                    .into(),
            ));
        }
        let path = format!("{}/api/embed", endpoint.path().trim_end_matches('/'));
        endpoint.set_path(&path);
        let id = config.profile.id("ollama", endpoint.as_str());
        let client = Client::builder()
            .timeout(Duration::from_secs(config.timeout_seconds))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| EmbedError::Permanent("could not initialize the Ollama client".into()))?;
        Ok(Self {
            endpoint,
            client,
            profile: config.profile,
            id,
        })
    }
}

#[derive(Serialize)]
struct Request<'a> {
    model: &'a str,
    input: Vec<String>,
    truncate: bool,
    dimensions: usize,
}

#[derive(Deserialize)]
struct Response {
    embeddings: Vec<Vec<f32>>,
}

#[async_trait::async_trait]
impl Provider for Ollama {
    fn profile_id(&self) -> &str {
        &self.id
    }

    fn dimensions(&self) -> usize {
        self.profile.dimensions
    }

    async fn embed(&self, purpose: Purpose, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
        if texts.is_empty() || texts.len() > 16 {
            return Err(EmbedError::Permanent(
                "Ollama batch must contain 1 to 16 inputs".into(),
            ));
        }
        let mut response = self
            .client
            .post(self.endpoint.clone())
            .json(&Request {
                model: &self.profile.model,
                input: texts
                    .iter()
                    .map(|text| self.profile.input(purpose, text))
                    .collect(),
                truncate: false,
                dimensions: self.profile.dimensions,
            })
            .send()
            .await
            .map_err(|_| {
                EmbedError::Transient(
                    "Ollama could not be reached before the request deadline".into(),
                )
            })?;
        let status = response.status();
        if !status.is_success() {
            let message = format!("Ollama returned HTTP {}", status.as_u16());
            return Err(match status.as_u16() {
                400 | 413 | 422 => EmbedError::Input(message),
                408 | 429 | 500..=599 => EmbedError::Transient(message),
                _ => EmbedError::Permanent(message),
            });
        }
        let mut body = Vec::new();
        let limit = texts.len() * self.profile.dimensions * 32 + 64 * 1024;
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| EmbedError::Transient("Ollama response was interrupted".into()))?
        {
            if chunk.len() > limit - body.len() {
                return Err(EmbedError::Permanent(
                    "Ollama embedding response exceeds its size limit".into(),
                ));
            }
            body.extend_from_slice(&chunk);
        }
        let decoded: Response = serde_json::from_slice(&body).map_err(|_| {
            EmbedError::Permanent("Ollama returned an invalid embedding response".into())
        })?;
        Ok(decoded.embeddings)
    }
}
