use std::{net::IpAddr, time::Duration};

use reqwest::{Client, Url, header::HeaderValue};
use sanctum_embedding::{EmbedError, Profile, Provider, Purpose};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OpenAiConfig {
    pub base_url: String,
    pub api_key_env: Option<String>,
    pub profile: Profile,
    #[serde(default)]
    pub send_dimensions: bool,
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u64,
}

fn default_timeout() -> u64 {
    30
}

impl OpenAiConfig {
    fn endpoint(&self) -> Result<Url, EmbedError> {
        self.profile.validate()?;
        if !(1..=300).contains(&self.timeout_seconds) {
            return Err(EmbedError::Permanent(
                "embedding timeout must be between 1 and 300 seconds".into(),
            ));
        }
        if self.api_key_env.as_deref().is_some_and(|name| {
            name.is_empty()
                || name.len() > 128
                || !name.bytes().enumerate().all(|(index, byte)| {
                    byte.is_ascii_alphabetic()
                        || byte == b'_'
                        || (index != 0 && byte.is_ascii_digit())
                })
        }) {
            return Err(EmbedError::Permanent(
                "invalid embedding API key environment variable name".into(),
            ));
        }
        let mut url = Url::parse(&self.base_url)
            .map_err(|_| EmbedError::Permanent("invalid OpenAI-compatible base URL".into()))?;
        let loopback = url.host_str().is_some_and(|host| {
            host == "localhost"
                || host
                    .trim_matches(['[', ']'])
                    .parse::<IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        });
        if !(url.scheme() == "https" || url.scheme() == "http" && loopback)
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(EmbedError::Permanent(
                "embedding base URL must be HTTPS or loopback HTTP without credentials, query, or fragment".into(),
            ));
        }
        url.path_segments_mut()
            .map_err(|_| EmbedError::Permanent("invalid embedding base URL path".into()))?
            .pop_if_empty()
            .push("embeddings");
        Ok(url)
    }
}

pub struct OpenAi {
    endpoint: Url,
    client: Client,
    config: OpenAiConfig,
    id: String,
}

impl OpenAi {
    pub fn new(config: OpenAiConfig) -> Result<Self, EmbedError> {
        let endpoint = config.endpoint()?;
        let options = if config.send_dimensions {
            "openai/v1/dimensions"
        } else {
            "openai/v1/default-dimensions"
        };
        let id = config.profile.id(options, endpoint.as_str());
        let client = Client::builder()
            .timeout(Duration::from_secs(config.timeout_seconds))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| {
                EmbedError::Permanent("could not initialize the OpenAI-compatible client".into())
            })?;
        Ok(Self {
            endpoint,
            client,
            config,
            id,
        })
    }

    fn authorization(&self) -> Result<Option<HeaderValue>, EmbedError> {
        let Some(name) = &self.config.api_key_env else {
            return Ok(None);
        };
        let key = std::env::var(name).map_err(|_| {
            EmbedError::Transient(format!(
                "embedding credential environment variable {name} is unavailable"
            ))
        })?;
        if key.trim().is_empty() {
            return Err(EmbedError::Transient(format!(
                "embedding credential environment variable {name} is empty"
            )));
        }
        let mut value = HeaderValue::from_str(&format!("Bearer {key}")).map_err(|_| {
            EmbedError::Transient(format!(
                "embedding credential environment variable {name} has invalid header characters"
            ))
        })?;
        value.set_sensitive(true);
        Ok(Some(value))
    }
}

#[derive(Serialize)]
struct Request<'a> {
    model: &'a str,
    input: Vec<String>,
    encoding_format: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    dimensions: Option<usize>,
}

#[derive(Deserialize)]
struct Response {
    data: Vec<IndexedVector>,
}

#[derive(Deserialize)]
struct IndexedVector {
    index: usize,
    embedding: Vec<f32>,
}

#[async_trait::async_trait]
impl Provider for OpenAi {
    fn profile_id(&self) -> &str {
        &self.id
    }

    fn dimensions(&self) -> usize {
        self.config.profile.dimensions
    }

    async fn embed(&self, purpose: Purpose, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
        if texts.is_empty() || texts.len() > 16 {
            return Err(EmbedError::Permanent(
                "embedding batch must contain 1 to 16 inputs".into(),
            ));
        }
        let mut request = self.client.post(self.endpoint.clone()).json(&Request {
            model: &self.config.profile.model,
            input: texts
                .iter()
                .map(|text| self.config.profile.input(purpose, text))
                .collect(),
            encoding_format: "float",
            dimensions: self.config.send_dimensions.then(|| self.dimensions()),
        });
        if let Some(value) = self.authorization()? {
            request = request.header(reqwest::header::AUTHORIZATION, value);
        }
        let mut response = request.send().await.map_err(|_| {
            EmbedError::Transient(
                "embedding endpoint could not be reached before the request deadline".into(),
            )
        })?;
        let status = response.status();
        if !status.is_success() {
            let message = format!("embedding endpoint returned HTTP {}", status.as_u16());
            return Err(match status.as_u16() {
                400 | 413 | 422 => EmbedError::Input(message),
                401 | 403 | 408 | 429 | 500..=599 => EmbedError::Transient(message),
                _ => EmbedError::Permanent(message),
            });
        }
        let mut body = Vec::new();
        let limit = texts.len() * self.dimensions() * 32 + 64 * 1024;
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| EmbedError::Transient("embedding response was interrupted".into()))?
        {
            if chunk.len() > limit - body.len() {
                return Err(EmbedError::Permanent(
                    "embedding response exceeds its size limit".into(),
                ));
            }
            body.extend_from_slice(&chunk);
        }
        let decoded: Response = serde_json::from_slice(&body).map_err(|_| {
            EmbedError::Permanent("embedding endpoint returned an invalid response".into())
        })?;
        ordered_vectors(decoded.data, texts.len())
    }
}

fn ordered_vectors(
    mut data: Vec<IndexedVector>,
    expected: usize,
) -> Result<Vec<Vec<f32>>, EmbedError> {
    data.sort_unstable_by_key(|item| item.index);
    if data.len() != expected
        || data
            .iter()
            .enumerate()
            .any(|(index, item)| index != item.index)
    {
        return Err(EmbedError::Permanent(
            "embedding response indices did not match the request".into(),
        ));
    }
    Ok(data.into_iter().map(|item| item.embedding).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn orders_response_by_input_and_rejects_duplicates() {
        let vector = |index, value| IndexedVector {
            index,
            embedding: vec![value],
        };
        let reordered = ordered_vectors(vec![vector(1, 20.0), vector(0, 10.0)], 2).unwrap();
        assert_eq!(reordered, [vec![10.0], vec![20.0]]);
        assert!(ordered_vectors(vec![vector(0, 10.0), vector(0, 20.0)], 2).is_err());
        assert!(ordered_vectors(vec![vector(0, 10.0), vector(2, 20.0)], 2).is_err());
    }
}
