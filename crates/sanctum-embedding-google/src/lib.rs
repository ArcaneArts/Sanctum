use std::time::Duration;

use google_cloud_auth::credentials::{AccessTokenCredentials, Builder};
use reqwest::{Client, Url};
use sanctum_embedding::{EmbedError, Profile, Provider, Purpose};
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;

const CLOUD_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GoogleConfig {
    pub project: String,
    pub location: String,
    pub profile: Profile,
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u64,
}

fn default_timeout() -> u64 {
    30
}

fn path_segment(segment: &str, extra: &[u8]) -> bool {
    !segment.is_empty()
        && segment.len() <= 128
        && segment
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || extra.contains(&byte))
}

impl GoogleConfig {
    fn endpoint(&self) -> Result<Url, EmbedError> {
        self.profile.validate()?;
        if !(1..=300).contains(&self.timeout_seconds) {
            return Err(EmbedError::Permanent(
                "embedding timeout must be between 1 and 300 seconds".into(),
            ));
        }
        if !path_segment(&self.project, b"-")
            || !path_segment(&self.location, b"-")
            || !path_segment(&self.profile.model, b"-_.@")
        {
            return Err(EmbedError::Permanent(
                "invalid Google Cloud project, location, or model".into(),
            ));
        }
        let host = if self.location == "global" {
            "aiplatform.googleapis.com".to_owned()
        } else {
            format!("{}-aiplatform.googleapis.com", self.location)
        };
        Url::parse(&format!(
            "https://{host}/v1/projects/{}/locations/{}/publishers/google/models/{}:predict",
            self.project, self.location, self.profile.model,
        ))
        .map_err(|_| EmbedError::Permanent("invalid Vertex AI endpoint".into()))
    }
}

pub struct Vertex {
    endpoint: Url,
    client: Client,
    credentials: OnceCell<AccessTokenCredentials>,
    profile: Profile,
    timeout: Duration,
    id: String,
}

impl Vertex {
    pub fn new(config: GoogleConfig) -> Result<Self, EmbedError> {
        let endpoint = config.endpoint()?;
        let timeout = Duration::from_secs(config.timeout_seconds);
        let id = config.profile.id(
            "vertex/predict/retrieval-tasks/no-truncation",
            endpoint.as_str(),
        );
        // Both the auth SDK and the HTTP client use the same Rustls provider.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let client = Client::builder()
            .timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| {
                EmbedError::Permanent("could not initialize the Vertex AI client".into())
            })?;
        Ok(Self {
            endpoint,
            client,
            credentials: OnceCell::new(),
            profile: config.profile,
            timeout,
            id,
        })
    }

    async fn access_token(&self) -> Result<String, EmbedError> {
        let credentials = self
            .credentials
            .get_or_try_init(|| async {
                tokio::task::spawn_blocking(|| {
                    Builder::default()
                        .with_scopes([CLOUD_SCOPE])
                        .build_access_token_credentials()
                })
                .await
                .map_err(|_| {
                    EmbedError::Transient("could not initialize Google credentials".into())
                })?
                .map_err(|_| {
                    EmbedError::Transient(
                        "Google Application Default Credentials are unavailable".into(),
                    )
                })
            })
            .await?;
        credentials
            .access_token()
            .await
            .map(|value| value.token)
            .map_err(|_| EmbedError::Transient("could not refresh Google credentials".into()))
    }

    async fn request(
        &self,
        purpose: Purpose,
        text: &str,
        token: &str,
    ) -> Result<Vec<f32>, EmbedError> {
        let mut response = self
            .client
            .post(self.endpoint.clone())
            .bearer_auth(token)
            .json(&Request {
                instances: [Instance {
                    content: self.profile.input(purpose, text),
                    task_type: task_type(purpose),
                }],
                parameters: Parameters {
                    auto_truncate: false,
                    output_dimensionality: self.profile.dimensions,
                },
            })
            .send()
            .await
            .map_err(|_| {
                EmbedError::Transient(
                    "Vertex AI could not be reached before the request deadline".into(),
                )
            })?;
        let status = response.status();
        if !status.is_success() {
            let message = format!("Vertex AI returned HTTP {}", status.as_u16());
            return Err(match status.as_u16() {
                400 | 413 | 422 => EmbedError::Input(message),
                401 | 403 | 408 | 429 | 500..=599 => EmbedError::Transient(message),
                _ => EmbedError::Permanent(message),
            });
        }
        let mut body = Vec::new();
        let limit = self.profile.dimensions * 32 + 64 * 1024;
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| EmbedError::Transient("Vertex AI response was interrupted".into()))?
        {
            if chunk.len() > limit - body.len() {
                return Err(EmbedError::Permanent(
                    "Vertex AI response exceeds its size limit".into(),
                ));
            }
            body.extend_from_slice(&chunk);
        }
        decode(&body)
    }
}

fn task_type(purpose: Purpose) -> &'static str {
    match purpose {
        Purpose::Document => "RETRIEVAL_DOCUMENT",
        Purpose::Query => "RETRIEVAL_QUERY",
    }
}

#[derive(Serialize)]
struct Request {
    instances: [Instance; 1],
    parameters: Parameters,
}

#[derive(Serialize)]
struct Instance {
    content: String,
    task_type: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Parameters {
    auto_truncate: bool,
    output_dimensionality: usize,
}

#[derive(Deserialize)]
struct Response {
    predictions: [Prediction; 1],
}

#[derive(Deserialize)]
struct Prediction {
    embeddings: Embeddings,
}

#[derive(Deserialize)]
struct Embeddings {
    values: Vec<f32>,
    statistics: Statistics,
}

#[derive(Deserialize)]
struct Statistics {
    truncated: bool,
}

fn decode(body: &[u8]) -> Result<Vec<f32>, EmbedError> {
    let decoded: Response = serde_json::from_slice(body)
        .map_err(|_| EmbedError::Permanent("Vertex AI returned an invalid prediction".into()))?;
    let [prediction] = decoded.predictions;
    if prediction.embeddings.statistics.truncated {
        return Err(EmbedError::Input(
            "Vertex AI truncated the embedding input".into(),
        ));
    }
    Ok(prediction.embeddings.values)
}

#[async_trait::async_trait]
impl Provider for Vertex {
    fn profile_id(&self) -> &str {
        &self.id
    }

    fn dimensions(&self) -> usize {
        self.profile.dimensions
    }

    fn max_batch_size(&self) -> u32 {
        1
    }

    async fn embed(&self, purpose: Purpose, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
        let [text] = texts else {
            return Err(EmbedError::Permanent(
                "Vertex AI adapter accepts one input per request".into(),
            ));
        };
        let token = tokio::time::timeout(self.timeout, self.access_token())
            .await
            .map_err(|_| EmbedError::Transient("Google credential request timed out".into()))??;
        Ok(vec![self.request(purpose, text, &token).await?])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_one_untruncated_prediction() {
        let response = |truncated| {
            format!(
                r#"{{"predictions":[{{"embeddings":{{"values":[3,4],"statistics":{{"truncated":{truncated}}}}}}}]}}"#
            )
        };
        assert_eq!(decode(response(false).as_bytes()).unwrap(), [3.0, 4.0]);
        assert!(matches!(
            decode(response(true).as_bytes()),
            Err(EmbedError::Input(_))
        ));
        assert!(decode(br#"{"predictions":[]}"#).is_err());
    }

    #[test]
    fn serializes_the_vertex_text_prediction_contract() {
        let request = Request {
            instances: [Instance {
                content: "sample".into(),
                task_type: task_type(Purpose::Document),
            }],
            parameters: Parameters {
                auto_truncate: false,
                output_dimensionality: 768,
            },
        };
        assert_eq!(
            serde_json::to_value(request).unwrap(),
            serde_json::json!({
                "instances": [{"content":"sample", "task_type":"RETRIEVAL_DOCUMENT"}],
                "parameters": {"autoTruncate":false, "outputDimensionality":768}
            })
        );
        assert_eq!(task_type(Purpose::Query), "RETRIEVAL_QUERY");
    }
}
