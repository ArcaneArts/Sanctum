use std::{collections::HashMap, net::SocketAddr, path::PathBuf};

use sanctum_core::{Error, Result};
use serde::{Deserialize, Serialize};

pub const CONFIG_ENV: &str = "SANCTUM_CONFIG_JSON";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub version: u32,
    #[serde(default)]
    pub storage_root: PathBuf,
    pub object_storage: Option<ObjectStorageConfig>,
    pub hosted_worker_listen: Option<SocketAddr>,
    #[serde(default = "default_grpc")]
    pub grpc_listen: SocketAddr,
    #[serde(default = "default_mcp")]
    pub mcp_listen: Option<SocketAddr>,
    #[serde(default = "default_max_source")]
    pub max_source_bytes: u64,
    #[serde(default = "default_worker_concurrency")]
    pub local_worker_concurrency: usize,
    pub embedding: Option<EmbeddingConfig>,
    pub url_import_origins: Option<HashMap<String, Option<String>>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "backend", rename_all = "snake_case", deny_unknown_fields)]
pub enum ObjectStorageConfig {
    S3 {
        bucket: String,
        region: String,
        endpoint: Option<String>,
        prefix: Option<String>,
        #[serde(default)]
        allow_http: bool,
        #[serde(default)]
        virtual_hosted_style: bool,
    },
    Gcs {
        bucket: String,
        prefix: Option<String>,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "provider", rename_all = "snake_case")]
pub enum EmbeddingConfig {
    Ollama(sanctum_embedding_ollama::OllamaConfig),
    #[serde(rename = "openai")]
    OpenAi(sanctum_embedding_openai::OpenAiConfig),
    Google(sanctum_embedding_google::GoogleConfig),
}

impl EmbeddingConfig {
    pub fn validate(&self) -> Result<()> {
        let result = match self {
            Self::Ollama(config) => {
                sanctum_embedding_ollama::Ollama::new(config.clone()).map(|_| ())
            }
            Self::OpenAi(config) => {
                sanctum_embedding_openai::OpenAi::new(config.clone()).map(|_| ())
            }
            Self::Google(config) => {
                sanctum_embedding_google::Vertex::new(config.clone()).map(|_| ())
            }
        };
        result.map_err(|error| Error::Invalid(error.to_string()))
    }
}

impl Config {
    pub fn from_json(json: &str) -> Result<Self> {
        let config: Self = serde_json::from_str(json)
            .map_err(|error| Error::Invalid(format!("invalid {CONFIG_ENV}: {error}")))?;
        if config.version != 1 {
            return Err(Error::Invalid("expected configuration version 1".into()));
        }
        if config.max_source_bytes == 0 {
            return Err(Error::Invalid("max_source_bytes must be positive".into()));
        }
        if config.object_storage.is_none() && config.storage_root.as_os_str().is_empty() {
            return Err(Error::Invalid(
                "storage_root is required for filesystem storage".into(),
            ));
        }
        if config.hosted_worker_listen.is_some() && config.object_storage.is_none() {
            return Err(Error::Invalid(
                "hosted_worker_listen requires object storage".into(),
            ));
        }
        if !(1..=32).contains(&config.local_worker_concurrency) {
            return Err(Error::Invalid(
                "local_worker_concurrency must be between 1 and 32".into(),
            ));
        }
        Ok(config)
    }

    pub fn from_env() -> Result<Self> {
        let json = std::env::var(CONFIG_ENV)
            .map_err(|_| Error::Invalid(format!("set {CONFIG_ENV} to a JSON configuration")))?;
        Self::from_json(&json)
    }
}

fn default_grpc() -> SocketAddr {
    ([127, 0, 0, 1], 47550).into()
}

fn default_mcp() -> Option<SocketAddr> {
    Some(([127, 0, 0, 1], 47551).into())
}

fn default_max_source() -> u64 {
    1024 * 1024 * 1024
}

fn default_worker_concurrency() -> usize {
    1
}
