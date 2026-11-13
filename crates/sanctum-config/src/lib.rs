use std::{net::SocketAddr, path::PathBuf};

use sanctum_core::{Error, Result};
use serde::Deserialize;

pub const CONFIG_ENV: &str = "SANCTUM_CONFIG_JSON";

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub version: u32,
    pub storage_root: PathBuf,
    #[serde(default = "default_grpc")]
    pub grpc_listen: SocketAddr,
    #[serde(default = "default_max_source")]
    pub max_source_bytes: u64,
}

impl Config {
    pub fn from_json(json: &str) -> Result<Self> {
        let config: Self = serde_json::from_str(json)
            .map_err(|error| Error::Invalid(format!("invalid {CONFIG_ENV}: {error}")))?;
        if config.version != 1 {
            return Err(Error::Invalid("expected configuration version 1".into()));
        }
        if config.storage_root.as_os_str().is_empty() || config.max_source_bytes == 0 {
            return Err(Error::Invalid(
                "storage_root and a positive max_source_bytes are required".into(),
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

fn default_max_source() -> u64 {
    1024 * 1024 * 1024
}
