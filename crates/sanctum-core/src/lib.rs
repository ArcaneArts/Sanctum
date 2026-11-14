use std::{fmt, io, str::FromStr};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    StaleRevision(String),
    #[error("{0}")]
    Busy(String),
    #[error("storage I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("internal operation failed: {0}")]
    Internal(#[source] Box<dyn std::error::Error + Send + Sync>),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ScopeId(String);

impl ScopeId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for ScopeId {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        let bytes = value.as_bytes();
        let alphanumeric = |byte: u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
        let device_name = matches!(value, "aux" | "con" | "nul" | "prn")
            || (bytes.len() == 4
                && (value.starts_with("com") || value.starts_with("lpt"))
                && bytes[3].is_ascii_digit());
        if bytes.len() > 64
            || !bytes.first().is_some_and(|&byte| alphanumeric(byte))
            || !bytes
                .iter()
                .all(|&byte| alphanumeric(byte) || matches!(byte, b'-' | b'_'))
            || device_name
        {
            return Err(Error::Invalid(
                "scope IDs must be 1 to 64 lowercase ASCII letters, digits, dashes, or underscores, start with a letter or digit, and avoid reserved device names".into(),
            ));
        }
        Ok(Self(value.into()))
    }
}

impl fmt::Display for ScopeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug)]
pub struct Scope {
    pub tenant: ScopeId,
    pub collection: ScopeId,
}

pub enum UsageScope {
    Collection(Scope),
    Tenant(ScopeId),
    Cluster,
}

pub struct UsageMeasurement {
    pub bytes: u64,
    pub files: u64,
    pub started_at: std::time::SystemTime,
    pub finished_at: std::time::SystemTime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ByteRange {
    pub start: u64,
    pub end: u64,
}

impl ByteRange {
    pub fn length(self) -> Result<u64> {
        self.end
            .checked_sub(self.start)
            .ok_or_else(|| Error::Invalid("byte range end precedes start".into()))
    }
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ExternalSource {
    pub namespace: String,
    pub id: String,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct UrlProvenance {
    pub requested_url: String,
    pub final_url: String,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub content_type: Option<String>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GitProvenance {
    pub repository: String,
    pub reference: String,
    pub commit: String,
    pub path: String,
    pub blob: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ChunkSettings {
    pub window_bytes: u32,
    pub overlap_bytes: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
}

impl Default for ChunkSettings {
    fn default() -> Self {
        Self {
            window_bytes: 4096,
            overlap_bytes: 400,
            max_tokens: None,
        }
    }
}

impl ChunkSettings {
    pub fn validate(self) -> Result<()> {
        if !(256..=16_384).contains(&self.window_bytes)
            || self.overlap_bytes > self.window_bytes / 2
        {
            return Err(Error::Invalid(
                "chunk window must be 256 to 16384 bytes, with overlap at most half the window"
                    .into(),
            ));
        }
        if self
            .max_tokens
            .is_some_and(|limit| !(16..=16_384).contains(&limit))
        {
            return Err(Error::Invalid(
                "chunk token budget must be 16 to 16384".into(),
            ));
        }
        Ok(())
    }

    pub fn identifier(self) -> String {
        if let Some(tokens) = self.max_tokens {
            return format!(
                "utf8-overlap-estimated-tokens-v1/{}/{}/{tokens}",
                self.window_bytes, self.overlap_bytes
            );
        }
        if self == Self::default() {
            return "utf8-overlap-v1".into();
        }
        format!(
            "utf8-overlap-v1/{}/{}",
            self.window_bytes, self.overlap_bytes
        )
    }
}

pub const UPLOAD_FRAME_BYTES: usize = 256 * 1024;

pub fn is_rtf(name: &str) -> bool {
    name.rsplit_once('.')
        .is_some_and(|(_, extension)| extension.eq_ignore_ascii_case("rtf"))
}
