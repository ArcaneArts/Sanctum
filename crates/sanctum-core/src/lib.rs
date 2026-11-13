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

#[derive(Clone, Debug, PartialEq)]
pub struct ExternalSource {
    pub namespace: String,
    pub id: String,
}

pub const UPLOAD_FRAME_BYTES: usize = 256 * 1024;
