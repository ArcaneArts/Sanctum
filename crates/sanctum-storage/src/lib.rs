use std::{fmt, pin::Pin, time::SystemTime};

use bytes::Bytes;
use sanctum_core::{ByteRange, Error, Result, UsageMeasurement};
use tokio::io::AsyncRead;

#[cfg(feature = "test-support")]
pub mod contract;

pub const MAX_CONTROL_BYTES: usize = 1024 * 1024;
pub const MAX_RANGE_BYTES: u64 = 4 * 1024 * 1024;
pub const MAX_LIST_PAGE: u32 = 1000;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Key(String);

impl Key {
    pub fn parse(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 1024
            || value.split('/').any(|part| {
                part.starts_with('.')
                    || part.ends_with('.')
                    || part.is_empty()
                    || part.len() > 255
                    || !part
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
                    || windows_reserved(part)
            })
        {
            return Err(Error::Invalid("invalid managed storage key".into()));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn windows_reserved(part: &str) -> bool {
    let stem = part
        .split('.')
        .next()
        .expect("split always returns one value")
        .to_ascii_lowercase();
    matches!(stem.as_str(), "aux" | "con" | "nul" | "prn")
        || (stem.len() == 4
            && (stem.starts_with("com") || stem.starts_with("lpt"))
            && stem.as_bytes()[3].is_ascii_digit())
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Version {
    pub etag: Option<String>,
    pub generation: Option<String>,
}

pub enum Condition {
    Create,
    Match(Version),
}

#[derive(Debug)]
pub struct Object {
    pub key: Key,
    pub length: u64,
    pub version: Version,
    pub modified_at: SystemTime,
}

#[derive(Debug)]
pub struct ListPage {
    pub objects: Vec<Object>,
    pub next_cursor: Option<String>,
}

pub type Reader = Pin<Box<dyn AsyncRead + Send>>;

pub struct Download {
    pub object: Object,
    pub reader: Reader,
}

#[async_trait::async_trait]
pub trait Storage: Send + Sync {
    async fn head(&self, key: &Key) -> Result<Object>;
    async fn get(&self, key: &Key) -> Result<Download>;

    /// Read exactly this half-open range without fetching the full object.
    async fn read_range(&self, key: &Key, range: ByteRange) -> Result<Bytes>;

    /// Stream to a fresh unique key owned by the caller.
    /// On ambiguous failure, retry with a new key or verify the previous object.
    /// Coordination keys use put_control with a storage-side precondition.
    async fn put_stream(&self, key: &Key, reader: Reader, length: u64) -> Result<Version>;
    async fn put_control(&self, key: &Key, bytes: Bytes, condition: Condition) -> Result<Version>;

    /// Prefixes select descendants at a path boundary. A cursor is specific to the prefix.
    /// A provider can return an empty page before the cursor is exhausted.
    async fn list(
        &self,
        prefix: Option<&Key>,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<ListPage>;
    async fn delete(&self, key: &Key) -> Result<()>;
}

pub fn range_len(range: ByteRange) -> Result<usize> {
    let size = range.length()?;
    if !(1..=MAX_RANGE_BYTES).contains(&size) {
        return Err(Error::Invalid(
            "storage range must be between 1 byte and 4 MiB".into(),
        ));
    }
    Ok(size as usize)
}

pub fn list_limit(limit: u32) -> Result<usize> {
    if !(1..=MAX_LIST_PAGE).contains(&limit) {
        return Err(Error::Invalid(
            "storage list page size must be between 1 and 1000".into(),
        ));
    }
    Ok(limit as usize)
}

pub async fn usage(store: &dyn Storage, prefix: Option<&Key>) -> Result<UsageMeasurement> {
    let started_at = SystemTime::now();
    let mut cursor = None;
    let mut bytes = 0u64;
    let mut files = 0u64;
    loop {
        let page = store.list(prefix, cursor.as_deref(), MAX_LIST_PAGE).await?;
        for object in page.objects {
            bytes = bytes
                .checked_add(object.length)
                .ok_or_else(usage_overflow)?;
            files = files.checked_add(1).ok_or_else(usage_overflow)?;
        }
        if page.next_cursor.is_none() {
            return Ok(UsageMeasurement {
                bytes,
                files,
                started_at,
                finished_at: SystemTime::now(),
            });
        }
        if page.next_cursor == cursor {
            return Err(Error::Internal(
                "storage list cursor did not advance".into(),
            ));
        }
        cursor = page.next_cursor;
    }
}

fn usage_overflow() -> Error {
    Error::Internal("storage usage exceeds 64 bits".into())
}
