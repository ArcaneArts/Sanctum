use sanctum_core::{ByteRange, Error, Result, Scope};
use uuid::Uuid;

use crate::{HostedCatalog, Service};

const MAX_RANGE_BYTES: u64 = 64 * 1024;

pub struct SourceRange {
    pub text: String,
    pub database_generation: Option<Uuid>,
}

impl Service {
    pub fn fetch_source_range(
        &self,
        scope: &Scope,
        source: Uuid,
        revision: Uuid,
        range: ByteRange,
    ) -> Result<SourceRange> {
        let length = validate_length(range)?;
        let _reader = self
            .retrieval
            .read()
            .map_err(|_| Error::Busy("retrieval lock is unavailable".into()))?;
        let size = self
            .database(scope)?
            .read_snapshot(|db| db.readable_revision_bytes(source, revision))?;
        validate_end(range, size)?;
        let bytes = self.files.read_chunk(scope, source, revision, range)?;
        decode(&bytes, length, None)
    }
}

impl HostedCatalog {
    pub async fn fetch_source_range(
        &self,
        scope: &Scope,
        source: Uuid,
        revision: Uuid,
        range: ByteRange,
    ) -> Result<SourceRange> {
        let length = validate_length(range)?;
        let (key, generation) = self
            .with_collection(scope, move |db| {
                let size = db.readable_revision_bytes(source, revision)?;
                validate_end(range, size)?;
                db.hosted_text_key(source, revision)
            })
            .await?;
        let key = self.checked_text_key(scope, key)?;
        let bytes = self.storage.read_range(&key, range).await?;
        decode(&bytes, length, Some(generation))
    }
}

fn validate_length(range: ByteRange) -> Result<u64> {
    let length = range.length()?;
    if !(1..=MAX_RANGE_BYTES).contains(&length) {
        return Err(Error::Invalid(
            "source range must span 1 to 65536 bytes".into(),
        ));
    }
    Ok(length)
}

fn validate_end(range: ByteRange, size: u64) -> Result<()> {
    if range.end > size {
        return Err(Error::Invalid(
            "source range exceeds the revision size".into(),
        ));
    }
    Ok(())
}

fn decode(bytes: &[u8], length: u64, generation: Option<Uuid>) -> Result<SourceRange> {
    if bytes.len() as u64 != length {
        return Err(Error::Internal(
            "source byte length differs from its range".into(),
        ));
    }
    let text = match std::str::from_utf8(bytes) {
        Ok(text) => text,
        Err(error) if bytes[0] & 0b1100_0000 == 0b1000_0000 || error.error_len().is_none() => {
            return Err(Error::Invalid(
                "source range must align to UTF-8 boundaries".into(),
            ));
        }
        Err(error) => return Err(Error::Internal(Box::new(error))),
    };
    Ok(SourceRange {
        text: text.into(),
        database_generation: generation,
    })
}
