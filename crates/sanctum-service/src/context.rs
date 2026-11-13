use sanctum_core::{ByteRange, Error, Result, Scope};

use crate::{Chunk, ChunkRef, Hit, HostedCatalog, Service, db, hosted_retrieval::locate};

const MAX_SIDE_BYTES: u32 = 16 * 1024;

pub struct ChunkContext {
    pub hit: Hit,
    pub start_byte: u64,
    pub end_byte: u64,
    pub text: String,
}

impl Service {
    pub fn fetch_context(
        &self,
        scope: &Scope,
        reference: &ChunkRef,
        before: u32,
        after: u32,
    ) -> Result<ChunkContext> {
        validate_budget(before, after)?;
        let _reader = self
            .retrieval
            .read()
            .map_err(|_| Error::Busy("retrieval lock is unavailable".into()))?;
        let (chunk, range) = self
            .database(scope)?
            .read_snapshot(|db| context_range(db, reference, before, after))?;
        let bytes = self
            .files
            .read_chunk(scope, reference.source, reference.revision, range)?;
        finish_context(chunk, range, &bytes)
    }
}

impl HostedCatalog {
    pub async fn fetch_context(
        &self,
        scope: &Scope,
        reference: ChunkRef,
        before: u32,
        after: u32,
    ) -> Result<ChunkContext> {
        validate_budget(before, after)?;
        let (((chunk, key), range), generation) = self
            .with_collection(scope, move |db| {
                let (chunk, range) = context_range(db, &reference, before, after)?;
                Ok((locate(db, chunk)?, range))
            })
            .await?;
        let key = self.checked_text_key(scope, key)?;
        let bytes = self.storage.read_range(&key, range).await?;
        let mut context = finish_context(chunk, range, &bytes)?;
        context.hit.database_generation = Some(generation);
        Ok(context)
    }
}

fn validate_budget(before: u32, after: u32) -> Result<()> {
    if before > MAX_SIDE_BYTES || after > MAX_SIDE_BYTES {
        return Err(Error::Invalid(
            "context allows at most 16384 bytes per side".into(),
        ));
    }
    Ok(())
}

fn context_range(
    db: &db::CollectionDb,
    reference: &ChunkRef,
    before: u32,
    after: u32,
) -> Result<(Chunk, ByteRange)> {
    let chunk = db.fetch(reference)?;
    let size = db
        .revision_summary(reference.source, reference.revision)?
        .ok_or_else(|| Error::Internal("chunk revision is missing".into()))?
        .bytes;
    if chunk.range.end > size || chunk.range.start >= chunk.range.end {
        return Err(Error::Internal("chunk bounds exceed its revision".into()));
    }
    let range = ByteRange {
        start: chunk.range.start.saturating_sub(u64::from(before)),
        end: chunk.range.end.saturating_add(u64::from(after)).min(size),
    };
    Ok((chunk, range))
}

fn finish_context(chunk: Chunk, range: ByteRange, bytes: &[u8]) -> Result<ChunkContext> {
    if bytes.len() as u64 != range.length()? {
        return Err(Error::Internal(
            "context byte length differs from its range".into(),
        ));
    }
    let chunk_start = (chunk.range.start - range.start) as usize;
    let chunk_end = (chunk.range.end - range.start) as usize;
    let hit = Hit::from_bytes(chunk, &bytes[chunk_start..chunk_end])?;
    // If the requested start cuts a code point, trim its continuation bytes.
    let start = bytes
        .iter()
        .take_while(|byte| **byte & 0b1100_0000 == 0b1000_0000)
        .count();
    let text = match std::str::from_utf8(&bytes[start..]) {
        Ok(text) => text,
        Err(error) if error.error_len().is_none() => {
            std::str::from_utf8(&bytes[start..start + error.valid_up_to()])
                .map_err(|error| Error::Internal(Box::new(error)))?
        }
        Err(error) => return Err(Error::Internal(Box::new(error))),
    };
    Ok(ChunkContext {
        hit,
        start_byte: range.start + start as u64,
        end_byte: range.start + (start + text.len()) as u64,
        text: text.to_owned(),
    })
}
