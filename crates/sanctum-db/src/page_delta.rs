//! Ordered page differences between immutable SQLite backups.

use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::Path,
};

use sanctum_core::Result;
use tempfile::NamedTempFile;

use crate::corrupt;

const MAGIC: [u8; 4] = *b"SPD1";
pub(crate) const HEADER_BYTES: u64 = 16;
const PAGE_NUMBER_BYTES: u64 = 8;
const BUFFER_BYTES: usize = 1024 * 1024;

fn page_size(file: &mut File) -> Result<u32> {
    let mut header = [0; 100];
    file.read_exact(&mut header)?;
    file.rewind()?;
    if &header[..16] != b"SQLite format 3\0" || header[18..20] != [1, 1] {
        return Err(corrupt(
            "delta requires a SQLite backup in DELETE journal mode",
        ));
    }
    let size = match u16::from_be_bytes([header[16], header[17]]) {
        1 => 65536,
        size => u32::from(size),
    };
    if !(512..=65536).contains(&size) || !size.is_power_of_two() {
        return Err(corrupt("invalid SQLite backup page size"));
    }
    Ok(size)
}

/// Returns None when a full backup is preferable. Both inputs are closed files
/// produced by SQLite. The buffer is bounded independently of database size.
pub(crate) fn encode(before: &Path, after: &Path, limit: u64) -> Result<Option<NamedTempFile>> {
    let mut old = File::open(before)?;
    let mut new = File::open(after)?;
    let size = page_size(&mut new)?;
    if page_size(&mut old)? != size || limit < HEADER_BYTES {
        return Ok(None);
    }
    let page_bytes = u64::from(size);
    let before_bytes = old.metadata()?.len();
    let after_bytes = new.metadata()?.len();
    if before_bytes % page_bytes != 0 || after_bytes % page_bytes != 0 {
        return Err(corrupt("SQLite backup does not contain complete pages"));
    }
    let patch = NamedTempFile::new()?;
    let mut writer = BufWriter::new(patch.reopen()?);
    writer.write_all(&MAGIC)?;
    writer.write_all(&size.to_be_bytes())?;
    writer.write_all(&after_bytes.to_be_bytes())?;
    let size = size as usize;
    let mut previous = vec![0; BUFFER_BYTES];
    let mut current = vec![0; BUFFER_BYTES];
    let mut bytes = HEADER_BYTES;
    let mut position = 0;
    while position < after_bytes {
        let next = (after_bytes - position).min(BUFFER_BYTES as u64) as usize;
        let previous_bytes = before_bytes.saturating_sub(position).min(next as u64) as usize;
        old.read_exact(&mut previous[..previous_bytes])?;
        new.read_exact(&mut current[..next])?;
        if previous_bytes != next || previous[..next] != current[..next] {
            for (index, page) in current[..next].chunks_exact(size).enumerate() {
                let offset = index * size;
                if previous
                    .get(..previous_bytes)
                    .and_then(|old| old.get(offset..offset + size))
                    == Some(page)
                {
                    continue;
                }
                bytes += PAGE_NUMBER_BYTES + page_bytes;
                if bytes > limit {
                    return Ok(None);
                }
                let number = position / page_bytes + index as u64;
                writer.write_all(&number.to_be_bytes())?;
                writer.write_all(page)?;
            }
        }
        position += next as u64;
    }
    writer.flush()?;
    Ok(Some(patch))
}

/// Applies a previously verified patch to a private, closed backup file.
pub(crate) fn apply(database: &mut File, delta: File, expected_database_bytes: u64) -> Result<()> {
    let size = page_size(database)?;
    let page_bytes = u64::from(size);
    let old_bytes = database.metadata()?.len();
    let delta_bytes = delta.metadata()?.len();
    let mut reader = BufReader::new(delta);
    let mut magic = [0; 4];
    let mut encoded_size = [0; 4];
    let mut encoded_total = [0; 8];
    reader.read_exact(&mut magic)?;
    reader.read_exact(&mut encoded_size)?;
    reader.read_exact(&mut encoded_total)?;
    let total = u64::from_be_bytes(encoded_total);
    let record_bytes = PAGE_NUMBER_BYTES + page_bytes;
    if magic != MAGIC
        || u32::from_be_bytes(encoded_size) != size
        || total != expected_database_bytes
        || total == 0
        || old_bytes % page_bytes != 0
        || total % page_bytes != 0
        || delta_bytes < HEADER_BYTES
        || !(delta_bytes - HEADER_BYTES).is_multiple_of(record_bytes)
    {
        return Err(corrupt("invalid SQLite page delta header or length"));
    }
    let records = (delta_bytes - HEADER_BYTES) / record_bytes;
    let mut next_new = old_bytes / page_bytes;
    let page_count = total / page_bytes;
    let mut previous = None;
    let mut number = [0; 8];
    let mut page = vec![0; size as usize];
    for _ in 0..records {
        reader.read_exact(&mut number)?;
        reader.read_exact(&mut page)?;
        let number = u64::from_be_bytes(number);
        if number >= page_count
            || previous.is_some_and(|previous| number <= previous)
            || (number >= next_new && number != next_new)
        {
            return Err(corrupt("invalid SQLite page delta sequence"));
        }
        if number == next_new {
            next_new += 1;
        }
        database.seek(SeekFrom::Start(number * page_bytes))?;
        database.write_all(&page)?;
        previous = Some(number);
    }
    if next_new < page_count {
        return Err(corrupt("SQLite page delta omits appended pages"));
    }
    database.set_len(total)?;
    Ok(())
}
