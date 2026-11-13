use std::{io::Read, str};

use sanctum_core::{ByteRange, ChunkSettings, Error, Result};

#[derive(Debug)]
pub struct TextIndex {
    pub hash: String,
    pub bytes: u64,
    pub chunks: u64,
}

/// Consumes a UTF-8 stream and emits overlapping ranges into the original bytes.
pub fn index_utf8(
    source: impl Read,
    emit: impl FnMut(ByteRange, &str) -> Result<()>,
) -> Result<TextIndex> {
    index_utf8_with_settings(source, ChunkSettings::default(), emit)
}

pub fn index_utf8_with_settings(
    mut source: impl Read,
    settings: ChunkSettings,
    mut emit: impl FnMut(ByteRange, &str) -> Result<()>,
) -> Result<TextIndex> {
    settings.validate()?;
    let capacity = settings.window_bytes as usize;
    let overlap = settings.overlap_bytes as usize;
    let mut window = Vec::with_capacity(capacity + 1);
    let mut offset = 0;
    let mut chunks = 0;
    let mut total_bytes = 0;
    let mut hash = blake3::Hasher::new();

    loop {
        let old_len = window.len();
        (&mut source)
            .take((capacity + 1 - old_len) as u64)
            .read_to_end(&mut window)?;
        hash.update(&window[old_len..]);
        total_bytes += (window.len() - old_len) as u64;
        if window.is_empty() {
            break;
        }

        let eof = window.len() <= capacity;
        let candidate = &window[..window.len().min(capacity)];
        let text = match str::from_utf8(candidate) {
            Ok(text) => text,
            Err(error) if !eof && error.error_len().is_none() => {
                str::from_utf8(&candidate[..error.valid_up_to()])
                    .map_err(|_| Error::Invalid("source is not valid UTF-8".into()))?
            }
            Err(_) => return Err(Error::Invalid("source is not valid UTF-8".into())),
        };
        if text.as_bytes().contains(&0) {
            return Err(Error::Invalid("source text contains a NUL byte".into()));
        }

        let end = if eof {
            text.len()
        } else {
            text.rmatch_indices('\n')
                .map(|(index, _)| index + 1)
                .find(|&index| index >= capacity * 3 / 4)
                .unwrap_or(text.len())
        };
        emit(
            ByteRange {
                start: offset,
                end: offset + end as u64,
            },
            &text[..end],
        )?;
        chunks += 1;
        if eof {
            break;
        }

        let mut advance = end - overlap;
        while !text.is_char_boundary(advance) {
            advance += 1;
        }
        window.drain(..advance);
        offset += advance as u64;
    }

    Ok(TextIndex {
        hash: hash.finalize().to_hex().to_string(),
        bytes: total_bytes,
        chunks,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const WINDOW_BYTES: usize = 4096;

    #[test]
    fn preserves_ranges_and_hash_across_split_utf8_and_long_lines() {
        let source = format!(
            "{}🪨{}\r\nlast",
            "x".repeat(WINDOW_BYTES - 1),
            "ü".repeat(6000)
        );
        let mut ranges = Vec::new();
        let result = index_utf8(source.as_bytes(), |range, text| {
            assert_eq!(text, &source[range.start as usize..range.end as usize]);
            assert!(text.len() <= WINDOW_BYTES);
            ranges.push(range);
            Ok(())
        })
        .unwrap();
        assert_eq!(ranges.first().unwrap().start, 0);
        assert_eq!(ranges.last().unwrap().end, source.len() as u64);
        assert!(
            ranges
                .windows(2)
                .all(|pair| pair[0].start < pair[1].start && pair[1].start < pair[0].end)
        );
        assert_eq!(result.bytes, source.len() as u64);
        assert_eq!(
            result.hash,
            blake3::hash(source.as_bytes()).to_hex().as_str()
        );
    }

    #[test]
    fn rejects_invalid_input_after_a_valid_window() {
        let mut bytes = vec![b'x'; WINDOW_BYTES + 1];
        bytes.push(0xff);
        assert!(matches!(
            index_utf8(bytes.as_slice(), |_, _| Ok(())),
            Err(Error::Invalid(_))
        ));
    }
}
