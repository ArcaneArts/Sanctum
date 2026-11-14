use std::{future::Future, io, sync::Arc};

use memchr::memmem;
use sanctum_core::{ByteRange, Error, ExternalSource, Result, Scope};
use sanctum_storage::{Key, Storage};
use tokio::io::AsyncReadExt;
use uuid::Uuid;

use crate::{JobState, Service};

const READ_BYTES: usize = 64 * 1024;
const MAX_LITERAL_BYTES: usize = 1024;
const MAX_SCAN_BYTES: u64 = 32 * 1024 * 1024;

pub struct ScanOptions {
    literal: String,
    byte_limit: u64,
    hit_limit: usize,
}

impl ScanOptions {
    pub fn new(literal: String, byte_limit: u64, hit_limit: u32) -> Result<Self> {
        if !(1..=MAX_LITERAL_BYTES).contains(&literal.len()) || literal.contains('\0') {
            return Err(Error::Invalid(
                "literal must contain 1 to 1024 UTF-8 bytes without NUL".into(),
            ));
        }
        if !(1..=MAX_SCAN_BYTES).contains(&byte_limit) || !(1..=100).contains(&hit_limit) {
            return Err(Error::Invalid(
                "literal scan requires a 1 to 33554432 byte budget and a 1 to 100 hit limit".into(),
            ));
        }
        Ok(Self {
            literal,
            byte_limit,
            hit_limit: hit_limit as usize,
        })
    }
}

pub struct PendingText {
    pub source: Option<Uuid>,
    pub revision: Uuid,
    pub name: String,
    pub external: Option<ExternalSource>,
    pub total_bytes: u64,
    pub(crate) input: ScanSource,
}

pub(crate) enum ScanSource {
    RtfOriginal,
    Local(tokio::fs::File),
    Hosted { storage: Arc<dyn Storage>, key: Key },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScanStop {
    Complete,
    ByteLimit,
    HitLimit,
    InvalidText,
    ExtractionPending,
}

impl ScanStop {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::ByteLimit => "byte_limit",
            Self::HitLimit => "hit_limit",
            Self::InvalidText => "invalid_text",
            Self::ExtractionPending => "extraction_pending",
        }
    }
}

pub struct ScanPage {
    pub scanned_bytes: u64,
    pub read_bytes: u64,
    pub hits: Vec<ByteRange>,
    pub stop: Option<ScanStop>,
}

impl Service {
    pub fn pending_text(&self, scope: &Scope, operation: Uuid) -> Result<PendingText> {
        let _reader = self
            .retrieval
            .read()
            .map_err(|_| Error::Busy("retrieval lock is unavailable".into()))?;
        self.require_collection(scope)?;
        let job = self.queue.get(scope, operation)?;
        if !matches!(
            job.state,
            JobState::Accepted | JobState::Indexing | JobState::Publishing
        ) {
            return Err(Error::Conflict(
                "literal scan requires pending text ingestion".into(),
            ));
        }
        let (input, total_bytes) =
            if job.extract_rtf && (job.extracting || job.state == JobState::Accepted) {
                (ScanSource::RtfOriginal, job.bytes)
            } else {
                let file = self.files.open_text(scope, job.source, job.revision)?;
                let length = file.metadata()?.len();
                if !job.extract_rtf && length != job.bytes {
                    return Err(Error::Internal(
                        "staged upload size differs from its receipt".into(),
                    ));
                }
                (ScanSource::Local(tokio::fs::File::from_std(file)), length)
            };
        Ok(PendingText {
            source: Some(job.source),
            revision: job.revision,
            name: job.name,
            external: job.external,
            total_bytes,
            input,
        })
    }
}

impl PendingText {
    pub async fn scan<F: Future<Output = bool> + Send>(
        mut self,
        options: &ScanOptions,
        mut send: impl FnMut(ScanPage) -> F + Send,
    ) -> Result<()> {
        if matches!(self.input, ScanSource::RtfOriginal) {
            send(ScanPage {
                scanned_bytes: 0,
                read_bytes: 0,
                hits: Vec::new(),
                stop: Some(ScanStop::ExtractionPending),
            })
            .await;
            return Ok(());
        }
        let finder = memmem::Finder::new(&options.literal);
        let needle = finder.needle().len();
        let end = self.total_bytes.min(options.byte_limit);
        let mut window = Vec::with_capacity(READ_BYTES + needle + 3);
        let mut scanned = 0;
        let mut validated = 0;
        let mut next_match = 0;
        let mut hits_left = options.hit_limit;
        loop {
            let start = scanned - window.len() as u64;
            let next = (scanned + READ_BYTES as u64).min(end);
            if next > scanned {
                self.input
                    .append(
                        &mut window,
                        ByteRange {
                            start: scanned,
                            end: next,
                        },
                    )
                    .await?;
                scanned = next;
            }
            let unchecked = (validated - start) as usize;
            let (valid, invalid) = match std::str::from_utf8(&window[unchecked..]) {
                Ok(text) => (unchecked + text.len(), false),
                Err(error) => (
                    unchecked + error.valid_up_to(),
                    error.error_len().is_some() || scanned == self.total_bytes,
                ),
            };
            let nul = memchr::memchr(0, &window[unchecked..valid]).map(|pos| unchecked + pos);
            let valid = nul.unwrap_or(valid);
            validated = start + valid as u64;
            let search_from = next_match.max(start).min(validated);
            let hits: Vec<_> = finder
                .find_iter(&window[(search_from - start) as usize..valid])
                .take(hits_left)
                .map(|pos| {
                    let start = search_from + pos as u64;
                    ByteRange {
                        start,
                        end: start + needle as u64,
                    }
                })
                .collect();
            hits_left -= hits.len();
            if let Some(hit) = hits.last() {
                next_match = hit.end;
            }
            let stop = if hits_left == 0 && next_match < self.total_bytes {
                Some(ScanStop::HitLimit)
            } else if invalid || nul.is_some() {
                Some(ScanStop::InvalidText)
            } else if scanned == self.total_bytes {
                Some(ScanStop::Complete)
            } else if scanned == end {
                Some(ScanStop::ByteLimit)
            } else {
                None
            };
            if !send(ScanPage {
                scanned_bytes: if hits_left == 0 {
                    next_match
                } else {
                    validated
                },
                read_bytes: scanned,
                hits,
                stop,
            })
            .await
                || stop.is_some()
            {
                return Ok(());
            }
            // Retain enough validated bytes for a split match and any incomplete code point.
            window.drain(..valid.saturating_sub(needle - 1));
        }
    }
}

impl ScanSource {
    async fn append(&mut self, window: &mut Vec<u8>, range: ByteRange) -> Result<()> {
        let length = range.length()? as usize;
        match self {
            Self::RtfOriginal => unreachable!("RTF original bytes are not canonical text"),
            Self::Local(file) => {
                let previous = window.len();
                window.resize(previous + length, 0);
                file.read_exact(&mut window[previous..]).await?;
            }
            Self::Hosted { storage, key } => {
                let bytes = storage.read_range(key, range).await?;
                if bytes.len() != length {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "literal scan range length mismatch",
                    )
                    .into());
                }
                window.extend_from_slice(&bytes);
            }
        }
        Ok(())
    }
}
