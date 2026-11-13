use std::io::{BufReader, BufWriter, Read};

use sanctum_chunk::index_utf8;
use sanctum_core::{Error, ExternalSource, Result, Scope, is_rtf};
use sanctum_extract::{RTF_EXTRACTOR, rtf_to_utf8};
use uuid::Uuid;

use crate::{ChunkRef, Job, JobState, Revision, RevisionContent, Service, StagedUpload};

const PROGRESS_STEP: u64 = 1024 * 1024;

impl Service {
    pub fn job(&self, scope: &Scope, id: Uuid) -> Result<Job> {
        self.queue.get(scope, id)
    }

    pub fn pause_queue(&self, paused: bool) -> Result<()> {
        self.queue.set_paused(paused)
    }

    pub fn queue_paused(&self) -> Result<bool> {
        self.queue.paused()
    }

    pub fn retry_job(&self, scope: &Scope, id: Uuid) -> Result<Job> {
        self.queue.retry(scope, id)
    }

    pub fn cancel_job(&self, scope: &Scope, id: Uuid) -> Result<Job> {
        let job = self.queue.cancel(scope, id)?;
        if job.state == JobState::Canceled {
            self.files.discard_text(scope, job.source, job.revision)?;
        }
        Ok(job)
    }

    /// Runs at most one job. The database claim is atomic across local workers.
    pub fn process_next(&self) -> Result<bool> {
        let Some(job) = self.queue.claim()? else {
            return Ok(false);
        };
        match self.index_job(&job) {
            Ok(revision) => {
                if job.extract_rtf
                    && !job.retain_original
                    && let Err(error) =
                        self.files
                            .discard_original(&job.scope, job.source, job.revision)
                {
                    self.queue
                        .fail(job.id, "retryable", "could not remove the staged original")?;
                    return Err(error);
                }
                self.queue.ready(job.id, &revision)?;
            }
            Err(error) => {
                let (code, message) = match &error {
                    Error::Invalid(message) => ("invalid_source", message.as_str()),
                    Error::Conflict(message) => ("conflict", message.as_str()),
                    _ => ("retryable", "indexing failed; see server logs"),
                };
                self.queue.fail(job.id, code, message)?;
                let state = self.queue.get(&job.scope, job.id)?.state;
                if code != "retryable" || state == JobState::Canceled {
                    self.files
                        .discard_text(&job.scope, job.source, job.revision)?;
                }
                if code == "retryable" && state != JobState::Canceled {
                    return Err(error);
                }
            }
        }
        Ok(true)
    }

    fn index_job(&self, job: &Job) -> Result<Revision> {
        let mut database = self.database(&job.scope)?;
        if let Some(content) = database.revision_summary(job.source, job.revision)? {
            // The collection commit survived a stop before the queue receipt.
            return Ok(revision_for(job, content));
        }
        if !self.queue.progress(job.id, 0)? {
            return Err(Error::Conflict("indexing canceled".into()));
        }
        let extracted = job.extract_rtf.then(|| self.extract_rtf(job)).transpose()?;
        let input = self.files.open_text(&job.scope, job.source, job.revision)?;
        let writer = database.write()?;
        if job.replacement {
            writer
                .find_source(Some(job.source), None)
                .map_err(|error| match error {
                    Error::NotFound(_) => {
                        Error::Conflict("replacement source is no longer active".into())
                    }
                    error => error,
                })?;
        }
        if !self.queue.is_latest_replacement(job)? {
            return Err(Error::Conflict(
                "a newer operation superseded this import or replacement".into(),
            ));
        }
        let mut ordinal = 0;
        let mut last_progress = 0;
        let indexed = index_utf8(input, |range, text| {
            if !job.extract_rtf && range.end - last_progress >= PROGRESS_STEP {
                if !self.queue.progress(job.id, range.end)? {
                    return Err(Error::Conflict("indexing canceled".into()));
                }
                last_progress = range.end;
            }
            let reference = ChunkRef {
                source: job.source,
                revision: job.revision,
                ordinal,
            };
            writer.add_chunk(
                &reference,
                range,
                blake3::hash(text.as_bytes()).to_hex().as_str(),
                text,
            )?;
            ordinal += 1;
            Ok(())
        })?;
        if !job.extract_rtf && indexed.bytes != job.bytes {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "queued input no longer matches its accepted size",
            )));
        }
        let original_hash = extracted.as_ref().map_or(&indexed.hash, |input| &input.0);
        if job
            .import_hash
            .as_ref()
            .is_some_and(|hash| hash != original_hash)
        {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "queued import does not match its accepted digest",
            )));
        }
        let content = RevisionContent {
            original_hash: original_hash.clone(),
            hash: indexed.hash,
            bytes: indexed.bytes,
            chunks: indexed.chunks,
            encoding: extracted.map_or_else(|| "UTF-8".into(), |input| input.1.into()),
            extractor: job.extract_rtf.then(|| RTF_EXTRACTOR.into()),
            retain_original: job.retain_original,
            url: job.url.clone(),
            git: job.git.clone(),
        };
        let revision = revision_for(job, content);
        writer.register(&revision, job.replacement, self.embedding_profile())?;
        if !self.queue.begin_publish(job.id)? {
            return Err(Error::Conflict("indexing canceled".into()));
        }
        writer.commit()?;
        Ok(revision)
    }
    fn extract_rtf(&self, job: &Job) -> Result<(String, &'static str)> {
        let original = self
            .files
            .open_original(&job.scope, job.source, job.revision)?;
        let text = self.files.stage(&job.scope)?;
        let mut reader = HashingReader {
            inner: original,
            hash: blake3::Hasher::new(),
            bytes: 0,
            progress_at: PROGRESS_STEP,
            job,
            service: self,
        };
        let result = rtf_to_utf8(BufReader::new(&mut reader), BufWriter::new(text.as_file()));
        let encoding = match result {
            Err(Error::Io(ref error)) if error.kind() == std::io::ErrorKind::ConnectionAborted => {
                return Err(Error::Conflict("extraction canceled".into()));
            }
            result => result?,
        };
        if reader.bytes != job.bytes {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "queued RTF no longer matches its accepted size",
            )));
        }
        if !self.queue.extracted(job.id)? {
            return Err(Error::Conflict("extraction canceled".into()));
        }
        self.files
            .publish_extracted(&job.scope, job.source, job.revision, text)?;
        Ok((reader.hash.finalize().to_hex().to_string(), encoding))
    }
}

fn revision_for(job: &Job, content: RevisionContent) -> Revision {
    Revision {
        source: job.source,
        revision: job.revision,
        name: job.name.clone(),
        external: job.external.clone(),
        content,
    }
}

impl StagedUpload {
    pub fn enqueue(self) -> Result<Job> {
        let bytes = self.file.as_file().metadata()?.len();
        if bytes > self.service.max_source_bytes {
            return Err(Error::Invalid(
                "source exceeds the configured upload limit".into(),
            ));
        }
        let mut database = self.service.database(&self.scope)?;
        let writer = database.write()?;
        let replacement = self.upload.replacement.as_ref();
        let (source, external) = if let Some(replacement) = replacement {
            let current = writer.find_source(replacement.source, self.upload.external.as_ref())?;
            if replacement
                .expected_revision
                .is_some_and(|expected| expected != current.revision)
            {
                return Err(Error::StaleRevision(
                    "the current source revision has changed".into(),
                ));
            }
            if self
                .upload
                .external
                .as_ref()
                .is_some_and(|external| Some(external) != current.external.as_ref())
            {
                return Err(Error::Conflict(
                    "replacement must preserve the external source reference".into(),
                ));
            }
            (current.source, current.external)
        } else {
            (Uuid::new_v4(), self.upload.external.clone())
        };
        let job = new_job(
            self.scope.clone(),
            self.upload.name.clone(),
            external,
            source,
            bytes,
            replacement.is_some(),
        );
        self.publish_job(job)
    }

    pub(crate) fn publish_job(self, mut job: Job) -> Result<Job> {
        job.retain_original = self.upload.retain_original;
        if job.extract_rtf {
            self.service
                .files
                .publish_original(&job.scope, job.source, job.revision, self.file)?;
        } else {
            self.service
                .files
                .publish(&job.scope, job.source, job.revision, self.file)?;
        }
        self.service.queue.insert(&mut job)?;
        Ok(job)
    }
}

pub(crate) fn new_job(
    scope: Scope,
    name: String,
    external: Option<ExternalSource>,
    source: Uuid,
    bytes: u64,
    replacement: bool,
) -> Job {
    let extract_rtf = is_rtf(&name);
    Job {
        id: Uuid::new_v4(),
        source,
        revision: Uuid::new_v4(),
        scope,
        name,
        external,
        bytes,
        processed_bytes: 0,
        attempts: 0,
        state: JobState::Accepted,
        failure: None,
        hash: None,
        chunks: None,
        replacement,
        update_sequence: 0,
        import_hash: None,
        extract_rtf,
        retain_original: false,
        extracting: false,
        canonical_bytes: None,
        original_hash: None,
        source_encoding: None,
        extractor: None,
        created_at: 0,
        url: None,
        git: None,
    }
}

struct HashingReader<'a> {
    inner: std::fs::File,
    hash: blake3::Hasher,
    bytes: u64,
    progress_at: u64,
    job: &'a Job,
    service: &'a Service,
}

impl Read for HashingReader<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buffer)?;
        self.hash.update(&buffer[..read]);
        self.bytes += read as u64;
        if self.bytes >= self.progress_at {
            self.progress_at = self.bytes.saturating_add(PROGRESS_STEP);
            let active = self
                .service
                .queue
                .progress(self.job.id, self.bytes)
                .map_err(std::io::Error::other)?;
            if !active {
                return Err(std::io::Error::from(std::io::ErrorKind::ConnectionAborted));
            }
        }
        Ok(read)
    }
}
