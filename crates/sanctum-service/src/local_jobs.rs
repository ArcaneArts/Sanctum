use std::{
    fmt,
    io::{self, BufReader, BufWriter, Read, Write},
    time::Instant,
};

use sanctum_chunk::index_utf8_with_settings;
use sanctum_core::{Error, ExternalSource, Result, Scope, is_rtf};
use sanctum_db_sqlite::queue::{RtfPreparation, StagedCleanup};
use sanctum_extract::{RTF_EXTRACTOR, rtf_to_utf8};
use uuid::Uuid;

use crate::{ChunkRef, Job, JobState, Revision, RevisionContent, Service, StagedUpload, telemetry};

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
        let _reader = self
            .retrieval
            .read()
            .map_err(|_| Error::Busy("retrieval lock is unavailable".into()))?;
        let job = self.queue.cancel(scope, id)?;
        if job.state == JobState::Canceled {
            self.reclaim_terminal_input(&job)?;
        }
        Ok(job)
    }

    fn reclaim_terminal_input(&self, job: &Job) -> Result<()> {
        self.reclaim_staged_input(&StagedCleanup {
            operation: job.id,
            scope: job.scope.clone(),
            source: job.source,
            revision: job.revision,
        })
    }

    pub(crate) fn reclaim_staged_input(&self, pending: &StagedCleanup) -> Result<()> {
        let result = self
            .files
            .discard_text(&pending.scope, pending.source, pending.revision);
        if let Err(error) = &result {
            tracing::warn!(
                %error,
                tenant = %pending.scope.tenant,
                collection = %pending.scope.collection,
                operation = %pending.operation,
                "could not reclaim unpublished local input"
            );
        }
        self.queue.record_cleanup(pending.operation, result.is_ok())
    }

    /// Runs at most one job. The database claim is atomic across local workers.
    pub fn process_next(&self) -> Result<bool> {
        self.process_next_until_shutdown(|| false)
    }

    pub(crate) fn process_next_until_shutdown(&self, stopping: impl Fn() -> bool) -> Result<bool> {
        let _reader = self
            .retrieval
            .read()
            .map_err(|_| Error::Busy("retrieval lock is unavailable".into()))?;
        if stopping() {
            return Ok(false);
        }
        let Some(job) = self.queue.claim()? else {
            return Ok(false);
        };
        let start = Instant::now();
        tracing::debug!(
            event = "local_job_claimed",
            tenant = %job.scope.tenant,
            collection = %job.scope.collection,
            operation = %job.id,
            attempt = job.attempts,
            queue_age_seconds = telemetry::queue_age(job.created_at),
            "claimed local ingestion work"
        );
        let result = self.process_claim(&job, &stopping);
        let outcome = match &result {
            Ok(outcome) => *outcome,
            Err(_) => "error",
        };
        telemetry::ingestion_stage(
            &job.scope,
            job.id,
            "local_index",
            job.created_at,
            job.bytes,
            start.elapsed(),
            outcome,
        );
        result.map(|_| true)
    }

    fn process_claim(&self, job: &Job, stopping: &impl Fn() -> bool) -> Result<&'static str> {
        match self.index_job(job, stopping) {
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
                Ok("text_ready")
            }
            // Startup recovers interrupted claims from the queue and collection receipts.
            Err(error) if IngestionShutdown::caused(&error) => Ok("interrupted"),
            Err(error) => {
                let (code, message) = match &error {
                    Error::Invalid(message) => ("invalid_source", message.as_str()),
                    Error::Conflict(message) => ("conflict", message.as_str()),
                    _ => ("retryable", "indexing failed; see server logs"),
                };
                self.queue.fail(job.id, code, message)?;
                let state = self.queue.get(&job.scope, job.id)?.state;
                if code != "retryable" || state == JobState::Canceled {
                    self.reclaim_terminal_input(job)?;
                }
                if state == JobState::Canceled {
                    Ok("canceled")
                } else if code == "retryable" {
                    Err(error)
                } else {
                    Ok(code)
                }
            }
        }
    }

    fn index_job(&self, job: &Job, stopping: &impl Fn() -> bool) -> Result<Revision> {
        IngestionShutdown::check(stopping)?;
        let mut database = self.database(&job.scope)?;
        if let Some(content) = database.revision_summary(job.source, job.revision)? {
            // The collection commit survived a stop before the queue receipt.
            return Ok(revision_for(job, content));
        }
        self.require_latest_update(job)?;
        if !self.queue.progress(job.id, job.processed_bytes)? {
            return Err(Error::Conflict("indexing canceled".into()));
        }
        let extracted = job
            .extract_rtf
            .then(|| self.prepare_rtf(job, stopping))
            .transpose()?;
        self.local_writes.run(&job.scope, || {
            self.publish_index(job, extracted, &mut database, stopping)
        })
    }

    fn publish_index(
        &self,
        job: &Job,
        extracted: Option<RtfPreparation>,
        database: &mut crate::db::CollectionDb,
        stopping: &impl Fn() -> bool,
    ) -> Result<Revision> {
        IngestionShutdown::check(stopping)?;
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
        self.require_latest_update(job)?;
        let input = self.files.open_text(&job.scope, job.source, job.revision)?;
        let mut ordinal = 0;
        let mut last_progress = 0;
        let indexed = index_utf8_with_settings(input, job.chunking, |range, text| {
            IngestionShutdown::check(stopping)?;
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
        if extracted.as_ref().is_some_and(|input| {
            input.canonical_hash != indexed.hash || input.canonical_bytes != indexed.bytes
        }) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "saved RTF text changed during indexing",
            )
            .into());
        }
        let original_hash = extracted
            .as_ref()
            .map_or(&indexed.hash, |input| &input.original_hash);
        verify_original_digest(job, original_hash)?;
        let content = RevisionContent {
            original_hash: original_hash.clone(),
            hash: indexed.hash,
            bytes: indexed.bytes,
            chunks: indexed.chunks,
            chunker: job.chunking.identifier(),
            encoding: extracted.map_or_else(|| "UTF-8".into(), |input| input.encoding),
            extractor: job.extract_rtf.then(|| RTF_EXTRACTOR.into()),
            retain_original: job.retain_original,
            url: job.url.clone(),
            git: job.git.clone(),
        };
        let revision = revision_for(job, content);
        crate::db::metadata::validate_metadata_ordinals(
            &job.initial_chunk_metadata,
            revision.content.chunks,
        )?;
        let profile = self.embedding_profile();
        writer.register(&revision, job.replacement, profile)?;
        writer.set_initial_metadata(
            &revision,
            &job.initial_metadata,
            &job.initial_chunk_metadata,
        )?;
        IngestionShutdown::check(stopping)?;
        if !self.queue.begin_publish(job.id)? {
            return Err(Error::Conflict("indexing canceled".into()));
        }
        let reused = profile
            .filter(|_| tracing::enabled!(tracing::Level::DEBUG))
            .and_then(|profile| {
                writer
                    .cached_revision_chunks(job.source, job.revision, profile)
                    .ok()
            });
        writer.commit()?;
        if let Some(reused) = reused {
            telemetry::vector_reuse(&job.scope, job.id, revision.content.chunks, reused);
        }
        Ok(revision)
    }

    fn require_latest_update(&self, job: &Job) -> Result<()> {
        if !self.queue.is_latest_replacement(job)? {
            return Err(Error::Conflict(
                "a newer operation superseded this import or replacement".into(),
            ));
        }
        Ok(())
    }

    fn prepare_rtf(&self, job: &Job, stopping: &impl Fn() -> bool) -> Result<RtfPreparation> {
        if let Some(prepared) = &job.rtf_preparation {
            if prepared.extractor == RTF_EXTRACTOR
                && self.matches_saved_text(job, prepared, stopping)?
            {
                if job.retain_original {
                    let original =
                        self.files
                            .open_original(&job.scope, job.source, job.revision)?;
                    let mut hash = blake3::Hasher::new();
                    hash.update_reader(IngestionReader {
                        inner: original.take(job.bytes.saturating_add(1)),
                        stopping,
                    })?;
                    verify_original_digest(job, hash.finalize().to_hex().as_str())?;
                }
                if job.extracting && !self.queue.extracted(job.id, prepared)? {
                    return Err(Error::Conflict("extraction canceled".into()));
                }
                return Ok(prepared.clone());
            }
            if !self.queue.extract_again(job.id)? {
                return Err(Error::Conflict("extraction canceled".into()));
            }
        }
        self.extract_rtf(job, stopping)
    }

    fn matches_saved_text(
        &self,
        job: &Job,
        prepared: &RtfPreparation,
        stopping: &impl Fn() -> bool,
    ) -> Result<bool> {
        let file = match self.files.open_text(&job.scope, job.source, job.revision) {
            Ok(file) => file,
            Err(Error::Io(error)) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        };
        let mut hash = blake3::Hasher::new();
        hash.update_reader(IngestionReader {
            inner: file.take(prepared.canonical_bytes.saturating_add(1)),
            stopping,
        })?;
        Ok(hash.finalize().to_hex().as_str() == prepared.canonical_hash)
    }

    fn extract_rtf(&self, job: &Job, stopping: &impl Fn() -> bool) -> Result<RtfPreparation> {
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
        let mut output = HashingWriter {
            inner: text.as_file(),
            hash: blake3::Hasher::new(),
        };
        let input = BufReader::new(IngestionReader {
            inner: &mut reader,
            stopping,
        });
        let encoding = match rtf_to_utf8(input, BufWriter::new(&mut output)) {
            Err(Error::Io(ref error)) if error.kind() == io::ErrorKind::ConnectionAborted => {
                return Err(Error::Conflict("extraction canceled".into()));
            }
            result => result?,
        };
        let original_hash = reader.hash.finalize().to_hex().to_string();
        if reader.bytes != job.bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "queued RTF no longer matches its accepted size",
            )
            .into());
        }
        verify_original_digest(job, &original_hash)?;
        let preparation = RtfPreparation {
            original_hash,
            canonical_hash: output.hash.finalize().to_hex().to_string(),
            canonical_bytes: text.as_file().metadata()?.len(),
            encoding: encoding.into(),
            extractor: RTF_EXTRACTOR.into(),
        };
        self.files
            .publish_extracted(&job.scope, job.source, job.revision, text)?;
        if !self.queue.extracted(job.id, &preparation)? {
            return Err(Error::Conflict("extraction canceled".into()));
        }
        Ok(preparation)
    }
}

fn verify_original_digest(job: &Job, digest: &str) -> Result<()> {
    let expected = job
        .upload_hash
        .as_deref()
        .or(job.import_hash.as_deref())
        .or(job
            .rtf_preparation
            .as_ref()
            .map(|prepared| prepared.original_hash.as_str()));
    if expected.is_some_and(|expected| expected != digest) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "staged source no longer matches its accepted digest",
        )
        .into());
    }
    Ok(())
}

#[derive(Debug)]
struct IngestionShutdown;

impl fmt::Display for IngestionShutdown {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("local ingestion interrupted by server shutdown")
    }
}

impl std::error::Error for IngestionShutdown {}

impl IngestionShutdown {
    fn check(stopping: &impl Fn() -> bool) -> io::Result<()> {
        if stopping() {
            return Err(io::Error::other(Self));
        }
        Ok(())
    }

    fn caused(error: &Error) -> bool {
        matches!(error, Error::Io(error)
            if error.get_ref().is_some_and(|cause| cause.is::<Self>()))
    }
}

struct IngestionReader<'a, R, S> {
    inner: R,
    stopping: &'a S,
}

impl<R: Read, S: Fn() -> bool> Read for IngestionReader<'_, R, S> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        IngestionShutdown::check(self.stopping)?;
        self.inner.read(buffer)
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
        self.enqueue_with_request(None)
    }

    pub fn enqueue_with_request(self, request: Option<Uuid>) -> Result<Job> {
        let bytes = self.file.as_file().metadata()?.len();
        if bytes > self.service.max_source_bytes {
            return Err(Error::Invalid(
                "source exceeds the configured upload limit".into(),
            ));
        }
        if request.is_some_and(|id| id.is_nil()) {
            return Err(Error::Invalid("request ID must be a nonzero UUID".into()));
        }
        let (digest, fingerprint) = self.hash_upload(bytes, request.is_some())?;
        let receipt = request.zip(fingerprint.as_deref());
        let service = self.service.clone();
        let _guard = receipt
            .map(|(request, _)| {
                service.upload_requests
                    [usize::from(request.as_bytes()[15]) % service.upload_requests.len()]
                .lock()
                .map_err(|_| Error::Busy("upload request lock is unavailable".into()))
            })
            .transpose()?;
        let _reader = service
            .retrieval
            .read()
            .map_err(|_| Error::Busy("retrieval lock is unavailable".into()))?;
        if let Some((request, fingerprint)) = receipt {
            service.require_collection(&self.scope)?;
            if let Some(job) = service
                .queue
                .replay_upload(&self.scope, request, fingerprint)?
            {
                return Ok(job);
            }
        }
        if self.upload.replacement.is_none() {
            return self.accept_upload(bytes, digest, receipt);
        }
        let scope = self.scope.clone();
        service
            .local_writes
            .run(&scope, || self.accept_upload(bytes, digest, receipt))
    }

    fn accept_upload(
        self,
        bytes: u64,
        digest: String,
        receipt: Option<(Uuid, &str)>,
    ) -> Result<Job> {
        let replacement = self.upload.replacement.as_ref();
        let (source, external) = if let Some(replacement) = replacement {
            let current = self
                .service
                .database(&self.scope)?
                .find_source(replacement.source, self.upload.external.as_ref())?;
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
        let mut job = new_job(
            self.scope.clone(),
            self.upload.name.clone(),
            external,
            source,
            bytes,
            replacement.is_some(),
        );
        job.upload_hash = Some(digest);
        self.publish_job_locked(job, receipt)
    }

    fn hash_upload(&self, bytes: u64, with_fingerprint: bool) -> Result<(String, Option<String>)> {
        let mut fingerprint = if with_fingerprint {
            // Preserve the request receipt format for uploads accepted by older servers.
            let header = serde_json::to_vec(&self.upload)
                .map_err(|error| Error::Internal(Box::new(error)))?;
            let mut hash = blake3::Hasher::new();
            hash.update(&(header.len() as u64).to_le_bytes());
            hash.update(&header);
            Some(hash)
        } else {
            None
        };
        let mut input = self.file.reopen()?.take(bytes.saturating_add(1));
        let mut content = blake3::Hasher::new();
        let mut buffer = [0; 64 * 1024];
        let mut read_total = 0;
        loop {
            let read = input.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            read_total += read as u64;
            content.update(&buffer[..read]);
            if let Some(hash) = &mut fingerprint {
                hash.update(&buffer[..read]);
            }
        }
        if read_total != bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "staged upload changed size before acceptance",
            )
            .into());
        }
        Ok((
            content.finalize().to_hex().to_string(),
            fingerprint.map(|hash| hash.finalize().to_hex().to_string()),
        ))
    }

    pub(crate) fn publish_job_locked(
        self,
        mut job: Job,
        request: Option<(Uuid, &str)>,
    ) -> Result<Job> {
        self.service.require_collection(&job.scope)?;
        job.retain_original = self.upload.retain_original;
        job.chunking = self.service.chunking;
        job.initial_metadata = self.upload.initial_metadata;
        job.initial_chunk_metadata = self.upload.initial_chunk_metadata;
        if job.extract_rtf {
            self.service
                .files
                .publish_original(&job.scope, job.source, job.revision, self.file)?;
        } else {
            self.service
                .files
                .publish(&job.scope, job.source, job.revision, self.file)?;
        }
        self.service.queue.insert(&mut job, request)?;
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
        upload_hash: None,
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
        chunking: Default::default(),
        initial_metadata: Vec::new(),
        initial_chunk_metadata: Vec::new(),
        rtf_preparation: None,
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

struct HashingWriter<W> {
    inner: W,
    hash: blake3::Hasher,
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(bytes)?;
        self.hash.update(&bytes[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;
    use crate::TextUpload;

    #[test]
    fn shutdown_restarts_claimed_text_and_rtf_without_losing_inputs_or_checkpoints() {
        for (name, keep_checkpoint) in [
            ("note.txt", false),
            ("note.rtf", false),
            ("prepared.rtf", true),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let open = || Service::open(directory.path(), 1024 * 1024).unwrap();
            let service = open();
            let scope = Scope {
                tenant: "default".parse().unwrap(),
                collection: "notes".parse().unwrap(),
            };
            service.create_collection(&scope).unwrap();
            let upload = service
                .stage_upload(
                    scope.clone(),
                    TextUpload {
                        name: name.into(),
                        external: None,
                        replacement: None,
                        retain_original: false,
                        initial_metadata: Vec::new(),
                        initial_chunk_metadata: Vec::new(),
                    },
                )
                .unwrap();
            let is_rtf = is_rtf(name);
            let mut file = upload.reopen().unwrap();
            if is_rtf {
                file.write_all(br"{\rtf1\ansi ").unwrap();
            }
            for _ in 0..4096 {
                file.write_all(b"amethyst cave\n").unwrap();
            }
            if is_rtf {
                file.write_all(b"}").unwrap();
            }
            drop(file);
            let job = upload.enqueue().unwrap();
            let checks = Cell::new(0);
            assert!(
                service
                    .process_next_until_shutdown(|| {
                        checks.set(checks.get() + 1);
                        if !keep_checkpoint {
                            return checks.get() >= 6;
                        }
                        service
                            .job(&scope, job.id)
                            .unwrap()
                            .rtf_preparation
                            .is_some()
                    })
                    .unwrap()
            );
            let interrupted = service.job(&scope, job.id).unwrap();
            assert_eq!(interrupted.state, JobState::Indexing, "{name}");
            assert!(interrupted.failure.is_none());
            assert_eq!(
                interrupted.rtf_preparation.is_some(),
                keep_checkpoint,
                "{name}"
            );
            assert!(
                service
                    .database(&scope)
                    .unwrap()
                    .revision_summary(job.source, job.revision)
                    .unwrap()
                    .is_none()
            );
            drop(service);

            let restarted = open();
            let pending = restarted.job(&scope, job.id).unwrap();
            assert_eq!(pending.state, JobState::Accepted);
            assert_eq!(pending.revision, job.revision);
            assert_eq!(
                pending.rtf_preparation.map(|saved| saved.canonical_hash),
                interrupted
                    .rtf_preparation
                    .map(|saved| saved.canonical_hash),
            );
            assert!(restarted.process_next().unwrap());
            let ready = restarted.job(&scope, job.id).unwrap();
            assert_eq!(ready.state, JobState::TextReady);
            assert_eq!(ready.attempts, 2);
            let hit = restarted
                .fetch(
                    &scope,
                    &ChunkRef {
                        source: job.source,
                        revision: job.revision,
                        ordinal: 0,
                    },
                )
                .unwrap();
            assert!(hit.excerpt.starts_with("amethyst cave"));
        }
    }
}
