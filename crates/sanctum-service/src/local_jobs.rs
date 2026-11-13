use sanctum_chunk::index_utf8;
use sanctum_core::{Error, ExternalSource, Result, Scope};
use uuid::Uuid;

use crate::{ChunkRef, Job, JobState, Revision, Service, StagedUpload};

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
            Ok(revision) => self.queue.ready(job.id, &revision)?,
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
        if let Some((hash, chunks)) = database.revision_summary(job.source, job.revision)? {
            // The collection commit survived a stop before the queue receipt.
            return Ok(revision_for(job, hash, chunks));
        }
        if !self.queue.progress(job.id, 0)? {
            return Err(Error::Conflict("indexing canceled".into()));
        }
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
            if range.end - last_progress >= PROGRESS_STEP {
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
        if indexed.bytes != job.bytes {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "queued input no longer matches its accepted size",
            )));
        }
        if job
            .import_hash
            .as_ref()
            .is_some_and(|hash| hash != &indexed.hash)
        {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "queued import does not match its accepted digest",
            )));
        }
        let revision = revision_for(job, indexed.hash, indexed.chunks);
        writer.register(&revision, job.replacement)?;
        if !self.queue.begin_publish(job.id)? {
            return Err(Error::Conflict("indexing canceled".into()));
        }
        writer.commit()?;
        Ok(revision)
    }
}

fn revision_for(job: &Job, hash: String, chunks: u64) -> Revision {
    Revision {
        source: job.source,
        revision: job.revision,
        name: job.name.clone(),
        external: job.external.clone(),
        bytes: job.bytes,
        chunks,
        hash,
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
        self.service
            .files
            .publish(&job.scope, job.source, job.revision, self.file)?;
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
    }
}
