use sanctum_chunk::index_utf8;
use sanctum_core::{Error, Result, Scope};
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
        let revision = revision_for(job, indexed.hash, indexed.chunks);
        writer.register(&revision)?;
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
        let job = Job {
            id: Uuid::new_v4(),
            source: Uuid::new_v4(),
            revision: Uuid::new_v4(),
            scope: self.scope,
            name: self.upload.name,
            external: self.upload.external,
            bytes,
            processed_bytes: 0,
            attempts: 0,
            state: JobState::Accepted,
            failure: None,
            hash: None,
            chunks: None,
        };
        self.service
            .files
            .publish(&job.scope, job.source, job.revision, self.file)?;
        self.service.queue.insert(&job)?;
        Ok(job)
    }
}
