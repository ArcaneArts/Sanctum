use std::{
    path::{Path, PathBuf},
    str::FromStr,
};

use rusqlite::{Connection, OptionalExtension, Row, TransactionBehavior, params};
use sanctum_core::{
    ChunkSettings, Error, ExternalSource, GitProvenance, Result, Scope, ScopeId, UrlProvenance,
};
use uuid::Uuid;

use super::{
    Revision, database_error, migrate, open, read_json, sql_integer, unsigned_column, write_json,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobState {
    Accepted,
    Indexing,
    Publishing,
    TextReady,
    Failed,
    Canceled,
}

impl JobState {
    fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "accepted" => Self::Accepted,
            "indexing" => Self::Indexing,
            "publishing" => Self::Publishing,
            "text_ready" => Self::TextReady,
            "failed" => Self::Failed,
            "canceled" => Self::Canceled,
            _ => return None,
        })
    }
}

#[derive(Clone, Debug)]
pub struct JobFailure {
    pub code: String,
    pub message: String,
}

#[derive(Clone, Debug)]
pub struct Job {
    pub id: Uuid,
    pub scope: Scope,
    pub source: Uuid,
    pub revision: Uuid,
    pub name: String,
    pub external: Option<ExternalSource>,
    pub bytes: u64,
    pub processed_bytes: u64,
    pub attempts: u32,
    pub state: JobState,
    pub failure: Option<JobFailure>,
    pub hash: Option<String>,
    pub chunks: Option<u64>,
    pub replacement: bool,
    pub update_sequence: u64,
    pub import_hash: Option<String>,
    pub extract_rtf: bool,
    pub retain_original: bool,
    pub extracting: bool,
    pub canonical_bytes: Option<u64>,
    pub original_hash: Option<String>,
    pub source_encoding: Option<String>,
    pub extractor: Option<String>,
    pub created_at: u64,
    pub url: Option<UrlProvenance>,
    pub git: Option<GitProvenance>,
    pub chunking: ChunkSettings,
    pub initial_metadata: Vec<crate::metadata::Mutation>,
    pub initial_chunk_metadata: Vec<crate::metadata::ChunkMetadata>,
}

#[derive(Clone, Debug)]
pub struct LocalQueue(PathBuf);

const COLUMNS: &str = "operation_id, tenant, collection, source_id, revision_id,
    name, external_namespace, external_id, total_bytes, processed_bytes,
    attempts, status, error_code, error_message, content_hash, chunks,
    replacement, update_sequence, import_hash, extract_rtf, retain_original, extracting,
    canonical_bytes, original_hash, source_encoding, extractor, created_at, url_provenance, git_provenance, chunk_window_bytes, chunk_overlap_bytes, initial_metadata, initial_chunk_metadata, chunk_max_tokens";

impl LocalQueue {
    /// The caller holds the storage owner's exclusive lock for this queue.
    pub fn open(path: PathBuf) -> Result<Self> {
        let mut connection = open(&path, true)?;
        migrate(
            &mut connection,
            &[
                include_str!("runtime.sql"),
                include_str!("runtime_v2.sql"),
                include_str!("runtime_v3.sql"),
                include_str!("runtime_v4.sql"),
                include_str!("runtime_v5.sql"),
                include_str!("runtime_v6.sql"),
                include_str!("runtime_v7.sql"),
                include_str!("runtime_v8.sql"),
                include_str!("runtime_v9.sql"),
                include_str!("runtime_v10.sql"),
                include_str!("runtime_v11.sql"),
                include_str!("runtime_v12.sql"),
                include_str!("runtime_v13.sql"),
                include_str!("runtime_v14.sql"),
            ],
        )?;
        connection
            .execute(
                "UPDATE ingestion_jobs
             SET status = 'accepted', processed_bytes = 0
             WHERE status IN ('indexing', 'publishing')",
                [],
            )
            .map_err(database_error)?;
        connection
            .execute(
                "UPDATE watched_folders SET next_scan_at = 0 WHERE next_scan_at <> 0",
                [],
            )
            .map_err(database_error)?;
        Ok(Self(path))
    }

    /// Removes a bounded page of terminal jobs after their operational retention.
    pub fn prune_finished(&self) -> Result<usize> {
        let mut connection = self.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error)?;
        let expired = {
            let mut statement = transaction
                .prepare(
                    "DELETE FROM ingestion_jobs WHERE operation_id IN (
                    SELECT operation_id FROM ingestion_jobs
                    WHERE finished_at <= unixepoch() - ?1 AND (
                        status IN ('text_ready', 'canceled')
                        OR (status = 'failed' AND error_code IS NOT 'retryable')
                    )
                    ORDER BY finished_at, operation_id LIMIT 256
                ) RETURNING operation_id",
                )
                .map_err(database_error)?;
            statement
                .query_map([45 * 24 * 3600], |row| row.get::<_, String>(0))
                .map_err(database_error)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(database_error)?
        };
        for operation in &expired {
            transaction
                .execute(
                    "UPDATE local_upload_requests SET operation_id = NULL, fingerprint = NULL
                 WHERE operation_id = ?1",
                    [operation],
                )
                .map_err(database_error)?;
        }
        transaction.commit().map_err(database_error)?;
        Ok(expired.len())
    }

    pub fn purge_collection(&self, scope: &Scope) -> Result<()> {
        let mut connection = self.connect()?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error)?;
        for table in [
            "ingestion_jobs",
            "replacement_sequences",
            "watched_folders",
            "local_upload_requests",
        ] {
            tx.execute(
                &format!("DELETE FROM {table} WHERE tenant = ?1 AND collection = ?2"),
                params![scope.tenant.as_str(), scope.collection.as_str()],
            )
            .map_err(database_error)?;
        }
        tx.commit().map_err(database_error)
    }

    pub fn replay_upload(
        &self,
        scope: &Scope,
        request: Uuid,
        fingerprint: &str,
    ) -> Result<Option<Job>> {
        let existing = self
            .connect()?
            .query_row(
                "SELECT operation_id, fingerprint FROM local_upload_requests
             WHERE tenant = ?1 AND collection = ?2 AND request_id = ?3",
                params![
                    scope.tenant.as_str(),
                    scope.collection.as_str(),
                    request.to_string()
                ],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, Option<String>>(1)?,
                    ))
                },
            )
            .optional()
            .map_err(database_error)?;
        let Some((operation, digest)) = existing else {
            return Ok(None);
        };
        let operation = operation.ok_or_else(|| {
            Error::Conflict("upload request ID has expired; use a new request ID".into())
        })?;
        if digest.as_deref() != Some(fingerprint) {
            return Err(Error::Conflict(
                "request ID already applies to a different upload".into(),
            ));
        }
        let operation = operation
            .parse()
            .map_err(|error| Error::Internal(Box::new(error)))?;
        match self.get(scope, operation) {
            Ok(job) => Ok(Some(job)),
            Err(Error::NotFound(_)) => Err(Error::Conflict(
                "upload request ID has expired; use a new request ID".into(),
            )),
            Err(error) => Err(error),
        }
    }

    pub fn insert(&self, job: &mut Job, request: Option<(Uuid, &str)>) -> Result<()> {
        let namespace = job.external.as_ref().map(|external| &external.namespace);
        let external_id = job.external.as_ref().map(|external| &external.id);
        let mut connection = self.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error)?;
        if job.replacement || job.import_hash.is_some() {
            job.update_sequence = transaction
                .query_row(
                    "INSERT INTO replacement_sequences (tenant, collection, source_id, sequence)
                 VALUES (?1, ?2, ?3, 1)
                 ON CONFLICT (tenant, collection, source_id)
                 DO UPDATE SET sequence = sequence + 1 RETURNING sequence",
                    params![
                        job.scope.tenant.as_str(),
                        job.scope.collection.as_str(),
                        job.source.to_string()
                    ],
                    |row| unsigned_column(row, 0),
                )
                .map_err(database_error)?;
        }
        job.created_at = transaction
            .query_row(
                "INSERT INTO ingestion_jobs (operation_id, tenant, collection, source_id,
                revision_id, name, external_namespace, external_id, total_bytes,
                replacement, update_sequence, import_hash, extract_rtf, retain_original, url_provenance, git_provenance, chunk_window_bytes, chunk_overlap_bytes, initial_metadata, initial_chunk_metadata, chunk_max_tokens)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21)
             RETURNING created_at",
                params![
                    job.id.to_string(),
                    job.scope.tenant.as_str(),
                    job.scope.collection.as_str(),
                    job.source.to_string(),
                    job.revision.to_string(),
                    job.name,
                    namespace,
                    external_id,
                    sql_integer(job.bytes)?,
                    job.replacement,
                    sql_integer(job.update_sequence)?,
                    job.import_hash,
                    job.extract_rtf,
                    job.retain_original,
                    write_json(job.url.as_ref())?,
                    write_json(job.git.as_ref())?,
                    job.chunking.window_bytes,
                    job.chunking.overlap_bytes,
                    write_json((!job.initial_metadata.is_empty()).then_some(&job.initial_metadata))?,
                    write_json((!job.initial_chunk_metadata.is_empty()).then_some(&job.initial_chunk_metadata))?,
                    job.chunking.max_tokens,
                ],
                |row| unsigned_column(row, 0),
            )
            .map_err(database_error)?;
        if let Some((request, fingerprint)) = request {
            transaction.execute(
                "INSERT INTO local_upload_requests (tenant, collection, request_id, operation_id, fingerprint)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![job.scope.tenant.as_str(), job.scope.collection.as_str(), request.to_string(), job.id.to_string(), fingerprint],
            ).map_err(database_error)?;
        }
        transaction.commit().map_err(database_error)
    }

    pub fn is_latest_replacement(&self, job: &Job) -> Result<bool> {
        if job.update_sequence == 0 {
            return Ok(true);
        }
        self.connect()?
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM replacement_sequences
             WHERE tenant = ?1 AND collection = ?2 AND source_id = ?3 AND sequence = ?4)",
                params![
                    job.scope.tenant.as_str(),
                    job.scope.collection.as_str(),
                    job.source.to_string(),
                    sql_integer(job.update_sequence)?
                ],
                |row| row.get(0),
            )
            .map_err(database_error)
    }

    pub fn activity(
        &self,
        tenant: &ScopeId,
        collection: Option<&ScopeId>,
        before: Option<Uuid>,
        limit: u32,
    ) -> Result<Vec<Job>> {
        let connection = self.connect()?;
        let collection = collection.map(ScopeId::as_str);
        let before = before.map(|value| value.to_string());
        let (timestamp, operation) = if let Some(operation) = before {
            let timestamp = connection
                .query_row(
                    "SELECT created_at FROM ingestion_jobs WHERE operation_id = ?1
                   AND tenant = ?2 AND (?3 IS NULL OR collection = ?3)",
                    params![operation, tenant.as_str(), collection],
                    |row| row.get::<_, i64>(0),
                )
                .optional()
                .map_err(database_error)?
                .ok_or_else(|| Error::Invalid("activity cursor is outside this scope".into()))?;
            (timestamp, operation)
        } else {
            (i64::MAX, String::new())
        };
        let condition = if collection.is_some() {
            " AND collection = ?5"
        } else {
            ""
        };
        let sql = format!(
            "SELECT {COLUMNS} FROM ingestion_jobs WHERE tenant = ?1
             AND (created_at, operation_id) < (?2, ?3){condition}
             ORDER BY created_at DESC, operation_id DESC LIMIT ?4"
        );
        let mut arguments: Vec<rusqlite::types::Value> = vec![
            tenant.as_str().to_owned().into(),
            timestamp.into(),
            operation.into(),
            i64::from(limit).into(),
        ];
        if let Some(collection) = collection {
            arguments.push(collection.to_owned().into());
        }
        super::query_all(
            &connection,
            &sql,
            rusqlite::params_from_iter(arguments),
            map_job,
        )
    }

    pub fn retains_input(&self, scope: &Scope, revision: super::RevisionKey) -> Result<bool> {
        self.connect()?
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM ingestion_jobs
             WHERE tenant = ?1 AND collection = ?2 AND source_id = ?3 AND revision_id = ?4
               AND (status IN ('accepted', 'indexing', 'publishing')
                    OR (status = 'failed' AND error_code = 'retryable')))",
                params![
                    scope.tenant.as_str(),
                    scope.collection.as_str(),
                    revision.source.to_string(),
                    revision.revision.to_string()
                ],
                |row| row.get(0),
            )
            .map_err(database_error)
    }

    pub fn pending_external(
        &self,
        scope: &Scope,
        external: &ExternalSource,
    ) -> Result<Option<Job>> {
        self.connect()?.query_row(
            &format!("SELECT {COLUMNS} FROM ingestion_jobs AS j
             WHERE j.tenant = ?1 AND j.collection = ?2
               AND j.external_namespace = ?3 AND j.external_id = ?4
               AND j.cancel_requested = 0
               AND (j.status IN ('accepted', 'indexing', 'publishing')
                    OR (j.status = 'failed' AND j.error_code = 'retryable'))
               AND (j.update_sequence = 0 OR j.update_sequence = (
                    SELECT sequence FROM replacement_sequences
                    WHERE tenant = j.tenant AND collection = j.collection AND source_id = j.source_id
               ))
             ORDER BY j.update_sequence DESC, j.created_at DESC, j.operation_id DESC LIMIT 1"),
            params![scope.tenant.as_str(), scope.collection.as_str(), external.namespace, external.id],
            map_job,
        ).optional().map_err(database_error)
    }

    pub fn pending_import_paths(
        &self,
        scope: &Scope,
        namespace: &str,
        after: &str,
    ) -> Result<Vec<String>> {
        super::query_all(
            &self.connect()?,
            "SELECT DISTINCT external_id FROM ingestion_jobs
             WHERE tenant = ?1 AND collection = ?2 AND external_namespace = ?3 AND external_id > ?4
               AND import_hash IS NOT NULL AND cancel_requested = 0
               AND (status IN ('accepted', 'indexing', 'publishing')
                    OR (status = 'failed' AND error_code = 'retryable'))
             ORDER BY external_id LIMIT 128",
            params![
                scope.tenant.as_str(),
                scope.collection.as_str(),
                namespace,
                after
            ],
            |row| row.get(0),
        )
    }

    pub fn supersede_import(&self, scope: &Scope, job: &Job) -> Result<bool> {
        let changed = self
            .connect()?
            .execute(
                "UPDATE replacement_sequences SET sequence = sequence + 1
             WHERE tenant = ?1 AND collection = ?2 AND source_id = ?3 AND sequence = ?4",
                params![
                    scope.tenant.as_str(),
                    scope.collection.as_str(),
                    job.source.to_string(),
                    sql_integer(job.update_sequence)?
                ],
            )
            .map_err(database_error)?;
        Ok(changed == 1)
    }

    pub fn get(&self, scope: &Scope, id: Uuid) -> Result<Job> {
        self.connect()?
            .query_row(
                &format!(
                    "SELECT {COLUMNS} FROM ingestion_jobs
                WHERE tenant = ?1 AND collection = ?2 AND operation_id = ?3"
                ),
                params![
                    scope.tenant.as_str(),
                    scope.collection.as_str(),
                    id.to_string()
                ],
                map_job,
            )
            .optional()
            .map_err(database_error)?
            .ok_or_else(|| Error::NotFound("ingestion operation was not found".into()))
    }

    pub fn claim(&self) -> Result<Option<Job>> {
        self.connect()?.query_row(
            &format!("UPDATE ingestion_jobs
                SET status = 'indexing', attempts = attempts + 1, processed_bytes = 0, extracting = extract_rtf
                WHERE operation_id = (
                    SELECT operation_id FROM ingestion_jobs
                    WHERE status = 'accepted' AND (SELECT paused = 0 FROM executor_settings WHERE id = 1)
                    ORDER BY created_at, operation_id LIMIT 1
                ) RETURNING {COLUMNS}"),
            [],
            map_job,
        ).optional().map_err(database_error)
    }

    pub fn progress(&self, id: Uuid, processed: u64) -> Result<bool> {
        let changed = self
            .connect()?
            .execute(
                "UPDATE ingestion_jobs SET processed_bytes = ?2
             WHERE operation_id = ?1 AND status = 'indexing' AND cancel_requested = 0",
                params![id.to_string(), sql_integer(processed)?],
            )
            .map_err(database_error)?;
        Ok(changed == 1)
    }

    pub fn extracted(&self, id: Uuid) -> Result<bool> {
        let updated = self
            .connect()?
            .execute(
                "UPDATE ingestion_jobs SET extracting = 0, processed_bytes = total_bytes
             WHERE operation_id = ?1 AND status = 'indexing' AND cancel_requested = 0",
                [id.to_string()],
            )
            .map_err(database_error)?;
        Ok(updated == 1)
    }

    /// This transition is the point after which cancellation cannot win.
    pub fn begin_publish(&self, id: Uuid) -> Result<bool> {
        let changed = self
            .connect()?
            .execute(
                "UPDATE ingestion_jobs SET status = 'publishing', publication_started = 1,
                 processed_bytes = total_bytes
             WHERE operation_id = ?1 AND status = 'indexing' AND cancel_requested = 0",
                [id.to_string()],
            )
            .map_err(database_error)?;
        Ok(changed == 1)
    }

    pub fn ready(&self, id: Uuid, revision: &Revision) -> Result<()> {
        self.connect()?
            .execute(
                "UPDATE ingestion_jobs
             SET status = 'text_ready', content_hash = ?2, chunks = ?3, extracting = 0,
                 canonical_bytes = ?4, original_hash = ?5, source_encoding = ?6, extractor = ?7,
                 processed_bytes = total_bytes, error_code = NULL, error_message = NULL,
                 finished_at = unixepoch(), initial_metadata = NULL, initial_chunk_metadata = NULL
             WHERE operation_id = ?1 AND status IN ('indexing', 'publishing')",
                params![
                    id.to_string(),
                    revision.content.hash,
                    sql_integer(revision.content.chunks)?,
                    sql_integer(revision.content.bytes)?,
                    revision.content.original_hash,
                    revision.content.encoding,
                    revision.content.extractor
                ],
            )
            .map_err(database_error)?;
        Ok(())
    }

    pub fn fail(&self, id: Uuid, code: &str, message: &str) -> Result<()> {
        self.connect()?
            .execute(
                "UPDATE ingestion_jobs SET
                 status = CASE WHEN cancel_requested = 1 THEN 'canceled' ELSE 'failed' END,
                 error_code = CASE WHEN cancel_requested = 0 THEN ?2 END,
                 error_message = CASE WHEN cancel_requested = 0 THEN ?3 END,
                 initial_metadata = CASE WHEN cancel_requested = 0 AND ?2 = 'retryable'
                                         THEN initial_metadata END,
                 initial_chunk_metadata = CASE WHEN cancel_requested = 0 AND ?2 = 'retryable'
                                               THEN initial_chunk_metadata END,
                 finished_at = unixepoch()
             WHERE operation_id = ?1 AND status IN ('indexing', 'publishing')",
                params![id.to_string(), code, message],
            )
            .map_err(database_error)?;
        Ok(())
    }

    pub fn cancel(&self, scope: &Scope, id: Uuid) -> Result<Job> {
        self.update_scoped(
            scope,
            id,
            "SET cancel_requested = 1,
                 status = CASE WHEN status = 'accepted' THEN 'canceled' ELSE status END,
                 finished_at = CASE WHEN status = 'accepted' THEN unixepoch() END,
                 initial_metadata = CASE WHEN status = 'indexing' THEN initial_metadata END,
                 initial_chunk_metadata = CASE WHEN status = 'indexing' THEN initial_chunk_metadata END",
            "status IN ('accepted', 'indexing') AND publication_started = 0",
            "only queued or indexing operations can be canceled",
        )
    }

    pub fn retry(&self, scope: &Scope, id: Uuid) -> Result<Job> {
        self.update_scoped(
            scope,
            id,
            "SET status = 'accepted', error_code = NULL, error_message = NULL,
                 processed_bytes = 0, finished_at = NULL",
            "status = 'failed' AND error_code = 'retryable'",
            "only retryable failures can be retried",
        )
    }

    pub fn set_paused(&self, paused: bool) -> Result<()> {
        self.connect()?
            .execute(
                "UPDATE executor_settings SET paused = ?1 WHERE id = 1",
                [paused],
            )
            .map_err(database_error)?;
        Ok(())
    }

    pub fn paused(&self) -> Result<bool> {
        self.connect()?
            .query_row(
                "SELECT paused FROM executor_settings WHERE id = 1",
                [],
                |row| row.get(0),
            )
            .map_err(database_error)
    }

    pub(super) fn connect(&self) -> Result<Connection> {
        open(Path::new(&self.0), false)
    }

    fn update_scoped(
        &self,
        scope: &Scope,
        id: Uuid,
        set: &str,
        condition: &str,
        conflict: &str,
    ) -> Result<Job> {
        let job = self
            .connect()?
            .query_row(
                &format!(
                    "UPDATE ingestion_jobs {set}
                WHERE tenant = ?1 AND collection = ?2 AND operation_id = ?3 AND {condition}
                RETURNING {COLUMNS}"
                ),
                params![
                    scope.tenant.as_str(),
                    scope.collection.as_str(),
                    id.to_string()
                ],
                map_job,
            )
            .optional()
            .map_err(database_error)?;
        if let Some(job) = job {
            return Ok(job);
        }
        self.get(scope, id)?;
        Err(Error::Conflict(conflict.into()))
    }
}

fn map_job(row: &Row<'_>) -> rusqlite::Result<Job> {
    let state: String = row.get(11)?;
    let state = JobState::parse(&state).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            11,
            rusqlite::types::Type::Text,
            Box::new(Error::Invalid("unrecognized ingestion job state".into())),
        )
    })?;
    let external = match (row.get(6)?, row.get(7)?) {
        (Some(namespace), Some(id)) => Some(ExternalSource { namespace, id }),
        _ => None,
    };
    let failure = match (row.get(12)?, row.get(13)?) {
        (Some(code), Some(message)) => Some(JobFailure { code, message }),
        _ => None,
    };
    Ok(Job {
        id: uuid(row, 0)?,
        scope: Scope {
            tenant: scope_id(row, 1)?,
            collection: scope_id(row, 2)?,
        },
        source: uuid(row, 3)?,
        revision: uuid(row, 4)?,
        name: row.get(5)?,
        external,
        bytes: unsigned_column(row, 8)?,
        processed_bytes: unsigned_column(row, 9)?,
        attempts: row.get(10)?,
        state,
        failure,
        hash: row.get(14)?,
        chunks: row
            .get::<_, Option<i64>>(15)?
            .map(|value| {
                u64::try_from(value)
                    .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(15, value))
            })
            .transpose()?,
        replacement: row.get(16)?,
        update_sequence: unsigned_column(row, 17)?,
        import_hash: row.get(18)?,
        extract_rtf: row.get(19)?,
        retain_original: row.get(20)?,
        extracting: row.get(21)?,
        canonical_bytes: row.get::<_, Option<i64>>(22)?.map(|n| n as u64),
        original_hash: row.get(23)?,
        source_encoding: row.get(24)?,
        extractor: row.get(25)?,
        created_at: unsigned_column(row, 26)?,
        url: read_json(row, 27)?,
        git: read_json(row, 28)?,
        chunking: ChunkSettings {
            window_bytes: row.get(29)?,
            overlap_bytes: row.get(30)?,
            max_tokens: row.get(33)?,
        },
        initial_metadata: read_json(row, 31)?.unwrap_or_default(),
        initial_chunk_metadata: read_json(row, 32)?.unwrap_or_default(),
    })
}

pub(super) fn uuid(row: &Row<'_>, column: usize) -> rusqlite::Result<Uuid> {
    let value: String = row.get(column)?;
    Uuid::parse_str(&value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })
}

pub(super) fn scope_id(row: &Row<'_>, column: usize) -> rusqlite::Result<ScopeId> {
    let value: String = row.get(column)?;
    ScopeId::from_str(&value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })
}
