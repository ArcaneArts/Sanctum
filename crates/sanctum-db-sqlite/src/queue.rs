use std::{
    path::{Path, PathBuf},
    str::FromStr,
};

use rusqlite::{Connection, OptionalExtension, Row, TransactionBehavior, params};
use sanctum_core::{Error, ExternalSource, Result, Scope, ScopeId};
use uuid::Uuid;

use super::{Revision, database_error, migrate, open, sql_integer, unsigned_column};

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
}

#[derive(Clone, Debug)]
pub struct LocalQueue(PathBuf);

const COLUMNS: &str = "operation_id, tenant, collection, source_id, revision_id,
    name, external_namespace, external_id, total_bytes, processed_bytes,
    attempts, status, error_code, error_message, content_hash, chunks,
    replacement, update_sequence";

impl LocalQueue {
    /// The caller holds the storage owner's exclusive lock for this queue.
    pub fn open(path: PathBuf) -> Result<Self> {
        let mut connection = open(&path, true)?;
        migrate(
            &mut connection,
            &[include_str!("runtime.sql"), include_str!("runtime_v2.sql")],
        )?;
        connection
            .execute(
                "UPDATE ingestion_jobs
             SET status = 'accepted', processed_bytes = 0
             WHERE status IN ('indexing', 'publishing')",
                [],
            )
            .map_err(database_error)?;
        Ok(Self(path))
    }

    pub fn insert(&self, job: &mut Job) -> Result<()> {
        let namespace = job.external.as_ref().map(|external| &external.namespace);
        let external_id = job.external.as_ref().map(|external| &external.id);
        let mut connection = self.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error)?;
        if job.replacement {
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
        transaction
            .execute(
                "INSERT INTO ingestion_jobs (operation_id, tenant, collection, source_id,
                revision_id, name, external_namespace, external_id, total_bytes,
                replacement, update_sequence)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
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
                    sql_integer(job.update_sequence)?
                ],
            )
            .map_err(database_error)?;
        transaction.commit().map_err(database_error)
    }

    pub fn is_latest_replacement(&self, job: &Job) -> Result<bool> {
        if !job.replacement {
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
                SET status = 'indexing', attempts = attempts + 1, processed_bytes = 0
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
             SET status = 'text_ready', content_hash = ?2, chunks = ?3,
                 processed_bytes = total_bytes, error_code = NULL, error_message = NULL
             WHERE operation_id = ?1 AND status IN ('indexing', 'publishing')",
                params![id.to_string(), revision.hash, sql_integer(revision.chunks)?],
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
                 error_message = CASE WHEN cancel_requested = 0 THEN ?3 END
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
                 status = CASE WHEN status = 'accepted' THEN 'canceled' ELSE status END",
            "status IN ('accepted', 'indexing') AND publication_started = 0",
            "only queued or indexing operations can be canceled",
        )
    }

    pub fn retry(&self, scope: &Scope, id: Uuid) -> Result<Job> {
        self.update_scoped(
            scope,
            id,
            "SET status = 'accepted', error_code = NULL, error_message = NULL, processed_bytes = 0",
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

    fn connect(&self) -> Result<Connection> {
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
    })
}

fn uuid(row: &Row<'_>, column: usize) -> rusqlite::Result<Uuid> {
    let value: String = row.get(column)?;
    Uuid::parse_str(&value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })
}

fn scope_id(row: &Row<'_>, column: usize) -> rusqlite::Result<ScopeId> {
    let value: String = row.get(column)?;
    ScopeId::from_str(&value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })
}
