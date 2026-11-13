use rusqlite::{OptionalExtension, Row, params};
use sanctum_core::{Error, Result, Scope};
use uuid::Uuid;

use crate::{database_error, query_all, queue::LocalQueue, sql_integer, unsigned_column};

pub struct Folder {
    pub id: Uuid,
    pub scope: Scope,
    pub path: String,
    pub options_json: String,
    pub interval: u32,
    pub scanned: Option<u64>,
    pub counts: Counts,
    pub error: Option<String>,
}

#[derive(Default)]
pub struct Counts {
    pub queued: u64,
    pub missing: u64,
    pub deleted: u64,
    pub failed: u64,
}

const COLUMNS: &str = "id, tenant, collection, path, options_json, interval_seconds, last_scan_at,
    queued, missing, deleted, failed, last_error";

impl LocalQueue {
    pub fn add_folder(
        &self,
        scope: &Scope,
        path: &str,
        options: &str,
        interval: u32,
    ) -> Result<Folder> {
        let db = self.connect()?;
        let id = Uuid::new_v4().to_string();
        db.query_row(&format!(
            "INSERT INTO watched_folders (id, tenant, collection, path, options_json, interval_seconds)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT (tenant, collection, path) DO NOTHING RETURNING {COLUMNS}"
        ), params![id, scope.tenant.as_str(), scope.collection.as_str(), path, options, interval], row_folder)
            .optional().map_err(database_error)?
            .ok_or_else(|| Error::Conflict("folder is already watched in this collection".into()))
    }

    pub fn folders(&self, scope: &Scope, after: &str, limit: u32) -> Result<Vec<Folder>> {
        query_all(
            &self.connect()?,
            &format!(
                "SELECT {COLUMNS} FROM watched_folders WHERE tenant = ?1 AND collection = ?2
              AND id > ?3 ORDER BY id LIMIT ?4"
            ),
            params![
                scope.tenant.as_str(),
                scope.collection.as_str(),
                after,
                limit
            ],
            row_folder,
        )
    }

    pub fn remove_folder(&self, scope: &Scope, id: Uuid) -> Result<()> {
        let removed = self
            .connect()?
            .execute(
                "DELETE FROM watched_folders WHERE tenant = ?1 AND collection = ?2 AND id = ?3",
                params![
                    scope.tenant.as_str(),
                    scope.collection.as_str(),
                    id.to_string()
                ],
            )
            .map_err(database_error)?;
        if removed == 0 {
            return Err(Error::NotFound("watched folder was not found".into()));
        }
        Ok(())
    }

    pub fn next_folder(&self) -> Result<Option<Folder>> {
        self.connect()?
            .query_row(
                &format!(
                    "SELECT {COLUMNS} FROM watched_folders
              WHERE next_scan_at <= unixepoch()
                AND (SELECT paused = 0 FROM executor_settings WHERE id = 1)
              ORDER BY next_scan_at, id LIMIT 1"
                ),
                [],
                row_folder,
            )
            .optional()
            .map_err(database_error)
    }

    pub fn scanned_folder(&self, id: Uuid, counts: Counts, error: Option<&str>) -> Result<()> {
        self.connect()?
            .execute(
                "UPDATE watched_folders SET last_scan_at = unixepoch(),
                next_scan_at = unixepoch() + interval_seconds, queued = ?2, missing = ?3,
                deleted = ?4, failed = ?5, last_error = ?6 WHERE id = ?1",
                params![
                    id.to_string(),
                    sql_integer(counts.queued)?,
                    sql_integer(counts.missing)?,
                    sql_integer(counts.deleted)?,
                    sql_integer(counts.failed)?,
                    error
                ],
            )
            .map_err(database_error)?;
        Ok(())
    }
}

fn row_folder(row: &Row<'_>) -> rusqlite::Result<Folder> {
    Ok(Folder {
        id: crate::queue::uuid(row, 0)?,
        scope: Scope {
            tenant: crate::queue::scope_id(row, 1)?,
            collection: crate::queue::scope_id(row, 2)?,
        },
        path: row.get(3)?,
        options_json: row.get(4)?,
        interval: row.get(5)?,
        scanned: row
            .get::<_, Option<i64>>(6)?
            .map(|value| {
                u64::try_from(value).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(6, value))
            })
            .transpose()?,
        counts: Counts {
            queued: unsigned_column(row, 7)?,
            missing: unsigned_column(row, 8)?,
            deleted: unsigned_column(row, 9)?,
            failed: unsigned_column(row, 10)?,
        },
        error: row.get(11)?,
    })
}
