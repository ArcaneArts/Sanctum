use std::{fs, path::Path};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use sanctum_core::{Error, Result};
use uuid::Uuid;

use super::{
    CATALOG_MIGRATIONS, COLLECTION_MIGRATIONS, activate_collection, collection_deleted,
    database_error, delete_catalog_collection, migrate, open, query_all, read_initialized_catalog,
};

const RECEIPT_AGE_SECONDS: i64 = 45 * 24 * 3600;

pub fn create(
    catalog_path: &Path,
    db_path: &Path,
    collection: &str,
    request: Option<Uuid>,
) -> Result<()> {
    fs::create_dir_all(catalog_path.parent().expect("catalog has a parent"))?;
    let mut catalog = open(catalog_path, true)?;
    migrate(&mut catalog, CATALOG_MIGRATIONS)?;
    if begin(&mut catalog, collection, request)? {
        finish(&mut catalog, db_path, collection)?;
    }
    Ok(())
}

pub fn pending(path: &Path) -> Result<Vec<String>> {
    Ok(read_initialized_catalog(path, |catalog, version| {
        if version < 7 {
            return Ok(Vec::new());
        }
        query_all(
            catalog,
            "SELECT collection FROM local_collection_creations WHERE finished_at IS NULL",
            [],
            |row| row.get(0),
        )
    })?
    .unwrap_or_default())
}

pub fn recover(catalog_path: &Path, db_path: &Path, collection: &str) -> Result<()> {
    finish(&mut open(catalog_path, false)?, db_path, collection)
}

/// Records the tombstone and optional replay receipt together. Returns whether
/// the collection files and queue entries still need to be reclaimed.
pub fn retire(path: &Path, collection: &str, request: Option<Uuid>) -> Result<bool> {
    if !path.try_exists()? {
        return Err(Error::NotFound("tenant catalog was not found".into()));
    }
    let mut catalog = open(path, false)?;
    migrate(&mut catalog, CATALOG_MIGRATIONS)?;
    let tx = catalog
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(database_error)?;
    tx.execute(
        "DELETE FROM local_collection_deletions WHERE request_id IN (
            SELECT request_id FROM local_collection_deletions
            WHERE recorded_at <= unixepoch() - ?1
            ORDER BY recorded_at LIMIT 256
        )",
        [RECEIPT_AGE_SECONDS],
    )
    .map_err(database_error)?;
    let request = request.map(|id| id.to_string());
    if let Some(request) = &request {
        let saved: Option<String> = tx
            .query_row(
                "SELECT collection FROM local_collection_deletions WHERE request_id = ?1",
                [request],
                |row| row.get(0),
            )
            .optional()
            .map_err(database_error)?;
        if saved.is_some_and(|saved| saved != collection) {
            return Err(Error::Conflict(
                "request ID already applies to another collection".into(),
            ));
        }
    }
    delete_catalog_collection(&tx, collection)?;
    if let Some(request) = &request {
        tx.execute(
            "INSERT OR IGNORE INTO local_collection_deletions (request_id, collection)
             VALUES (?1, ?2)",
            params![request, collection],
        )
        .map_err(database_error)?;
    }
    let pending = tx
        .query_row(
            "SELECT reclaimed = 0 FROM deleted_collections WHERE id = ?1",
            [collection],
            |row| row.get(0),
        )
        .map_err(database_error)?;
    tx.commit().map_err(database_error)?;
    Ok(pending)
}

fn begin(catalog: &mut Connection, collection: &str, request: Option<Uuid>) -> Result<bool> {
    let tx = catalog
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(database_error)?;
    let request = request.map(|id| id.to_string());
    tx.execute(
        "DELETE FROM local_collection_creations WHERE collection IN (
            SELECT collection FROM local_collection_creations
            WHERE finished_at <= unixepoch() - ?1
            ORDER BY finished_at LIMIT 256
        )",
        [RECEIPT_AGE_SECONDS],
    )
    .map_err(database_error)?;
    if let Some(request) = &request {
        let saved: Option<(String, bool)> = tx
            .query_row(
                "SELECT collection, finished_at IS NOT NULL FROM local_collection_creations
                 WHERE request_id = ?1",
                [request],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(database_error)?;
        if let Some((saved, complete)) = saved {
            if saved != collection {
                return Err(Error::Conflict(
                    "request ID already applies to another collection".into(),
                ));
            }
            tx.commit().map_err(database_error)?;
            return Ok(!complete);
        }
    }
    if collection_deleted(&tx, collection)? {
        return Err(Error::Conflict("collection ID was retired".into()));
    }
    let exists: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM collections WHERE id = ?1)",
            [collection],
            |row| row.get(0),
        )
        .map_err(database_error)?;
    if exists {
        return Err(Error::Conflict(format!(
            "collection {collection} already exists"
        )));
    }
    let inserted = tx
        .execute(
            "INSERT OR IGNORE INTO local_collection_creations (collection, request_id) VALUES (?1, ?2)",
            params![collection, request],
        )
        .map_err(database_error)?;
    if inserted == 0 {
        let unkeyed: bool = tx
            .query_row(
                "SELECT request_id IS NULL FROM local_collection_creations
                 WHERE collection = ?1 AND finished_at IS NULL",
                [collection],
                |row| row.get(0),
            )
            .map_err(database_error)?;
        if request.is_some() || !unkeyed {
            return Err(Error::Conflict(
                "another request is creating this collection".into(),
            ));
        }
    }
    tx.commit().map_err(database_error)?;
    Ok(true)
}

fn finish(catalog: &mut Connection, db_path: &Path, collection: &str) -> Result<()> {
    let tx = catalog
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(database_error)?;
    if !activate_collection(&tx, collection)? {
        return Err(Error::Conflict(format!(
            "collection {collection} already exists"
        )));
    }
    fs::create_dir_all(db_path.parent().expect("database has a parent"))?;
    migrate(&mut open(db_path, true)?, COLLECTION_MIGRATIONS)?;
    tx.execute(
        "UPDATE local_collection_creations SET finished_at = unixepoch()
         WHERE collection = ?1 AND request_id IS NOT NULL",
        [collection],
    )
    .map_err(database_error)?;
    tx.execute(
        "DELETE FROM local_collection_creations WHERE collection = ?1 AND request_id IS NULL",
        [collection],
    )
    .map_err(database_error)?;
    tx.commit().map_err(database_error)
}
