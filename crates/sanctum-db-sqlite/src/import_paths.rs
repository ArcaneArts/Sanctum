use rusqlite::Connection;
use sanctum_core::Result;
use tempfile::TempDir;

use crate::database_error;

/// Membership index for a single import scan. Its page cache stays small
/// even when the selected paths fill a large repository.
pub struct ImportPaths {
    connection: Connection,
    _directory: TempDir,
}

impl ImportPaths {
    pub fn open() -> Result<Self> {
        let directory = TempDir::new()?;
        let connection =
            Connection::open(directory.path().join("paths.db")).map_err(database_error)?;
        // The index is recreated on each scan and discarded on any write failure.
        connection
            .execute_batch(
                "PRAGMA cache_size = -1024;
                 PRAGMA journal_mode = OFF;
                 PRAGMA synchronous = OFF;
                 CREATE TABLE paths (path TEXT PRIMARY KEY) WITHOUT ROWID;
                 BEGIN;",
            )
            .map_err(database_error)?;
        Ok(Self {
            connection,
            _directory: directory,
        })
    }

    pub fn record(&self, path: &str) -> Result<()> {
        self.connection
            .prepare_cached("INSERT INTO paths (path) VALUES (?1) ON CONFLICT(path) DO NOTHING")
            .and_then(|mut insert| insert.execute([path]))
            .map(|_| ())
            .map_err(database_error)
    }

    pub fn finish(&self) -> Result<()> {
        self.connection
            .execute_batch("COMMIT;")
            .map_err(database_error)
    }

    pub fn contains(&self, path: &str) -> Result<bool> {
        self.connection
            .prepare_cached("SELECT EXISTS (SELECT 1 FROM paths WHERE path = ?1)")
            .and_then(|mut select| select.query_row([path], |row| row.get(0)))
            .map_err(database_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn large_path_index_spills_to_disk_before_the_scan_finishes() {
        let index = ImportPaths::open().unwrap();
        let directory = index._directory.path().to_owned();
        let basename = "é".repeat(250);
        for ordinal in 0..4096 {
            index.record(&format!("{ordinal}/{basename}")).unwrap();
        }
        assert!(directory.join("paths.db").metadata().unwrap().len() > 1024 * 1024);

        index.finish().unwrap();
        assert!(index.contains(&format!("0/{basename}")).unwrap());
        assert!(index.contains(&format!("4095/{basename}")).unwrap());
        assert!(!index.contains(&format!("4096/{basename}")).unwrap());
        drop(index);
        assert!(!directory.exists());
    }
}
