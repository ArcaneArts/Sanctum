use std::{fs, io, time::SystemTime};

use sanctum_core::{Error, Result, UsageMeasurement, UsageScope};
use walkdir::WalkDir;

use crate::LocalFiles;

impl LocalFiles {
    pub fn usage(&self, scope: &UsageScope) -> Result<UsageMeasurement> {
        self.usage_with_progress(scope, |_, _| Ok(()))
    }

    pub fn usage_with_progress(
        &self,
        scope: &UsageScope,
        mut progress: impl FnMut(u64, u64) -> Result<()>,
    ) -> Result<UsageMeasurement> {
        let path = match scope {
            UsageScope::Collection(scope) => self.collection_path(scope),
            UsageScope::Tenant(tenant) => self.root.join(tenant.as_str()),
            UsageScope::Cluster => self.root.clone(),
        };
        let started_at = SystemTime::now();
        let mut bytes = 0u64;
        let mut files = 0u64;
        for (index, entry) in WalkDir::new(&path)
            .follow_links(false)
            .follow_root_links(false)
            .into_iter()
            .enumerate()
        {
            if index != 0 && index % 256 == 0 {
                progress(bytes, files)?;
            }
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) if error.depth() == 0 && missing(&error) => {
                    return Err(Error::NotFound("storage scope was not found".into()));
                }
                Err(error) if missing(&error) => continue,
                Err(error) => return Err(io::Error::from(error).into()),
            };
            if entry.depth() == 0 && !entry.file_type().is_dir() {
                return Err(Error::Invalid("storage scope is not a directory".into()));
            }
            if !entry.file_type().is_file() {
                continue;
            }
            let metadata = match fs::symlink_metadata(entry.path()) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            if !metadata.is_file() {
                continue;
            }
            bytes = bytes
                .checked_add(metadata.len())
                .ok_or_else(|| overflow("byte count"))?;
            files = files.checked_add(1).ok_or_else(|| overflow("file count"))?;
        }
        Ok(UsageMeasurement {
            bytes,
            files,
            started_at,
            finished_at: SystemTime::now(),
        })
    }
}

fn missing(error: &walkdir::Error) -> bool {
    error
        .io_error()
        .is_some_and(|cause| cause.kind() == io::ErrorKind::NotFound)
}

fn overflow(count: &str) -> Error {
    io::Error::other(format!("storage usage {count} exceeds 64 bits")).into()
}
