use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

use sanctum_core::{ByteRange, Error, Result, Scope, ScopeId};
use tempfile::NamedTempFile;
use uuid::Uuid;

mod objects;
mod usage;
pub use objects::FileStorage;

pub struct LocalFiles {
    root: PathBuf,
    owner: File,
}

impl Drop for LocalFiles {
    fn drop(&mut self) {
        // Unlock explicitly even if a concurrent child process is between fork and exec.
        let _ = self.owner.unlock();
    }
}

impl LocalFiles {
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn open(root: &Path) -> Result<Self> {
        fs::create_dir_all(root)?;
        let root = fs::canonicalize(root)?;
        let owner = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join(".owner.lock"))?;
        owner.try_lock().map_err(|error| match error {
            std::fs::TryLockError::WouldBlock => {
                Error::Busy("another server owns this storage root".into())
            }
            std::fs::TryLockError::Error(error) => Error::Io(error),
        })?;
        Ok(Self { root, owner })
    }

    pub fn catalog_path(&self, tenant: &ScopeId) -> PathBuf {
        self.root.join(tenant.as_str()).join("root.db")
    }

    pub fn database_path(&self, scope: &Scope) -> PathBuf {
        self.collection_path(scope).join("meta.db")
    }

    pub fn runtime_path(&self) -> PathBuf {
        self.root.join("runtime.db")
    }

    pub fn tenants(&self) -> Result<Vec<ScopeId>> {
        let mut tenants = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            if entry.file_type()?.is_dir()
                && let Some(name) = entry.file_name().to_str()
                && let Ok(tenant) = name.parse()
            {
                tenants.push(tenant);
            }
        }
        Ok(tenants)
    }

    pub fn clear_staging(&self, scope: &Scope) -> Result<()> {
        for entry in entries_if_present(&self.collection_path(scope).join("staging"))? {
            let entry = entry?;
            if entry.file_type()?.is_file()
                && entry.file_name().to_string_lossy().starts_with(".tmp")
            {
                fs::remove_file(entry.path())?;
            }
        }
        Ok(())
    }

    pub fn visit_revisions(
        &self,
        scope: &Scope,
        mut visit: impl FnMut(Uuid, Uuid) -> Result<()>,
    ) -> Result<()> {
        for source in entries_if_present(&self.collection_path(scope).join("sources"))? {
            let source = source?;
            let Some(source_id) = uuid_dir(&source)? else {
                continue;
            };
            for revision in entries_if_present(&source.path())? {
                let revision = revision?;
                if let Some(revision_id) = uuid_dir(&revision)? {
                    visit(source_id, revision_id)?;
                }
            }
        }
        Ok(())
    }

    pub fn open_text(&self, scope: &Scope, source: Uuid, revision: Uuid) -> Result<File> {
        Ok(File::open(self.text_path(scope, source, revision))?)
    }

    pub fn discard_text(&self, scope: &Scope, source: Uuid, revision: Uuid) -> Result<()> {
        let text = self.text_path(scope, source, revision);
        let parent = text.parent().expect("text path has a revision directory");
        remove_file_if_present(&text)?;
        remove_file_if_present(&parent.join("original"))?;
        match fs::remove_dir(parent) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let source_dir = parent.parent().expect("revision has a source directory");
        match fs::remove_dir(source_dir) {
            Ok(()) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::DirectoryNotEmpty
                ) => {}
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    pub fn stage(&self, scope: &Scope) -> Result<NamedTempFile> {
        let staging = self.collection_path(scope).join("staging");
        fs::create_dir_all(&staging)?;
        Ok(NamedTempFile::new_in(staging)?)
    }

    pub fn publish(
        &self,
        scope: &Scope,
        source: Uuid,
        revision: Uuid,
        staged: NamedTempFile,
    ) -> Result<()> {
        self.publish_file(self.text_path(scope, source, revision), staged, false)
    }

    pub fn publish_original(
        &self,
        scope: &Scope,
        source: Uuid,
        revision: Uuid,
        staged: NamedTempFile,
    ) -> Result<()> {
        self.publish_file(
            self.text_path(scope, source, revision)
                .with_file_name("original"),
            staged,
            false,
        )
    }

    pub fn publish_extracted(
        &self,
        scope: &Scope,
        source: Uuid,
        revision: Uuid,
        staged: NamedTempFile,
    ) -> Result<()> {
        self.publish_file(self.text_path(scope, source, revision), staged, true)
    }

    pub fn open_original(&self, scope: &Scope, source: Uuid, revision: Uuid) -> Result<File> {
        Ok(File::open(
            self.text_path(scope, source, revision)
                .with_file_name("original"),
        )?)
    }

    pub fn discard_original(&self, scope: &Scope, source: Uuid, revision: Uuid) -> Result<()> {
        remove_file_if_present(
            &self
                .text_path(scope, source, revision)
                .with_file_name("original"),
        )
    }

    fn publish_file(&self, path: PathBuf, staged: NamedTempFile, replace: bool) -> Result<()> {
        let parent = path.parent().expect("text path has a revision directory");
        fs::create_dir_all(parent)?;
        staged.as_file().sync_all()?;
        if replace {
            staged.persist(&path).map_err(|error| error.error)?;
        } else {
            staged
                .persist_noclobber(&path)
                .map_err(|error| error.error)?;
        }
        sync_dirs(parent, &self.root)?;
        Ok(())
    }

    pub fn read_chunk(
        &self,
        scope: &Scope,
        source: Uuid,
        revision: Uuid,
        range: ByteRange,
    ) -> Result<Vec<u8>> {
        let length = range.length()?;
        if length > 64 * 1024 {
            return Err(Error::Invalid("chunk range exceeds the read limit".into()));
        }
        read_exact_range(
            File::open(self.text_path(scope, source, revision))?,
            range.start,
            length as usize,
        )
    }

    fn collection_path(&self, scope: &Scope) -> PathBuf {
        self.root
            .join(scope.tenant.as_str())
            .join(scope.collection.as_str())
    }

    fn text_path(&self, scope: &Scope, source: Uuid, revision: Uuid) -> PathBuf {
        self.collection_path(scope)
            .join("sources")
            .join(source.to_string())
            .join(revision.to_string())
            .join("text.utf8")
    }
}

fn read_exact_range(mut file: File, start: u64, length: usize) -> Result<Vec<u8>> {
    file.seek(SeekFrom::Start(start))?;
    let mut bytes = vec![0; length];
    file.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn remove_file_if_present(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn entries_if_present(path: &Path) -> Result<impl Iterator<Item = std::io::Result<fs::DirEntry>>> {
    match fs::read_dir(path) {
        Ok(entries) => Ok(Some(entries).into_iter().flatten()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(None.into_iter().flatten())
        }
        Err(error) => Err(error.into()),
    }
}

fn uuid_dir(entry: &fs::DirEntry) -> Result<Option<Uuid>> {
    if !entry.file_type()?.is_dir() {
        return Ok(None);
    }
    Ok(entry.file_name().to_str().and_then(|name| {
        Uuid::parse_str(name)
            .ok()
            .filter(|id| id.to_string() == name)
    }))
}

#[cfg(unix)]
fn sync_dirs(path: &Path, root: &Path) -> Result<()> {
    for parent in path.ancestors() {
        File::open(parent)?.sync_all()?;
        if parent == root {
            break;
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn sync_dirs(_path: &Path, _root: &Path) -> Result<()> {
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn dropping_owner_releases_lock_while_a_duplicate_descriptor_exists() {
        let dir = tempfile::tempdir().unwrap();
        let owner = LocalFiles::open(dir.path()).unwrap();
        let duplicate = owner.owner.try_clone().unwrap();
        assert!(matches!(LocalFiles::open(dir.path()), Err(Error::Busy(_))));
        drop(owner);
        let next_owner = LocalFiles::open(dir.path()).unwrap();
        drop(duplicate);
        assert!(matches!(LocalFiles::open(dir.path()), Err(Error::Busy(_))));
        drop(next_owner);
    }
}
