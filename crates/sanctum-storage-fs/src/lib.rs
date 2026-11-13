use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

use sanctum_core::{ByteRange, Error, Result, Scope, ScopeId};
use tempfile::NamedTempFile;
use uuid::Uuid;

pub struct LocalFiles {
    root: PathBuf,
    _owner: File,
}

impl LocalFiles {
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
        Ok(Self {
            root,
            _owner: owner,
        })
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

    pub fn open_text(&self, scope: &Scope, source: Uuid, revision: Uuid) -> Result<File> {
        Ok(File::open(self.text_path(scope, source, revision))?)
    }

    pub fn discard_text(&self, scope: &Scope, source: Uuid, revision: Uuid) -> Result<()> {
        let text = self.text_path(scope, source, revision);
        let parent = text.parent().expect("text path has a revision directory");
        match fs::remove_file(&text) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        }
        fs::remove_dir(parent)?;
        let source_dir = parent.parent().expect("revision has a source directory");
        match fs::remove_dir(source_dir) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::DirectoryNotEmpty => {}
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
        let path = self.text_path(scope, source, revision);
        let parent = path.parent().expect("text path has a revision directory");
        fs::create_dir_all(parent)?;
        staged.as_file().sync_all()?;
        staged
            .persist_noclobber(&path)
            .map_err(|error| error.error)?;
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
        let mut file = File::open(self.text_path(scope, source, revision))?;
        file.seek(SeekFrom::Start(range.start))?;
        let mut bytes = vec![0; length as usize];
        file.read_exact(&mut bytes)?;
        Ok(bytes)
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
