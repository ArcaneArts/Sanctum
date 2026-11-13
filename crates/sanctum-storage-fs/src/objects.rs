use std::{
    fs::{self, File},
    io::{Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use sanctum_core::{ByteRange, Error, Result};
use sanctum_storage::{
    Condition, Download, Key, ListPage, MAX_CONTROL_BYTES, Object, Reader, Storage, Version,
    list_limit, range_len,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{LocalFiles, read_exact_range, sync_dirs};

pub struct FileStorage {
    owner: Arc<LocalFiles>,
    mutations: Arc<Mutex<()>>,
}

impl FileStorage {
    pub fn open(root: &Path) -> Result<Self> {
        Ok(Self {
            owner: Arc::new(LocalFiles::open(root)?),
            mutations: Default::default(),
        })
    }

    fn path(&self, key: &Key) -> PathBuf {
        self.owner.root().join(key.as_str())
    }
}

#[async_trait::async_trait]
impl Storage for FileStorage {
    async fn head(&self, key: &Key) -> Result<Object> {
        let path = self.path(key);
        let key = key.clone();
        blocking(move || open_object(&path, &key).map(|(_, object)| object)).await
    }

    async fn get(&self, key: &Key) -> Result<Download> {
        let path = self.path(key);
        let key = key.clone();
        let (file, object) = blocking(move || open_object(&path, &key)).await?;
        Ok(Download {
            object,
            reader: Box::pin(tokio::fs::File::from_std(file)),
        })
    }

    async fn read_range(&self, key: &Key, range: ByteRange) -> Result<Bytes> {
        let size = range_len(range)?;
        let path = self.path(key);
        blocking(move || Ok(read_exact_range(open_file(&path)?, range.start, size)?.into())).await
    }

    async fn put_stream(&self, key: &Key, mut reader: Reader, length: u64) -> Result<Version> {
        let path = self.path(key);
        let parent = path.parent().expect("managed key has a parent").to_owned();
        let staged = blocking(move || stage(&parent)).await?;
        let file = staged.reopen()?;
        let mut output = tokio::fs::File::from_std(file);
        let copied = tokio::io::copy(&mut (&mut reader).take(length), &mut output).await?;
        if copied != length || reader.read(&mut [0]).await? != 0 {
            return Err(Error::Invalid(
                "object body differs from its declared length".into(),
            ));
        }
        output.flush().await?;
        drop(output);
        let mutations = self.mutations.clone();
        let owner = self.owner.clone();
        blocking(move || {
            let _guard = mutations.lock().map_err(|_| lock_error())?;
            publish(staged, &path, owner.root())
        })
        .await
    }

    async fn put_control(&self, key: &Key, bytes: Bytes, condition: Condition) -> Result<Version> {
        if bytes.len() > MAX_CONTROL_BYTES {
            return Err(Error::Invalid(
                "storage control object exceeds 1 MiB".into(),
            ));
        }
        let path = self.path(key);
        let mutations = self.mutations.clone();
        let owner = self.owner.clone();
        blocking(move || {
            let _guard = mutations.lock().map_err(|_| lock_error())?;
            match (open_file(&path), condition) {
                (Err(Error::NotFound(_)), Condition::Create) => {}
                (Ok(mut file), Condition::Match(expected)) if expected.etag.is_some() => {
                    if file.metadata()?.len() > MAX_CONTROL_BYTES as u64
                        || version(&mut file)?.etag != expected.etag
                    {
                        return Err(precondition());
                    }
                }
                (Ok(_), _) | (Err(Error::NotFound(_)), _) => return Err(precondition()),
                (Err(error), _) => return Err(error),
            }
            let mut staged = stage(path.parent().expect("managed key has a parent"))?;
            staged.write_all(&bytes)?;
            publish(staged, &path, owner.root())
        })
        .await
    }

    async fn list(
        &self,
        prefix: Option<&Key>,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<ListPage> {
        let limit = list_limit(limit)?;
        let cursor = cursor.map(Key::parse).transpose()?;
        let root = self.owner.root().to_owned();
        let start = prefix.map_or_else(|| root.clone(), |prefix| self.path(prefix));
        blocking(move || {
            let mut rows = std::collections::BTreeMap::new();
            for entry in walkdir::WalkDir::new(&start)
                .follow_links(false)
                .follow_root_links(false)
                .min_depth(1)
            {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(error)
                        if error
                            .io_error()
                            .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
                    {
                        continue;
                    }
                    Err(error) => return Err(std::io::Error::from(error).into()),
                };
                if !entry.file_type().is_file() {
                    continue;
                }
                let relative = entry
                    .path()
                    .strip_prefix(&root)
                    .expect("entry is under storage root");
                let relative = relative
                    .components()
                    .map(|part| part.as_os_str().to_string_lossy())
                    .collect::<Vec<_>>()
                    .join("/");
                let Ok(key) = Key::parse(relative) else {
                    continue;
                };
                if cursor.as_ref().is_some_and(|cursor| key <= *cursor) {
                    continue;
                }
                rows.insert(key, entry.into_path());
                if rows.len() > limit + 1 {
                    rows.pop_last();
                }
            }
            let has_more = rows.len() > limit;
            if has_more {
                rows.pop_last();
            }
            let next_cursor = has_more.then(|| {
                rows.last_key_value()
                    .expect("nonzero page limit")
                    .0
                    .to_string()
            });
            let objects = rows
                .into_iter()
                .filter_map(|(key, path)| match fs::symlink_metadata(path) {
                    Ok(meta) if meta.is_file() => Some(
                        meta.modified()
                            .map(|modified_at| Object {
                                key,
                                length: meta.len(),
                                version: Version {
                                    etag: None,
                                    generation: None,
                                },
                                modified_at,
                            })
                            .map_err(Error::from),
                    ),
                    Ok(_) => None,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                    Err(error) => Some(Err(error.into())),
                })
                .collect::<Result<_>>()?;
            Ok(ListPage {
                objects,
                next_cursor,
            })
        })
        .await
    }

    async fn delete(&self, key: &Key) -> Result<()> {
        let path = self.path(key);
        let mutations = self.mutations.clone();
        let owner = self.owner.clone();
        blocking(move || {
            let _guard = mutations.lock().map_err(|_| lock_error())?;
            match fs::remove_file(&path) {
                Ok(()) => sync_dirs(
                    path.parent().expect("managed key has a parent"),
                    owner.root(),
                ),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error.into()),
            }
        })
        .await
    }
}

fn open_file(path: &Path) -> Result<File> {
    let file = File::open(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            Error::NotFound("filesystem object was not found".into())
        } else {
            error.into()
        }
    })?;
    if !file.metadata()?.is_file() {
        return Err(Error::Invalid(
            "filesystem object is not a regular file".into(),
        ));
    }
    Ok(file)
}

fn open_object(path: &Path, key: &Key) -> Result<(File, Object)> {
    let mut file = open_file(path)?;
    let meta = file.metadata()?;
    let version = if meta.len() <= MAX_CONTROL_BYTES as u64 {
        version(&mut file)?
    } else {
        Version {
            etag: None,
            generation: None,
        }
    };
    Ok((
        file,
        Object {
            key: key.clone(),
            length: meta.len(),
            modified_at: meta.modified()?,
            version,
        },
    ))
}

fn version(file: &mut File) -> Result<Version> {
    let mut digest = blake3::Hasher::new();
    digest.update_reader(&mut *file)?;
    file.seek(SeekFrom::Start(0))?;
    Ok(Version {
        etag: Some(digest.finalize().to_hex().to_string()),
        generation: None,
    })
}

fn stage(parent: &Path) -> Result<tempfile::NamedTempFile> {
    fs::create_dir_all(parent)?;
    Ok(tempfile::NamedTempFile::new_in(parent)?)
}

fn publish(staged: tempfile::NamedTempFile, path: &Path, root: &Path) -> Result<Version> {
    staged.as_file().sync_all()?;
    let value = if staged.as_file().metadata()?.len() <= MAX_CONTROL_BYTES as u64 {
        version(&mut staged.reopen()?)?
    } else {
        Version {
            etag: None,
            generation: None,
        }
    };
    staged.persist(path).map_err(|error| error.error)?;
    sync_dirs(path.parent().expect("managed key has a parent"), root)?;
    Ok(value)
}

async fn blocking<T: Send + 'static>(
    task: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(task)
        .await
        .map_err(|error| Error::Internal(Box::new(error)))?
}

fn lock_error() -> Error {
    Error::Busy("filesystem object writer is unavailable".into())
}

fn precondition() -> Error {
    Error::Conflict("filesystem object precondition failed".into())
}
