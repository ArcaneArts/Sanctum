use std::{
    fs::File,
    io::{self, Read, Write},
    path::Path,
};

use sanctum_core::{Error, Result};
use tokio_util::sync::CancellationToken;

// A running blocking job cannot be aborted by dropping its JoinHandle.
pub(crate) async fn run<T: Send + 'static>(
    work: impl FnOnce(&CancellationToken) -> Result<T> + Send + 'static,
) -> Result<T> {
    let cancellation = CancellationToken::new();
    let _cancel_on_drop = cancellation.clone().drop_guard();
    tokio::task::spawn_blocking(move || {
        check(&cancellation)?;
        let result = work(&cancellation);
        check(&cancellation)?;
        result
    })
    .await
    .map_err(|error| Error::Internal(Box::new(error)))?
}

pub(crate) fn check(cancellation: &CancellationToken) -> Result<()> {
    if cancellation.is_cancelled() {
        return Err(Error::Busy("database snapshot canceled".into()));
    }
    Ok(())
}

pub(crate) fn copy(from: &Path, to: &Path, cancellation: &CancellationToken) -> Result<()> {
    let mut source = File::open(from)?;
    let mut destination = File::create(to)?;
    let mut buffer = vec![0; 1024 * 1024];
    loop {
        check(cancellation)?;
        let count = source.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        destination.write_all(&buffer[..count])?;
    }
    Ok(())
}

pub(crate) fn hash(file: &mut File, cancellation: &CancellationToken) -> Result<String> {
    let mut digest = blake3::Hasher::new();
    digest.update_reader(SnapshotReader { file, cancellation })?;
    Ok(digest.finalize().to_hex().to_string())
}

struct SnapshotReader<'a> {
    file: &'a mut File,
    cancellation: &'a CancellationToken,
}

impl Read for SnapshotReader<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        check(self.cancellation).map_err(io::Error::other)?;
        self.file.read(buffer)
    }
}
