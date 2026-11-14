use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};

use sanctum_core::{Error, Result, Scope, ScopeId};

type WriterMap = HashMap<(ScopeId, ScopeId), Weak<Mutex<()>>>;

#[derive(Default)]
pub(crate) struct LocalWrites {
    entries: Mutex<WriterMap>,
}

impl LocalWrites {
    // The caller holds the retrieval read lock. Collection deletion and
    // reclamation hold its write lock and cannot overlap a collection write.
    pub(crate) fn run<T>(&self, scope: &Scope, write: impl FnOnce() -> Result<T>) -> Result<T> {
        let writer = {
            let mut entries = self.entries.lock().map_err(lock_error)?;
            let key = (scope.tenant.clone(), scope.collection.clone());
            if let Some(writer) = entries.get(&key).and_then(Weak::upgrade) {
                writer
            } else {
                entries.retain(|_, writer| writer.strong_count() != 0);
                let writer = Arc::new(Mutex::new(()));
                entries.insert(key, Arc::downgrade(&writer));
                writer
            }
        };
        let _guard = writer.lock().map_err(lock_error)?;
        write()
    }
}

fn lock_error<T>(_error: std::sync::PoisonError<T>) -> Error {
    Error::Busy("local collection writer lock is unavailable".into())
}
