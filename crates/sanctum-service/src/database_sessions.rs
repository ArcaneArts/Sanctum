use std::{
    collections::HashMap,
    fs::{self, File},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant, SystemTime},
};

use sanctum_core::{Error, Result, Scope, ScopeId};
use sanctum_db_sqlite::snapshot;
use tempfile::NamedTempFile;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

use crate::Service;

const MAX_SESSIONS: usize = 4;
const SESSION_TTL: Duration = Duration::from_secs(300);

#[derive(Clone, PartialEq, Eq)]
pub struct DatabaseTarget {
    pub tenant: ScopeId,
    pub collection: Option<ScopeId>,
}

#[derive(Clone)]
pub(crate) struct DatabaseSessions(Arc<SessionRegistry>);

type Sessions = HashMap<Uuid, Arc<DatabaseSession>>;

struct SessionRegistry {
    sessions: Mutex<Sessions>,
    staging: PathBuf,
    slots: Arc<Semaphore>,
}

pub struct DatabaseSession {
    pub id: Uuid,
    pub expires_at: SystemTime,
    pub description: snapshot::Description,
    pub bytes: u64,
    target: DatabaseTarget,
    expires: Instant,
    file: NamedTempFile,
    _slot: OwnedSemaphorePermit,
}

impl DatabaseSession {
    pub fn reopen(&self) -> Result<File> {
        Ok(self.file.reopen()?)
    }

    pub fn query(
        &self,
        sql: &str,
        parameters: Vec<snapshot::SqlValue>,
        offset: u64,
        limit: u32,
        canceled: impl Fn() -> bool + Send + 'static,
    ) -> Result<snapshot::Page> {
        snapshot::query(self.file.path(), sql, parameters, offset, limit, canceled)
    }
}

impl DatabaseSessions {
    pub(crate) fn open(root: &Path) -> Self {
        let staging = root.join(".database-sessions");
        if let Err(error) = fs::remove_dir_all(&staging)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(%error, "could not clear old database snapshots");
        }
        Self(Arc::new(SessionRegistry {
            staging,
            sessions: Mutex::new(HashMap::new()),
            slots: Arc::new(Semaphore::new(MAX_SESSIONS)),
        }))
    }

    fn lock(&self) -> Result<MutexGuard<'_, Sessions>> {
        self.0
            .sessions
            .lock()
            .map_err(|_| Error::Busy("database session registry is unavailable".into()))
    }

    pub(crate) fn prune(&self) -> Result<()> {
        let now = Instant::now();
        self.lock()?.retain(|_, session| session.expires > now);
        Ok(())
    }

    fn reserve(&self) -> Result<OwnedSemaphorePermit> {
        self.prune()?;
        self.0
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Busy("all database session slots are in use".into()))
    }

    fn get(&self, target: &DatabaseTarget, id: Uuid) -> Result<Arc<DatabaseSession>> {
        self.prune()?;
        self.lock()?
            .get(&id)
            .filter(|session| session.target == *target)
            .cloned()
            .ok_or_else(missing_session)
    }

    fn close(&self, target: &DatabaseTarget, id: Uuid) -> Result<()> {
        self.prune()?;
        let mut sessions = self.lock()?;
        if sessions
            .get(&id)
            .is_some_and(|session| session.target == *target)
        {
            sessions.remove(&id);
            return Ok(());
        }
        Err(missing_session())
    }
}

impl Service {
    pub fn open_database_session(
        &self,
        target: DatabaseTarget,
        canceled: impl Fn() -> bool,
    ) -> Result<Arc<DatabaseSession>> {
        let source = self.session_source(&target)?;
        let slot = self.database_sessions.reserve()?;
        fs::create_dir_all(&self.database_sessions.0.staging)?;
        let file = tempfile::Builder::new()
            .prefix("sanctum-database-")
            .suffix(".sqlite")
            .tempfile_in(&self.database_sessions.0.staging)?;
        let description = snapshot::backup(&source, file.path(), &canceled)?;
        if canceled() {
            return Err(Error::Busy("database snapshot canceled".into()));
        }
        let bytes = file.as_file().metadata()?.len();
        let session = Arc::new(DatabaseSession {
            id: Uuid::new_v4(),
            expires_at: SystemTime::now() + SESSION_TTL,
            expires: Instant::now() + SESSION_TTL,
            description,
            bytes,
            target,
            file,
            _slot: slot,
        });
        self.database_sessions
            .lock()?
            .insert(session.id, session.clone());
        Ok(session)
    }

    pub fn database_session(
        &self,
        target: &DatabaseTarget,
        id: Uuid,
    ) -> Result<Arc<DatabaseSession>> {
        self.database_sessions.get(target, id)
    }

    pub fn close_database_session(&self, target: &DatabaseTarget, id: Uuid) -> Result<()> {
        self.database_sessions.close(target, id)
    }

    fn session_source(&self, target: &DatabaseTarget) -> Result<PathBuf> {
        if let Some(collection) = &target.collection {
            let scope = Scope {
                tenant: target.tenant.clone(),
                collection: collection.clone(),
            };
            self.database(&scope)?;
            return Ok(self.files.database_path(&scope));
        }
        let path = self.files.catalog_path(&target.tenant);
        snapshot::prepare_catalog(&path)?;
        Ok(path)
    }
}

fn missing_session() -> Error {
    Error::NotFound("database session was not found or expired".into())
}
