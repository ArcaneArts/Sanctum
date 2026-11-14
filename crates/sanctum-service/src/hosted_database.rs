use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use sanctum_core::{Error, Result, Scope};
use sanctum_db::{Generation, Materialized, Unit};
use sanctum_storage::{Condition, MAX_CONTROL_BYTES};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use uuid::Uuid;

use crate::{DatabaseTarget, HostedCatalog, database_snapshot};

const TTL: Duration = Duration::from_secs(300);

#[derive(Serialize, Deserialize)]
struct SessionRecord {
    expires_at: SystemTime,
    generation: Generation,
}

pub struct HostedDatabaseSession {
    pub id: Uuid,
    pub expires_at: SystemTime,
    pub description: database_snapshot::Description,
    pub bytes: u64,
    pub snapshot_generation: Uuid,
}

impl HostedCatalog {
    pub async fn open_database_session(
        &self,
        target: &DatabaseTarget,
    ) -> Result<HostedDatabaseSession> {
        let expires_at = SystemTime::now() + TTL;
        if let Some(collection) = &target.collection {
            self.require_collection(&Scope {
                tenant: target.tenant.clone(),
                collection: collection.clone(),
            })
            .await?;
        }
        let unit = self.database_unit(target)?;
        let snapshot = self
            .snapshots
            .read(&self.publication, &unit)
            .await?
            .ok_or_else(|| Error::NotFound("database was not found".into()))?;
        let generation = snapshot.generation.clone();
        let description =
            tokio::task::spawn_blocking(move || database_snapshot::describe_file(snapshot.path()))
                .await
                .map_err(|error| Error::Internal(Box::new(error)))??;
        if SystemTime::now() >= expires_at {
            return Err(Error::Busy(
                "database session expired during materialization".into(),
            ));
        }
        let id = Uuid::new_v4();
        let session = SessionRecord {
            expires_at,
            generation,
        };
        self.storage
            .put_control(
                &unit.snapshot_session(id)?,
                serde_json::to_vec(&session)
                    .map_err(|error| Error::Internal(Box::new(error)))?
                    .into(),
                Condition::Create,
            )
            .await?;
        Ok(HostedDatabaseSession {
            id,
            expires_at,
            description,
            bytes: session.generation.bytes,
            snapshot_generation: session.generation.id,
        })
    }

    pub async fn database_session(
        &self,
        target: &DatabaseTarget,
        id: Uuid,
    ) -> Result<Arc<Materialized>> {
        let unit = self.database_unit(target)?;
        let key = unit.snapshot_session(id)?;
        let record = self.storage.get(&key).await?;
        let length = record.object.length;
        if length > MAX_CONTROL_BYTES as u64 {
            return Err(Error::Internal(
                "database session record exceeds size limit".into(),
            ));
        }
        let mut bytes = Vec::new();
        record
            .reader
            .take(length + 1)
            .read_to_end(&mut bytes)
            .await?;
        if bytes.len() as u64 != length {
            return Err(Error::Internal(
                "database session record length mismatch".into(),
            ));
        }
        let session: SessionRecord =
            serde_json::from_slice(&bytes).map_err(|error| Error::Internal(Box::new(error)))?;
        if SystemTime::now() >= session.expires_at {
            return Err(Error::NotFound(
                "database session was not found or expired".into(),
            ));
        }
        self.snapshots
            .read_generation(&self.publication, &unit, session.generation)
            .await
    }

    pub async fn close_database_session(&self, target: &DatabaseTarget, id: Uuid) -> Result<()> {
        let key = self.database_unit(target)?.snapshot_session(id)?;
        self.storage.delete(&key).await
    }

    pub(crate) fn database_unit(&self, target: &DatabaseTarget) -> Result<Unit> {
        match &target.collection {
            Some(collection) => Unit::collection(
                self.prefix.as_ref(),
                &Scope {
                    tenant: target.tenant.clone(),
                    collection: collection.clone(),
                },
            ),
            None => Unit::catalog(self.prefix.as_ref(), &target.tenant),
        }
    }
}
