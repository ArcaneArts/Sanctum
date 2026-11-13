use sanctum_core::{Error, Result, Scope};
use sanctum_db_sqlite::RevisionKey;
use uuid::Uuid;

use crate::Service;

impl Service {
    pub fn delete_source(
        &self,
        scope: &Scope,
        source: Uuid,
        expected: Option<Uuid>,
    ) -> Result<Uuid> {
        let mut database = self.database(scope)?;
        let writer = database.write()?;
        let deleted = writer.delete_source(source, expected)?;
        writer.commit()?;
        Ok(deleted.revision)
    }

    pub fn reclaim_retired(&self, scope: &Scope) -> Result<()> {
        let mut database = self.database(scope)?;
        if !database.has_retirement_work()? {
            return Ok(());
        }
        let _exclusive = self
            .retrieval
            .write()
            .map_err(|_| Error::Busy("retrieval lock is unavailable".into()))?;
        let writer = database.write()?;
        for candidate in writer.retirement_candidates()? {
            if !self.queue.retains_input(scope, candidate)? {
                writer.retire(candidate)?;
            }
        }
        writer.expire_deletion_receipts()?;
        writer.commit()?;

        for garbage in database.garbage()? {
            self.files
                .discard_text(scope, garbage.source, garbage.revision)?;
            database.acknowledge_garbage(garbage)?;
        }
        Ok(())
    }

    pub(crate) fn reclaim_startup_orphans(&self) -> Result<()> {
        for scope in self.local_scopes()? {
            self.files.clear_staging(&scope)?;
            let database = self.database(&scope)?;
            self.files.visit_revisions(&scope, |source, revision| {
                let key = RevisionKey { source, revision };
                if !database.contains_revision(key)? && !self.queue.retains_input(&scope, key)? {
                    self.files.discard_text(&scope, source, revision)?;
                }
                Ok(())
            })?;
        }
        Ok(())
    }

    pub(crate) fn sweep_retired(&self) {
        match self.local_scopes() {
            Ok(scopes) => {
                for scope in scopes {
                    if let Err(error) = self.reclaim_retired(&scope) {
                        tracing::warn!(%error, tenant = %scope.tenant, collection = %scope.collection,
                            "could not reclaim retired revisions");
                    }
                }
            }
            Err(error) => {
                tracing::warn!(%error, "could not enumerate local collections for cleanup")
            }
        }
    }

    fn local_scopes(&self) -> Result<Vec<Scope>> {
        let mut scopes = Vec::new();
        for tenant in self.files.tenants()? {
            for collection in self.list_collections(&tenant)? {
                scopes.push(Scope {
                    tenant: tenant.clone(),
                    collection: collection.parse()?,
                });
            }
        }
        Ok(scopes)
    }
}
