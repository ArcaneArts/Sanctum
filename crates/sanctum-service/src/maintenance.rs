use sanctum_core::{Error, Result, Scope};
use sanctum_db_sqlite::RevisionKey;
use uuid::Uuid;

use crate::{Service, db};

impl Service {
    pub(crate) fn recover_local_collections(&self) -> Result<()> {
        for tenant in self.files.tenants()? {
            let catalog = self.files.catalog_path(&tenant);
            let pending = match db::local_collections::pending(&catalog) {
                Ok(pending) => pending,
                Err(error) => {
                    tracing::warn!(%error, %tenant, "could not enumerate pending collection creations");
                    continue;
                }
            };
            for collection in pending {
                let recover = || {
                    let scope = Scope {
                        tenant: tenant.clone(),
                        collection: collection.parse()?,
                    };
                    db::local_collections::recover(
                        &catalog,
                        &self.files.database_path(&scope),
                        &collection,
                    )
                };
                if let Err(error) = recover() {
                    tracing::warn!(%error, %tenant, %collection, "could not recover local collection creation");
                }
            }
        }
        Ok(())
    }

    pub fn delete_collection(&self, scope: &Scope) -> Result<()> {
        let _exclusive = self
            .retrieval
            .write()
            .map_err(|_| Error::Busy("retrieval lock is unavailable".into()))?;
        db::retire_local_collection(
            &self.files.catalog_path(&scope.tenant),
            scope.collection.as_str(),
        )?;
        self.purge_collection(scope)
    }

    fn purge_collection(&self, scope: &Scope) -> Result<()> {
        self.queue.purge_collection(scope)?;
        self.files.purge_collection(scope)?;
        db::mark_local_collection_reclaimed(
            &self.files.catalog_path(&scope.tenant),
            scope.collection.as_str(),
        )
    }

    pub(crate) fn reclaim_deleted_collections(&self) -> Result<()> {
        let _exclusive = self
            .retrieval
            .write()
            .map_err(|_| Error::Busy("retrieval lock is unavailable".into()))?;
        for tenant in self.files.tenants()? {
            for collection in db::deleted_catalog_collections(&self.files.catalog_path(&tenant))? {
                self.purge_collection(&Scope {
                    tenant: tenant.clone(),
                    collection: collection.parse()?,
                })?;
            }
        }
        Ok(())
    }

    pub fn delete_source(
        &self,
        scope: &Scope,
        source: Uuid,
        expected: Option<Uuid>,
    ) -> Result<Uuid> {
        let _reader = self
            .retrieval
            .read()
            .map_err(|_| Error::Busy("retrieval lock is unavailable".into()))?;
        self.local_writes.run(scope, || {
            let mut database = self.database(scope)?;
            let writer = database.write()?;
            let deleted = writer.delete_source(source, expected)?;
            writer.commit()?;
            Ok(deleted.revision)
        })
    }

    pub fn reclaim_retired(&self, scope: &Scope) -> Result<()> {
        let _exclusive = self
            .retrieval
            .write()
            .map_err(|_| Error::Busy("retrieval lock is unavailable".into()))?;
        let mut database = self.database(scope)?;
        let profile = self.embedding_profile();
        if !database.has_retirement_work(profile)? {
            return Ok(());
        }
        let writer = database.write()?;
        writer.release_retirements(profile)?;
        for candidate in writer.retirement_candidates(profile)? {
            if !self.queue.retains_input(scope, candidate)? {
                writer.retire(candidate)?;
            }
        }
        writer.expire_deletion_receipts()?;
        writer.prune_unreferenced_embeddings()?;
        writer.commit()?;

        let garbage = database.garbage()?;
        let count = garbage.len();
        for garbage in garbage {
            self.files
                .discard_text(scope, garbage.source, garbage.revision)?;
            database.acknowledge_garbage(garbage)?;
        }
        tracing::debug!(
            event = "cleanup_pass",
            kind = "local_source",
            tenant = %scope.tenant,
            collection = %scope.collection,
            reclaimed_revisions = count,
            "completed local revision cleanup"
        );
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

    pub(crate) fn local_scopes(&self) -> Result<Vec<Scope>> {
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
