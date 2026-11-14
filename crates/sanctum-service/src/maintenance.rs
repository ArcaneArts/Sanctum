use sanctum_core::{Error, Result, Scope};
use sanctum_db_sqlite::RevisionKey;
use uuid::Uuid;

use crate::{Service, db, local_jobs::revision_for};

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
        self.delete_collection_with_request(scope, None)
    }

    pub fn delete_collection_with_request(
        &self,
        scope: &Scope,
        request: Option<Uuid>,
    ) -> Result<()> {
        if request.is_some_and(|id| id.is_nil()) {
            return Err(Error::Invalid("request ID must be a nonzero UUID".into()));
        }
        let _exclusive = self
            .retrieval
            .write()
            .map_err(|_| Error::Busy("retrieval lock is unavailable".into()))?;
        if db::local_collections::retire(
            &self.files.catalog_path(&scope.tenant),
            scope.collection.as_str(),
            request,
        )? {
            self.purge_collection(scope)?;
        }
        Ok(())
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
            let pending = match db::deleted_catalog_collections(&self.files.catalog_path(&tenant)) {
                Ok(pending) => pending,
                Err(error) => {
                    tracing::warn!(%error, %tenant, "could not enumerate deleted local collections");
                    continue;
                }
            };
            for collection in pending {
                let reclaim = || {
                    let scope = Scope {
                        tenant: tenant.clone(),
                        collection: collection.parse()?,
                    };
                    self.purge_collection(&scope)
                };
                if let Err(error) = reclaim() {
                    tracing::warn!(%error, %tenant, %collection, "could not reclaim deleted local collection");
                }
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
        self.delete_source_with_request(scope, source, expected, None)
    }

    pub fn delete_source_with_request(
        &self,
        scope: &Scope,
        source: Uuid,
        expected: Option<Uuid>,
        request: Option<Uuid>,
    ) -> Result<Uuid> {
        if request.is_some_and(|id| id.is_nil()) {
            return Err(Error::Invalid("request ID must be a nonzero UUID".into()));
        }
        let _reader = self
            .retrieval
            .read()
            .map_err(|_| Error::Busy("retrieval lock is unavailable".into()))?;
        self.local_writes.run(scope, || {
            let mut database = self.database(scope)?;
            let writer = database.write()?;
            let revision = writer.delete_source_with_request(source, expected, request)?;
            writer.commit()?;
            Ok(revision)
        })
    }

    pub fn reclaim_retired(&self, scope: &Scope) -> Result<()> {
        let _exclusive = self
            .retrieval
            .write()
            .map_err(|_| Error::Busy("retrieval lock is unavailable".into()))?;
        let mut database = self.database(scope)?;
        let profile = self.embedding_profile();
        if database.has_retirement_work(profile)? {
            let writer = database.write()?;
            writer.release_retirements(profile)?;
            for candidate in writer.retirement_candidates(profile)? {
                if !self.queue.retains_input(scope, candidate)? {
                    writer.retire(candidate)?;
                }
            }
            writer.expire_deletion_receipts()?;
            writer.expire_local_deletion_requests()?;
            writer.prune_unreferenced_embeddings()?;
            writer.commit()?;
        }

        let garbage = database.due_revision_garbage()?;
        if garbage.is_empty() {
            return Ok(());
        }
        let mut reclaimed = 0;
        let mut deferred = 0;
        for garbage in garbage {
            match self
                .files
                .discard_text(scope, garbage.source, garbage.revision)
            {
                Ok(()) => {
                    database.acknowledge_garbage(garbage)?;
                    reclaimed += 1;
                }
                Err(error) => {
                    tracing::warn!(
                        %error, tenant = %scope.tenant, collection = %scope.collection,
                        source = %garbage.source, revision = %garbage.revision,
                        "retired local revision deletion will be retried"
                    );
                    database.defer_revision_cleanup(garbage)?;
                    deferred += 1;
                }
            }
        }
        tracing::debug!(
            event = "cleanup_pass",
            kind = "local_source",
            tenant = %scope.tenant,
            collection = %scope.collection,
            reclaimed_revisions = reclaimed,
            deferred_revisions = deferred,
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
                if !database.tracks_revision_files(key)?
                    && !self.queue.retains_input(&scope, key)?
                {
                    self.files.discard_text(&scope, source, revision)?;
                }
                Ok(())
            })?;
        }
        Ok(())
    }

    pub(crate) fn reclaim_staged_inputs(&self) -> Result<()> {
        let _reader = self
            .retrieval
            .read()
            .map_err(|_| Error::Busy("retrieval lock is unavailable".into()))?;
        let superseded = self.queue.settle_superseded_failures()?;
        if superseded > 0 {
            tracing::debug!(
                event = "local_superseded_failures",
                jobs = superseded,
                "scheduled cleanup for superseded ingestion inputs"
            );
        }
        for job in self.queue.failed_reconciliation_candidates()? {
            let reconcile = || {
                self.require_collection(&job.scope)?;
                // The retrieval guard prevents retirement. If a manual retry
                // starts during this read, the queue's attempt check wins.
                let database =
                    db::CollectionDb::open_read_only(&self.files.database_path(&job.scope))?;
                let (committed, deleted_source) = database.read_snapshot(|database| {
                    let committed = database.revision_summary(job.source, job.revision)?;
                    let deleted_source = committed.is_none()
                        && job.replacement
                        && !database.has_active_source(job.source)?;
                    Ok((
                        committed.map(|content| revision_for(&job, content)),
                        deleted_source,
                    ))
                })?;
                self.queue
                    .reconcile_failed_input(&job, committed.as_ref(), deleted_source)
            };
            let settled = reconcile();
            if let Err(error) = settled {
                self.queue.defer_failed_reconciliation(&job)?;
                tracing::warn!(
                    %error, tenant = %job.scope.tenant, collection = %job.scope.collection,
                    operation = %job.id, "could not reconcile a failed local upload"
                );
            }
        }
        for pending in self.queue.pending_cleanup()? {
            if let Err(error) = self.reclaim_staged_input(&pending) {
                tracing::warn!(%error, operation = %pending.operation, "could not record staged input cleanup");
            }
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
