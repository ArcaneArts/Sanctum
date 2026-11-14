use sanctum_core::{Error, Result, Scope, ScopeId};
use sanctum_db::{Command, Unit};
use uuid::Uuid;

use crate::{
    Change, HostedCatalog, MetadataEntry, MetadataMutation, RevisionTarget,
    db::{self, CollectionDb, metadata},
    hosted_retrieval::query_snapshot,
    metadata_filter::Level,
    metadata_query,
};

/// Progress from one hosted change poll. The catalog observation is only
/// reusable for the same collection, even within one tenant.
pub struct HostedChangePosition {
    scope: Scope,
    catalog_generation: Uuid,
    caught_up: Option<(Uuid, u64)>,
}

impl HostedCatalog {
    pub async fn metadata(
        &self,
        scope: &Scope,
        target: Option<RevisionTarget>,
    ) -> Result<Vec<MetadataEntry>> {
        match target {
            Some(target) => self
                .with_collection(scope, move |database| database.read_metadata(target))
                .await
                .map(|(entries, _)| entries),
            None => {
                self.with_catalog(scope, |path, collection| {
                    metadata::read_catalog_metadata(path, &collection)
                })
                .await
            }
        }
    }

    pub async fn mutate_metadata(
        &self,
        scope: &Scope,
        target: Option<RevisionTarget>,
        edits: Vec<MetadataMutation>,
    ) -> Result<Vec<MetadataEntry>> {
        let unit = match target {
            Some(_) => {
                self.require_collection(scope).await?;
                Unit::collection(self.prefix.as_ref(), scope)?
            }
            None => Unit::catalog(self.prefix.as_ref(), &scope.tenant)?,
        };
        let collection = scope.collection.to_string();
        let mut writer = self.publication.acquire_existing(&unit).await?;
        // Return the rows from this mutation's transaction. The publication receipt
        // only records the operation, so metadata payloads do not accumulate there.
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let result = writer
            .apply_stage(
                "metadata",
                vec![Command {
                    operation: Uuid::new_v4(),
                    payload: edits,
                }],
                move |tx, edits| {
                    let entries = match target {
                        Some(target) => metadata::mutate_revision_metadata(tx, target, &edits)?,
                        None => metadata::mutate_catalog_metadata(tx, &collection, &edits)?,
                    };
                    result_tx
                        .send(entries)
                        .map_err(|error| Error::Internal(Box::new(error)))?;
                    Ok("applied".into())
                },
            )
            .await;
        let release = writer.release().await;
        result?;
        release?;
        result_rx
            .try_recv()
            .map_err(|error| Error::Internal(Box::new(error)))
    }

    pub async fn query_metadata(
        &self,
        tenant: &ScopeId,
        collection: Option<ScopeId>,
        query: metadata_query::Query,
    ) -> Result<metadata_query::Page> {
        match (query.level, collection) {
            (Level::Collection, None) => {
                let snapshot = self.tenant_catalog_snapshot(tenant).await?;
                tokio::task::spawn_blocking(move || {
                    metadata_query::read_catalog(snapshot.as_ref().map(|db| db.path()), query)
                })
                .await
                .map_err(|error| Error::Internal(Box::new(error)))?
            }
            (Level::Source | Level::Chunk, Some(collection)) => {
                let scope = Scope {
                    tenant: tenant.clone(),
                    collection,
                };
                let values = if query.uses_collection_metadata() {
                    self.metadata(&scope, None).await?
                } else {
                    self.require_collection(&scope).await?;
                    Vec::new()
                };
                let snapshot = self.collection_snapshot(&scope).await?;
                let collection = scope.collection.to_string();
                tokio::task::spawn_blocking(move || {
                    db::CollectionDb::open_read_only(snapshot.path())?.query_metadata(
                        &collection,
                        query,
                        &values,
                    )
                })
                .await
                .map_err(|error| Error::Internal(Box::new(error)))?
            }
            _ => Err(Error::Invalid(
                "collection is required only for source or chunk metadata queries".into(),
            )),
        }
    }

    pub async fn change_cursor(&self, scope: &Scope) -> Result<u64> {
        self.with_collection(scope, CollectionDb::change_cursor)
            .await
            .map(|(cursor, _)| cursor)
    }

    pub async fn changes(&self, scope: &Scope, after: u64, limit: u32) -> Result<Vec<Change>> {
        self.poll_changes(scope, after, limit, None)
            .await
            .map(|(changes, _)| changes)
    }

    /// Checks both published manifests. An unchanged generation can reuse
    /// confirmed membership or a caught-up cursor from the previous poll.
    pub async fn poll_changes(
        &self,
        scope: &Scope,
        after: u64,
        limit: u32,
        previous: Option<&HostedChangePosition>,
    ) -> Result<(Vec<Change>, HostedChangePosition)> {
        super::validate_change_limit(limit)?;
        let previous = previous.filter(|position| {
            position.scope.tenant == scope.tenant && position.scope.collection == scope.collection
        });
        let catalog = Unit::catalog(self.prefix.as_ref(), &scope.tenant)?;
        let catalog_generation = self
            .publication
            .generation(&catalog)
            .await?
            .ok_or_else(|| Error::NotFound("tenant catalog was not found".into()))?;
        let catalog_id = catalog_generation.id;
        if previous.is_none_or(|position| position.catalog_generation != catalog_id) {
            let snapshot = self
                .snapshots
                .read_generation(&self.publication, &catalog, catalog_generation)
                .await?;
            let collection = scope.collection.to_string();
            tokio::task::spawn_blocking(move || {
                db::require_collection(snapshot.path(), &collection)
            })
            .await
            .map_err(|error| Error::Internal(Box::new(error)))??;
        }
        let unit = Unit::collection(self.prefix.as_ref(), scope)?;
        let generation = self
            .publication
            .generation(&unit)
            .await?
            .ok_or_else(|| Error::NotFound("collection database was not found".into()))?;
        let generation_id = generation.id;
        let changes = if previous
            .is_some_and(|position| position.caught_up == Some((generation_id, after)))
        {
            Vec::new()
        } else {
            let snapshot = self
                .snapshots
                .read_generation(&self.publication, &unit, generation)
                .await?;
            query_snapshot(snapshot, move |database| database.changes(after, limit))
                .await?
                .0
        };
        let caught_up = (changes.len() < limit as usize).then(|| {
            (
                generation_id,
                changes.last().map_or(after, |change| change.cursor),
            )
        });
        Ok((
            changes,
            HostedChangePosition {
                scope: scope.clone(),
                catalog_generation: catalog_id,
                caught_up,
            },
        ))
    }
}
