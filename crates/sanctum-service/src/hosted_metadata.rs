use sanctum_core::{Error, Result, Scope, ScopeId};
use sanctum_db::{Command, Unit};
use uuid::Uuid;

use crate::{
    Change, HostedCatalog, MetadataEntry, MetadataMutation, RevisionTarget,
    db::{CollectionDb, metadata},
    hosted_retrieval::query_snapshot,
    metadata_filter::Level,
    metadata_query,
};

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
                let unit = Unit::catalog(self.prefix.as_ref(), tenant)?;
                let snapshot = self.snapshots.read(&self.publication, &unit).await?;
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
                let values = self.metadata(&scope, None).await?;
                let collection = scope.collection.to_string();
                self.with_owned_collection(&scope, move |database| {
                    database.query_metadata(&collection, query, &values)
                })
                .await
                .map(|(page, _)| page)
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

    /// A stream caught up to a generation can skip querying it again.
    pub async fn poll_changes(
        &self,
        scope: &Scope,
        after: u64,
        limit: u32,
        caught_up_to: Option<Uuid>,
    ) -> Result<(Vec<Change>, Uuid)> {
        super::validate_change_limit(limit)?;
        self.require_collection(scope).await?;
        let unit = Unit::collection(self.prefix.as_ref(), scope)?;
        let generation = self
            .publication
            .generation(&unit)
            .await?
            .ok_or_else(|| Error::NotFound("collection database was not found".into()))?;
        if caught_up_to == Some(generation.id) {
            return Ok((Vec::new(), generation.id));
        }
        let snapshot = self
            .snapshots
            .read_generation(&self.publication, &unit, generation)
            .await?;
        query_snapshot(snapshot, move |database| database.changes(after, limit)).await
    }
}
