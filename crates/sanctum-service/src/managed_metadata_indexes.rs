use sanctum_core::{Error, Result, Scope};
use sanctum_db::Command;
use uuid::Uuid;

use crate::{
    DatabaseTarget, HostedCatalog, Service, db,
    metadata_indexes::{self, Edit, Index},
};

impl Service {
    pub fn metadata_indexes(&self, target: DatabaseTarget) -> Result<Vec<Index>> {
        let _reader = self
            .retrieval
            .read()
            .map_err(|_| Error::Busy("retrieval lock is unavailable".into()))?;
        match target.collection {
            Some(collection) => self
                .database(&Scope {
                    tenant: target.tenant,
                    collection,
                })?
                .metadata_indexes(),
            None => metadata_indexes::list_catalog(&self.files.catalog_path(&target.tenant)),
        }
    }

    pub fn edit_metadata_index(&self, target: DatabaseTarget, edit: Edit) -> Result<()> {
        edit.validate(target.collection.is_some())?;
        let _reader = self
            .retrieval
            .read()
            .map_err(|_| Error::Busy("retrieval lock is unavailable".into()))?;
        match target.collection {
            Some(collection) => {
                let scope = Scope {
                    tenant: target.tenant,
                    collection,
                };
                self.local_writes
                    .run(&scope, || self.database(&scope)?.edit_metadata_index(edit))
            }
            None => metadata_indexes::edit_catalog(&self.files.catalog_path(&target.tenant), edit),
        }
    }
}

impl HostedCatalog {
    pub async fn metadata_indexes(&self, target: DatabaseTarget) -> Result<Vec<Index>> {
        if let Some(collection) = target.collection {
            let scope = Scope {
                tenant: target.tenant,
                collection,
            };
            return self
                .with_collection(&scope, db::CollectionDb::metadata_indexes)
                .await
                .map(|(indexes, _)| indexes);
        }
        let unit = self.database_unit(&target)?;
        let snapshot = self
            .snapshots
            .read(&self.publication, &unit)
            .await?
            .ok_or_else(|| Error::NotFound("tenant catalog was not found".into()))?;
        tokio::task::spawn_blocking(move || metadata_indexes::list_catalog(snapshot.path()))
            .await
            .map_err(|error| Error::Internal(Box::new(error)))?
    }

    pub async fn edit_metadata_index(&self, target: DatabaseTarget, edit: Edit) -> Result<()> {
        let collection_database = target.collection.is_some();
        edit.validate(collection_database)?;
        if let Some(collection) = &target.collection {
            self.require_collection(&Scope {
                tenant: target.tenant.clone(),
                collection: collection.clone(),
            })
            .await?;
        }
        let unit = self.database_unit(&target)?;
        let mut writer = self.publication.acquire_existing(&unit).await?;
        let result = writer
            .apply_stage(
                "metadata_index",
                vec![Command {
                    operation: Uuid::new_v4(),
                    payload: edit,
                }],
                move |tx, edit| {
                    if collection_database {
                        db::initialize_collection(tx)?;
                    } else {
                        db::initialize_catalog(tx)?;
                    }
                    metadata_indexes::apply(tx, edit)?;
                    Ok("applied".into())
                },
            )
            .await;
        let release = writer.release().await;
        result?;
        release
    }
}
