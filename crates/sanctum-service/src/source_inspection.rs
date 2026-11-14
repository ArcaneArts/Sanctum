use sanctum_core::{Error, Result, Scope};
pub use sanctum_db_sqlite::SourceStatus;
use uuid::Uuid;

use crate::{HostedCatalog, Revision, Service};

impl Service {
    pub fn list_sources(
        &self,
        scope: &Scope,
        after: Option<Uuid>,
        limit: u32,
    ) -> Result<Vec<Revision>> {
        validate_limit(limit)?;
        self.database(scope)?.list_sources(after, limit)
    }

    pub fn source(&self, scope: &Scope, source: Uuid, expected: Option<Uuid>) -> Result<Revision> {
        self.database(scope)?.source(source, expected)
    }

    pub fn source_status(
        &self,
        scope: &Scope,
        source: Uuid,
        expected: Option<Uuid>,
    ) -> Result<SourceStatus> {
        self.database(scope)?
            .source_status(source, expected, self.embedding_profile())
    }
}

impl HostedCatalog {
    pub async fn list_sources(
        &self,
        scope: &Scope,
        after: Option<Uuid>,
        limit: u32,
    ) -> Result<(Vec<Revision>, Uuid)> {
        validate_limit(limit)?;
        self.with_collection(scope, move |db| db.list_sources(after, limit))
            .await
    }

    pub async fn source(
        &self,
        scope: &Scope,
        source: Uuid,
        expected: Option<Uuid>,
    ) -> Result<(Revision, Uuid)> {
        self.with_collection(scope, move |db| db.source(source, expected))
            .await
    }

    pub async fn source_status(
        &self,
        scope: &Scope,
        source: Uuid,
        expected: Option<Uuid>,
    ) -> Result<(SourceStatus, Uuid)> {
        let profile = self.embedding_profile().map(str::to_owned);
        self.with_collection(scope, move |db| {
            db.source_status(source, expected, profile.as_deref())
        })
        .await
    }
}

fn validate_limit(limit: u32) -> Result<()> {
    if !(1..=101).contains(&limit) {
        return Err(Error::Invalid(
            "source page size must be between 1 and 101".into(),
        ));
    }
    Ok(())
}
