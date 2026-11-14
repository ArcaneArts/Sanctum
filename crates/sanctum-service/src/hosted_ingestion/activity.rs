use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use sanctum_core::{Error, Result, Scope, ScopeId};
use sanctum_db::{collection_prefix, tenant_prefix};
use sanctum_storage::Key;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{HostedCatalog, HostedJob};
use crate::db;

const MAX_PAGES: usize = 8;
const MAX_TOKEN_BYTES: usize = 4096;

pub struct HostedActivityPage {
    pub jobs: Vec<(Scope, HostedJob)>,
    pub next: Option<String>,
}

#[derive(Deserialize, Serialize)]
struct ActivityCursor {
    prefix: String,
    token: String,
}

impl HostedCatalog {
    pub async fn ingestion_activity(
        &self,
        tenant: &ScopeId,
        collection: Option<&ScopeId>,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<HostedActivityPage> {
        if !(1..=100).contains(&limit) {
            return Err(Error::Invalid(
                "activity limit must be between 1 and 100".into(),
            ));
        }
        let prefix = match collection {
            Some(collection) => {
                let scope = Scope {
                    tenant: tenant.clone(),
                    collection: collection.clone(),
                };
                self.require_collection(&scope).await?;
                let root = collection_prefix(self.prefix.as_ref(), &scope)?;
                Key::parse(format!("{root}/operations"))?
            }
            None => {
                let root = tenant_prefix(self.prefix.as_ref(), tenant)?;
                Key::parse(format!("{root}/collections"))?
            }
        };
        let mut next = cursor
            .map(|cursor| decode_cursor(&prefix, cursor))
            .transpose()?;
        let mut jobs = Vec::with_capacity(limit as usize);
        let mut catalog = None;
        for _ in 0..MAX_PAGES {
            // Each object can produce at most one job. Consume the provider page
            // in full so its token never skips results past the requested limit.
            let remaining = limit - jobs.len() as u32;
            let page = self
                .storage
                .list(Some(&prefix), next.as_deref(), remaining)
                .await?;
            if page.next_cursor.is_some() && page.next_cursor == next {
                return Err(Error::Internal("job list cursor did not advance".into()));
            }
            let mut candidates: Vec<_> = page
                .objects
                .into_iter()
                .filter_map(|object| operation_path(&prefix, &object.key, tenant, collection))
                .collect();
            if collection.is_none() && !candidates.is_empty() {
                if catalog.is_none() {
                    catalog = self.tenant_catalog_snapshot(tenant).await?;
                }
                let Some(snapshot) = catalog.clone() else {
                    return Ok(HostedActivityPage { jobs, next: None });
                };
                candidates = tokio::task::spawn_blocking(move || -> Result<Vec<_>> {
                    let collections = candidates
                        .iter()
                        .map(|(scope, _)| scope.collection.as_str());
                    let membership = db::catalog_membership(snapshot.path(), collections)?;
                    Ok(candidates
                        .into_iter()
                        .zip(membership)
                        .filter_map(|(candidate, live)| live.then_some(candidate))
                        .collect())
                })
                .await
                .map_err(|error| Error::Internal(Box::new(error)))??;
            }
            for (scope, operation) in candidates {
                let operation_prefix = self.operation_prefix(&scope, operation)?;
                if let Some((record, _)) = self.read_operation(&operation_prefix, operation).await?
                {
                    jobs.push((scope, record.job()));
                }
            }
            next = page.next_cursor;
            if next.is_none() || jobs.len() >= limit as usize {
                break;
            }
        }
        Ok(HostedActivityPage {
            jobs,
            next: next
                .map(|token| encode_cursor(&prefix, token))
                .transpose()?,
        })
    }
}

fn operation_path(
    prefix: &Key,
    key: &Key,
    tenant: &ScopeId,
    collection: Option<&ScopeId>,
) -> Option<(Scope, Uuid)> {
    let path = key
        .as_str()
        .strip_prefix(prefix.as_str())?
        .strip_prefix('/')?;
    let mut parts = path.split('/');
    let collection = match collection {
        Some(collection) => collection.clone(),
        None => {
            let collection = parts.next()?.parse().ok()?;
            if parts.next()? != "operations" {
                return None;
            }
            collection
        }
    };
    let operation = parts.next()?.parse().ok()?;
    if parts.next()? != "operation.json" || parts.next().is_some() {
        return None;
    }
    Some((
        Scope {
            tenant: tenant.clone(),
            collection,
        },
        operation,
    ))
}

fn encode_cursor(prefix: &Key, token: String) -> Result<String> {
    let cursor = ActivityCursor {
        prefix: prefix.to_string(),
        token,
    };
    let json = serde_json::to_vec(&cursor).map_err(|error| Error::Internal(Box::new(error)))?;
    if json.len() > MAX_TOKEN_BYTES {
        return Err(Error::Internal(
            "storage activity cursor is too large".into(),
        ));
    }
    Ok(URL_SAFE_NO_PAD.encode(json))
}

fn decode_cursor(prefix: &Key, cursor: &str) -> Result<String> {
    let invalid = || Error::Invalid("invalid hosted activity cursor for this scope".into());
    if cursor.len() > MAX_TOKEN_BYTES * 2 {
        return Err(invalid());
    }
    let data = URL_SAFE_NO_PAD.decode(cursor).map_err(|_| invalid())?;
    if data.len() > MAX_TOKEN_BYTES {
        return Err(invalid());
    }
    let cursor: ActivityCursor = serde_json::from_slice(&data).map_err(|_| invalid())?;
    if cursor.prefix != prefix.as_str() || cursor.token.is_empty() {
        return Err(invalid());
    }
    Ok(cursor.token)
}
