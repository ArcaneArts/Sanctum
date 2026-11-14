use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use sanctum_core::{Error, Result, Scope, ScopeId};
use sanctum_db::{collection_prefix, tenant_prefix};
use sanctum_storage::{Key, ListPage};
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
    #[serde(flatten)]
    position: ActivityPosition,
}

#[derive(Deserialize, Serialize)]
#[serde(untagged)]
enum ActivityPosition {
    Collection {
        collection: String,
        after: Option<String>,
    },
    Objects {
        token: String,
    },
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
                self.activity_prefix(&scope)?
            }
            None => {
                let root = tenant_prefix(self.prefix.as_ref(), tenant)?;
                Key::parse(format!("{root}/collections"))?
            }
        };
        let position = cursor
            .map(|cursor| decode_cursor(&prefix, cursor))
            .transpose()?;
        let next = match (collection, position) {
            (_, Some(ActivityPosition::Objects { token })) => Some(token),
            (None, Some(ActivityPosition::Collection { collection, after })) => {
                return self
                    .tenant_activity(tenant, &prefix, collection, after, limit)
                    .await;
            }
            (None, None) => {
                return self
                    .tenant_activity(tenant, &prefix, String::new(), None, limit)
                    .await;
            }
            (Some(_), None) => None,
            (Some(_), Some(ActivityPosition::Collection { .. })) => return Err(invalid_cursor()),
        };
        self.object_activity(tenant, collection, &prefix, next, limit)
            .await
    }

    async fn tenant_activity(
        &self,
        tenant: &ScopeId,
        prefix: &Key,
        first: String,
        mut after: Option<String>,
        limit: u32,
    ) -> Result<HostedActivityPage> {
        let mut jobs = Vec::with_capacity(limit as usize);
        let Some(snapshot) = self.tenant_catalog_snapshot(tenant).await? else {
            return Ok(HostedActivityPage { jobs, next: None });
        };
        let resume = first.clone();
        // Keep one extra ID so a page can resume at the next unvisited collection.
        let collections = tokio::task::spawn_blocking(move || {
            db::catalog_collection_ids_from(snapshot.path(), &first, MAX_PAGES as u32 + 1)
        })
        .await
        .map_err(|error| Error::Internal(Box::new(error)))??;
        let mut pages_left = MAX_PAGES;
        for collection in collections {
            let scope = Scope {
                tenant: tenant.clone(),
                collection: collection.parse()?,
            };
            let operations = self.activity_prefix(&scope)?;
            let mut next = if collection == resume {
                after.take()
            } else {
                None
            };
            loop {
                if pages_left == 0 || jobs.len() == limit as usize {
                    let position = ActivityPosition::Collection {
                        collection,
                        after: next,
                    };
                    return Ok(HostedActivityPage {
                        jobs,
                        next: Some(encode_cursor(prefix, position)?),
                    });
                }
                pages_left -= 1;
                let page = self
                    .list_activity_page(&operations, next.as_deref(), limit - jobs.len() as u32)
                    .await?;
                let candidates = page.objects.into_iter().filter_map(|object| {
                    operation_path(&operations, &object.key, tenant, Some(&scope.collection))
                });
                self.append_activity(candidates, &mut jobs).await?;
                next = page.next_cursor;
                if next.is_none() {
                    break;
                }
            }
        }
        Ok(HostedActivityPage { jobs, next: None })
    }

    // Also resumes tenant-wide cursors written before scans used the live catalog.
    async fn object_activity(
        &self,
        tenant: &ScopeId,
        collection: Option<&ScopeId>,
        prefix: &Key,
        mut next: Option<String>,
        limit: u32,
    ) -> Result<HostedActivityPage> {
        let mut jobs = Vec::with_capacity(limit as usize);
        let mut catalog = None;
        for _ in 0..MAX_PAGES {
            let remaining = limit - jobs.len() as u32;
            let page = self
                .list_activity_page(prefix, next.as_deref(), remaining)
                .await?;
            let mut candidates: Vec<_> = page
                .objects
                .into_iter()
                .filter_map(|object| operation_path(prefix, &object.key, tenant, collection))
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
            self.append_activity(candidates, &mut jobs).await?;
            next = page.next_cursor;
            if next.is_none() || jobs.len() >= limit as usize {
                break;
            }
        }
        Ok(HostedActivityPage {
            jobs,
            next: next
                .map(|token| encode_cursor(prefix, ActivityPosition::Objects { token }))
                .transpose()?,
        })
    }

    fn activity_prefix(&self, scope: &Scope) -> Result<Key> {
        let root = collection_prefix(self.prefix.as_ref(), scope)?;
        Key::parse(format!("{root}/operations"))
    }

    async fn list_activity_page(
        &self,
        prefix: &Key,
        next: Option<&str>,
        remaining: u32,
    ) -> Result<ListPage> {
        // Consume the provider page in full so its token cannot skip a job past the limit.
        let page = self.storage.list(Some(prefix), next, remaining).await?;
        if page.next_cursor.is_some() && page.next_cursor.as_deref() == next {
            return Err(Error::Internal("job list cursor did not advance".into()));
        }
        Ok(page)
    }

    async fn append_activity(
        &self,
        candidates: impl IntoIterator<Item = (Scope, Uuid)>,
        jobs: &mut Vec<(Scope, HostedJob)>,
    ) -> Result<()> {
        for (scope, operation) in candidates {
            let operation_prefix = self.operation_prefix(&scope, operation)?;
            if let Some((record, _)) = self.read_operation(&operation_prefix, operation).await? {
                jobs.push((scope, record.job()));
            }
        }
        Ok(())
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

fn encode_cursor(prefix: &Key, position: ActivityPosition) -> Result<String> {
    let cursor = ActivityCursor {
        prefix: prefix.to_string(),
        position,
    };
    let json = serde_json::to_vec(&cursor).map_err(|error| Error::Internal(Box::new(error)))?;
    if json.len() > MAX_TOKEN_BYTES {
        return Err(Error::Internal(
            "storage activity cursor is too large".into(),
        ));
    }
    Ok(URL_SAFE_NO_PAD.encode(json))
}

fn decode_cursor(prefix: &Key, cursor: &str) -> Result<ActivityPosition> {
    if cursor.len() > MAX_TOKEN_BYTES * 2 {
        return Err(invalid_cursor());
    }
    let data = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| invalid_cursor())?;
    if data.len() > MAX_TOKEN_BYTES {
        return Err(invalid_cursor());
    }
    let cursor: ActivityCursor = serde_json::from_slice(&data).map_err(|_| invalid_cursor())?;
    if cursor.prefix != prefix.as_str() {
        return Err(invalid_cursor());
    }
    match &cursor.position {
        ActivityPosition::Objects { token } if !token.is_empty() => {}
        ActivityPosition::Collection { collection, after }
            if collection.parse::<ScopeId>().is_ok()
                && !after.as_deref().is_some_and(str::is_empty) => {}
        _ => return Err(invalid_cursor()),
    }
    Ok(cursor.position)
}

fn invalid_cursor() -> Error {
    Error::Invalid("invalid hosted activity cursor for this scope".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_the_original_tenant_activity_cursor() {
        let prefix = Key::parse("managed/default/collections").unwrap();
        let saved = URL_SAFE_NO_PAD
            .encode(br#"{"prefix":"managed/default/collections","token":"provider-token"}"#);
        assert!(matches!(decode_cursor(&prefix, &saved),
            Ok(ActivityPosition::Objects { token }) if token == "provider-token"));
    }
}
