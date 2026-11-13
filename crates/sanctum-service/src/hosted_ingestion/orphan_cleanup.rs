use std::time::{Duration, SystemTime};

use sanctum_core::{Error, Result, Scope};
use sanctum_storage::{Condition, Key, MAX_CONTROL_BYTES, Object};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use uuid::Uuid;

use super::{AcceptanceFence, Envelope, HostedCatalog, REGISTRATION_WINDOW, envelope_key};

const READER_GRACE: Duration = Duration::from_secs(900);
const SCAN_PAGE: u32 = 64;
const CANDIDATE_LIMIT: usize = 8;

#[derive(Default, Serialize, Deserialize)]
struct Cursor {
    next: Option<String>,
    pending: Vec<String>,
}

pub struct UploadSweep {
    pub deleted: usize,
    pub failed: usize,
    pub more: bool,
}

enum Candidate {
    Input { operation: Uuid, attempt: Uuid },
    Extracted { source: Uuid, revision: Uuid },
}

impl HostedCatalog {
    pub async fn sweep_abandoned_uploads(&self) -> Result<UploadSweep> {
        let path = "_maintenance/upload-sweep.json";
        let key = Key::parse(
            self.prefix
                .as_ref()
                .map_or_else(|| path.into(), |prefix| format!("{prefix}/{path}")),
        )?;
        let (mut cursor, condition) = match self.storage.get(&key).await {
            Ok(download) => {
                let size = download.object.length;
                if size > MAX_CONTROL_BYTES as u64 {
                    return Err(Error::Internal(
                        "upload sweep cursor exceeds size limit".into(),
                    ));
                }
                let mut bytes = Vec::new();
                download
                    .reader
                    .take(size + 1)
                    .read_to_end(&mut bytes)
                    .await?;
                if bytes.len() as u64 != size {
                    return Err(Error::Internal(
                        "upload sweep cursor length mismatch".into(),
                    ));
                }
                let cursor = serde_json::from_slice(&bytes)
                    .map_err(|error| Error::Internal(Box::new(error)))?;
                (cursor, Condition::Match(download.object.version))
            }
            Err(Error::NotFound(_)) => (Cursor::default(), Condition::Create),
            Err(error) => return Err(error),
        };
        if cursor.pending.is_empty() {
            let page = self
                .storage
                .list(self.prefix.as_ref(), cursor.next.as_deref(), SCAN_PAGE)
                .await?;
            if page.next_cursor.is_some() && page.next_cursor == cursor.next {
                return Err(Error::Internal(
                    "upload sweep cursor did not advance".into(),
                ));
            }
            let now = SystemTime::now();
            cursor.pending = page
                .objects
                .into_iter()
                .filter_map(|object| {
                    let (_, candidate) = candidate(self.prefix.as_ref(), &object.key)?;
                    old_enough(&object, &candidate, now).then(|| object.key.to_string())
                })
                .collect();
            cursor.next = page.next_cursor;
        }
        let mut report = UploadSweep {
            deleted: 0,
            failed: 0,
            more: false,
        };
        for _ in 0..CANDIDATE_LIMIT.min(cursor.pending.len()) {
            let object = Key::parse(cursor.pending.pop().expect("work is bounded by the queue"))?;
            let Some((scope, candidate)) = candidate(self.prefix.as_ref(), &object) else {
                continue;
            };
            match self.reclaim_candidate(&scope, &object, candidate).await {
                Ok(true) => report.deleted += 1,
                Ok(false) | Err(Error::NotFound(_)) => {}
                Err(error) => {
                    report.failed += 1;
                    tracing::warn!(%error, key = %object, "abandoned upload sweep will retry on the next pass");
                }
            }
        }
        report.more = cursor.next.is_some() || !cursor.pending.is_empty();
        self.storage
            .put_control(
                &key,
                serde_json::to_vec(&cursor)
                    .map_err(|error| Error::Internal(Box::new(error)))?
                    .into(),
                condition,
            )
            .await?;
        Ok(report)
    }

    async fn reclaim_candidate(
        &self,
        scope: &Scope,
        object: &Key,
        candidate: Candidate,
    ) -> Result<bool> {
        let metadata = self.storage.head(object).await?;
        if !old_enough(&metadata, &candidate, SystemTime::now()) {
            return Ok(false);
        }
        let abandoned = match candidate {
            Candidate::Input { operation, attempt } => {
                let prefix = self.operation_prefix(scope, operation)?;
                match self.read_envelope(&prefix, operation).await? {
                    Some((Envelope::Accepted(record), _)) => record.attempt != attempt,
                    existing => {
                        let condition = existing
                            .map_or(Condition::Create, |(_, version)| Condition::Match(version));
                        // Compete with admission on the same key. If acceptance wins,
                        // its reference must be checked afresh on a later pass.
                        let fence = AcceptanceFence {
                            operation,
                            fence_id: Uuid::new_v4(),
                        };
                        let bytes = serde_json::to_vec(&fence)
                            .map_err(|error| Error::Internal(Box::new(error)))?;
                        match self
                            .storage
                            .put_control(&envelope_key(&prefix)?, bytes.into(), condition)
                            .await
                        {
                            Ok(_) => true,
                            Err(Error::Conflict(_)) => false,
                            Err(error) => return Err(error),
                        }
                    }
                }
            }
            Candidate::Extracted { source, revision } => {
                let object = object.to_string();
                let (referenced, _) = self
                    .with_collection(scope, move |database| {
                        database.has_hosted_text_object(source, revision, &object)
                    })
                    .await?;
                !referenced
            }
        };
        if abandoned {
            self.storage.delete(object).await?;
        }
        Ok(abandoned)
    }
}

fn old_enough(object: &Object, candidate: &Candidate, now: SystemTime) -> bool {
    let minimum = match candidate {
        Candidate::Input { .. } => READER_GRACE,
        // Registration starts before extraction. Even a delayed index writer has
        // expired by now and cannot introduce this locator into a new snapshot.
        Candidate::Extracted { .. } => REGISTRATION_WINDOW + READER_GRACE,
    };
    now.duration_since(object.modified_at)
        .is_ok_and(|age| age >= minimum)
}

fn candidate(prefix: Option<&Key>, key: &Key) -> Option<(Scope, Candidate)> {
    let path = match prefix {
        Some(prefix) => key
            .as_str()
            .strip_prefix(prefix.as_str())?
            .strip_prefix('/')?,
        None => key.as_str(),
    };
    let mut parts = path.split('/');
    let tenant = parts.next()?.parse().ok()?;
    if parts.next()? != "collections" {
        return None;
    }
    let collection = parts.next()?.parse().ok()?;
    let candidate = match parts.next()? {
        "operations" => {
            let operation = parts.next()?.parse().ok()?;
            if parts.next()? != "inputs" {
                return None;
            }
            Candidate::Input {
                operation,
                attempt: parts.next()?.parse().ok()?,
            }
        }
        "sources" => {
            let source = parts.next()?.parse().ok()?;
            let revision = parts.next()?.parse().ok()?;
            if parts.next()? != "objects" {
                return None;
            }
            parts.next()?.parse::<Uuid>().ok()?;
            if parts.next()? != "text.utf8" {
                return None;
            }
            Candidate::Extracted { source, revision }
        }
        _ => return None,
    };
    if parts.next().is_some() {
        return None;
    }
    Some((Scope { tenant, collection }, candidate))
}
