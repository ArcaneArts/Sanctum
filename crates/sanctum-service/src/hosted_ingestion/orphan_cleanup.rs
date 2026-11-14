use std::time::{Duration, SystemTime};

use sanctum_core::{Error, Result, Scope, is_rtf};
use sanctum_storage::{Condition, Key, MAX_CONTROL_BYTES, Object};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use uuid::Uuid;

use super::{
    AcceptanceFence, Envelope, ExpiredOperation, HostedCatalog, REGISTRATION_WINDOW, Stage,
    envelope_key, unix_seconds,
};

const READER_GRACE: Duration = Duration::from_secs(900);
const OPERATION_RETENTION: Duration = Duration::from_secs(45 * 24 * 3600);
const SCAN_PAGE: u32 = 64;
const CANDIDATE_LIMIT: usize = 8;

#[derive(Default, Serialize, Deserialize)]
struct Cursor {
    next: Option<String>,
    pending: Vec<String>,
}

pub struct UploadSweep {
    pub deleted: usize,
    pub expired: usize,
    pub failed: usize,
    pub more: bool,
}

enum Candidate {
    Input { operation: Uuid, attempt: Uuid },
    Operation { operation: Uuid },
    Extracted { source: Uuid, revision: Uuid },
    PreparedEmbedding,
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
            expired: 0,
            failed: 0,
            more: false,
        };
        for _ in 0..CANDIDATE_LIMIT.min(cursor.pending.len()) {
            let object = Key::parse(cursor.pending.pop().expect("work is bounded by the queue"))?;
            let Some((scope, candidate)) = candidate(self.prefix.as_ref(), &object) else {
                continue;
            };
            let result = match candidate {
                Candidate::Operation { operation } => self
                    .expire_operation(&scope, &object, operation)
                    .await
                    .map(|expired| {
                        report.expired += usize::from(expired);
                    }),
                candidate => self
                    .reclaim_candidate(&scope, &object, candidate)
                    .await
                    .map(|deleted| {
                        report.deleted += usize::from(deleted);
                    }),
            };
            match result {
                Ok(()) | Err(Error::NotFound(_)) => {}
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
        tracing::debug!(
            event = "cleanup_pass",
            kind = "hosted_upload",
            deleted_objects = report.deleted,
            expired = report.expired,
            failed = report.failed,
            more = report.more,
            "completed a bounded cleanup pass"
        );
        Ok(report)
    }

    async fn expire_operation(&self, scope: &Scope, object: &Key, operation: Uuid) -> Result<bool> {
        let head = self.storage.head(object).await?;
        if !old_enough(
            &head,
            &Candidate::Operation { operation },
            SystemTime::now(),
        ) {
            return Ok(false);
        }
        let prefix = self.operation_prefix(scope, operation)?;
        let Some((Envelope::Accepted(record), version)) =
            self.read_envelope(&prefix, operation).await?
        else {
            return Ok(false);
        };
        if version != head.version {
            return Ok(false);
        }
        let retained_attempt = match record.stage {
            Stage::TextReady {
                embedding: None, ..
            } => (!is_rtf(&record.upload.name) || record.upload.retain_original)
                .then_some(record.attempt),
            Stage::Canceled { .. } | Stage::Failed { .. } => None,
            _ => return Ok(false),
        };
        self.clear_dispatch_hint(scope, operation).await?;
        let compact = ExpiredOperation {
            operation,
            expired_at: unix_seconds()?,
            retained_attempt,
        };
        let body =
            serde_json::to_vec(&compact).map_err(|error| Error::Internal(Box::new(error)))?;
        match self
            .storage
            .put_control(object, body.into(), Condition::Match(version))
            .await
        {
            Ok(_) => Ok(true),
            Err(Error::Conflict(_)) => Ok(false),
            Err(error) => Err(error),
        }
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
                    Some((Envelope::Expired(record), _)) => {
                        record.retained_attempt != Some(attempt)
                    }
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
            Candidate::Operation { .. } => return Ok(false),
            // Completed batches are deleted after publication. Abandoned results
            // can expire independently because later deliveries can compute again.
            Candidate::PreparedEmbedding => true,
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
        Candidate::Operation { .. } | Candidate::PreparedEmbedding => OPERATION_RETENTION,
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
            match parts.next()? {
                "inputs" => Candidate::Input {
                    operation,
                    attempt: parts.next()?.parse().ok()?,
                },
                "operation.json" => Candidate::Operation { operation },
                _ => return None,
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
        "prepared-embeddings" => {
            parts.next()?.strip_suffix(".json")?.parse::<Uuid>().ok()?;
            Candidate::PreparedEmbedding
        }
        _ => return None,
    };
    if parts.next().is_some() {
        return None;
    }
    Some((Scope { tenant, collection }, candidate))
}
