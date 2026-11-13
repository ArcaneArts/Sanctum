use sanctum_core::{Error, Result, Scope};
use sanctum_db::{Command, Unit, command_receipt};
use sanctum_db_sqlite::hosted::{Registration, release_canceled_intent};
use sanctum_storage::{Key, Version};
use uuid::Uuid;

use super::{
    HostedCatalog, HostedJob, Operation, REGISTRATION_WINDOW, RegistrationOutcome, Stage,
    indexing::{INDEX_STAGE, Outcome, decode},
    unix_seconds,
};

impl HostedCatalog {
    pub async fn cancel_upload(&self, scope: &Scope, operation: Uuid) -> Result<HostedJob> {
        self.require_collection(scope).await?;
        let prefix = self.operation_prefix(scope, operation)?;
        let (record, version) = self.require_operation(&prefix, operation).await?;
        match record.stage {
            Stage::Canceled { .. } => {
                self.clear_dispatch_hint(scope, operation).await?;
                return Ok(record.job());
            }
            Stage::PendingRegistration | Stage::PendingIndexing { .. } => {}
            _ => return Err(finished()),
        }
        let unit = Unit::collection(self.prefix.as_ref(), scope)?;
        let expires_at = record
            .created_at
            .saturating_add(REGISTRATION_WINDOW.as_secs());
        let mut writer = self.publication.acquire(&unit).await?;
        // Cancellation and indexing share the same receipt. Only the first
        // fenced publication chooses an outcome. The envelope follows it.
        let result = writer
            .apply_stage(
                INDEX_STAGE,
                vec![Command {
                    operation,
                    payload: record.revision,
                }],
                move |tx, revision| {
                    if unix_seconds()? >= expires_at {
                        return Err(Error::Conflict("the cancellation window expired".into()));
                    }
                    let registration = match command_receipt(tx, operation, "")?
                        .map(|receipt| super::decode_registration(&receipt))
                        .transpose()?
                    {
                        Some(RegistrationOutcome::Registered(registration)) => Some(registration),
                        Some(RegistrationOutcome::Failed { .. }) => return Err(finished()),
                        Some(RegistrationOutcome::Canceled) | None => None,
                    };
                    if let Some(registration) = &registration {
                        release_canceled_intent(tx, operation, revision, registration.source)?;
                    }
                    serde_json::to_string(&Outcome::Canceled(registration))
                        .map_err(|error| Error::Internal(Box::new(error)))
                },
            )
            .await;
        let released = writer.release().await;
        let (_, receipts) = result?;
        released?;
        let Outcome::Canceled(registration) = decode(&receipts[0].outcome)? else {
            return Err(finished());
        };
        self.checkpoint_canceled(scope, &prefix, record, version, registration)
            .await
    }

    pub(super) async fn checkpoint_canceled(
        &self,
        scope: &Scope,
        prefix: &Key,
        mut record: Operation,
        version: Version,
        registration: Option<Registration>,
    ) -> Result<HostedJob> {
        let operation = record.operation;
        self.discard_input(prefix, &record).await?;
        record.dispatched_at = None;
        record.stage = Stage::Canceled { registration };
        let job = self
            .checkpoint(prefix, record, version, |stage| {
                matches!(
                    stage,
                    Stage::PendingRegistration | Stage::PendingIndexing { .. }
                )
            })
            .await?;
        self.clear_dispatch_hint(scope, operation).await?;
        Ok(job)
    }
}

pub(super) fn index_was_canceled(receipt: Option<&str>) -> Result<bool> {
    let outcome = receipt.map(decode).transpose()?;
    Ok(matches!(outcome, Some(Outcome::Canceled(_))))
}

fn finished() -> Error {
    Error::Conflict("only accepted or registered operations can be canceled".into())
}
