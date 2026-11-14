use std::{future::Future, time::Duration};

use sanctum_core::{Error, Result};
use tokio::time::{Instant, timeout, timeout_at};

pub(crate) const REQUEST_TIMEOUT: Duration = Duration::from_secs(240);
const WORK_TIMEOUT: Duration = Duration::from_secs(210);
const ITEM_TIMEOUT: Duration = Duration::from_secs(180);

pub(crate) struct SweepBudget {
    work_deadline: Instant,
    item_timeout: Duration,
}

impl SweepBudget {
    pub(crate) fn start() -> Self {
        Self {
            work_deadline: Instant::now() + WORK_TIMEOUT,
            item_timeout: ITEM_TIMEOUT,
        }
    }

    #[cfg(test)]
    pub(crate) fn exhausted() -> Self {
        Self {
            work_deadline: Instant::now(),
            item_timeout: ITEM_TIMEOUT,
        }
    }

    // None keeps the current item in the checkpoint. A full attempt that
    // fails can be rediscovered in a later pass without blocking later items.
    pub(crate) async fn attempt<T>(
        &self,
        work: impl Future<Output = Result<T>>,
    ) -> Result<Option<T>> {
        if Instant::now() >= self.work_deadline {
            return Ok(None);
        }
        match timeout_at(self.work_deadline, timeout(self.item_timeout, work)).await {
            Ok(Ok(result)) => result.map(Some),
            Ok(Err(_)) => Err(Error::Busy("maintenance item timed out".into())),
            Err(_) => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn item_timeout_allows_progress_and_work_timeout_preserves_the_current_item() {
        let budget = SweepBudget {
            work_deadline: Instant::now() + Duration::from_secs(3),
            item_timeout: Duration::from_secs(2),
        };
        assert!(matches!(
            budget.attempt(std::future::pending::<Result<()>>()).await,
            Err(Error::Busy(_))
        ));
        assert_eq!(budget.attempt(async { Ok(7) }).await.unwrap(), Some(7));
        assert_eq!(
            budget
                .attempt(std::future::pending::<Result<()>>())
                .await
                .unwrap(),
            None
        );
        assert_eq!(budget.attempt(async { Ok(8) }).await.unwrap(), None);
    }
}
