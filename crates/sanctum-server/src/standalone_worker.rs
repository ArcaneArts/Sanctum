use std::{sync::Arc, time::Duration};

use sanctum_service::{DISPATCH_SHARDS, HostedCatalog};
use tokio::{sync::Notify, time::Instant};
use tokio_util::sync::CancellationToken;

const IDLE_INTERVAL: Duration = Duration::from_secs(10);
const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(60);

pub(crate) async fn run(catalog: HostedCatalog, wake: Arc<Notify>, stop: CancellationToken) {
    let mut next_maintenance = Instant::now();
    let mut discover_profile = true;
    while !stop.is_cancelled() {
        if discover_profile {
            match catalog.discover_profile_rebuild().await {
                Ok(page) => discover_profile = !page.complete,
                Err(error) => tracing::warn!(%error, "standalone profile discovery will retry"),
            }
        }
        let worked = scan(&catalog, &stop).await;
        if !stop.is_cancelled() && Instant::now() >= next_maintenance {
            maintenance(&catalog, &stop).await;
            next_maintenance = Instant::now() + MAINTENANCE_INTERVAL;
        }
        if !worked {
            tokio::select! {
                _ = stop.cancelled() => return,
                _ = wake.notified() => {},
                _ = tokio::time::sleep(IDLE_INTERVAL) => {},
            }
        }
    }
}

async fn scan(catalog: &HostedCatalog, stop: &CancellationToken) -> bool {
    let mut cursors: [Option<String>; DISPATCH_SHARDS as usize] = Default::default();
    let mut finished = [false; DISPATCH_SHARDS as usize];
    let mut worked = false;
    while finished.contains(&false) && !stop.is_cancelled() {
        for ((shard, cursor), done) in (0..DISPATCH_SHARDS).zip(&mut cursors).zip(&mut finished) {
            if stop.is_cancelled() {
                return worked;
            }
            if *done {
                continue;
            }
            match catalog
                .work_standalone_shard(shard, cursor.as_deref(), || stop.is_cancelled())
                .await
            {
                Ok(page) => {
                    *cursor = page.next_cursor;
                    *done = cursor.is_none();
                    worked |= page.dispatched > 0;
                }
                Err(error) => {
                    tracing::warn!(%error, shard, "standalone dispatch scan will retry the shard");
                    *done = true;
                }
            }
        }
    }
    worked
}

async fn maintenance(catalog: &HostedCatalog, stop: &CancellationToken) {
    if let Err(error) = catalog.sweep_hosted_sources().await {
        tracing::warn!(%error, "standalone source sweep will retry");
    }
    if !stop.is_cancelled()
        && let Err(error) = catalog.sweep_hosted_snapshots().await
    {
        tracing::warn!(%error, "standalone snapshot sweep will retry");
    }
    if !stop.is_cancelled()
        && let Err(error) = catalog.sweep_abandoned_uploads().await
    {
        tracing::warn!(%error, "standalone upload sweep will retry");
    }
}
