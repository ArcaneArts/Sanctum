use std::{sync::Arc, time::Duration};

use sanctum_service::{DISPATCH_SHARDS, HostedCatalog};
use tokio::{sync::Notify, time::Instant};
use tokio_util::sync::CancellationToken;

const IDLE_INTERVAL: Duration = Duration::from_secs(10);
const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(60);

pub(crate) async fn run(catalog: HostedCatalog, wake: Arc<Notify>, stop: CancellationToken) {
    // Dropping the in-flight stage leaves its durable hint and any committed
    // receipt available for the next worker.
    tokio::select! {
        biased;
        _ = stop.cancelled() => {}
        _ = run_worker(catalog, wake) => {}
    }
}

async fn run_worker(catalog: HostedCatalog, wake: Arc<Notify>) {
    let mut next_maintenance = Instant::now();
    let mut discover_profile = true;
    loop {
        if discover_profile {
            match catalog.discover_profile_rebuild().await {
                Ok(page) => discover_profile = !page.complete,
                Err(error) => tracing::warn!(%error, "standalone profile discovery will retry"),
            }
        }
        let worked = scan(&catalog).await;
        if Instant::now() >= next_maintenance {
            maintenance(&catalog).await;
            next_maintenance = Instant::now() + MAINTENANCE_INTERVAL;
        }
        if !worked {
            tokio::select! {
                _ = wake.notified() => {},
                _ = tokio::time::sleep(IDLE_INTERVAL) => {},
            }
        }
    }
}

async fn scan(catalog: &HostedCatalog) -> bool {
    let mut cursors: [Option<String>; DISPATCH_SHARDS as usize] = Default::default();
    let mut finished = [false; DISPATCH_SHARDS as usize];
    let mut worked = false;
    while finished.contains(&false) {
        for ((shard, cursor), done) in (0..DISPATCH_SHARDS).zip(&mut cursors).zip(&mut finished) {
            if *done {
                continue;
            }
            match catalog
                .work_standalone_shard(shard, cursor.as_deref(), || false)
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

async fn maintenance(catalog: &HostedCatalog) {
    if let Err(error) = catalog.sweep_hosted_sources().await {
        tracing::warn!(%error, "standalone source sweep will retry");
    }
    if let Err(error) = catalog.sweep_hosted_snapshots().await {
        tracing::warn!(%error, "standalone snapshot sweep will retry");
    }
    if let Err(error) = catalog.sweep_abandoned_uploads().await {
        tracing::warn!(%error, "standalone upload sweep will retry");
    }
}
