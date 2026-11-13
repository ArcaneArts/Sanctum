use std::time::Duration;

use tokio::sync::watch;

use crate::Service;

pub async fn run_local_worker(service: Service, mut stop: watch::Receiver<bool>) {
    while !*stop.borrow_and_update() {
        let service = service.clone();
        match tokio::task::spawn_blocking(move || service.process_next()).await {
            Ok(Ok(true)) => continue,
            Ok(Ok(false)) => {}
            Ok(Err(error)) => tracing::error!(%error, "local ingestion job failed"),
            Err(error) => tracing::error!(%error, "local ingestion worker failed"),
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(150)) => {}
            changed = stop.changed() => {
                if changed.is_err() {
                    break;
                }
            }
        }
    }
}

pub async fn run_local_maintenance(service: Service, mut stop: watch::Receiver<bool>) {
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    while !*stop.borrow_and_update() {
        tokio::select! {
            _ = interval.tick() => {
                let service = service.clone();
                if let Err(error) = tokio::task::spawn_blocking(move || service.sweep_retired()).await {
                    tracing::warn!(%error, "local cleanup worker failed");
                }
            }
            changed = stop.changed() => {
                if changed.is_err() {
                    break;
                }
            }
        }
    }
}
