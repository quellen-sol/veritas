use std::{
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::{
    sync::{mpsc::Receiver, RwLock},
    task::JoinHandle,
};
use veritas_sdk::utils::decimal_cache::DecimalCache;

use crate::price_points_liquidity::task::query_decimals;

/// No `Arc` for the clickhouse client, since we can clone it and use a pool
pub fn spawn_ch_cache_updator_task(
    decimals_cache: Arc<RwLock<DecimalCache>>,
    clickhouse_client: clickhouse::Client,
    mut req_rx: Receiver<String>,
    max_cache_updator_subtasks: u8,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let counter = Arc::new(AtomicU8::new(0));

        while let Some(mint) = req_rx.recv().await {
            while counter.load(Ordering::Relaxed) >= max_cache_updator_subtasks {
                // Wait for a task to become available
                tokio::time::sleep(Duration::from_millis(1)).await;
            }

            let counter = counter.clone();
            let clickhouse_client = clickhouse_client.clone();
            let decimals_cache = decimals_cache.clone();

            tokio::spawn(async move {
                match query_decimals(&clickhouse_client, &mint).await {
                    Ok(Some(d)) => {
                        let mut dc_write = decimals_cache.write().await;
                        dc_write.insert(mint, d);
                    }
                    Err(e) => {
                        log::error!("Error updating decimals cache for {mint}: {e}");
                    }
                    _ => {}
                }

                counter.fetch_sub(1, Ordering::Relaxed);
            });
        }
    })
}
