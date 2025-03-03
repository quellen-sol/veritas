use std::sync::Arc;

use tokio::{
    sync::{mpsc::Receiver, RwLock},
    task::JoinHandle,
};
use veritas_sdk::utils::decimal_cache::DecimalCache;

use crate::price_points_liquidity::task::query_decimals;

pub fn spawn_ch_cache_updator_task(
    decimals_cache: Arc<RwLock<DecimalCache>>,
    clickhouse_client: Arc<clickhouse::Client>,
    mut req_rx: Receiver<String>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(mint) = req_rx.recv().await {
            match query_decimals(clickhouse_client.clone(), &mint).await {
                Ok(Some(d)) => {
                    let mut dc_write = decimals_cache.write().await;
                    dc_write.insert(mint, d);
                }
                Err(e) => {
                    log::error!("Error updating decimals cache for {mint}: {e}");
                }
                _ => {
                    continue;
                }
            }
        }
    })
}
