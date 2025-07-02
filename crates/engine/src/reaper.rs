use std::time::Duration;

use chrono::{DateTime, Utc};
use clickhouse::Client;
use veritas_sdk::types::WrappedMintPricingGraph;

#[allow(clippy::unwrap_used)]
pub async fn reaper_task(graph: WrappedMintPricingGraph, clickhouse_client: Client) {
    loop {
        let now = Utc::now();
        log::info!("Starting reaper task at {}", now);
        {
            let g_lock = graph.write().unwrap(); // Write lock for exclusive access to the graph
            log::info!("Graph locked. Starting to clear usd_price from nodes...");
            for node in g_lock.node_weights() {
                let mut p_write = node
                    .usd_price
                    .write()
                    .expect("USD price write lock poisoned");
                p_write.take();
            }
            log::info!("Cleared nodes. Unlocking graph...");
        }

        let now_timestamp = now.timestamp();
        let one_week_ago_timestamp = now_timestamp - (86400 * 7);
        let Some(one_week_ago_time) = DateTime::from_timestamp(one_week_ago_timestamp, 0) else {
            log::error!(
                "Failed to convert one_week_ago_timestamp to DateTime ({})",
                one_week_ago_timestamp
            );
            continue;
        };
        log::info!(
            "Cleared usd_price from nodes. Starting to clear current_token_price_global_by_mint older than {}...",
            one_week_ago_time
        );

        let q_result = clickhouse_client
            .query(
                format!(
                    "DELETE FROM current_token_price_global_by_mint WHERE time < {}",
                    one_week_ago_timestamp
                )
                .as_str(),
            )
            .execute()
            .await;

        match q_result {
            Ok(_) => {
                log::info!(
                    "Cleared current_token_price_global_by_mint older than {}.",
                    one_week_ago_time
                );
            }
            Err(e) => {
                log::error!("Error during reaper task: {:?}", e);
            }
        }
        log::info!(
            "Reaper task completed at {}. Sleeping for 1 day...",
            Utc::now()
        );

        tokio::time::sleep(Duration::from_secs(86400)).await;
    }
}
