use std::time::Duration;

use chrono::Utc;
use veritas_sdk::types::WrappedMintPricingGraph;

#[allow(clippy::unwrap_used)]
pub async fn reaper_task(graph: WrappedMintPricingGraph) {
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

        log::info!(
            "Reaper task completed at {}. Sleeping for 1 day...",
            Utc::now()
        );

        tokio::time::sleep(Duration::from_secs(86400)).await;
    }
}
