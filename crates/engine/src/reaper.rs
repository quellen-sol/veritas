use std::time::Duration;

use clickhouse::Client;
use veritas_sdk::ppl_graph::graph::WrappedMintPricingGraph;

pub async fn reaper_task(graph: WrappedMintPricingGraph, clickhouse_client: Client) {
    loop {
        let g_read = graph.write().await; // Write lock for exclusive access to the graph
        for node in g_read.node_weights() {
            let mut p_write = node.usd_price.write().await;
            p_write.take();
        }

        let q_result = clickhouse_client.query("DELETE FROM current_token_price_global_by_mint WHERE time < now() - interval '1 week'").execute().await;

        match q_result {
            Ok(_) => {}
            Err(e) => {
                log::error!("Error during reaper task: {:?}", e);
            }
        }

        tokio::time::sleep(Duration::from_secs(86400)).await;
    }
}
