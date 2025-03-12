use std::{collections::HashSet, sync::Arc};

use petgraph::graph::NodeIndex;
use rust_decimal::Decimal;
use step_ingestooor_sdk::dooot::Dooot;
use tokio::sync::{mpsc::Sender, RwLock};
use veritas_sdk::{
    ppl_graph::graph::{MintPricingGraph, USDPriceWithSource},
    utils::{checked_math::is_significant_change, decimal_cache::DecimalCache},
};

use crate::calculator::algo::bfs_recalculate;

pub async fn handle_oracle_price_update(
    graph: Arc<RwLock<MintPricingGraph>>,
    token: NodeIndex,
    new_price: Decimal,
    decimals_cache: Arc<RwLock<DecimalCache>>,
    dooot_tx: Sender<Dooot>,
) {
    log::trace!("Getting graph read lock for OracleUSDPrice update");
    let g_read = graph.read().await;
    log::trace!("Got graph read lock for OracleUSDPrice update");
    let mut visited = HashSet::with_capacity(g_read.node_count());

    {
        // Update the price of the mint in the graph
        let Some(node_weight) = g_read.node_weight(token) else {
            return;
        };
        log::trace!("Getting price write lock for OracleUSDPrice update");
        let mut p_write = node_weight.usd_price.write().await;
        log::trace!("Got price write lock for OracleUSDPrice update");
        let old_price = p_write.as_ref().map(|p| p.extract_price());
        if let Some(old_price) = old_price {
            if !is_significant_change(old_price, &new_price) {
                // Not enough to warrant recalcing everything (avoids stuff like USDC moving around same price)
                return;
            }
        }

        p_write.replace(USDPriceWithSource::Oracle(new_price));
        log::trace!("Replaced price for OracleUSDPrice update");
    }

    log::trace!("Starting BFS recalculation for OracleUSDPrice update");
    let recalc_result = bfs_recalculate(
        &g_read,
        decimals_cache.clone(),
        token,
        &mut visited,
        dooot_tx.clone(),
        true,
    )
    .await;

    match recalc_result {
        Ok(_) => {
            log::trace!("Finished BFS recalculation for OracleUSDPrice update");
        }
        Err(e) => {
            log::error!("Error during BFS recalculation for OracleUSDPrice update: {e}");
        }
    }
}
