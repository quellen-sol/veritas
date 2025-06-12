use std::{collections::HashSet, sync::Arc};

use petgraph::graph::NodeIndex;
use rust_decimal::Decimal;
use step_ingestooor_sdk::dooot::Dooot;
use tokio::sync::{mpsc::Sender, RwLock};
use veritas_sdk::{
    constants::WSOL_MINT,
    ppl_graph::graph::{MintPricingGraph, USDPriceWithSource},
    utils::checked_math::is_significant_change,
};

use crate::calculator::algo::bfs_recalculate;

pub async fn handle_oracle_price_update(
    graph: Arc<RwLock<MintPricingGraph>>,
    token: NodeIndex,
    new_price: Decimal,
    dooot_tx: Sender<Dooot>,
    oracle_mint_set: &HashSet<String>,
    sol_index: Arc<RwLock<Option<Decimal>>>,
    max_price_impact: &Decimal,
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

        if node_weight.mint == WSOL_MINT {
            let mut sol_index_write = sol_index.write().await;
            sol_index_write.replace(new_price);
        }

        p_write.replace(USDPriceWithSource::Oracle(new_price));
        log::trace!("Replaced price for OracleUSDPrice update");
    }

    let sol_index = sol_index.read().await;

    log::trace!("Starting BFS recalculation for OracleUSDPrice update");
    let recalc_result = bfs_recalculate(
        &g_read,
        token,
        &mut visited,
        dooot_tx.clone(),
        oracle_mint_set,
        &sol_index,
        max_price_impact,
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
