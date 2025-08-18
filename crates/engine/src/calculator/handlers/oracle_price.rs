use std::{
    collections::HashSet,
    sync::{Arc, RwLock},
};

use petgraph::graph::NodeIndex;
use rust_decimal::Decimal;
use std::sync::mpsc::SyncSender;
use step_ingestooor_sdk::dooot::Dooot;
use veritas_sdk::{
    constants::WSOL_MINT, ppl_graph::graph::USDPriceWithSource, types::MintPricingGraph,
    utils::checked_math::is_significant_change,
};

use crate::calculator::algo::bfs_recalculate;

pub fn handle_oracle_price_update(
    graph: Arc<RwLock<MintPricingGraph>>,
    token: NodeIndex,
    new_price: Decimal,
    dooot_tx: SyncSender<Dooot>,
    oracle_mint_set: &HashSet<String>,
    sol_index: Arc<RwLock<Option<Decimal>>>,
    max_price_impact: &Decimal,
) {
    // Grab an exclusive lock on the graph, to prevent non-atomic updates.
    let mut g_write = graph.write().expect("Graph write lock poisoned");

    let mut visited = HashSet::with_capacity(g_write.node_count());

    // Update the price of the mint in the graph
    let Some(node_weight) = g_write.node_weight_mut(token) else {
        return;
    };

    {
        let mut p_write = node_weight
            .usd_price
            .write()
            .expect("Price write lock poisoned");

        let old_price = p_write.as_ref().map(|p| p.extract_price());
        if let Some(old_price) = old_price {
            if !is_significant_change(old_price, &new_price) {
                // Not enough to warrant recalcing everything (avoids stuff like USDC moving around same price)
                return;
            }
        }

        node_weight.dirty = true;

        p_write.replace(USDPriceWithSource::Oracle(new_price));
    }

    let sol_index_price = if node_weight.mint == WSOL_MINT {
        let mut sol_index_write = sol_index.write().expect("Sol index write lock poisoned");
        sol_index_write.replace(new_price);

        Some(new_price)
    } else {
        *sol_index.read().expect("Sol index read lock poisoned")
    };

    let mut g_scan_copy = g_write.clone();

    for node in g_write.node_weights_mut() {
        node.dirty = false;
    }

    for edge in g_write.edge_weights_mut() {
        *edge.dirty.write().expect("Dirty write lock poisoned") = false;
    }

    drop(g_write);

    let recalc_result = bfs_recalculate(
        &mut g_scan_copy,
        token,
        &mut visited,
        dooot_tx.clone(),
        oracle_mint_set,
        &sol_index_price,
        max_price_impact,
        true,
    );

    match recalc_result {
        Ok(_) => {}
        Err(e) => {
            log::error!("Error during BFS recalculation for OracleUSDPrice update: {e}");
        }
    }
}
