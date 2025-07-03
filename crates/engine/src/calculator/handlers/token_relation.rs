use std::{
    collections::HashSet,
    sync::{mpsc::SyncSender, Arc, RwLock},
};

use petgraph::graph::{EdgeIndex, NodeIndex};
use rust_decimal::Decimal;
use step_ingestooor_sdk::dooot::Dooot;
use veritas_sdk::types::MintPricingGraph;

use crate::calculator::algo::bfs_recalculate;

pub async fn _handle_token_relation_update(
    graph: Arc<RwLock<MintPricingGraph>>,
    token: NodeIndex,
    updated_edge: EdgeIndex,
    dooot_tx: SyncSender<Dooot>,
    oracle_mint_set: &HashSet<String>,
    sol_index: Arc<RwLock<Option<Decimal>>>,
    max_price_impact: &Decimal,
) {
    let g_read = graph.read().expect("Graph read lock poisoned");

    let mut visited = HashSet::with_capacity(g_read.node_count());

    let Some((src, _)) = g_read.edge_endpoints(updated_edge) else {
        return;
    };

    // Do not consider the source token of this relation
    visited.insert(src);

    let sol_index = sol_index.read().expect("Sol index read lock poisoned");

    let recalc_result = bfs_recalculate(
        &g_read,
        token,
        &mut visited,
        dooot_tx.clone(),
        oracle_mint_set,
        &sol_index,
        max_price_impact,
        true,
    );

    match recalc_result {
        Ok(_) => {}
        Err(e) => {
            log::error!("Error during BFS recalculation for NewTokenRatio update: {e}");
        }
    }
}
