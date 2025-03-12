use std::{collections::HashSet, sync::Arc};

use petgraph::graph::{EdgeIndex, NodeIndex};
use step_ingestooor_sdk::dooot::Dooot;
use tokio::sync::{mpsc::Sender, RwLock};
use veritas_sdk::{ppl_graph::graph::MintPricingGraph, utils::decimal_cache::DecimalCache};

use crate::calculator::algo::bfs_recalculate;

pub async fn handle_token_relation_update(
    graph: Arc<RwLock<MintPricingGraph>>,
    token: NodeIndex,
    updated_edge: EdgeIndex,
    decimals_cache: Arc<RwLock<DecimalCache>>,
    dooot_tx: Sender<Dooot>,
) {
    log::trace!("Getting graph read lock for NewTokenRatio update");
    let g_read = graph.read().await;
    log::trace!("Got graph read lock for NewTokenRatio update");
    let mut visited = HashSet::with_capacity(g_read.node_count());

    let Some((src, _)) = g_read.edge_endpoints(updated_edge) else {
        return;
    };

    // Do not consider the source token of this relation
    visited.insert(src);

    log::trace!("Starting BFS recalculation for NewTokenRatio update");
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
            log::trace!("Finished BFS recalculation for NewTokenRatio update");
        }
        Err(e) => {
            log::error!("Error during BFS recalculation for NewTokenRatio update: {e}");
        }
    }
}
