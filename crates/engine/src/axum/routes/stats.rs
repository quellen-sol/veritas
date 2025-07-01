use std::sync::{atomic::Ordering, Arc};

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use petgraph::Direction;
use serde::{Deserialize, Serialize};
use veritas_sdk::types::MintPricingGraph;

use crate::axum::task::VeritasServerState;

#[derive(Deserialize)]
pub struct GetStatsParams {
    pub top_n: Option<usize>,
}

#[derive(Serialize)]
pub struct VeritasStats {
    pub ingesting: bool,
    pub calculating: bool,
    pub node_count: usize,
    pub edge_count: usize,
    pub top_dominators: Vec<(String, usize)>, // (mint, edge count)
}

pub async fn get_stats(
    State(state): State<Arc<VeritasServerState>>,
    Query(params): Query<GetStatsParams>,
) -> Result<Json<VeritasStats>, StatusCode> {
    let top_n = params.top_n.unwrap_or(10);

    let mut result = VeritasStats {
        ingesting: false,
        calculating: false,
        node_count: 0,
        edge_count: 0,
        top_dominators: vec![],
    };
    let ingesting = !state.paused_ingestion.load(Ordering::Relaxed);
    let calculating = !state.paused_calculation.load(Ordering::Relaxed);
    let g_read = state.graph.read().await;
    let node_count = g_read.node_count();
    let edge_count = g_read.edge_count();
    let top_dominators =
        top_n_dominators(&g_read, top_n).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    result.ingesting = ingesting;
    result.calculating = calculating;
    result.node_count = node_count;
    result.edge_count = edge_count;
    result.top_dominators = top_dominators;

    Ok(Json(result))
}

// Bad dominator algorithm, but it's good enough for now
fn top_n_dominators(
    graph: &MintPricingGraph,
    top_n: usize,
) -> anyhow::Result<Vec<(String, usize)>> {
    let mut result = Vec::with_capacity(graph.node_count());

    for node in graph.node_indices() {
        let edges = graph.edges_directed(node, Direction::Incoming).count();
        if edges > 0 {
            let mint = graph
                .node_weight(node)
                .ok_or(anyhow::anyhow!("Node weight is None"))?
                .mint
                .clone();
            result.push((mint, edges));
        }
    }

    result.sort_by_key(|(_, count)| *count);
    result.reverse();
    result.truncate(top_n);

    Ok(result)
}
