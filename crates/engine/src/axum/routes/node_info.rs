use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};
use veritas_sdk::{liq_relation::LiqRelation, ppl_graph::graph::USDPriceWithSource};

use crate::axum::task::VeritasServerState;

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeInfo {
    pub mint: String,
    pub usd_price: Option<USDPriceWithSource>,
    pub non_vertex_relations: HashMap<String, LiqRelation>,
}

pub async fn get_node_info(
    State(state): State<Arc<VeritasServerState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<NodeInfo>, StatusCode> {
    let node_idx = params.get("node_idx").ok_or(StatusCode::BAD_REQUEST)?;

    let node_idx = node_idx
        .parse::<usize>()
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let g_read = state.graph.read().await;

    let node = g_read
        .node_weight(NodeIndex::new(node_idx))
        .ok_or(StatusCode::NOT_FOUND)?;

    let non_vertex_relations = node.non_vertex_relations.read().await;
    let mut parsed_relations = HashMap::new();
    for (mint, relation) in non_vertex_relations.iter() {
        let relation = relation.read().await.clone();
        parsed_relations.insert(mint.clone(), relation);
    }

    let node_info = NodeInfo {
        mint: node.mint.clone(),
        usd_price: node.usd_price.read().await.clone(),
        non_vertex_relations: parsed_relations,
    };

    Ok(Json(node_info))
}
