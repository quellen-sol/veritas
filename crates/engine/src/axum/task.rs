use std::{
    collections::HashMap,
    sync::{atomic::AtomicBool, Arc},
};

use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use itertools::Itertools;
use rust_decimal::Decimal;
use tokio::{sync::RwLock, task::JoinHandle};
use veritas_sdk::ppl_graph::{graph::WrappedMintPricingGraph, utils::get_price_by_node_idx};

use crate::price_points_liquidity::task::MintIndiciesMap;

use super::types::{NodeInfo, NodeRelationInfo, RelationWithLiq};

struct VeritasServerState {
    bootstrap_in_progress: Arc<AtomicBool>,
    graph: WrappedMintPricingGraph,
    mint_indicies: Arc<RwLock<MintIndiciesMap>>,
}

pub fn spawn_axum_server(
    bootstrap_in_progress: Arc<AtomicBool>,
    graph: WrappedMintPricingGraph,
    mint_indicies: Arc<RwLock<MintIndiciesMap>>,
) -> JoinHandle<()> {
    tokio::spawn(
        #[allow(clippy::unwrap_used)]
        async move {
            let state = Arc::new(VeritasServerState {
                bootstrap_in_progress,
                graph,
                mint_indicies,
            });

            let app = Router::new()
                .route("/healthcheck", get(handle_healthcheck))
                .route("/debug-node", get(debug_node_info))
                .with_state(state);

            let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
            axum::serve(listener, app).await.unwrap()
        },
    )
}

async fn handle_healthcheck(State(state): State<Arc<VeritasServerState>>) -> StatusCode {
    if state
        .bootstrap_in_progress
        .load(std::sync::atomic::Ordering::Relaxed)
    {
        // 503
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        // 200
        StatusCode::OK
    }
}

async fn debug_node_info(
    State(state): State<Arc<VeritasServerState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<NodeInfo>, StatusCode> {
    let mint = params.get("mint").ok_or(StatusCode::BAD_REQUEST)?;
    let mint_ix = {
        let mi_read = state.mint_indicies.read().await;
        mi_read.get(mint).cloned().ok_or(StatusCode::NOT_FOUND)?
    };

    let g_read = state.graph.read().await;
    let this_price = get_price_by_node_idx(&g_read, mint_ix).await;

    let mut node_info = NodeInfo {
        mint: mint.clone(),
        calculated_price: this_price,
        neighbors: vec![],
    };

    for neighbor in g_read.neighbors_undirected(mint_ix).unique() {
        let Some(neigh_weight) = g_read.node_weight(neighbor) else {
            log::error!("UNREACHABLE - {neighbor:?} should exist in graph??");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        };

        let neighbor_mint = &neigh_weight.mint;

        let mut relation_info = NodeRelationInfo {
            mint: neighbor_mint.clone(),
            incoming_relations: vec![],
            outgoing_relations: vec![],
        };

        // All outgoing edges
        for edge in g_read.edges_connecting(mint_ix, neighbor) {
            let e_weight = edge.weight();
            let relation = e_weight.inner_relation.read().await.clone();

            relation_info.outgoing_relations.push(RelationWithLiq {
                relation,
                liquidity: None,
                price: None,
            });
        }

        // All incoming edges
        for edge in g_read.edges_connecting(neighbor, mint_ix) {
            let e_weight = edge.weight();
            let relation = e_weight.inner_relation.read().await.clone();
            let price_neighbor = get_price_by_node_idx(&g_read, neighbor).await;
            let liq = price_neighbor.and_then(|p| relation.get_liquidity(p, Decimal::ZERO));
            let derived_price = price_neighbor.and_then(|p| relation.get_price(p));

            relation_info.incoming_relations.push(RelationWithLiq {
                relation,
                liquidity: liq,
                price: derived_price,
            });
        }

        node_info.neighbors.push(relation_info);
    }

    Ok(Json(node_info))
}
