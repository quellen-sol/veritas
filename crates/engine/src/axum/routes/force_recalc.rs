use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use axum::{extract::State, http::StatusCode, Json};
use rust_decimal::Decimal;
use serde::Deserialize;
use step_ingestooor_sdk::dooot::Dooot;
use veritas_sdk::constants::WSOL_MINT;

use crate::{axum::task::VeritasServerState, calculator::algo::bfs_recalculate};

#[derive(Deserialize)]
pub struct ForceRecalcParams {
    pub update_nodes: Option<bool>,
    pub start_mint: Option<String>,
}

pub async fn force_recalc(
    State(state): State<Arc<VeritasServerState>>,
    Json(params): Json<Option<ForceRecalcParams>>,
) -> Result<Json<HashMap<String, Decimal>>, StatusCode> {
    let params = params.as_ref();
    let update_nodes = params.and_then(|p| p.update_nodes).unwrap_or(false);
    let start_mint = params
        .and_then(|p| p.start_mint.clone())
        .unwrap_or(WSOL_MINT.to_string());

    let mut g_read = state.graph.write().expect("Graph read lock poisoned");
    let mut g_scan_copy = g_read.clone();

    for node in g_read.node_weights_mut() {
        node.dirty = false;
    }

    for edge in g_read.edge_weights_mut() {
        *edge.dirty.write().expect("Dirty write lock poisoned") = false;
    }

    drop(g_read);

    let sol_ix = *state
        .mint_indicies
        .read()
        .expect("Mint indicies read lock poisoned")
        .get(&start_mint)
        .ok_or(StatusCode::NOT_FOUND)?;
    let sol_price_index = *state
        .sol_price_index
        .read()
        .expect("Sol price index read lock poisoned");

    let node_count = g_scan_copy.node_count();
    let (price_tx, price_rx) = std::sync::mpsc::sync_channel::<Dooot>(node_count);

    let mut visited_nodes = HashSet::new();
    bfs_recalculate(
        &mut g_scan_copy,
        sol_ix,
        &mut visited_nodes,
        price_tx,
        &state.oracle_mint_set,
        &sol_price_index,
        &state.max_price_impact,
        update_nodes,
    )
    .inspect_err(|e| log::error!("Error during force recalc: {:?}", e))
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut updated_nodes = HashMap::new();
    while let Ok(Dooot::TokenPriceGlobal(price)) = price_rx.recv() {
        updated_nodes.insert(price.mint, price.price_usd);
    }

    Ok(Json(updated_nodes))
}
