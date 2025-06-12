use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use axum::{extract::State, http::StatusCode, Json};
use rust_decimal::Decimal;
use serde::Deserialize;
use step_ingestooor_sdk::dooot::Dooot;
use tokio::sync::mpsc;
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

    let g_read = state.graph.read().await;

    let sol_ix = *state
        .mint_indicies
        .read()
        .await
        .get(&start_mint)
        .ok_or(StatusCode::NOT_FOUND)?;
    let sol_price_index = *state.sol_price_index.read().await;

    let node_count = g_read.node_count();
    let (price_tx, mut price_rx) = mpsc::channel(node_count);

    let mut visited_nodes = HashSet::new();
    bfs_recalculate(
        &g_read,
        sol_ix,
        &mut visited_nodes,
        price_tx,
        &state.oracle_mint_set,
        &sol_price_index,
        &state.max_price_impact,
        update_nodes,
    )
    .await
    .inspect_err(|e| log::error!("Error during force recalc: {:?}", e))
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut updated_nodes = HashMap::with_capacity(price_rx.len());
    while let Some(Dooot::TokenPriceGlobal(price)) = price_rx.recv().await {
        updated_nodes.insert(price.mint, price.price_usd);
    }

    Ok(Json(updated_nodes))
}
