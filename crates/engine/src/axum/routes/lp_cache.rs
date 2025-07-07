use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use veritas_sdk::utils::lp_cache::LiquidityPool;

use crate::axum::task::VeritasServerState;

pub async fn get_lp_cache_pool(
    State(state): State<Arc<VeritasServerState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<LiquidityPool>, StatusCode> {
    let pool = params.get("pool").ok_or(StatusCode::BAD_REQUEST)?;

    let lp_cache = state.lp_cache.read().expect("LP cache read lock poisoned");
    let pool = lp_cache.get(pool).ok_or(StatusCode::NOT_FOUND)?.clone();

    Ok(Json(pool))
}
