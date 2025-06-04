use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use serde::Serialize;

use crate::axum::task::VeritasServerState;

#[derive(Serialize)]
pub struct DecimalCacheResponse {
    pub decimal: u8,
}

pub async fn get_decimal_cache_token(
    State(state): State<Arc<VeritasServerState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<DecimalCacheResponse>, StatusCode> {
    let token = params.get("mint").ok_or(StatusCode::BAD_REQUEST)?;

    let decimal_cache = state.decimal_cache.read().await;
    let decimal = *decimal_cache.get(token).ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(DecimalCacheResponse { decimal }))
}
