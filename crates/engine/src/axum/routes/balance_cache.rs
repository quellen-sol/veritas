use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use serde::Serialize;

use crate::axum::task::VeritasServerState;
use rust_decimal::prelude::ToPrimitive;

#[derive(Serialize)]
#[serde(tag = "type")]
pub enum BalanceCacheResponse {
    NotInMap,
    InMapButNull,
    BalanceExists { balance: u64 },
}

pub async fn get_balance_cache_token(
    State(state): State<Arc<VeritasServerState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<BalanceCacheResponse>, StatusCode> {
    let token = params.get("mint").ok_or(StatusCode::BAD_REQUEST)?;

    let token_balance_cache = state
        .token_balance_cache
        .read()
        .expect("Token balance cache read lock poisoned");
    let balance = token_balance_cache.get(token).cloned();

    let resp = match balance {
        None => BalanceCacheResponse::NotInMap,
        Some(op) => match op {
            Some(dec) => {
                let bal_u64 = dec.to_u64().ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
                BalanceCacheResponse::BalanceExists { balance: bal_u64 }
            }
            None => BalanceCacheResponse::InMapButNull,
        },
    };

    Ok(Json(resp))
}
