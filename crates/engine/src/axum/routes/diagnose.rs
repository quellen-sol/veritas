use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use serde::Serialize;
use veritas_sdk::utils::lp_cache::LiquidityPool;

use crate::axum::task::VeritasServerState;

#[derive(Debug, Serialize)]
pub struct DiagnosisPoolInfo {
    pub pk: String,
    #[serde(flatten)]
    pub pool: LiquidityPool,
}

#[derive(Debug, Serialize)]
pub struct MintDiagnosis {
    pub mint: String,
    pub in_num_pools: usize,
    pub pools: Vec<DiagnosisPoolInfo>,
    pub decimals: Option<u8>,
}

pub async fn diagnose_node(
    State(state): State<Arc<VeritasServerState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<MintDiagnosis>, StatusCode> {
    let mint = params.get("mint").ok_or(StatusCode::BAD_REQUEST)?;

    let decimals = {
        let dc_read = state
            .decimal_cache
            .read()
            .expect("Decimal cache read lock poisoned");
        dc_read.get(mint).cloned()
    };

    // Pool stats
    let (in_num_pools, pools) = {
        let lp_read = state.lp_cache.read().expect("LP cache read lock poisoned");
        let mut pools = Vec::new();
        for (pk, pool) in lp_read.iter() {
            if pool.underlyings.iter().any(|un| un.mint == *mint) {
                pools.push(DiagnosisPoolInfo {
                    pk: pk.clone(),
                    pool: pool.clone(),
                });
            }
        }

        (pools.len(), pools)
    };

    let diagnosis = MintDiagnosis {
        mint: mint.clone(),
        decimals,
        in_num_pools,
        pools,
    };

    Ok(Json(diagnosis))
}
