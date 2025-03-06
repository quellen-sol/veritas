use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use clickhouse::Row;
use serde::Deserialize;
use step_ingestooor_sdk::dooot::CurveType;

/// K = pool_id, V = LiquidityPool
pub type LpCache = HashMap<String, LiquidityPool>;

pub struct LiquidityPool {
    pub curve_type: CurveType,
}

#[derive(Deserialize, Row)]
pub struct LiquidityPoolRow {
    pub pool_pk: String,
    pub lp_mint: Option<String>,
    pub curve_type: u16,
}

pub async fn build_lp_cache(clickhouse_client: Arc<clickhouse::Client>) -> Result<LpCache> {
    // Allow for 1M liquidity pools, as of the first time writing this comment, there are 531K
    let mut cache = LpCache::with_capacity(1_000_000);

    let query = "
        SELECT
            base58Encode(pool) AS pool_pk,
            base58Encode(lp_mint) AS lp_mint,
            curve_type
        FROM lookup_lp_info lli
    ";

    let mut cursor = clickhouse_client.query(query).fetch::<LiquidityPoolRow>()?;

    log::info!("Building LP cache...");

    while let Some(row) = cursor.next().await? {
        cache.insert(
            row.pool_pk,
            LiquidityPool {
                // TODO: Need to impl From<u16> for CurveType in ingestooor
                curve_type: unsafe { std::mem::transmute::<u16, CurveType>(row.curve_type) },
            },
        );
    }

    log::info!("LP cache built with {} pools", cache.len());

    Ok(cache)
}
