use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use anyhow::Result;
use clickhouse::Row;
use rust_decimal::Decimal;
use serde::Deserialize;
use step_ingestooor_sdk::dooot::CurveType;

/// K = lp_mint, V = LiquidityPool
pub type LpCache = HashMap<String, LiquidityPool>;

pub struct LiquidityPool {
    pub curve_type: CurveType,
}

#[derive(Deserialize, Row)]
pub struct LiquidityPoolRow {
    pub lp_mint_pk: String,
    pub curve_type: CurveType,
}

pub async fn build_lp_cache(clickhouse_client: Arc<clickhouse::Client>) -> Result<LpCache> {
    // Allow for 1M liquidity pools, as of this commit, there are 531K
    let mut cache = LpCache::with_capacity(1_000_000);

    let query = "
        SELECT
          base58Encode(reinterpretAsString(lp_mint)) AS lp_mint_pk,
          curve_type
        FROM lookup_lp_info lli
        WHERE lp_mint IS NOT null
     ";

    let mut cursor = clickhouse_client.query(query).fetch::<LiquidityPoolRow>()?;

    while let Some(row) = cursor.next().await? {
        cache.insert(
            row.lp_mint_pk,
            LiquidityPool {
                curve_type: row.curve_type,
            },
        );
    }

    Ok(cache)
}
