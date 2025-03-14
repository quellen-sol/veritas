use std::collections::HashMap;

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
    pub lp_mint: Option<String>,
    pub curve_type: u16,
}

pub async fn build_lp_cache(clickhouse_client: clickhouse::Client) -> Result<LpCache> {
    // Allow for 1M liquidity pools, as of the first time writing this comment, there are 531K
    let mut lp_cache = LpCache::with_capacity(1_000_000);

    let query = "
        SELECT
            base58Encode(lp_mint) AS lp_mint,
            curve_type
        FROM lookup_lp_info lli
    ";

    let mut cursor = clickhouse_client.query(query).fetch::<LiquidityPoolRow>()?;

    log::info!("Building LP cache...");

    while let Some(row) = cursor.next().await? {
        let Some(lp_mint) = row.lp_mint else {
            continue;
        };

        lp_cache.insert(
            lp_mint,
            LiquidityPool {
                // TODO: Need to impl From<u16> for CurveType in ingestooor
                curve_type: unsafe { std::mem::transmute::<u16, CurveType>(row.curve_type) },
            },
        );
    }

    log::info!("LP cache built with {} pools", lp_cache.len());

    Ok(lp_cache)
}
