use std::collections::HashMap;

use anyhow::Result;
use clickhouse::Row;
use serde::{Deserialize, Serialize};
use step_ingestooor_sdk::dooot::{liquidity::LPInfoUnderlyingMintVault, CurveType};

/// K = pool_id, V = LiquidityPool
pub type LpCache = HashMap<String, LiquidityPool>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LiquidityPool {
    pub curve_type: CurveType,
    pub underlyings: Vec<LPInfoUnderlyingMintVault>,
}

#[derive(Deserialize, Row)]
pub struct LiquidityPoolRow {
    pub pool: String,
    pub lp_mint: Option<String>,
    pub underlyings: Vec<LPInfoUnderlyingMintVault>,
    pub curve_type: u16,
}

pub async fn build_lp_cache(
    clickhouse_client: &clickhouse::Client,
    skip_preloads: bool,
) -> Result<LpCache> {
    if skip_preloads {
        return Ok(LpCache::new());
    }
    // Allow for 1M liquidity pools, as of the first time writing this comment, there are 531K
    let mut lp_cache = LpCache::with_capacity(1_000_000);

    let query = "
        SELECT
            base58Encode(pool) as pool,
            base58Encode(lp_mint) AS lp_mint,
            arrayMap(x -> (base58Encode(x.mint), base58Encode(x.vault)), underlyings) as underlyings,
            curve_type
        FROM lookup_lp_info lli FINAL
    ";

    let mut cursor = clickhouse_client.query(query).fetch::<LiquidityPoolRow>()?;

    log::info!("Building LP cache...");

    while let Some(row) = cursor.next().await? {
        let addr = row.lp_mint.unwrap_or(row.pool);

        lp_cache.insert(
            addr,
            LiquidityPool {
                // TODO: Need to impl From<u16> for CurveType in ingestooor
                curve_type: unsafe { std::mem::transmute::<u16, CurveType>(row.curve_type) },
                underlyings: row.underlyings,
            },
        );
    }

    log::info!("LP cache built with {} pools", lp_cache.len());

    Ok(lp_cache)
}
