use std::{collections::HashMap, sync::Arc};

use crate::constants::*;
use anyhow::Result;
use serde::Deserialize;

pub type DecimalCache = HashMap<String, u8>;

#[derive(Deserialize, clickhouse::Row)]
pub struct MintDecimals {
    pub mint_pubkey: String,
    pub decimals: Option<u8>,
}

pub async fn build_decimal_cache(
    clickhouse_client: Arc<clickhouse::Client>,
) -> Result<DecimalCache> {
    log::info!("Building decimal cache...");
    // Allow for 100M mints
    let mut decimal_cache = DecimalCache::with_capacity(100_000_000);

    // Set a couple hard-coded values first
    decimal_cache.insert(USDC_MINT.to_string(), 6);
    decimal_cache.insert(USDT_MINT.to_string(), 6);
    decimal_cache.insert(WSOL_MINT.to_string(), 9);
    decimal_cache.insert(STEP_MINT.to_string(), 9);

    // Pull all mints from CH, that have decimals > 0, and don't end in "pump"
    let query = "
        SELECT
            base58Encode(reinterpretAsString(mint)) AS mint,
            anyLastMerge(decimals) AS decimals
        FROM lookup_mint_info lmi
        GROUP BY mint
        HAVING decimals > 0
        AND mint NOT LIKE '%pump'
    ";

    let mut cursor = clickhouse_client.query(query).fetch::<MintDecimals>()?;

    while let Some(row) = cursor.next().await? {
        if let Some(decimals) = row.decimals {
            log::debug!("Adding mint to decimal cache: {}", row.mint_pubkey);
            decimal_cache.insert(row.mint_pubkey, decimals);
        }
    }

    log::info!("Decimal cache built with {} mints", decimal_cache.len());

    Ok(decimal_cache)
}
