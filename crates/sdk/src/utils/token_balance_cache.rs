#![cfg_attr(feature = "swaps-only", allow(unused_imports))]
#![cfg_attr(feature = "swaps-only", allow(unused_variables))]
#![cfg_attr(feature = "swaps-only", allow(dead_code))]
use std::collections::HashMap;

use anyhow::Result;
use clickhouse::{Client, Row};
use rust_decimal::Decimal;
use serde::Deserialize;

pub type TokenBalanceCache = HashMap<String, Option<Decimal>>;

#[derive(Deserialize, Row)]
pub struct TokenBalanceRow {
    pub vault: String,
    pub balance: u64,
}

pub async fn build_token_balance_cache(
    client: &Client,
    skip_preloads: bool,
) -> Result<TokenBalanceCache> {
    #[cfg(feature = "swaps-only")]
    {
        log::info!("Skipping token balance cache build (swaps-only feature enabled)");
        Ok(TokenBalanceCache::new())
    }

    #[cfg(not(feature = "swaps-only"))]
    {
        if skip_preloads {
            return Ok(TokenBalanceCache::new());
        }
        log::info!("Building token balance cache...");

        let query = "
            SELECT * FROM vw_global_current_pool_vault_balances
        ";

        let mut cursor = client.query(query).fetch::<TokenBalanceRow>()?;
        let mut cache = HashMap::new();
        let mut count = 0;

        while let Some(row) = cursor.next().await? {
            cache.insert(row.vault, Some(row.balance.into()));
            count += 1;
        }

        log::info!("Token balance cache built with {} accounts", count);

        Ok(cache)
    }
}
