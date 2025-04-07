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

pub async fn build_token_balance_cache(client: &Client) -> Result<TokenBalanceCache> {
    log::info!("Building token balance cache...");

    let query = "
        WITH
            dlmm_vaults AS (
                SELECT
                DISTINCT u.vault AS vault
                FROM lookup_lp_info lli
                ARRAY JOIN underlyings AS u
                WHERE lli.curve_type = 7
            ),
            token_balance as (
                SELECT
                    token_account_pubkey,
                    balance
                from current_token_balance_by_user_mint
                where token_account_pubkey in (select vault from dlmm_vaults)
            )
        SELECT
            base58Encode(token_account_pubkey) AS vault,
            toUInt64(balance) AS balance
        FROM token_balance;
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
