use std::sync::Arc;

use step_ingestooor_sdk::dooot::MintInfoDooot;
use tokio::sync::RwLock;
use veritas_sdk::utils::decimal_cache::DecimalCache;

pub async fn handle_mint_info(info: MintInfoDooot, decimal_cache: Arc<RwLock<DecimalCache>>) {
    let MintInfoDooot { mint, decimals, .. } = info;

    if let Some(decimals) = decimals {
        let mint_str = mint.to_string();
        let mut decimal_cache_write = decimal_cache.write().await;
        let decimals = decimals as u8;
        decimal_cache_write.insert(mint_str, decimals);
    }
}
