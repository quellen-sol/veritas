use std::sync::Arc;

use step_ingestooor_sdk::dooot::MintInfoDooot;
use tokio::sync::RwLock;
use veritas_sdk::utils::decimal_cache::DecimalCache;

pub async fn handle_mint_info(info: MintInfoDooot, decimal_cache: Arc<RwLock<DecimalCache>>) {
    let MintInfoDooot { mint, decimals, .. } = info;

    if let Some(decimals) = decimals {
        let token_exists = decimal_cache.read().await.get(&mint).is_some();
        if !token_exists {
            let mut dc_write = decimal_cache.write().await;
            dc_write.insert(mint, decimals as u8);
        };
    }
}
