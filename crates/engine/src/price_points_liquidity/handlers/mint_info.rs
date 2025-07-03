use std::sync::{Arc, RwLock};

use step_ingestooor_sdk::dooot::MintInfoDooot;
use veritas_sdk::utils::decimal_cache::DecimalCache;

pub fn handle_mint_info(info: MintInfoDooot, decimal_cache: Arc<RwLock<DecimalCache>>) {
    let MintInfoDooot { mint, decimals, .. } = info;

    if let Some(decimals) = decimals {
        let token_exists = decimal_cache
            .read()
            .expect("Decimal cache read lock poisoned")
            .get(&mint)
            .is_some();

        if !token_exists {
            let mut dc_write = decimal_cache
                .write()
                .expect("Decimal cache write lock poisoned");

            dc_write.insert(mint, decimals as u8);
        };
    }
}
