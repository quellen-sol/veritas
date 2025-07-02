use std::sync::{Arc, RwLock};

use step_ingestooor_sdk::dooot::MintInfoDooot;
use veritas_sdk::utils::decimal_cache::DecimalCache;

pub fn handle_mint_info(info: MintInfoDooot, decimal_cache: Arc<RwLock<DecimalCache>>) {
    let MintInfoDooot { mint, decimals, .. } = info;

    if let Some(decimals) = decimals {
        log::trace!("Getting decimal cache read lock");
        let token_exists = decimal_cache
            .read()
            .expect("Decimal cache read lock poisoned")
            .get(&mint)
            .is_some();
        log::trace!("Got decimal cache read lock");
        if !token_exists {
            log::trace!("Getting decimal cache write lock");
            let mut dc_write = decimal_cache
                .write()
                .expect("Decimal cache write lock poisoned");
            log::trace!("Got decimal cache write lock");
            dc_write.insert(mint, decimals as u8);
        };
    }
}
