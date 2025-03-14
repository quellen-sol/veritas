use std::sync::Arc;

use step_ingestooor_sdk::dooot::LPInfoDooot;
use tokio::sync::RwLock;
use veritas_sdk::utils::lp_cache::{LiquidityPool, LpCache};

pub async fn handle_lp_info(info: LPInfoDooot, lp_cache: Arc<RwLock<LpCache>>) {
    let LPInfoDooot {
        lp_mint,
        curve_type,
        ..
    } = info;

    let Some(lp_mint) = lp_mint else {
        return;
    };

    let l_read = lp_cache.read().await;
    if l_read.contains_key(&lp_mint) {
        return;
    }

    // LP doesn't exist, drop the read and grab a write lock,
    // then insert the new LP
    drop(l_read);
    let mut l_write = lp_cache.write().await;
    l_write.insert(lp_mint, LiquidityPool { curve_type });
}
