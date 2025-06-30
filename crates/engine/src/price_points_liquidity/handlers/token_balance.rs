use std::sync::Arc;

use step_ingestooor_sdk::dooot::TokenBalanceUserDooot;
use tokio::sync::RwLock;
use veritas_sdk::utils::token_balance_cache::TokenBalanceCache;

pub async fn handle_token_balance(
    balance_dooot: TokenBalanceUserDooot,
    token_balance_cache: Arc<RwLock<TokenBalanceCache>>,
) {
    let TokenBalanceUserDooot {
        balance,
        token_account_pubkey,
        ..
    } = balance_dooot;

    {
        log::trace!("TOKEN BALANCE HANDLER - Getting token balance cache read lock");
        let tbc_read = token_balance_cache.read().await;
        log::trace!("TOKEN BALANCE HANDLER - Got token balance cache read lock");
        if !tbc_read.contains_key(&token_account_pubkey) {
            // Only update cache with balances we want to track. e.g. dlmm vault balance
            return;
        }
    }

    log::trace!("TOKEN BALANCE HANDLER - Getting token balance cache write lock");
    let mut tbc_write = token_balance_cache.write().await;
    log::trace!("TOKEN BALANCE HANDLER - Got token balance cache write lock");
    tbc_write.insert(token_account_pubkey, Some(balance));
}
