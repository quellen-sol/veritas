use std::{
    collections::HashMap,
    sync::{atomic::AtomicBool, mpsc::SyncSender, Arc, RwLock},
};

use step_ingestooor_sdk::dooot::{Dooot, OraclePriceEventDooot, TokenPriceGlobalDooot};
use veritas_sdk::{
    constants::{EMPTY_PUBKEY, WSOL_MINT},
    types::MintIndiciesMap,
};

use crate::{
    calculator::task::CalculatorUpdate,
    price_points_liquidity::handlers::utils::send_update_to_calculator,
};

#[allow(clippy::unwrap_used)]
pub fn handle_oracle_price_event(
    oracle_price: OraclePriceEventDooot,
    oracle_feed_map: Arc<HashMap<String, String>>,
    mint_indicies: Arc<RwLock<MintIndiciesMap>>,
    calculator_sender: SyncSender<CalculatorUpdate>,
    bootstrap_in_progress: Arc<AtomicBool>,
    price_sender: SyncSender<Dooot>,
) {
    let feed_id = &oracle_price.feed_account_pubkey;
    let Some(feed_mint) = oracle_feed_map.get(feed_id.as_str()).cloned() else {
        return;
    };

    let price = oracle_price.price;

    price_sender
        .send(Dooot::TokenPriceGlobal(TokenPriceGlobalDooot {
            mint: feed_mint.clone(),
            price_usd: price,
            time: oracle_price.time,
        }))
        .unwrap();

    if feed_mint == WSOL_MINT {
        // Send an extra price Dooot for "Native SOL" mint
        price_sender
            .send(Dooot::TokenPriceGlobal(TokenPriceGlobalDooot {
                mint: EMPTY_PUBKEY.to_string(),
                price_usd: price,
                time: oracle_price.time,
            }))
            .unwrap();
    }

    log::info!("New oracle price for {feed_mint}: {price}");

    log::trace!("Getting mint indicies read lock");
    let ix = mint_indicies
        .read()
        .expect("Mint indicies read lock poisoned")
        .get(&feed_mint)
        .cloned();
    log::trace!("Got mint indicies read lock");

    if let Some(ix) = ix {
        let update = CalculatorUpdate::OracleUSDPrice(ix, price);
        send_update_to_calculator(update, &calculator_sender, &bootstrap_in_progress);
    } else {
        log::warn!(
            "Mint {} not in graph, cannot send OracleUSDPrice update",
            feed_mint
        );
    }
}
