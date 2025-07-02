use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, SyncSender},
        Arc, RwLock,
    },
};

use step_ingestooor_sdk::dooot::Dooot;
use veritas_sdk::{
    types::{EdgeIndiciesMap, MintIndiciesMap, WrappedMintPricingGraph},
    utils::{
        decimal_cache::DecimalCache, lp_cache::LpCache, token_balance_cache::TokenBalanceCache,
    },
};

use crate::calculator::task::CalculatorUpdate;

#[inline]
#[allow(clippy::unwrap_used)]
pub fn send_update_to_calculator(
    update: CalculatorUpdate,
    calculator_sender: &SyncSender<CalculatorUpdate>,
    bootstrap_in_progress: &AtomicBool,
) {
    if bootstrap_in_progress.load(Ordering::Relaxed) {
        return;
    }

    calculator_sender
        .send(update)
        .inspect_err(|e| log::error!("Error sending CalculatorUpdate: {e}"))
        .unwrap();
}

/// Stores receivers as well so that they don't drop
#[allow(dead_code)]
pub struct TestHandlerState {
    pub lp_cache: Arc<RwLock<LpCache>>,
    pub graph: WrappedMintPricingGraph,
    pub calculator_sender: SyncSender<CalculatorUpdate>,
    pub calculator_receiver: Receiver<CalculatorUpdate>,
    pub decimal_cache: Arc<RwLock<DecimalCache>>,
    pub oracle_feed_map: Arc<RwLock<HashMap<String, String>>>,
    pub mint_indicies: Arc<RwLock<MintIndiciesMap>>,
    pub edge_indicies: Arc<RwLock<EdgeIndiciesMap>>,
    pub receiver_arc: Receiver<String>,
    pub sender_arc: SyncSender<String>,
    pub bootstrap_in_progress: Arc<AtomicBool>,
    pub token_balance_cache: Arc<RwLock<TokenBalanceCache>>,
    pub price_sender: SyncSender<Dooot>,
    pub price_receiver: Receiver<Dooot>,
}

#[cfg(test)]
pub fn build_test_handler_state() -> TestHandlerState {
    use veritas_sdk::types::MintPricingGraph;

    let lp_cache = Arc::new(RwLock::new(LpCache::new()));
    let graph = Arc::new(RwLock::new(MintPricingGraph::new()));
    let (calculator_sender, calculator_receiver) =
        std::sync::mpsc::sync_channel::<CalculatorUpdate>(10);
    let decimal_cache = Arc::new(RwLock::new(DecimalCache::new()));
    let oracle_feed_map = Arc::new(RwLock::new(HashMap::new()));
    let mint_indicies = Arc::new(RwLock::new(MintIndiciesMap::new()));
    let edge_indicies = Arc::new(RwLock::new(EdgeIndiciesMap::new()));
    let (sender_arc, receiver_arc) = std::sync::mpsc::sync_channel::<String>(10);
    let bootstrap_in_progress = Arc::new(AtomicBool::new(false));
    let token_balance_cache = Arc::new(RwLock::new(TokenBalanceCache::new()));
    let (price_sender, price_receiver) = std::sync::mpsc::sync_channel::<Dooot>(10);

    TestHandlerState {
        lp_cache,
        graph,
        calculator_sender,
        calculator_receiver,
        decimal_cache,
        oracle_feed_map,
        mint_indicies,
        edge_indicies,
        sender_arc,
        receiver_arc,
        bootstrap_in_progress,
        token_balance_cache,
        price_sender,
        price_receiver,
    }
}
