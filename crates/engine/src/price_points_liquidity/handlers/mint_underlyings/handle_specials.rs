use std::sync::Arc;

use step_ingestooor_sdk::dooot::MintUnderlyingsGlobalDooot;
use tokio::sync::RwLock;
use veritas_sdk::{ppl_graph::graph::WrappedMintPricingGraph, utils::decimal_cache::DecimalCache};

use crate::price_points_liquidity::{
    handlers::mint_underlyings::handle_carrot_dooot::handle_carrot_dooot, task::MintIndiciesMap,
};

pub async fn handle_special_mint_underlyings(
    dooot: &MintUnderlyingsGlobalDooot,
    graph: WrappedMintPricingGraph,
    mint_indicies: Arc<RwLock<MintIndiciesMap>>,
    decimal_cache: Arc<RwLock<DecimalCache>>,
) {
    if dooot.mint_pubkey.as_str() == "CRTx1JouZhzSU6XytsE42UQraoGqiHgxabocVfARTy2s" {
        handle_carrot_dooot(dooot, graph, mint_indicies, decimal_cache).await;
    }
}
