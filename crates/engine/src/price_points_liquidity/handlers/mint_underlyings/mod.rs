use std::sync::Arc;

use step_ingestooor_sdk::dooot::MintUnderlyingsGlobalDooot;
use tokio::sync::{mpsc::Sender, RwLock};
use veritas_sdk::{
    types::{EdgeIndiciesMap, MintIndiciesMap, MintPricingGraph},
    utils::{decimal_cache::DecimalCache, lp_cache::LpCache},
};

use crate::price_points_liquidity::handlers::mint_underlyings::handle_specials::handle_special_mint_underlyings;

mod handle_amm_lp;
mod handle_carrot_dooot;
mod handle_fixed;
mod handle_specials;

#[allow(clippy::unwrap_used)]
pub async fn handle_mint_underlyings(
    mu_dooot: MintUnderlyingsGlobalDooot,
    lp_cache: Arc<RwLock<LpCache>>,
    graph: Arc<RwLock<MintPricingGraph>>,
    decimal_cache: Arc<RwLock<DecimalCache>>,
    cache_updator_sender: Sender<String>,
    mint_indicies: Arc<RwLock<MintIndiciesMap>>,
    edge_indicies: Arc<RwLock<EdgeIndiciesMap>>,
) {
    let MintUnderlyingsGlobalDooot { mints, .. } = &mu_dooot;

    match mints.len() {
        1 => {
            // Fixed
            handle_fixed::handle_fixed(
                mu_dooot,
                graph,
                decimal_cache,
                cache_updator_sender,
                mint_indicies,
                edge_indicies,
            )
            .await;
        }
        2 => {
            // LP
            handle_amm_lp::handle_amm_lp(
                mu_dooot,
                lp_cache,
                graph,
                decimal_cache,
                cache_updator_sender,
                mint_indicies,
                edge_indicies,
            )
            .await;
        }
        _ => {
            handle_special_mint_underlyings(&mu_dooot, graph, mint_indicies, decimal_cache).await;
            // We can't handle yet
        }
    }
}
