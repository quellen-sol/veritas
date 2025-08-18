use std::{
    collections::HashSet,
    sync::{Arc, RwLock},
};

use step_ingestooor_sdk::dooot::TokenPriceGlobalDooot;
use veritas_sdk::{
    ppl_graph::graph::USDPriceWithSource,
    types::{MintIndiciesMap, WrappedMintPricingGraph},
};

pub fn handle_token_price(
    price_dooot: TokenPriceGlobalDooot,
    graph: WrappedMintPricingGraph,
    oracle_mint_set: &HashSet<String>,
    mint_indicies: Arc<RwLock<MintIndiciesMap>>,
) {
    let TokenPriceGlobalDooot {
        mint, price_usd, ..
    } = &price_dooot;

    if oracle_mint_set.contains(mint) {
        return;
    }

    let ix = mint_indicies
        .read()
        .expect("Mint indicies read lock poisoned")
        .get(mint)
        .cloned();

    let Some(ix) = ix else {
        return;
    };

    let g_read = graph.read().expect("Graph read lock poisoned");
    let Some(node) = g_read.node_weight(ix) else {
        return;
    };

    let mut p_write = node
        .usd_price
        .write()
        .expect("USD price write lock poisoned");

    p_write.replace(USDPriceWithSource::Relation(*price_usd));
}
