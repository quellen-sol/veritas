use std::{collections::HashMap, sync::Arc};

use step_ingestooor_sdk::dooot::OraclePriceEventDooot;
use tokio::sync::{mpsc::Sender, RwLock};
use veritas_sdk::{
    ppl_graph::graph::{MintPricingGraph, USDPriceWithSource},
    utils::oracle_cache::OraclePriceCache,
};

use crate::{calculator::task::CalculatorUpdate, price_points_liquidity::task::MintIndiciesMap};

pub async fn handle_oracle_price_event(
    oracle_price: OraclePriceEventDooot,
    oracle_feed_map: Arc<HashMap<String, String>>,
    oracle_cache: Arc<RwLock<OraclePriceCache>>,
    graph: Arc<RwLock<MintPricingGraph>>,
    mint_indicies: Arc<RwLock<MintIndiciesMap>>,
    calculator_sender: Arc<Sender<CalculatorUpdate>>,
) {
    let feed_id = &oracle_price.feed_account_pubkey;
    let Some(feed_mint) = oracle_feed_map.get(feed_id.as_str()).cloned() else {
        return;
    };

    let price = oracle_price.price;

    log::info!("New oracle price for {feed_mint}: {price}");

    // Quick lock to update the oracle cache
    {
        let mut oc_write = oracle_cache.write().await;
        oc_write.insert(feed_mint.clone(), price);
    }

    let ix = {
        let mint_indicies_read = mint_indicies.read().await;
        mint_indicies_read.get(&feed_mint).cloned()
    };
    if let Some(ix) = ix {
        // Update the price of the mint in the graph
        {
            let g_read = graph.read().await;
            let node_weight = g_read.node_weight(ix).unwrap();
            let mut p_write = node_weight.usd_price.write().await;
            p_write.replace(USDPriceWithSource::Oracle(price));
        }

        calculator_sender
            .send(CalculatorUpdate::OracleUSDPrice(ix))
            .await
            .unwrap();
    } else {
        log::warn!(
            "Mint {} not in graph, cannot send OracleUSDPrice update",
            feed_mint
        );
    }
}
