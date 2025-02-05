#![allow(unused)]

use std::{collections::HashMap, sync::Arc};

use anyhow::{Context, Result};
use chrono::Utc;
use petgraph::graph::{Node, NodeIndex};
use rust_decimal::{Decimal, MathematicalOps};
use serde::Deserialize;
use step_ingestooor_sdk::dooot::{Dooot, TokenPriceGlobalDooot};
use tokio::{
    sync::{mpsc::Receiver, RwLock},
    task::JoinHandle,
};
use veritas_sdk::{
    ppl_graph::graph::MintPricingGraph,
    utils::decimal_cache::{DecimalCache, MintDecimals},
};

use crate::amqp::AMQPManager;

#[derive(Debug)]
pub enum CalculatorUpdate {
    /// Price of USD (from oracle) and index in the graph
    USDPrice(Decimal, NodeIndex),
    /// Mint to recalculate final price for
    UpdatedTokenPrice(NodeIndex),
}

pub fn spawn_calculator_task(
    mut calculator_receiver: Receiver<CalculatorUpdate>,
    amqp_manager: Arc<AMQPManager>,
    clickhouse_client: Arc<clickhouse::Client>,
    graph: Arc<RwLock<MintPricingGraph>>,
    mut decimals_cache: HashMap<String, u8>,
) -> JoinHandle<()> {
    let mut current_usdc_price = None;
    let mut usdc_graph_index = None;

    log::info!("Spawning Calculator task...");
    tokio::spawn(async move {
        while let Some(update) = calculator_receiver.recv().await {
            match update {
                CalculatorUpdate::USDPrice(price, idx) => {
                    log::info!("Oracle price for USDC: {price}");
                    current_usdc_price = Some(price);
                    usdc_graph_index = Some(idx);
                    let g_read = graph.read().await;

                    let dooots = match calculate_token_price(
                        &g_read,
                        clickhouse_client.clone(),
                        &mut decimals_cache,
                        idx,
                        price,
                        idx,
                    )
                    .await
                    {
                        Ok(dooots) => dooots,
                        Err(e) => {
                            log::error!("Error calculating token price: {e}");
                            continue;
                        }
                    };

                    amqp_manager.publish_dooots(dooots).await;
                }
                CalculatorUpdate::UpdatedTokenPrice(_token) => {
                    // TODO: impl
                    let (Some(usdc_price), Some(usdc_idx)) = (current_usdc_price, usdc_graph_index)
                    else {
                        log::error!("USDC price and graph index not set, cannot recalc atm");
                        continue;
                    };
                    let g_read = graph.read().await;
                    let Ok(new_price) = calculate_token_price(
                        &g_read,
                        clickhouse_client.clone(),
                        &mut decimals_cache,
                        usdc_idx, // TODO: use `token`
                        usdc_price,
                        usdc_idx,
                    )
                    .await
                    .inspect_err(|e| log::error!("{e}")) else {
                        continue;
                    };

                    log::info!("Calculated new prices: {new_price:?}");

                    amqp_manager.publish_dooots(new_price).await;
                }
            }
        }
    })
}

/// v0.5 ALGORITHM
///
/// When one token is updated here, look at all neighbors OUTGOING, and calc price for them ONLY
/// Next iteration will address looking deeper than 1 level of neighbors
///
/// TODO: Recursively search neighbors (how to hold temporary prices?)
pub async fn calculate_token_price(
    graph: &MintPricingGraph,
    clickhouse_client: Arc<clickhouse::Client>,
    decimals_cache: &mut DecimalCache,
    token: NodeIndex,
    usdc_price_usd: Decimal,
    usdc_idx: NodeIndex,
) -> Result<Vec<Dooot>> {
    let mut dooots = Vec::new();
    let this_token = graph
        .node_weight(token)
        .context("Token should exist in graph!!")?;
    let mut per_token_prices = HashMap::new();
    let local_neighbors = graph.neighbors_directed(token, petgraph::Direction::Outgoing);
    for neighbor in local_neighbors {
        let neighbor_token = graph
            .node_weight(neighbor)
            .context("Neighbor should exist in graph!!")?;
        let mint_entry = per_token_prices
            .entry(&neighbor_token.mint)
            .or_insert(vec![]);

        get_mint_decimals(
            &[&this_token.mint, &neighbor_token.mint],
            decimals_cache,
            clickhouse_client.clone(),
        )
        .await?;

        let this_decimals = *decimals_cache.get(&this_token.mint).context(format!(
            "decimals expected from get_mint_decimals. Is CH missing {}?",
            &this_token.mint
        ))? as i16;
        let neighbor_decimals = *decimals_cache.get(&neighbor_token.mint).context(format!(
            "decimals expected from get_mint_decimals. Is CH missing {}?",
            &neighbor_token.mint
        ))? as i16;
        let decimal_factor = Decimal::from(10).powi((neighbor_decimals - this_decimals) as i64);

        let edges = graph.edges_connecting(token, neighbor);
        for edge in edges {
            let w = edge.weight().read().await;
            let Some(this_per_that) = w.this_per_that else {
                // No price set yet, skip
                continue;
            };

            mint_entry.push(this_per_that);
        }

        // Just do an average for now
        // log::info!("All prices: {:?}", mint_entry);
        let len = Decimal::from(mint_entry.len());
        if len == Decimal::ZERO {
            continue;
        }
        let total = mint_entry.iter().sum::<Decimal>();
        let average_price = (total / len) * usdc_price_usd * decimal_factor;

        log::info!(
            "Calculated price for {}: {}",
            neighbor_token.mint,
            average_price
        );

        dooots.push(Dooot::TokenPriceGlobal(TokenPriceGlobalDooot {
            mint: neighbor_token.mint.clone(),
            price_usd: average_price,
            time: Utc::now().naive_utc(),
        }));
    }

    Ok(dooots)
}

pub async fn get_mint_decimals(
    mints: &[&str],
    decimals_cache: &mut HashMap<String, u8>,
    clickhouse_client: Arc<clickhouse::Client>,
) -> Result<Vec<MintDecimals>> {
    let mut decimal_vals = Vec::with_capacity(mints.len());
    let mut mints_to_query = Vec::with_capacity(mints.len());

    for mint in mints {
        let decimals = decimals_cache.get(*mint).cloned();
        if let Some(decimals) = decimals {
            let dec_val = MintDecimals {
                mint: mint.to_string(),
                decimals: Some(decimals),
            };

            decimal_vals.push(dec_val);
        } else {
            mints_to_query.push(mint);
        }
    }

    if !mints_to_query.is_empty() {
        let dec_query_res = clickhouse_client
            .query(
                "
                    SELECT
                        base58Encode(reinterpretAsString(mint)) as mint,
                        anyLastMerge(decimals) AS decimals
                    FROM lookup_mint_info lmi
                    WHERE mint in ?
                    GROUP BY mint
                ",
            )
            .bind(&mints_to_query)
            .fetch_all::<MintDecimals>()
            .await?;

        for dec in &dec_query_res {
            if let Some(decimals) = dec.decimals {
                decimals_cache.insert(dec.mint.clone(), decimals);
            }
        }

        decimal_vals.extend(dec_query_res);
    }

    Ok(decimal_vals)
}
