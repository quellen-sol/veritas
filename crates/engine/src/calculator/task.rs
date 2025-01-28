#![allow(unused)]

use std::{collections::HashMap, sync::Arc};

use anyhow::{Context, Result};
use petgraph::graph::{Node, NodeIndex};
use rust_decimal::Decimal;
use tokio::{
    sync::{mpsc::Receiver, RwLock},
    task::JoinHandle,
};
use veritas_sdk::ppl_graph::graph::MintPricingGraph;

#[derive(Debug)]
pub enum CalculatorUpdate {
    /// Price of USDC (from oracle) and index in the graph
    USDCPrice(Decimal, NodeIndex),
    /// Mint to recalculate final price for
    UpdatedTokenPrice(NodeIndex),
}

pub fn spawn_calculator_task(
    mut calculator_receiver: Receiver<CalculatorUpdate>,
    graph: Arc<RwLock<MintPricingGraph>>,
) -> JoinHandle<()> {
    let mut current_usdc_price = None;
    let mut usdc_graph_index = None;

    tokio::spawn(async move {
        while let Some(update) = calculator_receiver.recv().await {
            match update {
                CalculatorUpdate::USDCPrice(price, idx) => {
                    log::info!("CALCULATOR - Oracle price for USDC: {price}");
                    current_usdc_price = Some(price);
                    usdc_graph_index = Some(idx);
                    let g_read = graph.read().await;
                    // log::info!("Got read lock");
                    calculate_token_price(&g_read, idx, price, idx).await.ok();
                }
                CalculatorUpdate::UpdatedTokenPrice(token) => {
                    // TODO: impl
                    // let (Some(usdc_price), Some(usdc_idx)) = (current_usdc_price, usdc_graph_index)
                    // else {
                    //     log::error!("USDC price and graph index not set, cannot recalc atm");
                    //     continue;
                    // };
                    // let g_read = graph.read().await;
                    // log::info!("Got read lock");
                    // let Ok(new_price) = calculate_token_price(&g_read, token, usdc_price, usdc_idx)
                    //     .await
                    //     .inspect_err(|e| log::error!("{e}"))
                    // else {
                    //     continue;
                    // };
                    // log::info!("Calculated new price: {new_price}");
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
    token: NodeIndex,
    usdc_price: Decimal,
    usdc_idx: NodeIndex,
) -> Result<()> {
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
        let total = mint_entry.iter().sum::<Decimal>();
        let len = Decimal::from(mint_entry.len());
        let average_price = total / len;

        log::info!(
            "Average (unweighted) price of {}: {:.9}",
            neighbor_token.mint,
            average_price
        );
    }

    Ok(())
}
