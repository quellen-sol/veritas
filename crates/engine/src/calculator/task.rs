use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use anyhow::{Context, Result};
use chrono::{NaiveDateTime, Utc};
use petgraph::graph::{Node, NodeIndex};
use rust_decimal::{Decimal, MathematicalOps};
use serde::Deserialize;
use step_ingestooor_sdk::dooot::{Dooot, TokenPriceGlobalDooot};
use tokio::{
    sync::{mpsc::Receiver, RwLock},
    task::JoinHandle,
};
use veritas_sdk::{
    ppl_graph::graph::{MintPricingGraph, USDPriceWithSource},
    utils::decimal_cache::{DecimalCache, MintDecimals},
};

use crate::amqp::AMQPManager;

#[derive(Debug)]
pub enum CalculatorUpdate {
    /// Price of USD (from oracle) and index in the graph
    OracleUSDPrice(Decimal, NodeIndex),
    /// A relation going **OUT FROM** this token has changed,
    /// recalc everything from this
    ///
    /// PPL Thread will normally send two updates "at once",
    /// so this will cause a recalc in both directions,
    /// but the algo should treat a single update as
    /// recalc'ing recursive neighbors pointing **AWAY FROM** this
    NewTokenRatio(NodeIndex),
}

pub fn spawn_calculator_task(
    mut calculator_receiver: Receiver<CalculatorUpdate>,
    amqp_manager: Arc<AMQPManager>,
    clickhouse_client: Arc<clickhouse::Client>,
    graph: Arc<RwLock<MintPricingGraph>>,
    mut decimals_cache: HashMap<String, u8>,
) -> JoinHandle<()> {
    log::info!("Spawning Calculator task...");

    tokio::spawn(async move {
        while let Some(update) = calculator_receiver.recv().await {
            match update {
                CalculatorUpdate::OracleUSDPrice(price, idx) => {
                    log::info!("Oracle price for USDC: {price}");
                    let g_read = graph.read().await;

                    // Add usd_price to this node,
                    // Separate block to drop the write lock at end,
                    // since `calculate_token_price` will grab a read
                    {
                        let node = g_read
                            .node_weight(idx)
                            .expect("This node should already exist!")
                            .clone();
                        let mut node = node.write().await;
                        node.usd_price.replace(USDPriceWithSource::Oracle(price));
                    }
                    let mut visited = HashSet::with_capacity(g_read.node_count());
                    let mut dooots = Vec::with_capacity(g_read.node_count());

                    match calculate_token_price(
                        &g_read,
                        clickhouse_client.clone(),
                        &mut decimals_cache,
                        idx,
                        &mut visited,
                        &mut dooots,
                    )
                    .await
                    {
                        Ok(dooots) => dooots,
                        Err(e) => {
                            log::error!("Error calculating token price: {e}");
                            continue;
                        }
                    };

                    amqp_manager.publish_dooots(dooots).await.unwrap();
                }
                CalculatorUpdate::NewTokenRatio(token) => {
                    // TODO: impl
                    // let (Some(usdc_price), Some(usdc_idx)) = (current_usdc_price, usdc_graph_index)
                    // else {
                    //     log::error!("USDC price and graph index not set, cannot recalc atm");
                    //     continue;
                    // };
                    let g_read = graph.read().await;
                    let mut visited = HashSet::with_capacity(g_read.node_count());
                    let mut dooots = Vec::with_capacity(g_read.node_count());

                    let Ok(new_price) = calculate_token_price(
                        &g_read,
                        clickhouse_client.clone(),
                        &mut decimals_cache,
                        token,
                        &mut visited,
                        &mut dooots,
                    )
                    .await
                    .inspect_err(|e| log::error!("{e}")) else {
                        continue;
                    };

                    amqp_manager.publish_dooots(dooots).await.unwrap();
                }
            }
        }
    })
}

/// v1.0 ALGORITHM
///
/// When one token is updated here, look at all neighbors OUTGOING, and calc price for them ONLY
/// Next iteration will address looking deeper than 1 level of neighbors
///
/// TODO: Recursively search neighbors (how to hold temporary prices?)
///
/// Notes:
/// - Dominator algorithms can locate "popular" tokens
///
/// Many other algos for finding liq paths https://docs.rs/petgraph/latest/petgraph/algo/index.html#modules
pub async fn calculate_token_price(
    graph: &MintPricingGraph,
    clickhouse_client: Arc<clickhouse::Client>,
    decimals_cache: &mut DecimalCache,
    token: NodeIndex,
    visted_nodes: &mut HashSet<NodeIndex>,
    dooots: &mut Vec<Dooot>,
) -> Result<()> {
    let this_token = graph
        .node_weight(token)
        .context("Token should exist in graph!!")?
        .clone();

    visted_nodes.insert(token);

    // Just grab what we need from locks quickly, then drop
    // Even if that means consuming more mem
    let this_token = this_token.read().await;
    let Some(this_price) = this_token.usd_price.clone() else {
        return Ok(());
    };
    let this_price = this_price.extract_price();

    let this_mint = this_token.mint.clone();
    drop(this_token);

    let neighbors = graph.neighbors_directed(token, petgraph::Direction::Outgoing);
    for neighbor in neighbors {
        if visted_nodes.contains(&neighbor) {
            continue;
        }
        let neighbor_token = graph
            .node_weight(neighbor)
            .context("Neighbor should exist in graph!!")?
            .clone();
        let r_neighbor = neighbor_token.read().await;

        if matches!(r_neighbor.usd_price, Some(USDPriceWithSource::Oracle(_))) {
            continue;
        }

        let neighbor_mint = r_neighbor.mint.clone();
        drop(r_neighbor);

        get_mint_decimals(
            &[&this_mint, &neighbor_mint],
            decimals_cache,
            clickhouse_client.clone(),
        )
        .await?;

        let this_decimals = *decimals_cache.get(&this_mint).context(format!(
            "decimals expected from get_mint_decimals. Is CH missing {}?",
            &this_mint
        ))? as i16;
        let neighbor_decimals = *decimals_cache.get(&neighbor_mint).context(format!(
            "decimals expected from get_mint_decimals. Is CH missing {}?",
            &neighbor_mint
        ))? as i16;
        let decimal_factor = Decimal::from(10).powi((neighbor_decimals - this_decimals) as i64);

        let Some((w_ratio, _liq)) = get_liq_weighted_price_ratio(token, neighbor, graph).await
        else {
            log::warn!("Unable to get weighted ratio between {this_mint} -> {neighbor_mint}");
            continue;
        };

        let final_price = this_price * w_ratio * decimal_factor;

        let mut w_neighbor = neighbor_token.write().await;
        w_neighbor
            .usd_price
            .replace(USDPriceWithSource::Relation(final_price.clone()));
        drop(w_neighbor);

        let price_dooot = Dooot::TokenPriceGlobal(TokenPriceGlobalDooot {
            time: Utc::now().naive_utc(),
            mint: neighbor_mint,
            price_usd: final_price,
        });

        dooots.push(price_dooot);

        visted_nodes.insert(neighbor);

        Box::pin(
            calculate_token_price(
                graph,
                clickhouse_client.clone(),
                decimals_cache,
                neighbor,
                visted_nodes,
                dooots,
            )
            .await,
        );
    }

    Ok(())
}

/// From A -> B, directed
///
/// https://gist.github.com/quellen-sol/ae4cfcce79af1c72c596180e1dde60e1#master-formula
///
/// Returns (weighted_price, total_liquidity)
pub async fn get_liq_weighted_price_ratio(
    a: NodeIndex,
    b: NodeIndex,
    graph: &MintPricingGraph,
) -> Option<(Decimal, Decimal)> {
    let node_a = graph.node_weight(a)?.clone();
    let mint_a = &node_a.read().await.mint;

    let edges_iter = graph.edges_connecting(a, b);

    let mut curr_ratio = Decimal::ONE;
    let mut curr_numerator = Decimal::ONE;
    let mut curr_denominator = Decimal::ONE;
    let mut total_liq = Decimal::ZERO;

    for edge in edges_iter {
        let e_read = edge.weight().read().await;
        let (Some(liq), Some(ratio)) = (&e_read.liquidity, &e_read.this_per_that) else {
            // No liq value or ratio
            continue;
        };

        let liq_val = liq.get_liq_for_mint(&mint_a).ok()?;

        total_liq += liq_val;

        // Canceling out old averaging terms, see formula
        let factor_numerator = curr_denominator * (curr_numerator + (liq_val * ratio));
        let factor_denominator = (curr_numerator + liq_val) * (curr_numerator);

        curr_numerator *= factor_numerator;
        curr_denominator *= factor_denominator;

        curr_ratio = curr_numerator / curr_denominator;
    }

    if curr_ratio == Decimal::ONE {
        return None;
    }

    Some((curr_ratio, total_liq))
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
