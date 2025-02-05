use std::{collections::HashSet, sync::Arc, time::Instant};

use anyhow::{Context, Result};
use chrono::Utc;
use petgraph::graph::NodeIndex;
use rust_decimal::{Decimal, MathematicalOps};
use step_ingestooor_sdk::dooot::{Dooot, TokenPriceGlobalDooot};
use tokio::{
    sync::{
        mpsc::{Receiver, Sender},
        RwLock, Semaphore,
    },
    task::JoinHandle,
};
use veritas_sdk::{
    ppl_graph::graph::{MintPricingGraph, USDPriceWithSource},
    utils::decimal_cache::{DecimalCache, MintDecimals},
};

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
    clickhouse_client: Arc<clickhouse::Client>,
    graph: Arc<RwLock<MintPricingGraph>>,
    decimals_cache: Arc<RwLock<DecimalCache>>,
    dooot_tx: Arc<Sender<Dooot>>,
    max_calculator_subtasks: u8,
) -> JoinHandle<()> {
    log::info!("Spawning Calculator task...");

    tokio::spawn(async move {
        // Semaphore to limit the number of concurrent calculator tasks
        // Belongs to the parent task
        let semaphore = Arc::new(Semaphore::new(max_calculator_subtasks as usize));
        while let Some(update) = calculator_receiver.recv().await {
            // Can we progress?
            let permit = semaphore.acquire().await.unwrap();

            let graph = graph.clone();
            let clickhouse_client = clickhouse_client.clone();
            let decimals_cache = decimals_cache.clone();
            let dooot_tx = dooot_tx.clone();

            tokio::spawn(async move {
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

                        match calculate_token_price(
                            &g_read,
                            clickhouse_client.clone(),
                            decimals_cache,
                            idx,
                            &mut visited,
                            dooot_tx,
                        )
                        .await
                        {
                            Ok(_) => {}
                            Err(e) => {
                                log::error!("Error calculating token price: {e}");
                            }
                        }
                    }
                    CalculatorUpdate::NewTokenRatio(token) => {
                        // TODO: impl
                        // let (Some(usdc_price), Some(usdc_idx)) = (current_usdc_price, usdc_graph_index)
                        // else {
                        //     log::error!("USDC price and graph index not set, cannot recalc atm");
                        //     continue;
                        // };
                        // let g_read = graph.read().await;
                        // let mut visited = HashSet::with_capacity(g_read.node_count());

                        // let now = Instant::now();
                        // match calculate_token_price(
                        //     &g_read,
                        //     clickhouse_client.clone(),
                        //     decimals_cache,
                        //     token,
                        //     &mut visited,
                        //     dooot_tx,
                        // )
                        // .await
                        // {
                        //     Ok(_) => {
                        //         // log::info!("Graph recalc took {:?}", now.elapsed());
                        //     }
                        //     Err(e) => {
                        //         log::error!("Error calculating token price: {e}");
                        //     }
                        // }
                    }
                }

                // Release the permit
                // This is going to happen anyway, but for clarity
                // drop(permit);
            })
            .await
            .unwrap();

            // Release the permit
            drop(permit);
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
    decimals_cache: Arc<RwLock<DecimalCache>>,
    token: NodeIndex,
    visted_nodes: &mut HashSet<NodeIndex>,
    dooot_tx: Arc<Sender<Dooot>>,
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
            decimals_cache.clone(),
            clickhouse_client.clone(),
        )
        .await?;

        let d_read = decimals_cache.read().await;
        let this_decimals = *d_read.get(&this_mint).context(format!(
            "decimals expected from get_mint_decimals. Is CH missing {}?",
            &this_mint
        ))? as i16;
        let neighbor_decimals = *d_read.get(&neighbor_mint).context(format!(
            "decimals expected from get_mint_decimals. Is CH missing {}?",
            &neighbor_mint
        ))? as i16;
        drop(d_read);
        let decimal_factor = Decimal::from(10).powi((neighbor_decimals - this_decimals) as i64);

        let Some((w_ratio, _liq)) =
            get_liq_weighted_price_ratio(token, neighbor, graph, this_decimals, decimal_factor)
                .await
        else {
            // log::warn!("Unable to get weighted ratio between {this_mint} -> {neighbor_mint}");
            continue;
        };

        let final_price = this_price * w_ratio;
        // log::info!("Calculated price for {neighbor_mint}: {final_price}");

        {
            let mut w_neighbor = neighbor_token.write().await;
            w_neighbor
                .usd_price
                .replace(USDPriceWithSource::Relation(final_price));
        }

        let price_dooot = Dooot::TokenPriceGlobal(TokenPriceGlobalDooot {
            time: Utc::now().naive_utc(),
            mint: neighbor_mint,
            price_usd: final_price,
        });

        dooot_tx.send(price_dooot).await?;

        visted_nodes.insert(neighbor);

        Box::pin(calculate_token_price(
            graph,
            clickhouse_client.clone(),
            decimals_cache.clone(),
            neighbor,
            visted_nodes,
            dooot_tx.clone(),
        ))
        .await?;
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
    decimals_a: i16,
    decimal_factor: Decimal,
) -> Option<(Decimal, Decimal)> {
    let node_a = graph.node_weight(a)?.clone();
    let r_node_a = node_a.read().await;
    let mint_a = r_node_a.mint.clone();
    drop(r_node_a);

    let edges_iter = graph.edges_connecting(a, b);

    // let mut curr_ratio = Decimal::ONE;
    // let mut curr_numerator = Decimal::ONE;
    // let mut curr_denominator = Decimal::ONE;
    let mut cm_weighted_price = Decimal::ZERO;
    let mut total_liq = Decimal::ZERO;

    for edge in edges_iter {
        let e_read = edge.weight().read().await;
        let (Some(liq), Some(ratio)) = (&e_read.liquidity, &e_read.this_per_that) else {
            // No liq value or ratio
            continue;
        };

        let liq_val =
            liq.get_liq_for_mint(&mint_a).ok()? / Decimal::from(10).powi(decimals_a as i64);

        total_liq += liq_val;
        cm_weighted_price += liq_val * ratio * decimal_factor;

        // // Canceling out old averaging terms, see formula
        // let factor_numerator = curr_denominator * (curr_numerator + (liq_val * ratio * decimal_factor));
        // let factor_denominator = (curr_numerator + liq_val) * (curr_numerator);

        // curr_numerator *= factor_numerator;
        // curr_denominator *= factor_denominator;

        // curr_ratio = curr_numerator / curr_denominator;
    }

    if total_liq == Decimal::ZERO {
        return None;
    }

    let curr_ratio = cm_weighted_price / total_liq;

    Some((curr_ratio, total_liq))
}

pub async fn get_mint_decimals(
    mints: &[&str],
    decimals_cache: Arc<RwLock<DecimalCache>>,
    clickhouse_client: Arc<clickhouse::Client>,
) -> Result<Vec<MintDecimals>> {
    let mut decimal_vals = Vec::with_capacity(mints.len());
    let mut mints_to_query = Vec::with_capacity(mints.len());
    let d_read = decimals_cache.read().await;

    for mint in mints {
        let decimals = d_read.get(*mint).cloned();
        if let Some(decimals) = decimals {
            let dec_val = MintDecimals {
                mint_pubkey: mint.to_string(),
                decimals: Some(decimals),
            };

            decimal_vals.push(dec_val);
        } else {
            mints_to_query.push(mint);
        }
    }

    drop(d_read);

    if !mints_to_query.is_empty() {
        let query_mints_bytes = mints_to_query
            .iter()
            .map(|s| bs58::decode(s).into_vec().unwrap().try_into().unwrap())
            .collect::<Vec<[u8; 32]>>();
        let query_start = Instant::now();
        let dec_query_res = clickhouse_client
            .query(
                "
                    SELECT
                        base58Encode(reinterpretAsString(mint)) as mint_pubkey,
                        anyLastMerge(decimals) AS decimals
                    FROM lookup_mint_info lmi
                    WHERE mint in (
                        SELECT arrayJoin(arrayMap(x -> base58Decode(x), ?))
                    )
                    GROUP BY mint
                ",
            )
            .bind(mints_to_query)
            .fetch_all::<MintDecimals>()
            .await?;
        log::info!("CH query done in {:?}", query_start.elapsed());

        let mut d_write = None;
        for dec in &dec_query_res {
            if let Some(decimals) = dec.decimals {
                let mut write_guard = match d_write {
                    Some(g) => g,
                    None => decimals_cache.write().await,
                };
                write_guard.insert(dec.mint_pubkey.clone(), decimals);

                d_write = Some(write_guard);
            }
        }

        decimal_vals.extend(dec_query_res);
    }

    Ok(decimal_vals)
}
