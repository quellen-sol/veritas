use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::{Context, Result};
use chrono::Utc;
use petgraph::graph::NodeIndex;
use rust_decimal::{Decimal, MathematicalOps};
use step_ingestooor_sdk::{
    dooot::{Dooot, TokenPriceGlobalDooot},
    utils::simple_stack::SimpleStack,
};
use tokio::{
    sync::{
        mpsc::{Receiver, Sender},
        RwLock,
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
    NewTokenRatio(Decimal, NodeIndex),
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
        let task_counter = Arc::new(AtomicU8::new(0));
        while let Some(update) = calculator_receiver.recv().await {
            let task_counter = task_counter.clone();
            let mut current_count = task_counter.load(Ordering::Relaxed);
            while current_count >= max_calculator_subtasks {
                // Noop, wait for a task to finish
                tokio::time::sleep(Duration::from_millis(1)).await;
                current_count = task_counter.load(Ordering::Relaxed);
            }

            task_counter.fetch_add(1, Ordering::Relaxed);

            let graph = graph.clone();
            let clickhouse_client = clickhouse_client.clone();
            let decimals_cache = decimals_cache.clone();
            let dooot_tx = dooot_tx.clone();

            tokio::spawn(async move {
                match update {
                    CalculatorUpdate::OracleUSDPrice(price, idx) => {
                        // log::info!("Oracle price for USDC: {price}");
                        // let g_read = graph.read().await;

                        // // Add usd_price to this node,
                        // // Separate block to drop the write lock at end,
                        // // since `calculate_token_price` will grab a read
                        // {
                        //     let node = g_read
                        //         .node_weight(idx)
                        //         .expect("This node should already exist!")
                        //         .clone();
                        //     let mut node = node.write().await;
                        //     node.usd_price.replace(USDPriceWithSource::Oracle(price));
                        // }
                        // let mut visited = SimpleStack::with_capacity(g_read.node_count());

                        // // let now = Instant::now();
                        // match calculate_token_price(
                        //     &g_read,
                        //     clickhouse_client.clone(),
                        //     decimals_cache,
                        //     idx,
                        //     &mut visited,
                        //     dooot_tx,
                        // )
                        // .await
                        // {
                        //     Ok(_) => {
                        //         // log::info!("Token price calc took {:?}", now.elapsed());
                        //     }
                        //     Err(e) => {
                        //         log::error!("Error calculating token price: {e}");
                        //     }
                        // }

                        // Just emit a price dooot
                        let g_read = graph.read().await;
                        let Some(node) = g_read.node_weight(idx).cloned() else {
                            return;
                        };

                        let node = node.read().await;
                        let mint = node.mint.clone();
                        drop(node);
                        drop(g_read);

                        let price_dooot = Dooot::TokenPriceGlobal(TokenPriceGlobalDooot {
                            mint,
                            price_usd: price,
                            time: Utc::now().naive_utc(),
                        });

                        dooot_tx.send(price_dooot).await.unwrap();
                    }
                    CalculatorUpdate::NewTokenRatio(price, token) => {
                        // let g_read = graph.read().await;
                        // let mut visited = SimpleStack::with_capacity(g_read.node_count());

                        // // let now = Instant::now();
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

                        let g_read = graph.read().await;
                        let Some(node) = g_read.node_weight(token).cloned() else {
                            return;
                        };

                        let node = node.read().await;
                        let mint = node.mint.clone();
                        drop(node);
                        drop(g_read);

                        let price_dooot = Dooot::TokenPriceGlobal(TokenPriceGlobalDooot {
                            mint,
                            price_usd: price,
                            time: Utc::now().naive_utc(),
                        });

                        dooot_tx.send(price_dooot).await.unwrap();
                    } 
                }

                task_counter.fetch_sub(1, Ordering::Relaxed);
            })
            .await
            .unwrap();
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
    visted_nodes: &mut SimpleStack<NodeIndex>,
    dooot_tx: Arc<Sender<Dooot>>,
) -> Result<()> {
    if visted_nodes.exists(&token) {
        return Ok(());
    }

    visted_nodes.push(token);

    let this_token = graph
        .node_weight(token)
        .context("UNREACHABLE - Token should exist in graph!!")?
        .clone();

    // Just grab what we need from locks quickly, then drop
    // Even if that means consuming more mem, clone away
    let this_token = this_token.read().await;
    let Some(this_price) = this_token.usd_price.clone() else {
        return Ok(());
    };
    let is_oracle_priced = this_price.is_oracle();
    let this_mint = this_token.mint.clone();
    drop(this_token);

    let mut neighbor_prices_and_liq = vec![];

    let neighbors = graph.neighbors_directed(token, petgraph::Direction::Incoming);
    for neighbor in neighbors {
        if is_oracle_priced {
            // Just calculate the neighbors, no need to price this token itself,
            // since all oracles are "taken for granted"
            Box::pin(calculate_token_price(
                graph,
                clickhouse_client.clone(),
                decimals_cache.clone(),
                neighbor,
                visted_nodes,
                dooot_tx.clone(),
            ))
            .await?;

            continue;
        }

        let neighbor_token = graph
            .node_weight(neighbor)
            .context("UNREACHABLE - Neighbor should exist in graph!!")?
            .clone();

        let r_neighbor = neighbor_token.read().await;
        let neighbor_price = r_neighbor.usd_price.clone();
        let neighbor_mint = r_neighbor.mint.clone();
        drop(r_neighbor);
        let neighbor_price_is_oracle = neighbor_price.as_ref().is_some_and(|v| v.is_oracle());

        // Filters before we actually go and calculate
        //
        // Don't price using an unpriced neighbor (it should be picked up automatically when it's related to an orcale token)
        if let Some(neighbor_price) = neighbor_price {
            // Don't price using something that is already in the "pricing stack"
            // BUT allow pricing using an orcale token, but we won't recurse into that oracle token
            // Also saves iteration cycles
            if visted_nodes.exists(&neighbor) && !neighbor_price_is_oracle {
                continue;
            }

            get_mint_decimals(
                &[&this_mint, &neighbor_mint],
                decimals_cache.clone(),
                clickhouse_client.clone(),
            )
            .await?;

            let d_read = decimals_cache.read().await;
            let Some(&this_decimals) = d_read.get(&this_mint) else {
                // Can't calculate price for this token, nor its neighbors, return
                // log::error!(
                //     "decimals expected from get_mint_decimals. Is CH missing {}?",
                //     &this_mint
                // );
                return Ok(());
            };
            let Some(&neighbor_decimals) = d_read.get(&neighbor_mint) else {
                // log::error!(
                //     "decimals expected from get_mint_decimals. Is CH missing {}?",
                //     &neighbor_mint
                // );
                continue;
            };
            drop(d_read);
            let decimal_factor = Decimal::from(10)
                .powi(((neighbor_decimals as i16) - (this_decimals as i16)) as i64);

            let Some((w_ratio, liq)) = get_liq_weighted_price_ratio(
                neighbor,
                token,
                graph,
                neighbor_decimals as i16,
                decimal_factor,
            )
            .await
            else {
                // log::warn!("Unable to get weighted ratio between {neighbor_mint} -> {this_mint}");
                continue;
            };

            let neighbor_price = neighbor_price.extract_price();
            let final_price = neighbor_price * w_ratio;
            let neighbor_liq_usd = liq * neighbor_price;
            // log::info!("Calculated price for {neighbor_mint}: {final_price}");
            neighbor_prices_and_liq.push((
                neighbor,
                final_price,
                neighbor_liq_usd, // Denominated in neighbor tokens, so we'll multiply together to get complete final price
            ));
        }

        if !neighbor_price_is_oracle {
            // Recurse into our neighbor token to price that as well
            Box::pin(calculate_token_price(
                graph,
                clickhouse_client.clone(),
                decimals_cache.clone(),
                neighbor,
                visted_nodes,
                dooot_tx.clone(),
            ))
            .await?;
        } else {
            // Don't recurse into oracle-priced tokens,
            // causing them to be put on the stack,
            // and therefore not considered
            //
            // Also, if we *do* recurse into them, "unrelated" tokens could end up pricing this one.
            // This avoid excess calculations as well
            continue;
        }
    }

    // Calc final price weighted by all tokens pointing to this.
    let mut cm_price = Decimal::ZERO;
    for (n_ix, price, liq) in neighbor_prices_and_liq {}

    // Pop our index off the stack, as we unravel
    visted_nodes.pop();

    Ok(())
}

/// From A -> B, directed
///
/// https://gist.github.com/quellen-sol/ae4cfcce79af1c72c596180e1dde60e1#master-formula
///
/// Returns (weighted_price, total_liquidity)
///
/// # `total_liquidity` is denominated in **`token_a_units`**
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
        // let query_start = Instant::now();
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
        // log::info!("CH query done in {:?}", query_start.elapsed());

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
