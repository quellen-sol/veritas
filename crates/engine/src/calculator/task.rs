use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::Result;
use chrono::Utc;
use petgraph::graph::NodeIndex;
use rust_decimal::Decimal;
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
    ppl_graph::{graph::MintPricingGraph, structs::TokenTarget},
    utils::{
        decimal_cache::{DecimalCache, MintDecimals},
        lp_cache::LpCache,
        oracle_cache::OraclePriceCache,
    },
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
    lp_cache: Arc<RwLock<LpCache>>,
    oracle_cache: Arc<RwLock<OraclePriceCache>>,
    oracle_feed_map: Arc<HashMap<String, String>>,
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
/// First recalc the price of the token itself, using it's neighbors pointing to it
///
/// Next, recursively recalc the price of all neighbors pointing away from this token,
/// in order to capture things like staked ratio tokens (mSOL, xSTEP, etc.)
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
    visited_nodes: &mut SimpleStack<NodeIndex>,
    dooot_tx: Arc<Sender<Dooot>>,
) -> Result<()> {
    if visited_nodes.exists(&token) {
        return Ok(());
    }

    visited_nodes.push(token);

    // Recalc from every token pointing TO this
    for neighbor in graph.neighbors_directed(token, petgraph::Direction::Incoming) {}

    // Now recurse into everything pointing away,

    visited_nodes.pop();

    Ok(())
}

/// From A -> B, directed
///
/// https://gist.github.com/quellen-sol/ae4cfcce79af1c72c596180e1dde60e1#master-formula
///
/// # Returns (weighted_price, total_liquidity)
///
/// ## `total_liquidity` is denominated in **`token_a_units`**
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
    let price_a = r_node_a
        .usd_price
        .as_ref()
        .map(|p| p.extract_price())
        .cloned()
        .unwrap_or(Decimal::ZERO);
    drop(r_node_a);

    let edges_iter = graph.edges_connecting(a, b);

    let node_b = graph.node_weight(b)?.clone();
    let nb_read = node_b.read().await;
    let mint_b = nb_read.mint.clone();
    let price_b = nb_read
        .usd_price
        .as_ref()
        .map(|p| p.extract_price())
        .cloned()
        .unwrap_or(Decimal::ZERO);
    drop(nb_read);

    let mut cm_weighted_price = Decimal::ZERO;
    let mut total_liq = Decimal::ZERO;

    for edge in edges_iter {
        let e_read = edge.weight();

        let relation = e_read.inner_relation.read().await;
        // THIS MAY BE WRONG AF
        // Just get liquidity of A, since this token may be exclusively "priced" by A
        let liq = relation.get_liquidity(price_a, Decimal::ZERO);
        let price_between = relation.get_price(TokenTarget::Destination, price_b);
        // let price_between = relation.get_price(TokenTarget::Origin, price_b);
        // let price_between = relation.get_price(TokenTarget::Destination, price_a);
        // let price_between = relation.get_price(TokenTarget::Origin, price_a);
    }

    // if total_liq == Decimal::ZERO {
    //     return None;
    // }

    // let curr_ratio = cm_weighted_price / total_liq;

    // Some((curr_ratio, total_liq))
    Some((Decimal::ZERO, Decimal::ZERO))
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
