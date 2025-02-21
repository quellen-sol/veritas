#![allow(unused)]
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::{Context, Result};
use chrono::Utc;
use petgraph::{graph::NodeIndex, Direction};
use rust_decimal::Decimal;
use solana_sdk::signer::Signer;
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

pub async fn spawn_calculator_task(
    mut calculator_receiver: Receiver<CalculatorUpdate>,
    clickhouse_client: Arc<clickhouse::Client>,
    graph: Arc<RwLock<MintPricingGraph>>,
    decimals_cache: Arc<RwLock<DecimalCache>>,
    lp_cache: Arc<RwLock<LpCache>>,
    oracle_cache: Arc<RwLock<OraclePriceCache>>,
    oracle_feed_map: Arc<HashMap<String, String>>,
    dooot_tx: Arc<Sender<Dooot>>,
    max_calculator_subtasks: u8,
) {
    log::info!("Spawning Calculator tasks...");

    // Spawn a task to accept token updates
    let update_task = tokio::spawn(async move {
        while let Some(update) = calculator_receiver.recv().await {
            // Do a full BFS and update price of every token if Oracle
            match update {
                CalculatorUpdate::OracleUSDPrice(price, token) => {}
                _ => {
                    continue;
                }
            }
        }
    });

    // Task to periodically grab a copy of the graph and recalc
    let periodic_task = tokio::spawn(async move { loop {} });

    tokio::select! {
        _ = update_task => {
            log::warn!("Update task exited!");
        }
        _ = periodic_task => {
            log::warn!("Graph copier task exited!")
        }
    }
}

/// Used when oracles update
pub async fn bfs_recalculate(
    graph: &MintPricingGraph,
    clickhouse_client: Arc<clickhouse::Client>,
    decimals_cache: Arc<RwLock<DecimalCache>>,
    this_node: NodeIndex,
    visited_nodes: &mut Vec<NodeIndex>,
    dooot_tx: Arc<Sender<Dooot>>,
) -> Result<()> {
    if visited_nodes.contains(&this_node) {
        return Ok(());
    }

    visited_nodes.push(this_node);

    let node_weight = graph
        .node_weight(this_node)
        .context("UNREACHABLE - start node not found")?;

    let p_read = node_weight.usd_price.read().await;
    let price = p_read.as_ref();
    let is_oracle = price.is_some_and(|p| p.is_oracle());
    drop(p_read);

    // Don't calc this token if it's an orcale,
    // but we still want to recurse, so this is a non-guarding branch
    if !is_oracle {
        let Some(this_price) = get_total_weighted_price(graph, this_node).await else {
            return Ok(());
        };

        let mint = node_weight.mint.clone();
        log::debug!("Got price of {mint}: {this_price}");

        let dooot = Dooot::TokenPriceGlobal(TokenPriceGlobalDooot {
            mint: node_weight.mint.clone(),
            price_usd: this_price,
            time: Utc::now().naive_utc(),
        });

        dooot_tx.send(dooot);
    }

    for neighbor in graph.neighbors(this_node) {
        if visited_nodes.contains(&neighbor) {
            continue;
        }

        Box::pin(bfs_recalculate(
            graph,
            clickhouse_client.clone(),
            decimals_cache.clone(),
            neighbor,
            visited_nodes,
            dooot_tx.clone(),
        ))
        .await;
    }

    Ok(())
}

pub async fn get_total_weighted_price(
    graph: &MintPricingGraph,
    this_node: NodeIndex,
) -> Option<Decimal> {
    let mut cm_weighted_price = Decimal::ZERO;
    let mut total_liq = Decimal::ZERO;
    for neighbor in graph.neighbors_directed(this_node, Direction::Incoming) {
        let Some((weighted, liq)) = get_liq_weighted_price_ratio(this_node, neighbor, graph).await
        else {
            // Illiquid or price doesn't exist. Skip
            continue;
        };

        cm_weighted_price += weighted * liq;
        total_liq += liq;
    }

    if total_liq == Decimal::ZERO {
        return None;
    }

    let final_price = cm_weighted_price / total_liq;

    Some(final_price)
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
) -> Option<(Decimal, Decimal)> {
    let node_a = graph.node_weight(a)?;
    let price_a = node_a
        .usd_price
        .read()
        .await
        .as_ref()?
        .extract_price()
        .clone();

    let edges_iter = graph.edges_connecting(a, b);

    let mut cm_weighted_price = Decimal::ZERO;
    let mut total_liq = Decimal::ZERO;

    for edge in edges_iter {
        let e_read = edge.weight();

        let relation = e_read.inner_relation.read().await;
        // THIS MAY BE WRONG AF
        // Just get liquidity of A, since this token may be exclusively "priced" by A
        let liq = relation.get_liquidity(price_a, Decimal::ZERO);

        if liq == Decimal::ZERO {
            continue;
        }

        let price_b_usd = relation.get_price(price_a);

        cm_weighted_price += price_b_usd * liq;
        total_liq += liq;
    }

    if total_liq == Decimal::ZERO {
        return None;
    }

    let weighted_price = cm_weighted_price / total_liq;

    Some((weighted_price, total_liq))
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

#[cfg(test)]
mod tests {
    use chrono::{NaiveDateTime, Utc};
    use petgraph::Graph;
    use rust_decimal::{prelude::FromPrimitive, Decimal};
    use tokio::sync::RwLock;
    use veritas_sdk::ppl_graph::{
        graph::{MintEdge, MintNode, USDPriceWithSource},
        structs::LiqRelationEnum,
    };

    use crate::calculator::task::get_liq_weighted_price_ratio;

    #[tokio::test]
    async fn weighted_liq() {
        let step_node = MintNode {
            mint: "STEP".into(),
            usd_price: RwLock::new(None),
        };

        let usdc_node = MintNode {
            mint: "USDC".into(),
            usd_price: RwLock::new(Some(USDPriceWithSource::Oracle(Decimal::from(1)))),
        };

        let illiquid_node = MintNode {
            mint: "DUMB".into(),
            usd_price: RwLock::new(None),
        };

        let mut graph = Graph::new();

        let step_x = graph.add_node(step_node);
        let usdc_x = graph.add_node(usdc_node);
        let il_x = graph.add_node(illiquid_node);

        graph.add_edge(
            usdc_x,
            step_x,
            MintEdge {
                dirty: false,
                id: "SomeMarket".into(),
                inner_relation: RwLock::new(LiqRelationEnum::CpLp {
                    amt_origin: Decimal::from(10),
                    amt_dest: Decimal::from(5),
                }),
                last_updated: RwLock::new(Utc::now().naive_utc()),
            },
        );

        graph.add_edge(
            usdc_x,
            step_x,
            MintEdge {
                dirty: false,
                id: "OtherMarket".into(),
                inner_relation: RwLock::new(LiqRelationEnum::CpLp {
                    amt_origin: Decimal::from(10),
                    amt_dest: Decimal::from(2),
                }),
                last_updated: RwLock::new(Utc::now().naive_utc()),
            },
        );

        graph.add_edge(
            il_x,
            step_x,
            MintEdge {
                dirty: false,
                id: "IlliquidMarket".into(),
                inner_relation: RwLock::new(LiqRelationEnum::CpLp {
                    amt_origin: Decimal::from(10),
                    amt_dest: Decimal::from(2),
                }),
                last_updated: RwLock::new(Utc::now().naive_utc()),
            },
        );

        let (weighted, liq) = get_liq_weighted_price_ratio(usdc_x, step_x, &graph)
            .await
            .unwrap();

        assert_eq!(weighted, Decimal::from_f64(3.5).unwrap());
        assert_eq!(liq, Decimal::from(20));
    }
}
