// #![allow(unused)]
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    }, time::Instant,
};

use chrono::Utc;
use petgraph::{graph::NodeIndex, Direction};
use rust_decimal::Decimal;
use step_ingestooor_sdk::dooot::{Dooot, TokenPriceGlobalDooot};
use tokio::sync::{
    mpsc::{Receiver, Sender},
    RwLock,
};
use veritas_sdk::{
    ppl_graph::{
        graph::{MintPricingGraph, USDPriceWithSource},
        structs::LiqAmount,
    },
    utils::{decimal_cache::DecimalCache, lp_cache::LpCache, oracle_cache::OraclePriceCache},
};

#[derive(Debug)]
pub enum CalculatorUpdate {
    /// Price of USD (from oracle) and index in the graph
    OracleUSDPrice(NodeIndex),
    /// A Relation pointing TO this token has changed,
    NewTokenRatio(NodeIndex),
}

#[allow(clippy::too_many_arguments)]
pub async fn spawn_calculator_task(
    mut calculator_receiver: Receiver<CalculatorUpdate>,
    clickhouse_client: Arc<clickhouse::Client>,
    graph: Arc<RwLock<MintPricingGraph>>,
    decimals_cache: Arc<RwLock<DecimalCache>>,
    _lp_cache: Arc<RwLock<LpCache>>,
    _oracle_cache: Arc<RwLock<OraclePriceCache>>,
    _oracle_feed_map: Arc<HashMap<String, String>>,
    dooot_tx: Arc<Sender<Dooot>>,
    max_calculator_subtasks: u8,
) {
    log::info!("Spawning Calculator tasks...");

    // Spawn a task to accept token updates
    let update_task = tokio::spawn(async move {
        let counter = Arc::new(AtomicU8::new(0));

        while let Some(update) = calculator_receiver.recv().await {
            while counter.load(Ordering::Relaxed) >= max_calculator_subtasks {
                // Noop, wait for a task to become available
            }

            let now = Instant::now();
            counter.fetch_add(1, Ordering::AcqRel);

            // Make clones for the task
            let graph = graph.clone();
            let clickhouse_client = clickhouse_client.clone();
            let decimals_cache = decimals_cache.clone();
            let dooot_tx = dooot_tx.clone();
            let counter = counter.clone();

            tokio::spawn(async move {
                // Do a full BFS and update price of every token if Oracle
                match update {
                    CalculatorUpdate::OracleUSDPrice(token) => {
                        let g_read = graph.read().await;
                        let mut visited = Vec::new();

                        bfs_recalculate(
                            &g_read,
                            clickhouse_client.clone(),
                            decimals_cache.clone(),
                            token,
                            &mut visited,
                            dooot_tx.clone(),
                        )
                        .await;
                    }
                    CalculatorUpdate::NewTokenRatio(token) => {
                        let g_read = graph.read().await;
                        let mut visited = Vec::new();

                        bfs_recalculate(
                            &g_read,
                            clickhouse_client.clone(),
                            decimals_cache.clone(),
                            token,
                            &mut visited,
                            dooot_tx.clone(),
                        )
                        .await;
                    }
                }

                log::debug!("Calculator task finished in {:?}", now.elapsed());
                counter.fetch_sub(1, Ordering::Relaxed);
            });
        }
    });

    // Task to periodically grab a copy of the graph and recalc
    // let periodic_task = tokio::spawn(async move { loop {} });

    tokio::select! {
        _ = update_task => {
            log::warn!("Update task exited!");
        }
        // _ = periodic_task => {
        //     log::warn!("Graph copier task exited!")
        // }
    }
}

/// Used when oracles update
#[allow(clippy::unwrap_used)]
pub async fn bfs_recalculate(
    graph: &MintPricingGraph,
    clickhouse_client: Arc<clickhouse::Client>,
    decimals_cache: Arc<RwLock<DecimalCache>>,
    this_node: NodeIndex,
    visited_nodes: &mut Vec<NodeIndex>,
    dooot_tx: Arc<Sender<Dooot>>,
) {
    if visited_nodes.contains(&this_node) {
        return;
    }

    visited_nodes.push(this_node);

    // Guaranteed to be possible
    let node_weight = graph.node_weight(this_node).unwrap();

    let p_read = node_weight.usd_price.read().await;
    let price = p_read.as_ref();
    let is_oracle = price.is_some_and(|p| p.is_oracle());
    drop(p_read);

    // Don't calc this token if it's an orcale,
    // but we still want to recurse, so this is a non-guarding branch
    if !is_oracle {
        let Some(this_price) = get_total_weighted_price(graph, this_node).await else {
            return;
        };

        let mint = node_weight.mint.clone();
        log::debug!("Got price of {mint}: {this_price}");

        {
            let mut price_mut = node_weight.usd_price.write().await;
            price_mut.replace(USDPriceWithSource::Relation(this_price));
        }

        let dooot = Dooot::TokenPriceGlobal(TokenPriceGlobalDooot {
            mint,
            price_usd: this_price,
            time: Utc::now().naive_utc(),
        });

        dooot_tx.send(dooot).await.unwrap();
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

        let liq = match liq {
            LiqAmount::Amount(amt) => amt,
            LiqAmount::Inf => return Some(weighted),
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
) -> Option<(Decimal, LiqAmount)> {
    let node_a = graph.node_weight(a)?;
    let price_a = *node_a.usd_price.read().await.as_ref()?.extract_price();

    let edges_iter = graph.edges_connecting(a, b);

    let mut cm_weighted_price = Decimal::ZERO;
    let mut total_liq = Decimal::ZERO;

    for edge in edges_iter {
        let e_read = edge.weight();

        let relation = e_read.inner_relation.read().await;
        // THIS MAY BE WRONG AF
        // Just get liquidity based on A, since this token may be exclusively "priced" by A
        let liq = relation.get_liquidity(price_a, Decimal::ZERO);

        let liq = match liq {
            LiqAmount::Amount(amt) => {
                if amt == Decimal::ZERO {
                    continue;
                }
                amt
            }
            LiqAmount::Inf => return Some((relation.get_price(price_a), LiqAmount::Inf)),
        };

        let price_b_usd = relation.get_price(price_a);

        cm_weighted_price += price_b_usd * liq;
        total_liq += liq;
    }

    if total_liq == Decimal::ZERO {
        return None;
    }

    let weighted_price = cm_weighted_price / total_liq;

    Some((weighted_price, LiqAmount::Amount(total_liq)))
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use petgraph::Graph;
    use rust_decimal::{prelude::FromPrimitive, Decimal};
    use tokio::sync::RwLock;
    use veritas_sdk::ppl_graph::{
        graph::{MintEdge, MintNode, USDPriceWithSource},
        structs::{LiqAmount, LiqRelationEnum},
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

        match liq {
            LiqAmount::Amount(amt) => {
                assert_eq!(amt, Decimal::from(20));
            }
            LiqAmount::Inf => {
                panic!("Liq for this test should not be Inf!");
            }
        };
    }
}
