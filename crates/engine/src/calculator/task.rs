// #![allow(unused)]
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use chrono::Utc;
use petgraph::{graph::NodeIndex, Direction};
use rust_decimal::{prelude::FromPrimitive, Decimal};
use step_ingestooor_sdk::dooot::{Dooot, TokenPriceGlobalDooot};
use tokio::{sync::{
    mpsc::{Receiver, Sender},
    RwLock,
}, task::JoinHandle};
use veritas_sdk::{
    ppl_graph::{
        graph::{MintPricingGraph, USDPriceWithSource},
        structs::LiqAmount,
    },
    utils::decimal_cache::DecimalCache,
};

#[derive(Debug)]
pub enum CalculatorUpdate {
    /// Price of USD (from oracle) and index in the graph
    OracleUSDPrice(NodeIndex),
    /// A Relation pointing TO this token has changed,
    NewTokenRatio(NodeIndex),
}

#[allow(clippy::too_many_arguments)]
pub fn spawn_calculator_task(
    mut calculator_receiver: Receiver<CalculatorUpdate>,
    graph: Arc<RwLock<MintPricingGraph>>,
    decimals_cache: Arc<RwLock<DecimalCache>>,
    dooot_tx: Arc<Sender<Dooot>>,
    max_calculator_subtasks: u8,
    bootstrap_in_progress: Arc<AtomicBool>,
) -> [JoinHandle<()>; 2] {
    log::info!("Spawning Calculator tasks...");

    // Spawn a task to accept token updates
    let updator_graph = graph.clone();
    let updator_decimals_cache = decimals_cache.clone();
    let updator_dooot_tx = dooot_tx.clone();

    let update_task = tokio::spawn(async move {
        let counter = Arc::new(AtomicU8::new(0));

        while let Some(update) = calculator_receiver.recv().await {
            if bootstrap_in_progress.load(Ordering::Relaxed) {
                // Do not process graph updates while bootstrapping,
                // Avoids thousands of recalcs while bootstrapping
                continue;
            }

            while counter.load(Ordering::Relaxed) >= max_calculator_subtasks {
                // Wait for a task to become available
                tokio::time::sleep(Duration::from_millis(1)).await;
            }

            let now = Instant::now();

            // Make clones for the task
            let graph = updator_graph.clone();
            let decimals_cache = updator_decimals_cache.clone();
            let dooot_tx = updator_dooot_tx.clone();
            let counter = counter.clone();

            tokio::spawn(async move {
                counter.fetch_add(1, Ordering::Relaxed);
                // Do a full BFS and update price of every token if Oracle
                match update {
                    CalculatorUpdate::OracleUSDPrice(token) => {
                        log::trace!("Getting graph read lock for OracleUSDPrice update");
                        let g_read = graph.read().await;
                        log::trace!("Got graph read lock for OracleUSDPrice update");
                        let mut visited: HashSet<_> = HashSet::with_capacity(g_read.node_count());

                        log::trace!("Starting BFS recalculation for OracleUSDPrice update");
                        bfs_recalculate(    
                            &g_read,
                            decimals_cache.clone(),
                            token,
                            &mut visited,
                            dooot_tx.clone(),
                        )
                        .await;
                        log::trace!("Finished BFS recalculation for OracleUSDPrice update");
                    }
                    CalculatorUpdate::NewTokenRatio(token) => {
                        log::trace!("Getting graph read lock for NewTokenRatio update");
                        let g_read = graph.read().await;
                        log::trace!("Got graph read lock for NewTokenRatio update");
                        let mut visited: HashSet<_> = HashSet::with_capacity(g_read.node_count());

                        log::trace!("Starting BFS recalculation for NewTokenRatio update");
                        bfs_recalculate(
                            &g_read,
                            decimals_cache.clone(),
                            token,
                            &mut visited,
                            dooot_tx.clone(),
                        )
                        .await;
                        log::trace!("Finished BFS recalculation for NewTokenRatio update");
                    }
                }

                log::debug!("Calculator task finished in {:?}", now.elapsed());
                counter.fetch_sub(1, Ordering::Relaxed);
            });
        }
    });

    // Task to periodically recalc the entire graph
    let periodic_task = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;

            let decimals_cache = decimals_cache.clone();
            let dooot_tx = dooot_tx.clone();

            log::trace!("Getting graph read lock for periodic task");
            let g_read = graph.read().await;
            log::trace!("Got graph read lock for periodic task");
            let mut visited: HashSet<_> = HashSet::with_capacity(g_read.node_count());

            log::trace!("Starting BFS recalculation for periodic task");
            bfs_recalculate(
                &g_read,
                decimals_cache,
                NodeIndex::new(0),
                &mut visited,
                dooot_tx,
            )
            .await;
            log::trace!("Finished BFS recalculation for periodic task");
        }
    });

    [
        update_task,
        periodic_task,
    ]
}

#[allow(clippy::unwrap_used)]
pub async fn bfs_recalculate(
    graph: &MintPricingGraph,
    decimals_cache: Arc<RwLock<DecimalCache>>,
    node: NodeIndex,
    visited_nodes: &mut HashSet<NodeIndex>,
    dooot_tx: Arc<Sender<Dooot>>,
) {
    if visited_nodes.contains(&node) {
        return;
    }

    visited_nodes.insert(node);

    let Some(node_weight) = graph.node_weight(node) else {
        return;
    };
    let is_oracle = {
        let p_read = node_weight.usd_price.read().await;
        p_read.as_ref().is_some_and(|p| p.is_oracle())
    };

    // Don't calc this token if it's an orcale,
    // but we still want to recurse, so this is a non-guarding branch
    if !is_oracle {
        let mint = node_weight.mint.clone();
        let Some(new_price) = get_total_weighted_price(graph, node).await else {
            // log::warn!("Failed to calculate price for {mint}");
            return;
        };

        log::debug!("Calculated price of {mint}: {new_price}");

        {
            let mut price_mut = node_weight.usd_price.write().await;
            if let Some(old_price) = price_mut.as_ref() {
                let old_price = old_price.extract_price();
                let pct_diff = ((new_price - old_price) / old_price).abs();
                if pct_diff < Decimal::from_f64(0.001).unwrap() {
                    // Not a significant enough change. Stop here and don't emit a dooot
                    return;
                }
            }

            price_mut.replace(USDPriceWithSource::Relation(new_price));
        }

        let dooot = Dooot::TokenPriceGlobal(TokenPriceGlobalDooot {
            mint,
            price_usd: new_price,
            time: Utc::now().naive_utc(),
        });

        dooot_tx.send(dooot).await.unwrap();
    }

    for neighbor in graph.neighbors(node) {
        Box::pin(bfs_recalculate(
            graph,
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
        let Some((weighted, liq)) = get_single_wighted_price(neighbor, this_node, graph).await
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
pub async fn get_single_wighted_price(
    a: NodeIndex,
    b: NodeIndex,
    graph: &MintPricingGraph,
) -> Option<(Decimal, LiqAmount)> {
    let node_a = graph.node_weight(a)?;
    let price_a = {
        let n_read = node_a.usd_price.read().await;
        *n_read.as_ref()?.extract_price()
    };

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
        structs::{LiqAmount, LiqRelation},
    };

    use crate::calculator::task::{get_single_wighted_price, get_total_weighted_price};

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
                inner_relation: RwLock::new(LiqRelation::CpLp {
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
                inner_relation: RwLock::new(LiqRelation::CpLp {
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
                inner_relation: RwLock::new(LiqRelation::CpLp {
                    amt_origin: Decimal::from(10),
                    amt_dest: Decimal::from(2),
                }),
                last_updated: RwLock::new(Utc::now().naive_utc()),
            },
        );

        let (weighted, liq) = get_single_wighted_price(usdc_x, step_x, &graph)
            .await
            .unwrap();

        assert_eq!(weighted, Decimal::from_f64(3.5).unwrap());

        let total = get_total_weighted_price(&graph, step_x).await;
        assert!(total.is_some());
        assert_eq!(total.unwrap(), Decimal::from_f64(3.5).unwrap());

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
