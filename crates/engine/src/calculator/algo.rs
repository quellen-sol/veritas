use anyhow::{anyhow, Result};
use chrono::Utc;
use itertools::Itertools;
use rust_decimal::Decimal;
use std::collections::{HashSet, VecDeque};

use petgraph::{graph::NodeIndex, Direction};
use step_ingestooor_sdk::dooot::{Dooot, TokenPriceGlobalDooot};
use tokio::sync::mpsc::Sender;
use veritas_sdk::{
    ppl_graph::{
        graph::{MintPricingGraph, USDPriceWithSource},
        structs::LiqAmount,
        utils::get_price_by_node_idx,
    },
    utils::checked_math::{clamp_to_scale, is_significant_change},
};

pub async fn bfs_recalculate(
    graph: &MintPricingGraph,
    start: NodeIndex,
    visited_nodes: &mut HashSet<NodeIndex>,
    dooot_tx: Sender<Dooot>,
    oracle_mint_set: &HashSet<String>,
    sol_index: &Option<Decimal>,
    max_price_impact: &Decimal,
) -> Result<()> {
    let mut is_start = true;
    let mut queue = VecDeque::with_capacity(graph.node_count());
    queue.push_back(start);

    while !queue.is_empty() {
        let Some(node) = queue.pop_front() else {
            log::error!(
                "UNREACHABLE - There should always be one entry in the queue by this point"
            );
            return Ok(());
        };

        // Calc
        let Some(node_weight) = graph.node_weight(node) else {
            log::error!("UNREACHABLE - NodeIndex {node:?} should always exist");
            return Ok(());
        };

        let mint = &node_weight.mint;
        let is_oracle = oracle_mint_set.contains(mint);

        // Don't calc this token if it's an oracle
        if !is_oracle {
            log::trace!("Getting total weighted price for {mint}");
            let Some(new_price) =
                get_total_weighted_price(graph, node, sol_index, max_price_impact).await
            else {
                log::trace!("Failed to calculate price for {mint}");
                continue;
            };

            log::debug!("Calculated price of {mint}: {new_price}");

            {
                log::trace!("Getting price write lock for price calc");
                let mut price_mut = node_weight.usd_price.write().await;
                log::trace!("Got price write lock for price calc");
                if let Some(old_price) = price_mut.as_ref() {
                    let old_price = old_price.extract_price();

                    if !is_significant_change(old_price, &new_price) {
                        continue;
                    }
                }

                price_mut.replace(USDPriceWithSource::Relation(new_price));
                log::trace!("Replaced price for price calc");
            }

            let dooot = Dooot::TokenPriceGlobal(TokenPriceGlobalDooot {
                mint: mint.to_owned(),
                price_usd: new_price,
                time: Utc::now().naive_utc(),
            });

            log::trace!("Sending price calc Dooot");
            dooot_tx
                .send(dooot)
                .await
                .map_err(|e| anyhow!("Error sending Dooot after price calc: {e}"))?;
            log::trace!("Sent price calc Dooot");
        } else if !is_start {
            // Stop at oracles when travering throughout the graph, but allow us to at least start at one
            continue;
        }

        is_start = false;

        for neighbor in graph.neighbors(node).unique() {
            if !visited_nodes.insert(neighbor) {
                continue;
            }

            queue.push_back(neighbor);
        }
    }

    Ok(())
}

pub async fn get_total_weighted_price(
    graph: &MintPricingGraph,
    this_node: NodeIndex,
    sol_index: &Option<Decimal>,
    max_price_impact: &Decimal,
) -> Option<Decimal> {
    let mut cm_weighted_price = Decimal::ZERO;
    let mut total_liq = Decimal::ZERO;
    for neighbor in graph
        .neighbors_directed(this_node, Direction::Incoming)
        .unique()
    {
        let neighbor_mint = {
            let Some(neighbor_mint) = graph.node_weight(neighbor).map(|n| &n.mint) else {
                log::error!("UNREACHABLE - Neighbor node should always exist");
                continue;
            };

            neighbor_mint
        };
        log::trace!("Getting single weighted price from neighbor {neighbor_mint}");
        let Some((weighted, liq)) = get_single_wighted_price(
            neighbor,
            this_node,
            graph,
            sol_index,
            max_price_impact,
            neighbor_mint,
        )
        .await
        else {
            // Illiquid or price doesn't exist. Skip
            continue;
        };

        let liq = match liq {
            LiqAmount::Amount(amt) => amt,
            LiqAmount::Inf => return Some(weighted),
        };

        cm_weighted_price = cm_weighted_price.checked_add(weighted.checked_mul(liq)?)?;
        total_liq = total_liq.checked_add(liq)?;
    }

    if total_liq == Decimal::ZERO {
        return None;
    }

    let final_decimal = cm_weighted_price.checked_div(total_liq)?;
    let final_price = clamp_to_scale(&final_decimal);

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
    sol_index: &Option<Decimal>,
    max_price_impact: &Decimal,
    neighbor_mint: &str,
) -> Option<(Decimal, LiqAmount)> {
    let price_a = get_price_by_node_idx(graph, a).await?;

    let edges_iter = graph.edges_connecting(a, b);

    let mut cm_weighted_price = Decimal::ZERO;
    let mut total_liq = Decimal::ZERO;

    for edge in edges_iter {
        let e_read = edge.weight();

        let relation = e_read.inner_relation.read().await;
        // THIS MAY BE WRONG AF
        // Just get liquidity based on A, since this token may be exclusively "priced" by A
        let Some(liq) = relation.get_liquidity(price_a, Decimal::ZERO) else {
            continue;
        };

        let liq = match liq {
            LiqAmount::Amount(amt) => {
                if amt == Decimal::ZERO {
                    continue;
                }
                amt
            }
            LiqAmount::Inf => return Some((relation.get_price(price_a)?, LiqAmount::Inf)),
        };

        if let Some(sol_price) = sol_index {
            // Check liquidity levels if sol price now exists

            let Some(tokens_a_per_sol) = sol_price.checked_div(price_a) else {
                // price_a exists, but the math overflowed somewhere.
                // Values are therefore way too high/low to be considered.
                continue;
            };

            log::trace!("Getting liq levels for {neighbor_mint}");
            let Some(liq_levels) = relation.get_liq_levels(tokens_a_per_sol) else {
                // Math overflow in calc'ing liq levels.
                // We can assume this to be illiquid if values got that high
                continue;
            };
            log::trace!("Got liq levels for {neighbor_mint}");

            if !liq_levels.acceptable(max_price_impact) {
                continue;
            }
        }

        let Some(price_b_usd) = relation.get_price(price_a) else {
            continue;
        };

        cm_weighted_price = cm_weighted_price.checked_add(price_b_usd.checked_mul(liq)?)?;
        total_liq = total_liq.checked_add(liq)?;
    }

    if total_liq == Decimal::ZERO {
        return None;
    }

    let weighted_price = cm_weighted_price.checked_div(total_liq)?;

    Some((weighted_price, LiqAmount::Amount(total_liq)))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use chrono::Utc;
    use petgraph::Graph;
    use rust_decimal::{prelude::FromPrimitive, Decimal};
    use tokio::sync::RwLock;
    use veritas_sdk::{
        liq_relation::LiqRelation,
        ppl_graph::{
            graph::{MintEdge, MintNode, MintPricingGraph, USDPriceWithSource},
            structs::LiqAmount,
        },
    };

    use crate::calculator::algo::{get_single_wighted_price, get_total_weighted_price};

    use super::bfs_recalculate;

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

        let max_price_impact = Decimal::from_f64(0.25).unwrap();

        let (weighted, liq) =
            get_single_wighted_price(usdc_x, step_x, &graph, &None, &max_price_impact, "USDC")
                .await
                .unwrap();

        assert_eq!(
            weighted,
            Decimal::from_f64(3.5).unwrap(),
            "Single weighted price should be 3.5"
        );

        let total = get_total_weighted_price(&graph, step_x, &None, &max_price_impact).await;
        assert!(total.is_some());
        assert_eq!(
            total.unwrap(),
            Decimal::from_f64(3.5).unwrap(),
            "Total weighted price should be 3.5"
        );

        match liq {
            LiqAmount::Amount(amt) => {
                assert_eq!(amt, Decimal::from(20));
            }
            LiqAmount::Inf => {
                panic!("Liq for this test should not be Inf!");
            }
        };
    }

    #[test]
    fn liq_levels() {
        let relation = LiqRelation::CpLp {
            amt_origin: Decimal::from(10),
            amt_dest: Decimal::from(10),
        };

        let sol_price = Decimal::ONE_HUNDRED;
        let token_a_price = Decimal::TEN;
        let tokens_a_per_sol = sol_price / token_a_price;

        let levels = relation.get_liq_levels(tokens_a_per_sol);

        assert!(levels.is_some(), "Liq levels calc should not overflow")
    }

    /// Test that oracles are not passed-through so that unneccesary tokens are calc'd
    #[tokio::test]
    async fn oracles_gate_calc() {
        let mut graph = MintPricingGraph::new();
        let oracle_token_mint = "ORACLE_TOKEN".to_string();
        let oracle_price = Decimal::ONE_HUNDRED;

        let oracle_node = graph.add_node(MintNode {
            mint: oracle_token_mint.clone(),
            usd_price: RwLock::new(Some(USDPriceWithSource::Oracle(oracle_price))),
        });

        let test_token_a = graph.add_node(MintNode {
            mint: "TOKEN_A".into(),
            usd_price: RwLock::new(None),
        });

        let test_token_b = graph.add_node(MintNode {
            mint: "TOKEN_B".into(),
            usd_price: RwLock::new(None),
        });

        // Link Oracle -> Token A and vice versa
        graph.add_edge(
            oracle_node,
            test_token_a,
            MintEdge {
                id: "market_a".into(),
                dirty: false,
                last_updated: RwLock::default(),
                inner_relation: RwLock::new(LiqRelation::CpLp {
                    amt_origin: Decimal::from(100_000), // 100k ORACLE TOKEN @ $100
                    amt_dest: Decimal::from(10_000),    // 10k TOKEN A (To be priced)
                }),
            },
        );
        graph.add_edge(
            test_token_a,
            oracle_node,
            MintEdge {
                id: "market_a".into(),
                dirty: false,
                last_updated: RwLock::default(),
                inner_relation: RwLock::new(LiqRelation::CpLp {
                    amt_origin: Decimal::from(10_000), // 10k TOKEN A (To be priced)
                    amt_dest: Decimal::from(100_000),  // 100k ORACLE TOKEN @ $100
                }),
            },
        );

        // Link Oracle -> Token B and vice versa
        graph.add_edge(
            oracle_node,
            test_token_b,
            MintEdge {
                id: "market_b".into(),
                dirty: false,
                last_updated: RwLock::default(),
                inner_relation: RwLock::new(LiqRelation::CpLp {
                    amt_origin: Decimal::from(100_000), // 100k ORACLE TOKEN @ $100
                    amt_dest: Decimal::from(10_000),    // 10k TOKEN B (To be priced)
                }),
            },
        );
        graph.add_edge(
            test_token_b,
            oracle_node,
            MintEdge {
                id: "market_b".into(),
                dirty: false,
                last_updated: RwLock::default(),
                inner_relation: RwLock::new(LiqRelation::CpLp {
                    amt_origin: Decimal::from(10_000), // 10k TOKEN AB (To be priced)
                    amt_dest: Decimal::from(100_000),  // 100k ORACLE TOKEN @ $100
                }),
            },
        );

        // Recalc the graph starting from token A (this should NOT calc token B!)
        let (tx, _rx) = tokio::sync::mpsc::channel(100);
        let mut oracle_mint_set = HashSet::new();
        oracle_mint_set.insert(oracle_token_mint);

        let max_price_impact = Decimal::from_f64(0.25).unwrap();

        bfs_recalculate(
            &graph,
            test_token_a,
            &mut HashSet::new(),
            tx,
            &oracle_mint_set,
            &Some(oracle_price),
            &max_price_impact,
        )
        .await
        .unwrap();

        let b_node = graph.node_weight(test_token_b).unwrap();
        let price_exists = { b_node.usd_price.read().await.as_ref().is_some() };

        assert!(
            !price_exists,
            "Test Token B's price should NOT have been calculated!"
        );
    }
}
