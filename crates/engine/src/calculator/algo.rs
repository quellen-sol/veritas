use anyhow::Result;
use chrono::Utc;
use itertools::Itertools;
use rust_decimal::Decimal;
use std::{
    collections::{HashSet, VecDeque},
    time::Instant,
};

use petgraph::{
    graph::{EdgeIndex, NodeIndex},
    visit::EdgeRef,
    Direction,
};
use std::sync::mpsc::SyncSender;
use step_ingestooor_sdk::dooot::{Dooot, TokenPriceGlobalDooot};
use veritas_sdk::{
    ppl_graph::{graph::USDPriceWithSource, structs::LiqAmount, utils::get_price_by_node_idx},
    types::MintPricingGraph,
    utils::{
        checked_math::{clamp_to_scale, is_significant_change},
        traits::option_helper::VeritasOptionHelper,
    },
};

pub fn bfs_recalculate(
    graph: &MintPricingGraph,
    start: NodeIndex,
    visited_nodes: &mut HashSet<NodeIndex>,
    dooot_tx: SyncSender<Dooot>,
    oracle_mint_set: &HashSet<String>,
    sol_index: &Option<Decimal>,
    max_price_impact: &Decimal,
    update_nodes: bool,
) -> Result<()> {
    let calc_time = Utc::now().naive_utc();
    let now = Instant::now();
    let mut is_start = true;
    let mut queue = VecDeque::with_capacity(graph.node_count());
    queue.push_back(start);
    let mut price_dooots = Vec::with_capacity(graph.node_count());

    while !queue.is_empty() {
        let Some(node) = queue.pop_front() else {
            log::error!(
                "UNREACHABLE - There should always be one entry in the queue by this point"
            );
            return Ok(());
        };

        let Some(node_weight) = graph.node_weight(node) else {
            log::error!("UNREACHABLE - NodeIndex {node:?} should always exist");
            return Ok(());
        };

        let mint = &node_weight.mint;
        let is_oracle = oracle_mint_set.contains(mint);

        // Don't calc this token if it's an oracle
        if !is_oracle {
            // let now = Instant::now();
            // Last time we checked, this takes about 1ms per node, no bueno
            let new_price_res = get_total_weighted_price(graph, node, sol_index, max_price_impact);
            // let elapsed = now.elapsed();

            // // `from_millis` is const, gucci
            // if elapsed > Duration::from_millis(1) {
            //     log::warn!("{mint} took a long time to calculate: {elapsed:?}");
            // }

            let Some(new_price) = new_price_res else {
                continue;
            };

            if !update_nodes {
                continue;
            }

            let (replaced, _) = node_weight
                .usd_price
                .write()
                .expect("Node price lock poisoned")
                .replace_if(USDPriceWithSource::Relation(new_price), |p| {
                    is_significant_change(p.extract_price(), &new_price)
                });

            if replaced {
                price_dooots.push(Dooot::TokenPriceGlobal(TokenPriceGlobalDooot {
                    mint: mint.clone(),
                    price_usd: new_price,
                    time: calc_time,
                    deleted: false,
                }));
            }
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

    for dooot in price_dooots {
        dooot_tx.send(dooot)?;
    }

    log::info!("BFS Recalc Took {:?}", now.elapsed());
    Ok(())
}

pub fn get_total_weighted_price(
    graph: &MintPricingGraph,
    this_node: NodeIndex,
    sol_index: &Option<Decimal>,
    max_price_impact: &Decimal,
) -> Option<Decimal> {
    let mut cm_weighted_price = Decimal::ZERO;
    let mut total_liq = Decimal::ZERO;
    let cached_fixed_relation = {
        let Some(this_node_weight) = graph.node_weight(this_node) else {
            log::error!("UNREACHABLE - This node should always exist");
            return None;
        };
        *this_node_weight
            .cached_fixed_relation
            .read()
            .expect("Cached fixed relation read lock poisoned")
    };

    if let Some(cached_fixed_relation) = cached_fixed_relation {
        let Some(origin) = graph.edge_endpoints(cached_fixed_relation).map(|(a, _)| a) else {
            log::error!("UNREACHABLE - Cached fixed relation should always have an origin");
            return None;
        };

        let origin_price = get_price_by_node_idx(graph, origin)?;

        let Some(edge_weight) = graph.edge_weight(cached_fixed_relation) else {
            log::error!("UNREACHABLE - Cached fixed relation should always exist");
            return None;
        };

        let relation = edge_weight
            .inner_relation
            .read()
            .expect("Inner relation read lock poisoned");
        let price = relation.get_price(origin_price, graph)?;

        return Some(price);
    }

    // Check non-vertex relations first
    let Some(this_node_weight) = graph.node_weight(this_node) else {
        log::error!("UNREACHABLE - This node should always exist");
        return None;
    };

    {
        let non_vertex_relations = this_node_weight
            .non_vertex_relations
            .read()
            .expect("Non vertex relations read lock poisoned");
        for (_, relation) in non_vertex_relations.iter() {
            let relation = relation.read().expect("Relation read lock poisoned");

            // let liquidity_levels = relation.get_liq_levels(Decimal::ZERO);
            let liquidity_amount = relation.get_liquidity(Decimal::ZERO, Decimal::ZERO);
            let derived_price = relation.get_price(Decimal::ZERO, graph);

            match liquidity_amount {
                Some(amt) => match amt {
                    LiqAmount::Inf => {
                        return derived_price.and_then(|p| clamp_to_scale(&p));
                    }
                    LiqAmount::Amount(liq) => {
                        if let Some(price) = derived_price {
                            cm_weighted_price =
                                cm_weighted_price.checked_add(price.checked_mul(liq)?)?;
                            total_liq = total_liq.checked_add(liq)?;
                        } else {
                            continue;
                        }
                    }
                },
                None => continue,
            }
        }
    }

    for neighbor in graph
        .neighbors_directed(this_node, Direction::Incoming)
        .unique()
    {
        let Some((weighted, liq, fixed_edge_id)) =
            get_single_wighted_price(neighbor, this_node, graph, sol_index, max_price_impact)
        else {
            // Illiquid or price doesn't exist. Skip
            continue;
        };

        // If the algo returned a fixed edge, we need to cache it for this node
        // so that we don't have to traverse the graph to find it later
        if let Some(edge_id) = fixed_edge_id {
            let Some(this_node_weight) = graph.node_weight(this_node) else {
                log::error!("UNREACHABLE - tried setting cache, this_node should always exist");
                continue;
            };

            let mut this_node_weight_mut = this_node_weight
                .cached_fixed_relation
                .write()
                .expect("Cached fixed relation write lock poisoned");
            if this_node_weight_mut.is_none() {
                this_node_weight_mut.replace(edge_id);
            }
        }

        let liq = match liq {
            LiqAmount::Amount(amt) => amt,
            LiqAmount::Inf => return clamp_to_scale(&weighted),
        };

        cm_weighted_price = cm_weighted_price.checked_add(weighted.checked_mul(liq)?)?;
        total_liq = total_liq.checked_add(liq)?;
    }

    if total_liq == Decimal::ZERO {
        return None;
    }

    let final_decimal = cm_weighted_price.checked_div(total_liq)?;
    let final_price = clamp_to_scale(&final_decimal)?;

    Some(final_price)
}

/// From A -> B, directed
///
/// https://gist.github.com/quellen-sol/ae4cfcce79af1c72c596180e1dde60e1#master-formula
///
/// # Returns (weighted_price, total_liquidity, edge_id if fixed)
///
/// ## `total_liquidity` is denominated in **`token_a_units`**
pub fn get_single_wighted_price(
    a: NodeIndex,
    b: NodeIndex,
    graph: &MintPricingGraph,
    sol_index: &Option<Decimal>,
    max_price_impact: &Decimal,
) -> Option<(Decimal, LiqAmount, Option<EdgeIndex>)> {
    let price_a = get_price_by_node_idx(graph, a)?;

    let edges_iter = graph.edges_connecting(a, b);

    let mut cm_weighted_price = Decimal::ZERO;
    let mut total_liq = Decimal::ZERO;

    for edge in edges_iter {
        let e_read = edge.weight();

        let relation = e_read
            .inner_relation
            .read()
            .expect("Inner relation read lock poisoned");
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
            LiqAmount::Inf => {
                return Some((
                    relation.get_price(price_a, graph)?,
                    LiqAmount::Inf,
                    Some(edge.id()),
                ))
            }
        };

        if let Some(sol_price) = sol_index {
            // Check liquidity levels if sol price now exists

            let Some(tokens_a_per_sol) = sol_price.checked_div(price_a) else {
                // price_a exists, but the math overflowed somewhere.
                // Values are therefore way too high/low to be considered.
                continue;
            };

            let Some(liq_levels) = relation.get_liq_levels(tokens_a_per_sol) else {
                // Math overflow in calc'ing liq levels.
                // We can assume this to be illiquid if values got that high
                continue;
            };

            if !liq_levels.acceptable(max_price_impact) {
                continue;
            }
        }

        let Some(price_b_usd) = relation.get_price(price_a, graph) else {
            continue;
        };

        cm_weighted_price = cm_weighted_price.checked_add(price_b_usd.checked_mul(liq)?)?;
        total_liq = total_liq.checked_add(liq)?;
    }

    if total_liq == Decimal::ZERO {
        return None;
    }

    let weighted_price = cm_weighted_price.checked_div(total_liq)?;

    Some((weighted_price, LiqAmount::Amount(total_liq), None))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        sync::RwLock,
    };

    use chrono::Utc;
    use petgraph::Graph;
    use rust_decimal::{prelude::FromPrimitive, Decimal};
    use step_ingestooor_sdk::dooot::Dooot;
    use veritas_sdk::{
        liq_relation::LiqRelation,
        ppl_graph::{
            graph::{MintEdge, MintNode, USDPriceWithSource},
            structs::LiqAmount,
        },
        types::MintPricingGraph,
    };

    use crate::calculator::algo::{get_single_wighted_price, get_total_weighted_price};

    use super::bfs_recalculate;

    #[tokio::test]
    async fn weighted_liq() {
        let step_node = MintNode {
            mint: "STEP".into(),
            usd_price: RwLock::new(None),
            cached_fixed_relation: RwLock::new(None),
            non_vertex_relations: RwLock::new(HashMap::new()),
        };

        let usdc_node = MintNode {
            mint: "USDC".into(),
            usd_price: RwLock::new(Some(USDPriceWithSource::Oracle(Decimal::from(1)))),
            cached_fixed_relation: RwLock::new(None),
            non_vertex_relations: RwLock::new(HashMap::new()),
        };

        let illiquid_node = MintNode {
            mint: "DUMB".into(),
            usd_price: RwLock::new(None),
            cached_fixed_relation: RwLock::new(None),
            non_vertex_relations: RwLock::new(HashMap::new()),
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
                    pool_id: "SomeMarket".into(),
                }),
                last_updated: RwLock::new(Utc::now().naive_utc()),
                cached_price_and_liq: RwLock::new(None),
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
                    pool_id: "OtherMarket".into(),
                }),
                last_updated: RwLock::new(Utc::now().naive_utc()),
                cached_price_and_liq: RwLock::new(None),
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
                    pool_id: "IlliquidMarket".into(),
                }),
                last_updated: RwLock::new(Utc::now().naive_utc()),
                cached_price_and_liq: RwLock::new(None),
            },
        );

        let max_price_impact = Decimal::from_f64(0.25).unwrap();

        let (weighted, liq, _) =
            get_single_wighted_price(usdc_x, step_x, &graph, &None, &max_price_impact).unwrap();

        assert_eq!(
            weighted,
            Decimal::from_f64(3.5).unwrap(),
            "Single weighted price should be 3.5"
        );

        let total = get_total_weighted_price(&graph, step_x, &None, &max_price_impact);
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
            pool_id: "SomeMarket".into(),
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
            cached_fixed_relation: RwLock::new(None),
            non_vertex_relations: RwLock::new(HashMap::new()),
        });

        let test_token_a = graph.add_node(MintNode {
            mint: "TOKEN_A".into(),
            usd_price: RwLock::new(None),
            cached_fixed_relation: RwLock::new(None),
            non_vertex_relations: RwLock::new(HashMap::new()),
        });

        let test_token_b = graph.add_node(MintNode {
            mint: "TOKEN_B".into(),
            usd_price: RwLock::new(None),
            cached_fixed_relation: RwLock::new(None),
            non_vertex_relations: RwLock::new(HashMap::new()),
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
                    pool_id: "SomeMarket".into(),
                }),
                cached_price_and_liq: RwLock::new(None),
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
                    pool_id: "SomeMarket".into(),
                }),
                cached_price_and_liq: RwLock::new(None),
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
                    pool_id: "SomeMarket".into(),
                }),
                cached_price_and_liq: RwLock::new(None),
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
                    pool_id: "SomeMarket".into(),
                }),
                cached_price_and_liq: RwLock::new(None),
            },
        );

        // Recalc the graph starting from token A (this should NOT calc token B!)
        let (tx, _rx) = std::sync::mpsc::sync_channel::<Dooot>(100);
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
            true,
        )
        .unwrap();

        let b_node = graph.node_weight(test_token_b).unwrap();
        let price_exists = {
            b_node
                .usd_price
                .read()
                .expect("Price read lock poisoned")
                .as_ref()
                .is_some()
        };

        assert!(
            !price_exists,
            "Test Token B's price should NOT have been calculated!"
        );
    }
}
