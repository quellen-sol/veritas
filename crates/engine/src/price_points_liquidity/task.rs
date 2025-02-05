use std::{collections::HashMap, sync::Arc};

use anyhow::{Context, Result};
use petgraph::{graph::NodeIndex, visit::EdgeRef};
use rust_decimal::{prelude::FromPrimitive, Decimal};
use step_ingestooor_sdk::dooot::{Dooot, MintUnderlyingsGlobalDooot, SwapEventDooot};
use tokio::{
    sync::{
        mpsc::{Receiver, Sender},
        OwnedRwLockWriteGuard, RwLock,
    },
    task::JoinHandle,
};
use veritas_sdk::{
    constants::{USDC_FEED_ACCOUNT_ID, USDC_MINT},
    ppl_graph::graph::{MintEdge, MintLiquidity, MintNode, WrappedMintPricingGraph},
};

use crate::calculator::task::CalculatorUpdate;

type MintIndiciesMap = HashMap<String, NodeIndex>;

pub fn spawn_price_points_liquidity_task(
    mut msg_rx: Receiver<Dooot>,
    graph: WrappedMintPricingGraph,
    calculator_sender: Sender<CalculatorUpdate>,
) -> Result<JoinHandle<()>> {
    log::info!("Spawning price points liquidity task (PPL)");

    // Only to be used if we never *remove* nodes from the graph
    // See https://docs.rs/petgraph/latest/petgraph/graph/struct.Graph.html#graph-indices
    let mut mint_indicies: MintIndiciesMap = HashMap::new();

    let task = tokio::spawn(
        #[allow(clippy::unwrap_used)]
        async move {
            while let Some(dooot) = msg_rx.recv().await {
                match dooot {
                    Dooot::SwapEvent(swap) => {
                        let SwapEventDooot {
                            in_mint_pubkey,
                            out_mint_pubkey,
                            in_amount,
                            out_amount,
                            market_pubkey,
                            time,
                            ..
                        } = swap;

                        if in_amount == Decimal::ZERO {
                            // Skip swaps where the input amount is 0
                            continue;
                        }

                        // Extra debugging check. This will only consider USDC swaps
                        // if in_mint_pubkey != USDC_MINT && out_mint_pubkey != USDC_MINT {
                        //     // Only process USDC-based swaps for now
                        //     continue;
                        // }

                        // // Extra debugging check. This will only consider WSOL swaps
                        // if in_mint_pubkey != WSOL_MINT && out_mint_pubkey != WSOL_MINT {
                        //     // Only process WSOL-based swaps for now
                        //     continue;
                        // }

                        // // Extra debugging check. This will only consider STEP swaps
                        // if in_mint_pubkey != STEP_MINT && out_mint_pubkey != STEP_MINT {
                        //     // Only process STEP-based swaps for now
                        //     continue;
                        // }

                        if in_mint_pubkey == out_mint_pubkey {
                            // Skip self-swapping arbs
                            continue;
                        }

                        let Some(market) = market_pubkey else {
                            // Disallow aggregator swaps for now
                            continue;
                        };

                        let indicies = get_or_add_mint_indicies(
                            &[&in_mint_pubkey, &out_mint_pubkey],
                            graph.clone(),
                            &mut mint_indicies,
                        )
                        .await;

                        let in_mint_ix = indicies[0];
                        let out_mint_ix = indicies[1];

                        let out_per_in = out_amount / in_amount;

                        let mint_edge_res =
                            access_or_add_edge(out_mint_ix, in_mint_ix, graph.clone(), |e| {
                                e.market == Some(market.clone())
                            })
                            .await;

                        let mut mint_edge = match mint_edge_res {
                            Ok(edge) => edge,
                            Err(e) => {
                                log::error!("Error accessing or adding edge: {}", e);
                                continue;
                            }
                        };

                        mint_edge.last_updated = time;
                        mint_edge.market.replace(market);
                        mint_edge.this_per_that.replace(out_per_in);

                        let out_update = CalculatorUpdate::NewTokenRatio(out_mint_ix);
                        calculator_sender.send(out_update).await.unwrap();

                        // Don't send in_update for now
                        // let in_update = CalculatorUpdate::NewTokenRatio(in_mint_ix);
                        // calculator_sender.send(in_update).await.unwrap();
                    }
                    Dooot::MintUnderlyingsGlobal(mu_dooot) => {
                        let MintUnderlyingsGlobalDooot {
                            time,
                            mint_pubkey,
                            discriminant_id,
                            mints,
                            total_underlying_amounts,
                            mints_qty_per_one_parent,
                            ..
                        } = mu_dooot;

                        // Two things need to happen with MintUnderlyings:
                        // 1. Add the PARENT mint to graph and set edges, both price and liq!
                        // 2. Find the underlying mint edges, and update the liquidity (all directions, between all underlyings)

                        let mut all_mints = Vec::with_capacity(1 + mints.len());
                        all_mints.push(mint_pubkey.as_str());
                        for u_mint in mints.iter() {
                            all_mints.push(u_mint.as_str());
                        }

                        let indicies =
                            get_or_add_mint_indicies(&all_mints, graph.clone(), &mut mint_indicies)
                                .await;

                        let parent_mint_ix = indicies[0];
                        for (i, _) in all_mints.iter().enumerate().skip(1) {
                            let mint_ix = indicies[i];
                            let edge_res =
                                access_or_add_edge(mint_ix, parent_mint_ix, graph.clone(), |e| {
                                    e.market == Some(discriminant_id.clone())
                                })
                                .await;

                            let mut edge = match edge_res {
                                Ok(edge) => edge,
                                Err(e) => {
                                    log::error!("Error accessing or adding edge: {}", e);
                                    continue;
                                }
                            };

                            edge.last_updated = time;
                            edge.liquidity.replace(MintLiquidity {
                                liquidity: total_underlying_amounts.clone(),
                                mints: mints.clone(),
                            });
                            let price_val = mints_qty_per_one_parent[i - 1];
                            let price_val_decimal =
                                Decimal::from_f64(price_val).unwrap_or_else(|| {
                                    panic!(
                                        "UNREACHABLE - Cannot parse Decimal from f64: {price_val}"
                                    )
                                });
                            edge.this_per_that.replace(price_val_decimal);
                        }

                        // Add a liquidity relation between every underlying mint
                        for mint_a in mints.iter() {
                            for mint_b in mints.iter() {
                                if mint_a == mint_b {
                                    continue;
                                }

                                let indicies = get_or_add_mint_indicies(
                                    &[mint_a, mint_b],
                                    graph.clone(),
                                    &mut mint_indicies,
                                )
                                .await;

                                let mint_a_ix = indicies[0];
                                let mint_b_ix = indicies[1];

                                let edge_res =
                                    access_or_add_edge(mint_a_ix, mint_b_ix, graph.clone(), |e| {
                                        e.market == Some(discriminant_id.clone())
                                    })
                                    .await;

                                let mut edge = match edge_res {
                                    Ok(edge) => edge,
                                    Err(e) => {
                                        log::error!("Error accessing or adding edge: {}", e);
                                        continue;
                                    }
                                };

                                edge.last_updated = time;
                                edge.liquidity.replace(MintLiquidity {
                                    mints: mints.clone(),
                                    liquidity: total_underlying_amounts.clone(),
                                });
                            }
                        }
                    }
                    Dooot::OraclePriceEvent(oracle_price) => {
                        let feed_id = &oracle_price.feed_account_pubkey;
                        if feed_id != USDC_FEED_ACCOUNT_ID {
                            continue;
                        }

                        let price = oracle_price.price;
                        let ix = mint_indicies.get(USDC_MINT).cloned();
                        if let Some(ix) = ix {
                            calculator_sender
                                .send(CalculatorUpdate::OracleUSDPrice(price, ix))
                                .await
                                .unwrap();
                        } else {
                            log::error!("USDC not in graph, cannot recalc atm");
                        }
                    }
                    _ => {
                        continue;
                    }
                }

                // Quick debug dump of the graph
                #[cfg(feature = "debug-graph")]
                {
                    use petgraph::dot::Dot;
                    use std::fs;
                    let g_read = graph.read().await;
                    let formatted_dot_str = format!("{:?}", Dot::new(&(*g_read)));
                    fs::write("./graph.dot", formatted_dot_str).unwrap();

                    // // Quick debug dump of the graph size
                    // let num_nodes = g_read.node_count();
                    // let num_edges = g_read.edge_count();
                    // let node_bytes = num_nodes * NODE_SIZE;
                    // let edge_bytes = num_edges * EDGE_SIZE;
                    // // in KB
                    // log::info!("Num nodes: {num_nodes}, num edges: {num_edges}");
                    // log::info!("Node bytes: {node_bytes}, edge bytes: {edge_bytes}");
                    // log::info!(
                    //     "Total bytes: {:.2}K",
                    //     (node_bytes + edge_bytes) as f64 / 1024.0
                    // );
                }
            }

            log::warn!("Price points liquidity task (PPL) shutting down. Channel closed.");
        },
    );

    Ok(task)
}

pub async fn get_or_add_mint_indicies(
    mints: &[&str],
    graph: WrappedMintPricingGraph,
    mint_indicies: &mut MintIndiciesMap,
) -> Vec<NodeIndex> {
    let mut result = Vec::with_capacity(mints.len());

    let mut g_write = None;
    for mint in mints {
        if let Some(node_ix) = mint_indicies.get(*mint) {
            result.push(*node_ix);
        } else {
            let mut write_lock = match g_write {
                Some(g_w) => g_w,
                None => graph.write().await,
            };

            let ix = write_lock.add_node(Arc::new(RwLock::new(MintNode {
                mint: mint.to_string(),
                usd_price: None,
            })));

            mint_indicies.insert(mint.to_string(), ix);

            result.push(ix);

            g_write = Some(write_lock);
        }
    }

    result
}

pub async fn access_or_add_edge<P>(
    ix_a: NodeIndex,
    ix_b: NodeIndex,
    graph: WrappedMintPricingGraph,
    edge_predicate: P,
) -> Result<OwnedRwLockWriteGuard<MintEdge>>
where
    P: Fn(&MintEdge) -> bool,
{
    let g_read = graph.read().await;
    let mut edge_ix = None;
    for edge in g_read.edges_connecting(ix_a, ix_b) {
        let e_read = &edge.weight().read().await;
        if edge_predicate(e_read) {
            edge_ix = Some(edge.id());
            break;
        }
    }

    if let Some(edge_ix) = edge_ix {
        let edge = g_read
            .edge_weight(edge_ix)
            .context("UNREACHABLE - EdgeIndex should exist in graph!")?
            .clone();
        Ok(edge.write_owned().await)
    } else {
        drop(g_read);
        let mut g_write = graph.write().await;
        let new_edge = Arc::new(RwLock::new(MintEdge::default()));

        g_write.add_edge(ix_a, ix_b, new_edge.clone());

        Ok(new_edge.write_owned().await)
    }
}
