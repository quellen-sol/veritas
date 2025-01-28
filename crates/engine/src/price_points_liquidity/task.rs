use std::{collections::HashMap, fs, sync::Arc};

use anyhow::Result;
use petgraph::{dot::Dot, graph::NodeIndex, visit::EdgeRef};
use rust_decimal::Decimal;
use step_ingestooor_sdk::dooot::{Dooot, SwapEventDooot};
use tokio::{
    sync::{
        mpsc::{Receiver, Sender},
        Mutex, RwLock,
    },
    task::JoinHandle,
};
use veritas_sdk::{
    constants::{STEP_MINT, USDC_FEED_ACCOUNT_ID, USDC_MINT, WSOL_MINT},
    ppl_graph::graph::{MintEdge, MintNode, MintPricingGraph, EDGE_SIZE, NODE_SIZE},
};

use crate::calculator::task::CalculatorUpdate;

pub fn spawn_price_points_liquidity_task(
    mut msg_rx: Receiver<Dooot>,
    graph: Arc<RwLock<MintPricingGraph>>,
    calculator_sender: Sender<CalculatorUpdate>,
) -> Result<JoinHandle<()>> {
    log::info!("Spawning price points liquidity task (PPL)");

    // Only to be used if we never *remove* nodes from the graph
    // See https://docs.rs/petgraph/latest/petgraph/graph/struct.Graph.html#graph-indices
    let mut mint_market_indicies = HashMap::new();

    let task = tokio::spawn(async move {
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

                    if in_mint_pubkey != USDC_MINT && out_mint_pubkey != USDC_MINT {
                        // Only process USDC-based swaps for now
                        continue;
                    }

                    // // Extra debugging check. This will only consider WSOL/USDC swaps
                    // if in_mint_pubkey != WSOL_MINT && out_mint_pubkey != WSOL_MINT {
                    //     // Only process WSOL-based swaps for now
                    //     continue;
                    // }

                    // // Extra debugging check. This will only consider STEP/USDC swaps
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

                    // Cheap to clone NodeIndex
                    let mut out_mint_ix = mint_market_indicies.get(&out_mint_pubkey).cloned();

                    let mut in_mint_ix = mint_market_indicies.get(&in_mint_pubkey).cloned();

                    if out_mint_ix.is_none() || in_mint_ix.is_none() {
                        // At least one is NONE, grab a write lock.
                        let mut g_write = graph.write().await;
                        if out_mint_ix.is_none() {
                            let ix = g_write.add_node(MintNode {
                                mint: out_mint_pubkey.clone(),
                            });
                            mint_market_indicies.insert(out_mint_pubkey.clone(), ix);
                            out_mint_ix = Some(ix);
                        }

                        if in_mint_ix.is_none() {
                            let ix = g_write.add_node(MintNode {
                                mint: in_mint_pubkey.clone(),
                            });
                            mint_market_indicies.insert(in_mint_pubkey.clone(), ix);

                            in_mint_ix = Some(ix);
                        }
                    }

                    let out_mint_ix =
                        out_mint_ix.expect("UNREACHABLE - out_mint_ix should be set above");
                    let in_mint_ix =
                        in_mint_ix.expect("UNREACHABLE - out_mint_ix should be set above");

                    let g_read = graph.read().await;
                    let mut edge_ix = None;
                    for edge in g_read.edges_connecting(out_mint_ix, in_mint_ix) {
                        let e_read = edge.weight().read().await;
                        if e_read.market == Some(market.clone()) {
                            edge_ix = Some(edge.id());
                            break;
                        }
                    }
                    if in_amount == Decimal::ZERO {
                        // Skip swaps where the input amount is 0
                        continue;
                    }
                    let out_per_in = out_amount / in_amount;

                    if let Some(edge_ix) = edge_ix {
                        let weight = g_read
                            .edge_weight(edge_ix)
                            .expect("UNREACHABLE - EdgeIndex should exist in graph!")
                            // Just clones the `Arc`
                            .clone();
                        let mut w_weight = weight.write().await;
                        w_weight.this_per_that.replace(out_per_in);
                    } else {
                        // Not liking this logic that much, but over time
                        // This happens less and less, as more nodes added, and more atomic writes happen
                        drop(g_read);
                        let mut g_write = graph.write().await;
                        // Dangerous, MUST CHECK IF EDGE EXISTS FIRST TO AVOID DOUBLE-ADDING
                        g_write.add_edge(
                            out_mint_ix,
                            in_mint_ix,
                            Arc::new(RwLock::new(MintEdge::new(
                                Some(out_per_in),
                                None,
                                Some(market),
                                time,
                            ))),
                        );
                    }

                    let in_update = CalculatorUpdate::UpdatedTokenPrice(in_mint_ix);
                    let out_update = CalculatorUpdate::UpdatedTokenPrice(out_mint_ix);
                    calculator_sender.send(in_update).await.unwrap();
                    calculator_sender.send(out_update).await.unwrap();

                    // Quick debug dump of the graph
                    #[cfg(feature = "debug-graph")]
                    {
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
                Dooot::OraclePriceEvent(oracle_price) => {
                    let feed_id = &oracle_price.feed_account_pubkey;
                    if feed_id != USDC_FEED_ACCOUNT_ID {
                        continue;
                    }

                    let price = oracle_price.price;
                    let ix = mint_market_indicies.get(USDC_MINT).cloned();
                    if let Some(ix) = ix {
                        calculator_sender
                            .send(CalculatorUpdate::USDCPrice(price, ix))
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
        }

        log::warn!("Price points liquidity task (PPL) shutting down. Channel closed.");
    });

    Ok(task)
}
