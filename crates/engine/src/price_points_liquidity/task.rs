use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use petgraph::graph::NodeIndex;
use step_ingestooor_sdk::dooot::{Dooot, SwapEventDooot};
use tokio::{
    sync::{mpsc::Receiver, Mutex, RwLock},
    task::JoinHandle,
};
use veritas_sdk::{
    constants::USDC_MINT,
    ppl_graph::graph::{MintEdge, MintNode, MintPricingGraph},
};

pub async fn spawn_price_points_liquidity_task(
    mut msg_rx: Receiver<Dooot>,
    graph: Arc<RwLock<MintPricingGraph>>,
) -> Result<JoinHandle<()>> {
    log::info!("Spawning price points liquidity task (PPL)");

    // Only to be used if we never *remove* nodes from the graph
    // See https://docs.rs/petgraph/latest/petgraph/graph/struct.Graph.html#graph-indices
    let mut mint_market_indicies: HashMap<(String, String), NodeIndex> = HashMap::new();
    let task = tokio::spawn(async move {
        while let Some(dooot) = msg_rx.recv().await {
            log::info!("Received dooot: {:?}", dooot);
            // ... TODO: Implement PPL

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

                    let Some(market) = market_pubkey else {
                        // Disallow aggregator swaps for now
                        continue;
                    };

                    // Cheap to clone NodeIndex
                    let mut out_mint_ix = mint_market_indicies
                        .get(&(out_mint_pubkey.clone(), market.clone()))
                        .cloned();

                    let mut in_mint_ix = mint_market_indicies
                        .get(&(in_mint_pubkey.clone(), market.clone()))
                        .cloned();

                    if out_mint_ix.is_none() || in_mint_ix.is_none() {
                        // At least one is NONE, grab a write lock.
                        let mut g_write = graph.write().await;
                        if out_mint_ix.is_none() {
                            let ix = g_write.add_node(MintNode {
                                market: market.clone(),
                                mint: out_mint_pubkey.clone(),
                                last_updated: time,
                            });
                            mint_market_indicies
                                .insert((out_mint_pubkey.clone(), market.clone()), ix);
                            out_mint_ix = Some(ix);
                        }

                        if in_mint_ix.is_none() {
                            let ix = g_write.add_node(MintNode {
                                market: market.clone(),
                                mint: in_mint_pubkey.clone(),
                                last_updated: time,
                            });
                            mint_market_indicies
                                .insert((in_mint_pubkey.clone(), market.clone()), ix);

                            in_mint_ix = Some(ix);
                        }
                    }

                    let out_mint_ix =
                        out_mint_ix.expect("UNREACHABLE - out_mint_ix should be set above");
                    let in_mint_ix =
                        in_mint_ix.expect("UNREACHABLE - out_mint_ix should be set above");

                    let g_read = graph.read().await;
                    let edge_ix = g_read.find_edge(out_mint_ix, in_mint_ix);
                    let out_per_in = out_amount / in_amount;

                    if let Some(edge_ix) = edge_ix {
                        let weight = g_read
                            .edge_weight(edge_ix)
                            .expect("UNREACHABLE - EdgeIndex should exist in graph!")
                            .clone();
                        let mut w_weight = weight.lock().await;
                        w_weight.this_per_that.replace(out_per_in);
                    } else {
                        // Not liking this logic that much, but over time
                        // This happens less and less, as more nodes added, and more atomic writes happen
                        drop(g_read);
                        let mut g_write = graph.write().await;
                        g_write.update_edge(
                            out_mint_ix,
                            in_mint_ix,
                            Arc::new(Mutex::new(MintEdge::new(Some(out_per_in), None))),
                        );
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
