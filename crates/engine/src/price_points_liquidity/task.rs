use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use petgraph::graph::NodeIndex;
use rust_decimal::Decimal;
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

                    let mut g_write = graph.write().await;

                    // Cheap to clone NodeIndex
                    let mut out_mint_ix = mint_market_indicies
                        .get(&(out_mint_pubkey.clone(), market.clone()))
                        .cloned()
                        .unwrap_or_else(|| {
                            let ix = g_write.add_node(MintNode {
                                market: market.clone(),
                                mint: out_mint_pubkey.clone(),
                            });
                            mint_market_indicies
                                .insert((out_mint_pubkey.clone(), market.clone()), ix);
                            ix
                        });

                    let mut in_mint_ix = mint_market_indicies
                        .get(&(in_mint_pubkey.clone(), market.clone()))
                        .cloned()
                        .unwrap_or_else(|| {
                            let ix = g_write.add_node(MintNode {
                                market: market.clone(),
                                mint: in_mint_pubkey.clone(),
                            });
                            mint_market_indicies
                                .insert((in_mint_pubkey.clone(), market.clone()), ix);
                            ix
                        });

                    let mut edge_ix = g_write.find_edge(out_mint_ix, in_mint_ix);
                    let out_per_in = out_amount / in_amount;
                    let new_edge = MintEdge::new_mutex(Some(out_per_in), None);

                    if let Some(edge_ix) = edge_ix {
                    let g_read = g_write.downgrade();
                        let weight = g_read.edge_weight(edge_ix).expect("UNREACHABLE - EdgeIndex doesn't exist in graph!");
                        let w_weight = weight
                    } else {
                        let ix = g_write.update_edge(out_mint_ix, in_mint_ix, new_edge);
                        drop(g_write);
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
