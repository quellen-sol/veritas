use std::sync::Arc;

use anyhow::Result;
use step_ingestooor_sdk::dooot::{Dooot, SwapEventDooot};
use tokio::{sync::mpsc::Receiver, task::JoinHandle};
use veritas_sdk::{constants::USDC_MINT, ppl_graph::graph::MintPricingGraph};

pub async fn spawn_price_points_liquidity_task(
    mut msg_rx: Receiver<Dooot>,
    graph: Arc<MintPricingGraph>,
) -> Result<JoinHandle<()>> {
    log::info!("Spawning price points liquidity task (PPL)");
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
                    } = &swap;

                    if in_mint_pubkey != USDC_MINT && out_mint_pubkey != USDC_MINT {
                        // Only process USDC-based swaps for now
                        continue;
                    }

                    let Some(market) = market_pubkey else {
                        // Disallow aggregator swaps for now
                        continue;
                    };

                    
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
