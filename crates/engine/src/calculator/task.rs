// #![allow(unused)]
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};

use petgraph::graph::{EdgeIndex, NodeIndex};
use rust_decimal::Decimal;
use step_ingestooor_sdk::dooot::Dooot;
use tokio::{
    sync::{
        mpsc::{Receiver, Sender},
        RwLock,
    },
    task::JoinHandle,
};
use veritas_sdk::ppl_graph::graph::MintPricingGraph;

use crate::calculator::handlers::oracle_price::handle_oracle_price_update;

#[derive(Debug)]
pub enum CalculatorUpdate {
    /// Price of USD (from oracle) and index in the graph
    OracleUSDPrice(NodeIndex, Decimal),
    /// A Relation pointing TO this token has changed (edge id provided)
    NewTokenRatio(NodeIndex, EdgeIndex),
}

pub fn spawn_calculator_task(
    mut calculator_receiver: Receiver<CalculatorUpdate>,
    graph: Arc<RwLock<MintPricingGraph>>,
    dooot_tx: Sender<Dooot>,
    max_calculator_subtasks: u8,
    bootstrap_in_progress: Arc<AtomicBool>,
    oracle_mint_set: HashSet<String>,
) -> JoinHandle<()> {
    log::info!("Spawning Calculator tasks...");

    // Spawn a task to accept token updates
    tokio::spawn(async move {
        let counter = Arc::new(AtomicU8::new(0));
        let oracle_mint_set = Arc::new(oracle_mint_set);

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

            counter.fetch_add(1, Ordering::Relaxed);

            // Make clones for the task
            let graph = graph.clone();
            let dooot_tx = dooot_tx.clone();
            let counter = counter.clone();
            let oracle_mint_set = oracle_mint_set.clone();

            log::trace!("Spawning task for token update");
            tokio::spawn(async move {
                match update {
                    CalculatorUpdate::OracleUSDPrice(token, new_price) => {
                        handle_oracle_price_update(
                            graph,
                            token,
                            new_price,
                            dooot_tx,
                            &oracle_mint_set,
                        )
                        .await;
                    }
                    CalculatorUpdate::NewTokenRatio(token, updated_edge) => {
                        // handle_token_relation_update(
                        //     graph,
                        //     token,
                        //     updated_edge,
                        //     dooot_tx,
                        //     &oracle_mint_set,
                        // )
                        // .await;
                    }
                }

                counter.fetch_sub(1, Ordering::Relaxed);
            });
        }

        log::warn!("Calculator task finished");
    })
}
