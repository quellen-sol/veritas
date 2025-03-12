// #![allow(unused)]
use std::{
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
use veritas_sdk::{ppl_graph::graph::MintPricingGraph, utils::decimal_cache::DecimalCache};

use crate::calculator::handlers::{
    oracle_price::handle_oracle_price_update, token_relation::handle_token_relation_update,
};

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
    decimals_cache: Arc<RwLock<DecimalCache>>,
    dooot_tx: Sender<Dooot>,
    max_calculator_subtasks: u8,
    bootstrap_in_progress: Arc<AtomicBool>,
) -> JoinHandle<()> {
    log::info!("Spawning Calculator tasks...");

    // Spawn a task to accept token updates
    tokio::spawn(async move {
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

            counter.fetch_add(1, Ordering::Relaxed);

            // Make clones for the task
            let graph = graph.clone();
            let decimals_cache = decimals_cache.clone();
            let dooot_tx = dooot_tx.clone();
            let counter = counter.clone();

            tokio::spawn(async move {
                match update {
                    CalculatorUpdate::OracleUSDPrice(token, new_price) => {
                        handle_oracle_price_update(
                            graph,
                            token,
                            new_price,
                            decimals_cache,
                            dooot_tx,
                        )
                        .await;
                    }
                    CalculatorUpdate::NewTokenRatio(token, updated_edge) => {
                        handle_token_relation_update(
                            graph,
                            token,
                            updated_edge,
                            decimals_cache,
                            dooot_tx,
                        )
                        .await;
                    }
                }

                counter.fetch_sub(1, Ordering::Relaxed);
            });
        }

        log::warn!("Calculator task finished");
    })
}
