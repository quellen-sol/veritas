// #![allow(unused)]
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, SyncSender},
        Arc, RwLock,
    },
    thread::{self, JoinHandle},
};

use petgraph::graph::{EdgeIndex, NodeIndex};
use rust_decimal::Decimal;
use step_ingestooor_sdk::dooot::Dooot;
use veritas_sdk::types::MintPricingGraph;

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
    calculator_receiver: Receiver<CalculatorUpdate>,
    graph: Arc<RwLock<MintPricingGraph>>,
    dooot_tx: SyncSender<Dooot>,
    bootstrap_in_progress: Arc<AtomicBool>,
    oracle_mint_set: Arc<HashSet<String>>,
    sol_index: Arc<RwLock<Option<Decimal>>>,
    max_price_impact: Decimal,
    paused_calculation: Arc<AtomicBool>,
) -> JoinHandle<()> {
    log::info!("Spawning Calculator task...");

    // Spawn a task to accept token updates
    thread::spawn(move || {
        while let Ok(update) = calculator_receiver.recv() {
            if bootstrap_in_progress.load(Ordering::Relaxed)
                || paused_calculation.load(Ordering::Relaxed)
            {
                // Do not process graph updates while bootstrapping,
                // Avoids thousands of recalcs while bootstrapping
                continue;
            }

            // Make clones for the fn call
            let graph = graph.clone();
            let dooot_tx = dooot_tx.clone();
            let sol_index = sol_index.clone();

            match update {
                CalculatorUpdate::OracleUSDPrice(token, new_price) => {
                    handle_oracle_price_update(
                        graph,
                        token,
                        new_price,
                        dooot_tx,
                        &oracle_mint_set,
                        sol_index,
                        &max_price_impact,
                    );
                }
                CalculatorUpdate::NewTokenRatio(token, updated_edge) => {
                    handle_token_relation_update(
                        graph,
                        token,
                        updated_edge,
                        dooot_tx,
                        &oracle_mint_set,
                        sol_index,
                        &max_price_impact,
                    );
                }
            }
        }

        log::warn!("Calculator task finished");
    })
}
