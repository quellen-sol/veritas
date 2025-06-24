use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::{Context, Result};
use chrono::NaiveDateTime;
use petgraph::graph::{EdgeIndex, NodeIndex};
use step_ingestooor_sdk::dooot::Dooot;
use tokio::{
    sync::{
        mpsc::{Receiver, Sender},
        RwLock,
    },
    task::JoinHandle,
};
use veritas_sdk::{
    liq_relation::LiqRelation,
    ppl_graph::graph::{MintEdge, MintNode, MintPricingGraph, WrappedMintPricingGraph},
    utils::{
        decimal_cache::DecimalCache, lp_cache::LpCache, token_balance_cache::TokenBalanceCache,
    },
};

use crate::{
    calculator::task::CalculatorUpdate,
    price_points_liquidity::handlers::{
        clmm::handle_clmm, dlmm::handle_dlmm, lp_info::handle_lp_info, mint_info::handle_mint_info,
        mint_underlyings::handle_mint_underlyings, oracle_price_event::handle_oracle_price_event,
        token_balance::handle_token_balance,
    },
};

pub type MintIndiciesMap = HashMap<String, NodeIndex>;
pub type EdgeIndiciesMap = HashMap<String, [Option<EdgeIndex>; 2]>; // Given one discriminant (market), we should only have max 2 relations (A -> B, and B -> A)

pub fn spawn_price_points_liquidity_task(
    mut msg_rx: Receiver<Dooot>,
    graph: WrappedMintPricingGraph,
    calculator_sender: Sender<CalculatorUpdate>,
    decimal_cache: Arc<RwLock<DecimalCache>>,
    lp_cache: Arc<RwLock<LpCache>>,
    oracle_feed_map: Arc<HashMap<String, String>>,
    max_ppl_subtasks: u8,
    ch_cache_updator_req_tx: Sender<String>,
    bootstrap_in_progress: Arc<AtomicBool>,
    mint_indicies: Arc<RwLock<MintIndiciesMap>>,
    price_sender: Sender<Dooot>,
    token_balance_cache: Arc<RwLock<TokenBalanceCache>>,
) -> Result<JoinHandle<()>> {
    log::info!("Spawning price points liquidity task (PPL)");

    let edge_indicies = Arc::new(RwLock::new(EdgeIndiciesMap::new()));

    let task = tokio::spawn(
        #[allow(clippy::unwrap_used)]
        async move {
            let counter = Arc::new(AtomicU8::new(0));

            while let Some(dooot) = msg_rx.recv().await {
                while counter.load(Ordering::Relaxed) >= max_ppl_subtasks {
                    // Wait for the other subtasks to finish
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }

                counter.fetch_add(1, Ordering::Relaxed);

                let counter = counter.clone();
                let lp_cache = lp_cache.clone();
                let graph = graph.clone();
                let calculator_sender = calculator_sender.clone();
                let decimal_cache = decimal_cache.clone();
                let oracle_feed_map = oracle_feed_map.clone();
                let mint_indicies = mint_indicies.clone();
                let edge_indicies = edge_indicies.clone();
                let sender_arc = ch_cache_updator_req_tx.clone();
                let bootstrap_in_progress = bootstrap_in_progress.clone();
                let price_sender = price_sender.clone();
                let token_balance_cache = token_balance_cache.clone();

                tokio::spawn(async move {
                    match dooot {
                        Dooot::MintUnderlyingsGlobal(mu_dooot) => {
                            handle_mint_underlyings(
                                mu_dooot,
                                lp_cache,
                                graph,
                                decimal_cache,
                                sender_arc,
                                mint_indicies,
                                edge_indicies,
                            )
                            .await;
                        }
                        Dooot::OraclePriceEvent(oracle_price) => {
                            handle_oracle_price_event(
                                oracle_price,
                                oracle_feed_map,
                                mint_indicies,
                                calculator_sender,
                                bootstrap_in_progress,
                                price_sender,
                            )
                            .await;
                        }
                        Dooot::MintInfo(info) => {
                            handle_mint_info(info, decimal_cache).await;
                        }
                        Dooot::LPInfo(info) => {
                            handle_lp_info(info, lp_cache).await;
                        }
                        Dooot::DlmmGlobal(dooot) => {
                            handle_dlmm(
                                dooot,
                                graph,
                                lp_cache,
                                decimal_cache,
                                mint_indicies,
                                edge_indicies,
                                sender_arc,
                                token_balance_cache,
                                calculator_sender,
                                bootstrap_in_progress,
                            )
                            .await;
                        }
                        Dooot::TokenBalanceUser(balance) => {
                            handle_token_balance(balance, token_balance_cache).await;
                        }
                        Dooot::ClmmGlobal(_) | Dooot::ClmmTickGlobal(_) => {
                            handle_clmm(
                                dooot,
                                graph,
                                lp_cache,
                                decimal_cache,
                                mint_indicies,
                                edge_indicies,
                                sender_arc,
                                token_balance_cache,
                                calculator_sender,
                                bootstrap_in_progress,
                            )
                            .await;
                        }
                        _ => {}
                    }

                    // Quick debug dump of the graph
                    #[cfg(feature = "debug-graph")]
                    {
                        let g_read = graph.read().await;

                        use petgraph::dot::Dot;
                        use std::fs;
                        let formatted_dot_str = format!("{:?}", Dot::new(&(*g_read)));
                        fs::write("./graph.dot", formatted_dot_str).unwrap();

                        // // Quick debug dump of the graph size
                        // use veritas_sdk::ppl_graph::graph::EDGE_SIZE;
                        // use veritas_sdk::ppl_graph::graph::NODE_SIZE;
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

                    counter.fetch_sub(1, Ordering::Relaxed);
                });
            }

            log::warn!("Price points liquidity task (PPL) shutting down. Channel closed.");
        },
    );

    Ok(task)
}

/// Returns the decimals for a mint, or None if the decimals are not in the cache
///
/// If the decimals are not in the cache, it will dispatch a request to get the decimals
#[inline]
pub fn get_or_dispatch_decimals(
    sender_arc: &Sender<String>,
    dc_read: &HashMap<String, u8>,
    mint_x: &str,
) -> Option<u8> {
    let decimals_x = match dc_read.get(mint_x) {
        Some(&d) => d,
        None => {
            // Dispatch a request to get the decimals
            sender_arc.try_send(mint_x.to_string()).ok();
            return None;
        }
    };
    Some(decimals_x)
}

/// Get the mint index for a mint, or add it to the graph if it doesn't exist
///
/// Returns the mint index and a boolean indicating if the mint was added to the graph
#[inline]
pub async fn get_or_add_mint_ix(
    mint: &str,
    graph: WrappedMintPricingGraph,
    mint_indicies: Arc<RwLock<MintIndiciesMap>>,
) -> (NodeIndex, bool) {
    log::trace!("Getting mint indicies read lock");
    let mint_index_op = mint_indicies.read().await.get(mint).cloned();
    log::trace!("Got mint indicies read lock");

    match mint_index_op {
        Some(ix) => (ix, false),
        None => {
            log::trace!("Getting graph write lock");
            let mut g_write = graph.write().await;
            log::trace!("Got graph write lock");
            log::trace!("Getting mint indicies write lock");
            let mut mi_write = mint_indicies.write().await;
            log::trace!("Got mint indicies write lock");

            // Check if the mint is already in the graph, since the above check is too optimistic
            let (ix, created) = mi_write
                .get(mint)
                .cloned()
                .map(|i| (i, false))
                .unwrap_or_else(|| {
                    (
                        g_write.add_node(MintNode {
                            mint: mint.to_string(),
                            usd_price: RwLock::new(None),
                            cached_fixed_relation: RwLock::new(None),
                        }),
                        true,
                    )
                });

            mi_write.insert(mint.to_string(), ix);

            (ix, created)
        }
    }
}

/// Get edge from A -> B that matches the discriminant ID
#[inline]
pub async fn get_edge_by_discriminant(
    ix_a: NodeIndex,
    ix_b: NodeIndex,
    graph: WrappedMintPricingGraph,
    edge_indicies: Arc<RwLock<EdgeIndiciesMap>>,
    discriminant_id: &str,
) -> Option<EdgeIndex> {
    log::trace!("Getting edge indicies read lock");
    let indexed_edge = edge_indicies.read().await.get(discriminant_id).cloned()?;
    log::trace!("Got edge indicies read lock");

    for i_edge in indexed_edge.iter().flatten() {
        log::trace!("Getting graph read lock");
        let g_read = graph.read().await;
        log::trace!("Got graph read lock");
        let Some((src, target)) = g_read.edge_endpoints(*i_edge) else {
            continue;
        };

        if src == ix_a && target == ix_b {
            return Some(*i_edge);
        }
    }

    None
}

/// Get edge from A -> B that matches the discriminant ID
#[inline]
pub async fn get_edge_by_discriminant_with_locks(
    ix_a: NodeIndex,
    ix_b: NodeIndex,
    graph: &MintPricingGraph,
    edge_indicies: &EdgeIndiciesMap,
    discriminant_id: &str,
) -> Option<EdgeIndex> {
    let indexed_edge = edge_indicies.get(discriminant_id)?;

    for i_edge in indexed_edge.iter().flatten() {
        let Some((src, target)) = graph.edge_endpoints(*i_edge) else {
            continue;
        };

        if src == ix_a && target == ix_b {
            return Some(*i_edge);
        }
    }

    None
}

#[inline]
pub async fn add_or_update_two_way_relation_edge(
    ix_a: NodeIndex,
    ix_b: NodeIndex,
    edge_indicies: Arc<RwLock<EdgeIndiciesMap>>,
    graph: WrappedMintPricingGraph,
    update_with: LiqRelation,
    update_with_rev: LiqRelation,
    discriminant_id: &str,
    time: NaiveDateTime,
) -> Result<(EdgeIndex, EdgeIndex)> {
    let edge = get_edge_by_discriminant(
        ix_b,
        ix_a,
        graph.clone(),
        edge_indicies.clone(),
        discriminant_id,
    )
    .await;
    let edge_rev = get_edge_by_discriminant(
        ix_a,
        ix_b,
        graph.clone(),
        edge_indicies.clone(),
        discriminant_id,
    )
    .await;

    match (edge_rev, edge) {
        (Some(edge_rev), Some(edge)) => {
            let g_read = graph.read().await;
            let e_r = g_read
                .edge_weight(edge)
                .context("UNREACHABLE - Edge index {edge:?} should be present in graph!")?;
            let e_r_rev = g_read
                .edge_weight(edge_rev)
                .context("UNREACHABLE - Edge index {edge_rev:?} should be present in graph!")?;

            {
                let mut relation = e_r.inner_relation.write().await;
                let mut relation_rev = e_r_rev.inner_relation.write().await;

                *relation = update_with;
                *relation_rev = update_with_rev;
            }

            Ok((edge, edge_rev))
        }
        (None, None) => {
            let new_edge = MintEdge {
                id: discriminant_id.to_string(),
                dirty: true,
                last_updated: RwLock::new(time),
                inner_relation: RwLock::new(update_with),
            };

            let new_edge_rev = MintEdge {
                id: discriminant_id.to_string(),
                dirty: true,
                last_updated: RwLock::new(time),
                inner_relation: RwLock::new(update_with_rev),
            };

            let (new_ix, new_ix_rev) = {
                log::trace!("Getting graph write lock");
                let mut g_write = graph.write().await;
                log::trace!("Got graph write lock");
                log::trace!("Getting edge indicies write lock");
                let mut ei_write = edge_indicies.write().await;
                log::trace!("Got edge indicies write lock");
                // Now that we have graph exclusively locked, let's double check once more that we don't have an edge already
                // The last check was too optimistic, and we could have raced with another thread
                let edge = get_edge_by_discriminant_with_locks(
                    ix_b,
                    ix_a,
                    &g_write,
                    &ei_write,
                    discriminant_id,
                )
                .await;
                let edge_rev = get_edge_by_discriminant_with_locks(
                    ix_a,
                    ix_b,
                    &g_write,
                    &ei_write,
                    discriminant_id,
                )
                .await;

                let new_ix = edge.unwrap_or_else(|| g_write.add_edge(ix_b, ix_a, new_edge));
                let new_ix_rev =
                    edge_rev.unwrap_or_else(|| g_write.add_edge(ix_a, ix_b, new_edge_rev));

                update_edge_index(&mut ei_write, discriminant_id, new_ix)?;
                update_edge_index(&mut ei_write, discriminant_id, new_ix_rev)?;

                (new_ix, new_ix_rev)
            };

            Ok((new_ix, new_ix_rev))
        }
        _ => Err(anyhow::anyhow!(
            "UNREACHABLE - Both edges should have been set! Likely race condition!"
        )),
    }
}

/// Adds or updates edge A -> B
///
/// Returns the edge idx that was updated
#[inline]
#[allow(clippy::unwrap_used)]
pub async fn add_or_update_relation_edge(
    ix_a: NodeIndex,
    ix_b: NodeIndex,
    edge_indicies: Arc<RwLock<EdgeIndiciesMap>>,
    graph: WrappedMintPricingGraph,
    update_with: LiqRelation,
    discriminant_id: &str,
    time: NaiveDateTime,
) -> Result<EdgeIndex>
where
{
    let edge = get_edge_by_discriminant(
        ix_a,
        ix_b,
        graph.clone(),
        edge_indicies.clone(),
        discriminant_id,
    )
    .await;

    match edge {
        Some(edge_ix) => {
            // Edge already exists, update it
            // Guaranteed by being Some
            {
                log::trace!("Getting graph read lock");
                let g_read = graph.read().await;
                log::trace!("Got graph read lock");
                let e_r = g_read
                    .edge_weight(edge_ix)
                    .context("UNREACHABLE - Edge index {edge_ix:?} should be present in graph!")?;

                // Quick update of the last updated time
                {
                    log::trace!("Getting last updated write lock");
                    let mut last_updated = e_r.last_updated.write().await;
                    log::trace!("Got last updated write lock");
                    *last_updated = time;
                }

                // Quick update of the relation
                {
                    log::trace!("Getting inner relation write lock");
                    let mut relation = e_r.inner_relation.write().await;
                    log::trace!("Got inner relation write lock");
                    *relation = update_with;
                }
            }

            Ok(edge_ix)
        }
        None => {
            let new_ix = {
                log::trace!("Getting edge indicies write lock");
                let mut ei_write = edge_indicies.write().await;
                log::trace!("Got edge indicies write lock");
                log::trace!("Getting graph write lock");
                let mut g_write = graph.write().await;
                log::trace!("Got graph write lock");

                // Now that we have graph exclusively locked, let's double check once more that we don't have an edge already
                // The last check was too optimistic, and we could have raced with another thread
                let edge = get_edge_by_discriminant_with_locks(
                    ix_b,
                    ix_a,
                    &g_write,
                    &ei_write,
                    discriminant_id,
                )
                .await;

                let new_edge = MintEdge {
                    id: discriminant_id.to_string(),
                    dirty: true,
                    last_updated: RwLock::new(time),
                    inner_relation: RwLock::new(update_with),
                };

                let new_ix = edge.unwrap_or_else(|| g_write.add_edge(ix_a, ix_b, new_edge));

                update_edge_index(&mut ei_write, discriminant_id, new_ix)?;

                new_ix
            };

            Ok(new_ix)
        }
    }
}

pub fn update_edge_index(
    edge_indicies: &mut EdgeIndiciesMap,
    discriminant_id: &str,
    index: EdgeIndex,
) -> Result<()> {
    match edge_indicies.get_mut(discriminant_id) {
        None => {
            edge_indicies.insert(discriminant_id.to_string(), [Some(index), None]);

            Ok(())
        }
        Some(entry) => {
            for item in entry.iter_mut() {
                match item {
                    Some(ix) => {
                        if *ix == index {
                            // Leave unchanged
                            return Ok(());
                        }
                    }
                    None => {
                        *item = Some(index);
                        return Ok(());
                    }
                }
            }

            // If we get here, we're trying to add a third edge with the same discriminant,
            // This is not allowed, so error!
            Err(anyhow::anyhow!(
                "Trying to add a third edge with the same discriminant"
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use rust_decimal::Decimal;
    use veritas_sdk::ppl_graph::graph::MintPricingGraph;

    use super::*;

    #[test]
    fn test_updating_edge_indicies() {
        let mut edge_indicies = EdgeIndiciesMap::new();
        let discriminant_id = "test_disc";

        let ix_a = EdgeIndex::new(0);
        let ix_b = EdgeIndex::new(1);

        update_edge_index(&mut edge_indicies, discriminant_id, ix_a).unwrap();
        update_edge_index(&mut edge_indicies, discriminant_id, ix_b).unwrap();

        assert_eq!(
            edge_indicies.get(discriminant_id),
            Some(&[Some(ix_a), Some(ix_b)])
        );

        update_edge_index(&mut edge_indicies, discriminant_id, ix_b).unwrap();

        assert_eq!(
            edge_indicies.get(discriminant_id),
            Some(&[Some(ix_a), Some(ix_b)])
        );

        update_edge_index(&mut edge_indicies, discriminant_id, ix_a).unwrap();

        assert_eq!(
            edge_indicies.get(discriminant_id),
            Some(&[Some(ix_a), Some(ix_b)])
        );
    }

    #[tokio::test]
    async fn test_get_edge_by_discriminant() {
        let mut graph = MintPricingGraph::new();
        let mut edge_indicies = EdgeIndiciesMap::new();

        let ix_a = graph.add_node(MintNode {
            mint: "test_mint_a".to_string(),
            usd_price: RwLock::new(None),
            cached_fixed_relation: RwLock::new(None),
        });
        let ix_b = graph.add_node(MintNode {
            mint: "test_mint_b".to_string(),
            usd_price: RwLock::new(None),
            cached_fixed_relation: RwLock::new(None),
        });

        let ix_edge = graph.add_edge(
            ix_a,
            ix_b,
            MintEdge {
                id: "test_disc".to_string(),
                dirty: true,
                last_updated: RwLock::new(Utc::now().naive_utc()),
                inner_relation: RwLock::new(LiqRelation::Fixed {
                    amt_per_parent: Decimal::from(100),
                }),
            },
        );

        let ix_edge_2 = graph.add_edge(
            ix_b,
            ix_a,
            MintEdge {
                id: "test_disc".to_string(),
                dirty: true,
                last_updated: RwLock::new(Utc::now().naive_utc()),
                inner_relation: RwLock::new(LiqRelation::Fixed {
                    amt_per_parent: Decimal::from(100),
                }),
            },
        );

        update_edge_index(&mut edge_indicies, "test_disc", ix_edge).unwrap();
        update_edge_index(&mut edge_indicies, "test_disc", ix_edge_2).unwrap();
        let graph = Arc::new(RwLock::new(graph));
        let edge_indicies = Arc::new(RwLock::new(edge_indicies));

        let edge_1 = get_edge_by_discriminant(
            ix_a,
            ix_b,
            graph.clone(),
            edge_indicies.clone(),
            "test_disc",
        )
        .await;
        let edge_2 = get_edge_by_discriminant(
            ix_b,
            ix_a,
            graph.clone(),
            edge_indicies.clone(),
            "test_disc",
        )
        .await;

        assert_eq!(edge_1, Some(ix_edge));
        assert_eq!(edge_2, Some(ix_edge_2));
    }
}
