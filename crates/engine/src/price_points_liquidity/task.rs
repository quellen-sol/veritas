use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::Result;
use chrono::NaiveDateTime;
use clickhouse::Row;
use petgraph::{
    graph::{EdgeIndex, NodeIndex},
    visit::EdgeRef,
};
use rust_decimal::{prelude::FromPrimitive, Decimal, MathematicalOps};
use serde::Deserialize;
use step_ingestooor_sdk::dooot::{
    CurveType, Dooot, LPInfoDooot, MintInfoDooot, MintUnderlyingsGlobalDooot,
};
use tokio::{
    sync::{
        mpsc::{Receiver, Sender},
        RwLock,
    },
    task::JoinHandle,
};
use veritas_sdk::{
    ppl_graph::{
        graph::{
            MintEdge, MintNode, MintPricingGraph, USDPriceWithSource, WrappedMintPricingGraph,
        },
        structs::LiqRelation,
    },
    utils::{
        decimal_cache::DecimalCache,
        lp_cache::{LiquidityPool, LpCache},
        oracle_cache::OraclePriceCache,
    },
};

use crate::calculator::task::CalculatorUpdate;

type MintIndiciesMap = HashMap<String, NodeIndex>;

#[allow(clippy::too_many_arguments)]
pub fn spawn_price_points_liquidity_task(
    mut msg_rx: Receiver<Dooot>,
    graph: WrappedMintPricingGraph,
    calculator_sender: Sender<CalculatorUpdate>,
    decimal_cache: Arc<RwLock<DecimalCache>>,
    lp_cache: Arc<RwLock<LpCache>>,
    oracle_cache: Arc<RwLock<OraclePriceCache>>,
    oracle_feed_map: Arc<HashMap<String, String>>,
    max_ppl_subtasks: u8,
    ch_cache_updator_req_tx: Sender<String>,
) -> Result<JoinHandle<()>> {
    log::info!("Spawning price points liquidity task (PPL)");

    // Only to be used if we never *remove* nodes from the graph
    // See https://docs.rs/petgraph/latest/petgraph/graph/struct.Graph.html#graph-indices
    let mint_indicies = Arc::new(RwLock::new(HashMap::new()));

    let task = tokio::spawn(
        #[allow(clippy::unwrap_used)]
        async move {
            let counter = Arc::new(AtomicU8::new(0));
            let sender_arc = Arc::new(ch_cache_updator_req_tx);

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
                let oracle_cache = oracle_cache.clone();
                let sender_arc = sender_arc.clone();

                tokio::spawn(async move {
                    let now = Instant::now();
                    match dooot {
                        Dooot::MintUnderlyingsGlobal(mu_dooot) => {
                            let MintUnderlyingsGlobalDooot {
                                time,
                                mint_pubkey: parent_mint,
                                mints,
                                total_underlying_amounts,
                                mints_qty_per_one_parent,
                                ..
                            } = mu_dooot;

                            // None if theres no LP associated with this
                            let curve_type = {
                                let lpc_read = lp_cache.read().await;
                                lpc_read.get(&parent_mint).map(|lp| lp.curve_type.clone())
                            };

                            let mut g_write = graph.write().await;
                            let mut mint_indicies = mint_indicies.write().await;

                            // Ordered with `mints`
                            let mut underlying_idxs = Vec::with_capacity(mints.len());

                            for mint in mints.iter() {
                                let mint_ix =
                                    get_or_add_mint_ix(mint, &mut g_write, &mut mint_indicies);

                                underlying_idxs.push(mint_ix);
                            }

                            let dc_read = decimal_cache.read().await;

                            // Add a Fixed relation to the parent if theres only one mint
                            if mints.len() == 1 {
                                let amt_per_parent = mints_qty_per_one_parent[0];
                                let Some(amt_per_parent) = Decimal::from_f64(amt_per_parent) else {
                                    log::error!("Could not parse amt_per_parent into a Decimal: {amt_per_parent}");
                                    counter.fetch_sub(1, Ordering::Relaxed);
                                    return;
                                };

                                let parent_ix = get_or_add_mint_ix(
                                    &parent_mint,
                                    &mut g_write,
                                    &mut mint_indicies,
                                );

                                let Some(dec_parent) =
                                    get_or_dispatch_decimals(&sender_arc, &dc_read, &parent_mint)
                                else {
                                    counter.fetch_sub(1, Ordering::Relaxed);
                                    return;
                                };

                                let this_mint = &mints[0];

                                let Some(dec_this) =
                                    get_or_dispatch_decimals(&sender_arc, &dc_read, this_mint)
                                else {
                                    counter.fetch_sub(1, Ordering::Relaxed);
                                    return;
                                };

                                let exp = (dec_parent as i64) - (dec_this as i64);
                                let Some(dec_factor) = Decimal::from(10).checked_powi(exp) else {
                                    log::warn!(
                                        "Decimal overflow when trying to get decimal factor for {parent_mint} and {this_mint}: Decimals are {dec_parent} and {dec_this} respectively"
                                    );
                                    counter.fetch_sub(1, Ordering::Relaxed);
                                    return;
                                };

                                let amt_per_parent = amt_per_parent * dec_factor;
                                let relation = LiqRelation::Fixed { amt_per_parent };

                                add_or_update_relation_edge(
                                    underlying_idxs[0],
                                    parent_ix,
                                    &mut g_write,
                                    |e| e.id == parent_mint,
                                    relation,
                                    &parent_mint,
                                    time,
                                )
                                .await;
                                drop(g_write);

                                let update = CalculatorUpdate::NewTokenRatio(parent_ix);
                                calculator_sender.send(update).await.unwrap();

                                log::debug!("New token ratio update in {:?}", now.elapsed());
                            } else {
                                // Create edges for all underlying mints (likely an LP)
                                for (i_x, un_x) in underlying_idxs.iter().cloned().enumerate() {
                                    let mint_x = &mints[i_x];

                                    let Some(dec_factor_x) = get_or_dispatch_decimal_factor(
                                        &sender_arc,
                                        &dc_read,
                                        mint_x,
                                    ) else {
                                        continue;
                                    };

                                    let amt_x = total_underlying_amounts[i_x] / dec_factor_x;

                                    for (i_y, un_y) in underlying_idxs.iter().cloned().enumerate() {
                                        if un_x == un_y {
                                            continue;
                                        }

                                        let mint_y = &mints[i_y];

                                        let Some(dec_factor_y) = get_or_dispatch_decimal_factor(
                                            &sender_arc,
                                            &dc_read,
                                            mint_y,
                                        ) else {
                                            continue;
                                        };

                                        let amt_y = total_underlying_amounts[i_y] / dec_factor_y;

                                        match curve_type {
                                            Some(ref ct) => match ct {
                                                CurveType::ConstantProduct => {
                                                    let new_relation = LiqRelation::CpLp {
                                                        amt_origin: amt_x,
                                                        amt_dest: amt_y,
                                                    };

                                                    add_or_update_relation_edge(
                                                        un_x,
                                                        un_y,
                                                        &mut g_write,
                                                        |e| e.id == parent_mint,
                                                        new_relation,
                                                        &parent_mint,
                                                        time,
                                                    )
                                                    .await;
                                                }
                                                _ => {
                                                    // Unsupported CurveType
                                                    continue;
                                                }
                                            },
                                            None => {
                                                continue;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Dooot::OraclePriceEvent(oracle_price) => {
                            let feed_id = &oracle_price.feed_account_pubkey;
                            let Some(feed_mint) = oracle_feed_map.get(feed_id.as_str()).cloned()
                            else {
                                counter.fetch_sub(1, Ordering::Relaxed);
                                return;
                            };

                            let price = oracle_price.price;

                            log::info!("New oracle price for {feed_mint}: {price}");

                            // Quick lock to update the oracle cache
                            {
                                let mut oc_write = oracle_cache.write().await;
                                oc_write.insert(feed_mint.clone(), price);
                            }

                            let ix = {
                                let mint_indicies_read = mint_indicies.read().await;
                                mint_indicies_read.get(&feed_mint).cloned()
                            };
                            if let Some(ix) = ix {
                                // Update the price of the mint in the graph
                                {
                                    let g_read = graph.read().await;
                                    let node_weight = g_read.node_weight(ix).unwrap();
                                    let mut p_write = node_weight.usd_price.write().await;
                                    p_write.replace(USDPriceWithSource::Oracle(price));
                                }

                                calculator_sender
                                    .send(CalculatorUpdate::OracleUSDPrice(ix))
                                    .await
                                    .unwrap();
                            } else {
                                log::warn!(
                                    "Mint {} not in graph, cannot send OracleUSDPrice update",
                                    feed_mint
                                );
                            }
                        }
                        Dooot::MintInfo(info) => {
                            let MintInfoDooot { mint, decimals, .. } = info;

                            if let Some(decimals) = decimals {
                                let mint_str = mint.to_string();
                                let mut decimal_cache_write = decimal_cache.write().await;
                                let decimals = decimals as u8;
                                decimal_cache_write.insert(mint_str, decimals);
                            }
                        }
                        Dooot::LPInfo(info) => {
                            let LPInfoDooot {
                                lp_mint,
                                curve_type,
                                ..
                            } = info;

                            let Some(lp_mint) = lp_mint else {
                                counter.fetch_sub(1, Ordering::Relaxed);
                                return;
                            };

                            let l_read = lp_cache.read().await;
                            if l_read.contains_key(&lp_mint) {
                                counter.fetch_sub(1, Ordering::Relaxed);
                                return;
                            }

                            // LP doesn't exist, drop the read and grab a write lock,
                            // then insert the new LP
                            drop(l_read);
                            let mut l_write = lp_cache.write().await;
                            l_write.insert(lp_mint, LiquidityPool { curve_type });
                        }
                        _ => {
                            counter.fetch_sub(1, Ordering::Relaxed);
                            return;
                        }
                    }

                    log::debug!("PPL task finished in {:?}", now.elapsed());

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
fn get_or_dispatch_decimals(
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

/// Returns the decimal factor for a mint, or None if the decimal factor is not in the cache
///
/// If the decimal factor is not in the cache, it will dispatch a request to get the decimal factor
#[inline]
fn get_or_dispatch_decimal_factor(
    sender_arc: &Sender<String>,
    dc_read: &HashMap<String, u8>,
    mint_x: &str,
) -> Option<Decimal> {
    let dec = get_or_dispatch_decimals(sender_arc, dc_read, mint_x)?;

    let dec_factor = Decimal::from(10).checked_powi(dec as i64);

    if dec_factor.is_none() {
        log::warn!("Decimal overflow when trying to get decimals for {mint_x}: Decimals {dec}");
    }

    dec_factor
}

#[derive(Deserialize, Row)]
pub struct DecimalResult {
    decimals: Option<u8>,
}

#[inline]
pub async fn query_decimals(
    clickhouse_client: &clickhouse::Client,
    mint: &str,
) -> Result<Option<u8>> {
    let query = clickhouse_client
        .query(
            "
                SELECT
                    anyLastMerge(decimals) as decimals
                FROM lookup_mint_info
                WHERE mint = base58Decode(?)
                GROUP BY mint
            ",
        )
        .bind(mint);

    match query.fetch_one::<DecimalResult>().await {
        Ok(v) => match v.decimals {
            Some(d) => Ok(Some(d)),
            None => Ok(None),
        },
        Err(e) => match e {
            clickhouse::error::Error::RowNotFound => Ok(None),
            _ => Err(e.into()),
        },
    }
}

#[inline]
pub fn get_or_add_mint_ix(
    mint: &str,
    graph: &mut MintPricingGraph,
    mint_indicies: &mut MintIndiciesMap,
) -> NodeIndex {
    let ix = match mint_indicies.get(mint).cloned() {
        Some(ix) => ix,
        None => {
            let ix = graph.add_node(MintNode {
                mint: mint.to_string(),
                usd_price: RwLock::new(None),
            });

            mint_indicies.insert(mint.to_string(), ix);

            ix
        }
    };

    ix
}

#[inline]
pub fn get_edge_by_predicate<P>(
    ix_a: NodeIndex,
    ix_b: NodeIndex,
    graph: &MintPricingGraph,
    edge_predicate: P,
) -> Option<EdgeIndex>
where
    P: Fn(&MintEdge) -> bool,
{
    for edge in graph.edges_connecting(ix_a, ix_b) {
        let e = edge.weight();
        if edge_predicate(e) {
            return Some(edge.id());
        }
    }

    None
}

/// Returns the edge idx that was updated
#[inline]
#[allow(clippy::unwrap_used)]
pub async fn add_or_update_relation_edge<P>(
    ix_a: NodeIndex,
    ix_b: NodeIndex,
    graph: &mut MintPricingGraph,
    get_predicate: P,
    update_with: LiqRelation,
    discriminant_id: &str,
    time: NaiveDateTime,
) -> EdgeIndex
where
    P: Fn(&MintEdge) -> bool,
{
    let edge = get_edge_by_predicate(ix_a, ix_b, graph, get_predicate);

    log::debug!("Adding or updating relation edge for {discriminant_id}: {update_with:?}");

    match edge {
        Some(edge_ix) => {
            // Edge already exists, update it
            // Guaranteed by being Some
            let e_w = graph.edge_weight_mut(edge_ix).unwrap();
            e_w.dirty = true;

            // Quick update of the last updated time
            {
                let mut last_updated = e_w.last_updated.write().await;
                *last_updated = time;
            }

            // Quick update of the relation
            {
                let mut relation = e_w.inner_relation.write().await;
                *relation = update_with;
            }

            edge_ix
        }
        None => {
            let new_edge = MintEdge {
                id: discriminant_id.to_string(),
                dirty: true,
                last_updated: RwLock::new(time),
                inner_relation: RwLock::new(update_with),
            };

            graph.add_edge(ix_a, ix_b, new_edge)
        }
    }
}
