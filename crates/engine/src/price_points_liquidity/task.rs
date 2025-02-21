use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use chrono::NaiveDateTime;
use petgraph::{
    graph::{EdgeIndex, NodeIndex},
    visit::EdgeRef,
};
use rust_decimal::{prelude::FromPrimitive, Decimal, MathematicalOps};
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
        graph::{MintEdge, MintNode, MintPricingGraph, WrappedMintPricingGraph},
        structs::LiqRelationEnum,
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
    clickhouse_client: Arc<clickhouse::Client>,
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

                        // None if theres no LP associated with this
                        let curve_type = {
                            let lpc_read = lp_cache.read().await;
                            lpc_read
                                .get(&discriminant_id)
                                .map(|lp| lp.curve_type.clone())
                        };

                        let mut g_write = graph.write().await;

                        let parent_ix =
                            get_or_add_mint_ix(&mint_pubkey, &mut g_write, &mut mint_indicies);

                        // Ordered with `mints`
                        let mut underlying_idxs = Vec::with_capacity(mints.len());

                        for mint in mints.iter() {
                            let mint_ix =
                                get_or_add_mint_ix(mint, &mut g_write, &mut mint_indicies);

                            underlying_idxs.push(mint_ix);
                        }

                        // Add a Fixed relation to the parent if theres only one mint
                        if mints.len() == 1 {
                            let amt_per_parent = mints_qty_per_one_parent[0];
                            if let Some(amt_per_parent) = Decimal::from_f64(amt_per_parent) {
                                let relation = LiqRelationEnum::Fixed { amt_per_parent };

                                add_or_update_relation_edge(
                                    underlying_idxs[0],
                                    parent_ix,
                                    &mut g_write,
                                    |e| e.id == discriminant_id,
                                    relation,
                                    &discriminant_id,
                                    time,
                                )
                                .await;

                                let update = CalculatorUpdate::NewTokenRatio(parent_ix);
                                calculator_sender.send(update).await.unwrap();
                            } else {
                                log::error!("Could not parse amt_per_parent into a Decimal: {amt_per_parent}");
                            }

                            continue;
                        }

                        let dc_read = decimal_cache.read().await;

                        // Create edges for all underlying mints (likely an LP)
                        for (i_x, un_x) in underlying_idxs.iter().cloned().enumerate() {
                            let mint_x = &mints[i_x];
                            let decimals_x = {
                                match dc_read.get(mint_x) {
                                    Some(&d) => d,
                                    None => {
                                        match query_decimals(clickhouse_client.clone(), mint_x)
                                            .await
                                        {
                                            Ok(Some(d)) => d,
                                            Ok(None) => continue,
                                            Err(e) => {
                                                log::error!("{e}");
                                                continue;
                                            }
                                        }
                                    }
                                }
                            };
                            let amt_x = total_underlying_amounts[i_x]
                                / Decimal::from(10).powi(decimals_x as i64);

                            for (i_y, un_y) in underlying_idxs.iter().cloned().enumerate() {
                                if un_x == un_y {
                                    continue;
                                }

                                let mint_y = &mints[i_y];
                                let decimals_y = {
                                    match dc_read.get(mint_y) {
                                        Some(&d) => d,
                                        None => {
                                            match query_decimals(clickhouse_client.clone(), mint_y)
                                                .await
                                            {
                                                Ok(Some(d)) => d,
                                                Ok(None) => continue,
                                                Err(e) => {
                                                    log::error!("{e}");
                                                    continue;
                                                }
                                            }
                                        }
                                    }
                                };

                                let amt_y = total_underlying_amounts[i_y]
                                    / Decimal::from(10).powi(decimals_y as i64);

                                match curve_type {
                                    Some(ref ct) => match ct {
                                        CurveType::ConstantProduct => {
                                            let new_relation = LiqRelationEnum::CpLp {
                                                amt_origin: amt_x,
                                                amt_dest: amt_y,
                                            };

                                            add_or_update_relation_edge(
                                                un_x,
                                                un_y,
                                                &mut g_write,
                                                |e| e.id == discriminant_id,
                                                new_relation,
                                                &discriminant_id,
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
                    Dooot::OraclePriceEvent(oracle_price) => {
                        let feed_id = &oracle_price.feed_account_pubkey;
                        let Some(feed_mint) = oracle_feed_map.get(feed_id.as_str()).cloned() else {
                            continue;
                        };

                        let price = oracle_price.price;

                        // Quick lock to update the oracle cache
                        {
                            let mut oc_write = oracle_cache.write().await;
                            oc_write.insert(feed_mint.clone(), price);
                        }

                        let ix = mint_indicies.get(&feed_mint).cloned();
                        if let Some(ix) = ix {
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
                            continue;
                        };

                        let l_read = lp_cache.read().await;
                        if l_read.contains_key(&lp_mint) {
                            continue;
                        }
                        drop(l_read);

                        let mut l_write = lp_cache.write().await;
                        l_write.insert(lp_mint, LiquidityPool { curve_type });
                    }
                    _ => {
                        continue;
                    }
                }

                // Quick debug dump of the graph
                #[cfg(feature = "debug-graph")]
                {
                    use veritas_sdk::ppl_graph::graph::EDGE_SIZE;
                    use veritas_sdk::ppl_graph::graph::NODE_SIZE;
                    // use petgraph::dot::Dot;
                    // use std::fs;

                    let g_read = graph.read().await;

                    // let formatted_dot_str = format!("{:?}", Dot::new(&(*g_read)));
                    // fs::write("./graph.dot", formatted_dot_str).unwrap();

                    // Quick debug dump of the graph size
                    let num_nodes = g_read.node_count();
                    let num_edges = g_read.edge_count();
                    let node_bytes = num_nodes * NODE_SIZE;
                    let edge_bytes = num_edges * EDGE_SIZE;
                    // in KB
                    log::info!("Num nodes: {num_nodes}, num edges: {num_edges}");
                    log::info!("Node bytes: {node_bytes}, edge bytes: {edge_bytes}");
                    log::info!(
                        "Total bytes: {:.2}K",
                        (node_bytes + edge_bytes) as f64 / 1024.0
                    );
                }
            }

            log::warn!("Price points liquidity task (PPL) shutting down. Channel closed.");
        },
    );

    Ok(task)
}

pub async fn query_decimals(
    clickhouse_client: Arc<clickhouse::Client>,
    mint: &str,
) -> Result<Option<u8>> {
    let query = clickhouse_client
        .query(
            "
                SELECT
                    base58Encode(reinterpretAsString(mint)) as mint_pubkey,
                    anyLastMerge(decimals) as decimals
                FROM lookup_mint_info
                WHERE mint_pubkey = ?
                GROUP BY mint
            ",
        )
        .bind(mint);

    match query.fetch_one::<i16>().await {
        Ok(v) => Ok(Some(v as u8)),
        Err(e) => match e {
            clickhouse::error::Error::RowNotFound => Ok(None),
            _ => Err(e.into()),
        },
    }
}

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

#[allow(clippy::unwrap_used)]
/// Returns the edge idx that was updated
pub async fn add_or_update_relation_edge<P>(
    ix_a: NodeIndex,
    ix_b: NodeIndex,
    graph: &mut MintPricingGraph,
    get_predicate: P,
    update_with: LiqRelationEnum,
    discriminant_id: &str,
    time: NaiveDateTime,
) -> EdgeIndex
where
    P: Fn(&MintEdge) -> bool,
{
    let edge = get_edge_by_predicate(ix_a, ix_b, graph, get_predicate);

    match edge {
        Some(edge_ix) => {
            // Edge already exists, update it
            // Guaranteed by above get_edge_by_predicate
            let e_w = graph.edge_weight_mut(edge_ix).unwrap();
            e_w.dirty = true;

            {
                // Quick update of the last updated time
                let mut last_updated = e_w.last_updated.write().await;
                *last_updated = time;
            }

            // Need to update the edge with the new amount
            let mut relation = e_w.inner_relation.write().await;
            *relation = update_with;

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
