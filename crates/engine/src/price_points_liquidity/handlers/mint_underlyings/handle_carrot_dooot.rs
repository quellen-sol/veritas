use std::{collections::HashMap, sync::Arc};

use rust_decimal::{prelude::FromPrimitive, Decimal};
use step_ingestooor_sdk::dooot::MintUnderlyingsGlobalDooot;
use tokio::sync::RwLock;
use veritas_sdk::{
    liq_relation::{IndexPart, LiqRelation},
    ppl_graph::graph::MintNode,
    types::{MintIndiciesMap, WrappedMintPricingGraph},
    utils::decimal_cache::DecimalCache,
};

pub async fn handle_carrot_dooot(
    dooot: &MintUnderlyingsGlobalDooot,
    graph: WrappedMintPricingGraph,
    mint_indicies: Arc<RwLock<MintIndiciesMap>>,
    decimal_cache: Arc<RwLock<DecimalCache>>,
) {
    // CRTx1JouZhzSU6XytsE42UQraoGqiHgxabocVfARTy2s
    let pk = &dooot.mint_pubkey;

    let (crt_node_ix, mut mints_ixs) = {
        let mi_read = mint_indicies.read().await;
        let crt_ix = mi_read.get(pk).cloned();
        let mints_ixs = dooot
            .mints
            .iter()
            .map(|m| mi_read.get(m).cloned())
            .collect::<Vec<_>>();

        (crt_ix, mints_ixs)
    };

    // Update block so that ixs for mints are added to the graph
    for (i, mint_ix) in mints_ixs.iter_mut().enumerate() {
        if mint_ix.is_some() {
            continue;
        }

        let mint_str = dooot.mints[i].clone();

        let new_ix = {
            let mut g_write = graph.write().await;

            let new_node = MintNode {
                mint: mint_str.clone(),
                non_vertex_relations: RwLock::new(HashMap::new()),
                usd_price: RwLock::new(None),
            };

            g_write.add_node(new_node)
        };

        mint_ix.replace(new_ix);

        let mut mi_write = mint_indicies.write().await;
        mi_write.insert(mint_str, new_ix);
    }

    let build_parts = async || {
        let dc_read = decimal_cache.read().await;
        let mut new_parts = Vec::with_capacity(dooot.mints.capacity());
        for (i, mint_ix) in mints_ixs.clone().into_iter().enumerate() {
            let Some(ix) = mint_ix else {
                log::error!("UNREACHABLE - Mint indicies should be set above");
                return None;
            };

            let mint = &dooot.mints[i];
            let decimals = dc_read.get(mint)?;

            let amt_per_parent = dooot.mints_qty_per_one_parent[i];
            let Some(ratio) = Decimal::from_f64(amt_per_parent) else {
                log::error!("Could not parse f64 into decimal: {amt_per_parent}");
                return None;
            };

            let part = IndexPart {
                decimals: *decimals,
                mint: mint.clone(),
                node_idx: ix.index() as u32,
                ratio,
            };

            new_parts.push(part);
        }

        Some(new_parts)
    };

    let build_relation = async || {
        let parts = build_parts().await?;

        let decimals_crt = {
            let dc_read = decimal_cache.read().await;
            let d = dc_read.get(pk)?;

            *d
        };

        let new_relation = LiqRelation::IndexLike {
            market_id: pk.clone(),
            decimals_parent: decimals_crt,
            parts,
        };

        Some(new_relation)
    };

    match crt_node_ix {
        Some(ix) => {
            let g_read = graph.read().await;
            let Some(node_weight) = g_read.node_weight(ix) else {
                log::error!("UNREACHABLE - Node should exist");
                return;
            };

            let crt_relations_read = node_weight.non_vertex_relations.read().await;
            let crt_market = crt_relations_read.get(pk);

            match crt_market {
                Some(market) => {
                    // Update relation
                    let mut m_write = market.write().await;
                    match *m_write {
                        LiqRelation::IndexLike { ref mut parts, .. } => {
                            let Some(new_parts) = build_parts().await else {
                                return;
                            };
                            *parts = new_parts;
                        }
                        _ => {
                            log::error!("UNREACHABLE - I mean seriously... How did we even get this far? CRT Relation should be IndexLike, not anything else");
                        }
                    }
                }
                None => {
                    // Add new relation
                    let Some(relation) = build_relation().await else {
                        return;
                    };

                    drop(crt_relations_read);
                    let mut map_write = node_weight.non_vertex_relations.write().await;

                    map_write.insert(pk.clone(), RwLock::new(relation));
                }
            }
        }
        None => {
            // Add new node and relation
            let Some(relation) = build_relation().await else {
                return;
            };

            let mut relation_map = HashMap::new();
            relation_map.insert(pk.clone(), RwLock::new(relation));

            let new_node = MintNode {
                mint: pk.clone(),
                usd_price: RwLock::new(None),
                non_vertex_relations: RwLock::new(relation_map),
            };

            let new_ix = {
                let mut g_write = graph.write().await;
                g_write.add_node(new_node)
            };

            let mut mi_write = mint_indicies.write().await;
            mi_write.insert(pk.clone(), new_ix);
        }
    }
}
