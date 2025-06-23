use std::sync::Arc;

use rust_decimal::{prelude::FromPrimitive, Decimal, MathematicalOps};
use step_ingestooor_sdk::dooot::{CurveType, MintUnderlyingsGlobalDooot};
use tokio::sync::{mpsc::Sender, RwLock};
use veritas_sdk::{
    liq_relation::LiqRelation,
    ppl_graph::graph::MintPricingGraph,
    utils::{decimal_cache::DecimalCache, lp_cache::LpCache},
};

use crate::price_points_liquidity::{
    handlers::mint_underlyings::handle_specials::handle_special_mint_underlyings,
    task::{
        add_or_update_relation_edge, get_edge_by_discriminant, get_or_add_mint_ix,
        get_or_dispatch_decimals, EdgeIndiciesMap, MintIndiciesMap,
    },
};

mod handle_carrot_dooot;
mod handle_specials;

#[allow(clippy::unwrap_used)]
pub async fn handle_mint_underlyings(
    mu_dooot: MintUnderlyingsGlobalDooot,
    lp_cache: Arc<RwLock<LpCache>>,
    graph: Arc<RwLock<MintPricingGraph>>,
    decimal_cache: Arc<RwLock<DecimalCache>>,
    cache_updator_sender: Sender<String>,
    mint_indicies: Arc<RwLock<MintIndiciesMap>>,
    edge_indicies: Arc<RwLock<EdgeIndiciesMap>>,
) {
    let MintUnderlyingsGlobalDooot {
        time,
        mint_pubkey: parent_mint,
        mints,
        ..
    } = &mu_dooot;

    let is_pool = match mints.len() {
        1 => {
            // Fixed
            false
        }
        2 => {
            // LP
            true
        }
        _ => {
            handle_special_mint_underlyings(&mu_dooot, graph, mint_indicies, decimal_cache).await;
            return;
        }
    };

    let (mint_a, mint_b) = if is_pool {
        (&mints[0], &mints[1])
    } else {
        (&mints[0], parent_mint)
    };

    let (decimals_a, decimals_b) = {
        log::trace!("Getting decimal cache read lock");
        let dc_read = decimal_cache.read().await;
        log::trace!("Got decimal cache read lock");

        let Some(decimals_a) = get_or_dispatch_decimals(&cache_updator_sender, &dc_read, mint_a)
        else {
            return;
        };
        let Some(decimals_b) = get_or_dispatch_decimals(&cache_updator_sender, &dc_read, mint_b)
        else {
            return;
        };

        (decimals_a, decimals_b)
    };

    let (mut mint_a_ix, mut mint_b_ix) = {
        log::trace!("Getting mint indicies read lock");
        let mi_read = mint_indicies.read().await;
        log::trace!("Got mint indicies read lock");
        let mint_a_ix = mi_read.get(mint_a).cloned();
        let mint_b_ix = mi_read.get(mint_b).cloned();

        (mint_a_ix, mint_b_ix)
    };

    // Do we need to add these to graph?
    let add_mint_a = mint_a_ix.is_none();
    let add_mint_b = mint_b_ix.is_none();

    if add_mint_a || add_mint_b {
        let mut g_write = graph.write().await;
        {
            log::trace!("Getting mint indicies write lock");
            let mut mi_write = mint_indicies.write().await;
            log::trace!("Got mint indicies write lock");

            if add_mint_a {
                mint_a_ix = get_or_add_mint_ix(mint_a, &mut g_write, &mut mi_write).into();
            }

            if add_mint_b {
                mint_b_ix = get_or_add_mint_ix(mint_b, &mut g_write, &mut mi_write).into();
            }
        }

        let (Some(mint_a_ix), Some(mint_b_ix)) = (mint_a_ix, mint_b_ix) else {
            log::error!("UNREACHABLE - Both indicies should have been set just now");
            return;
        };

        let Some(new_relation) = build_mu_relation(
            &mu_dooot,
            lp_cache.clone(),
            parent_mint,
            mint_a,
            mint_b,
            decimals_a,
            decimals_b,
            is_pool,
            false,
        )
        .await
        else {
            // log::error!("Could not build relation for {mu_dooot:?}");
            return;
        };

        let mut ei_write = edge_indicies.write().await;

        match add_or_update_relation_edge(
            mint_a_ix,
            mint_b_ix,
            &mut ei_write,
            &mut g_write,
            new_relation,
            parent_mint,
            *time,
        )
        .await
        {
            Ok(_) => {}
            Err(e) => {
                log::error!("Error adding new relation edge for {mint_a} -> {mint_b}: {e}");
            }
        };

        if is_pool {
            let Some(new_relation_opp) = build_mu_relation(
                &mu_dooot,
                lp_cache,
                parent_mint,
                mint_a,
                mint_b,
                decimals_a,
                decimals_b,
                is_pool,
                true,
            )
            .await
            else {
                // log::error!("Could not build relation for {mu_dooot:?}");
                return;
            };

            match add_or_update_relation_edge(
                mint_b_ix,
                mint_a_ix,
                &mut ei_write,
                &mut g_write,
                new_relation_opp,
                parent_mint,
                *time,
            )
            .await
            {
                Ok(_) => {}
                Err(e) => {
                    log::error!("Error adding new relation edge for {mint_b} -> {mint_a}: {e}");
                }
            };
        }
    } else {
        // Exists, but need to update/create relation
        let (Some(ix_a), Some(ix_b)) = (mint_a_ix, mint_b_ix) else {
            log::error!("UNREACHABLE - Both indicies should have been set {mu_dooot:?}");
            return;
        };

        let g_read = graph.read().await;
        let ei_read = edge_indicies.read().await;

        let edge_ix = get_edge_by_discriminant(ix_a, ix_b, &g_read, &ei_read, parent_mint);

        match edge_ix {
            Some(edge_ix) => {
                // Just need to update them.
                let Some(edge) = g_read.edge_weight(edge_ix) else {
                    log::error!("UNREACHABLE - Edge should exist for {mu_dooot:?}");
                    return;
                };

                let mut e_write = edge.inner_relation.write().await;

                if is_pool {
                    let Some(edge_ix_opposite) =
                        get_edge_by_discriminant(ix_b, ix_a, &g_read, &ei_read, parent_mint)
                    else {
                        log::error!("UNREACHABLE - Opp edge should exist for {mu_dooot:?}");
                        return;
                    };
                    let Some(edge_opp) = g_read.edge_weight(edge_ix_opposite) else {
                        log::error!("UNREACHABLE - Edge should exist for {mu_dooot:?}");
                        return;
                    };
                    let mut e_opp_write = edge_opp.inner_relation.write().await;
                    let LiqRelation::CpLp {
                        amt_origin: ref mut amt_origin_dooot,
                        amt_dest: ref mut amt_dest_dooot,
                        ..
                    } = *e_write
                    else {
                        log::error!("UNREACHABLE - Edge should be a CP LP for {mu_dooot:?}");
                        return;
                    };

                    let LiqRelation::CpLp {
                        amt_origin: ref mut amt_origin_dooot_opp,
                        amt_dest: ref mut amt_dest_dooot_opp,
                        ..
                    } = *e_opp_write
                    else {
                        log::error!("UNREACHABLE - Edge should be a CP LP for {mu_dooot:?}");
                        return;
                    };

                    let Some(relation) = build_mu_relation(
                        &mu_dooot,
                        lp_cache,
                        parent_mint,
                        mint_a,
                        mint_b,
                        decimals_a,
                        decimals_b,
                        is_pool,
                        false,
                    )
                    .await
                    else {
                        // log::error!("Could not build relation for {mu_dooot:?}");
                        return;
                    };

                    let (amt_origin, amt_dest) = match relation {
                        LiqRelation::CpLp {
                            amt_origin,
                            amt_dest,
                            ..
                        } => (amt_origin, amt_dest),
                        _ => {
                            log::error!("UNREACHABLE - Edge should be a CP LP for {mu_dooot:?}");
                            return;
                        }
                    };

                    *amt_origin_dooot = amt_origin;
                    *amt_dest_dooot = amt_dest;

                    *amt_origin_dooot_opp = amt_dest;
                    *amt_dest_dooot_opp = amt_origin;
                } else {
                    let LiqRelation::Fixed {
                        amt_per_parent: ref mut amt_per_parent_dooot,
                    } = *e_write
                    else {
                        log::error!("UNREACHABLE - Edge should be a Fixed for {mu_dooot:?}");
                        return;
                    };

                    let Some(relation) = build_mu_relation(
                        &mu_dooot,
                        lp_cache,
                        parent_mint,
                        mint_a,
                        mint_b,
                        decimals_a,
                        decimals_b,
                        is_pool,
                        false,
                    )
                    .await
                    else {
                        // log::error!("Could not build relation for {mu_dooot:?}");
                        return;
                    };

                    let amt_per_parent = match relation {
                        LiqRelation::Fixed { amt_per_parent } => amt_per_parent,
                        _ => {
                            log::error!("UNREACHABLE - Edge should be a Fixed for {mu_dooot:?}");
                            return;
                        }
                    };

                    *amt_per_parent_dooot = amt_per_parent;
                }
            }
            None => {
                drop(g_read);
                drop(ei_read);
                // Need to create new ones

                let Some(new_relation) = build_mu_relation(
                    &mu_dooot,
                    lp_cache.clone(),
                    parent_mint,
                    mint_a,
                    mint_b,
                    decimals_a,
                    decimals_b,
                    is_pool,
                    false,
                )
                .await
                else {
                    // log::error!("Could not build relation for {mu_dooot:?}");
                    return;
                };

                let mut g_write = graph.write().await;
                let mut ei_write = edge_indicies.write().await;

                match add_or_update_relation_edge(
                    ix_a,
                    ix_b,
                    &mut ei_write,
                    &mut g_write,
                    new_relation,
                    parent_mint,
                    *time,
                )
                .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        log::error!(
                            "Error adding new relation edge for {mint_a} and {mint_b}: {e}"
                        );
                    }
                };

                if is_pool {
                    let Some(new_relation_opp) = build_mu_relation(
                        &mu_dooot,
                        lp_cache,
                        parent_mint,
                        mint_a,
                        mint_b,
                        decimals_a,
                        decimals_b,
                        is_pool,
                        true,
                    )
                    .await
                    else {
                        // log::error!("Could not build relation for {mu_dooot:?}");
                        return;
                    };
                    match add_or_update_relation_edge(
                        ix_b,
                        ix_a,
                        &mut ei_write,
                        &mut g_write,
                        new_relation_opp,
                        parent_mint,
                        *time,
                    )
                    .await
                    {
                        Ok(_) => {}
                        Err(e) => {
                            log::error!(
                                "Error adding new relation edge for {mint_a} and {mint_b}: {e}"
                            );
                        }
                    };
                }
            }
        }
    }

    // drop(ei_write);
    // drop(g_write);
    // drop(dc_read);

    // for (ix, edge) in updated_ixs {
    //     let update = CalculatorUpdate::NewTokenRatio(ix, edge);
    //     log::trace!("Sending NewTokenRatio update for {ix:?}");
    //     send_update_to_calculator(update, &calculator_sender, &bootstrap_in_progress).await;
    //     log::trace!("Sent NewTokenRatio update for {ix:?}");
    // }
}

async fn build_mu_relation(
    mu_dooot: &MintUnderlyingsGlobalDooot,
    lp_cache: Arc<RwLock<LpCache>>,
    discriminant_id: &str,
    mint_a: &str,
    mint_b: &str,
    decimals_a: u8,
    decimals_b: u8,
    is_pool: bool,
    is_reverse: bool,
) -> Option<LiqRelation> {
    if is_pool {
        let curve_type = {
            let lp = lp_cache.read().await.get(discriminant_id).cloned();
            let Some(lp) = lp else {
                // log::warn!("LP mint missing from cache: {mu_dooot:?}");
                return None;
            };

            lp.curve_type
        };

        match curve_type {
            CurveType::ConstantProduct => {
                let Some(amt_a) = mu_dooot.total_underlying_amounts.first() else {
                    log::error!("MALFORMED CPLP DOOOT: {mu_dooot:?}");
                    return None;
                };
                let Some(amt_b) = mu_dooot.total_underlying_amounts.get(1) else {
                    log::error!("MALFORMED CPLP DOOOT: {mu_dooot:?}");
                    return None;
                };
                let Some(a_dec_factor) = Decimal::TEN.checked_powi(decimals_a as i64) else {
                    log::error!("Math overflowed for CPLP {mint_a} ({decimals_a}) {mu_dooot:?}");
                    return None;
                };
                let Some(b_dec_factor) = Decimal::TEN.checked_powi(decimals_b as i64) else {
                    log::error!("Math overflowed for CPLP {mint_b} ({decimals_b}) {mu_dooot:?}");
                    return None;
                };
                let amt_a_units = amt_a.checked_div(a_dec_factor)?;
                let amt_b_units = amt_b.checked_div(b_dec_factor)?;
                let relation = if !is_reverse {
                    LiqRelation::CpLp {
                        amt_origin: amt_a_units,
                        amt_dest: amt_b_units,
                        pool_id: discriminant_id.to_string(),
                    }
                } else {
                    LiqRelation::CpLp {
                        amt_origin: amt_b_units,
                        amt_dest: amt_a_units,
                        pool_id: discriminant_id.to_string(),
                    }
                };
                Some(relation)
            }
            _ => None,
        }
    } else {
        let Some(ratio) = Decimal::from_f64(mu_dooot.mints_qty_per_one_parent[0]) else {
            log::error!("Could not convert mints_qty_per_one_parent[0] to Decimal: {mu_dooot:?}");
            return None;
        };
        let Some(decimal_factor) = Decimal::TEN.checked_powi(decimals_b as i64 - decimals_a as i64)
        else {
            log::error!("Math overflowed for Fixed {mint_b} ({decimals_b}) {mu_dooot:?}");
            return None;
        };

        let Some(amt_per_parent) = ratio.checked_mul(decimal_factor) else {
            log::error!("Math overflowed for Fixed {mint_b} ({decimals_b}) {mu_dooot:?}");
            return None;
        };

        let relation = LiqRelation::Fixed { amt_per_parent };
        Some(relation)
    }
}
