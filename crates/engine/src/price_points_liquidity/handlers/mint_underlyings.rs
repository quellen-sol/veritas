use std::{collections::HashSet, sync::Arc};

use rust_decimal::{prelude::FromPrimitive, Decimal, MathematicalOps};
use step_ingestooor_sdk::dooot::{CurveType, MintUnderlyingsGlobalDooot};
use tokio::sync::{mpsc::Sender, RwLock};
use veritas_sdk::{
    ppl_graph::{graph::MintPricingGraph, structs::LiqRelation},
    utils::{decimal_cache::DecimalCache, lp_cache::LpCache},
};

use crate::{
    calculator::task::CalculatorUpdate,
    price_points_liquidity::task::{
        add_or_update_relation_edge, get_or_add_mint_ix, get_or_dispatch_decimal_factor,
        get_or_dispatch_decimals, MintIndiciesMap,
    },
};

#[allow(clippy::unwrap_used)]
pub async fn handle_mint_underlyings(
    mu_dooot: MintUnderlyingsGlobalDooot,
    lp_cache: Arc<RwLock<LpCache>>,
    graph: Arc<RwLock<MintPricingGraph>>,
    decimal_cache: Arc<RwLock<DecimalCache>>,
    sender_arc: Arc<Sender<String>>,
    calculator_sender: Arc<Sender<CalculatorUpdate>>,
    mint_indicies: Arc<RwLock<MintIndiciesMap>>,
) {
    let MintUnderlyingsGlobalDooot {
        time,
        mint_pubkey: parent_mint,
        mints,
        total_underlying_amounts,
        mints_qty_per_one_parent,
        ..
    } = mu_dooot;

    log::trace!("Getting graph write lock");
    let mut g_write = graph.write().await;
    log::trace!("Got graph write lock");
    log::trace!("Getting mint indicies write lock");
    let mut mint_indicies = mint_indicies.write().await;
    log::trace!("Got mint indicies write lock");

    // Ordered with `mints`
    let mut underlying_idxs = Vec::with_capacity(mints.len());

    for mint in mints.iter() {
        let mint_ix = get_or_add_mint_ix(mint, &mut g_write, &mut mint_indicies);

        underlying_idxs.push(mint_ix);
    }

    log::trace!("Getting decimal cache read lock");
    let dc_read = decimal_cache.read().await;
    log::trace!("Got decimal cache read lock");

    // Add a Fixed relation to the parent if theres only one mint
    if mints.len() == 1 {
        let amt_per_parent = mints_qty_per_one_parent[0];
        let Some(amt_per_parent) = Decimal::from_f64(amt_per_parent) else {
            log::error!("Could not parse amt_per_parent into a Decimal: {amt_per_parent}");
            return;
        };

        let parent_ix = get_or_add_mint_ix(&parent_mint, &mut g_write, &mut mint_indicies);
        drop(mint_indicies);

        let Some(dec_parent) = get_or_dispatch_decimals(&sender_arc, &dc_read, &parent_mint) else {
            return;
        };

        let this_mint = &mints[0];

        let Some(dec_this) = get_or_dispatch_decimals(&sender_arc, &dc_read, this_mint) else {
            return;
        };

        let exp = (dec_parent as i64) - (dec_this as i64);
        let Some(dec_factor) = Decimal::from(10).checked_powi(exp) else {
            log::warn!("Decimal overflow when trying to get decimal factor for {parent_mint} ({dec_parent}) and {this_mint} ({dec_this})");
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
        log::trace!("Sending NewTokenRatio update for {parent_mint}");
        calculator_sender.send(update).await.unwrap();
        log::trace!("Sent NewTokenRatio update for {parent_mint}");
    } else {
        // No longer needed
        drop(mint_indicies);

        // None if theres no LP associated with this
        let curve_type = {
            log::trace!("Getting LP cache read lock");
            let lpc_read = lp_cache.read().await;
            log::trace!("Got LP cache read lock");
            lpc_read.get(&parent_mint).map(|lp| lp.curve_type.clone())
        };

        let Some(ref curve_type) = curve_type else {
            // No LP, so we can't create an edge
            return;
        };

        let mut updated_ixs = HashSet::new();

        // Create edges for all underlying mints (likely an LP)
        for (i_x, un_x) in underlying_idxs.iter().cloned().enumerate() {
            let mint_x = &mints[i_x];

            let Some(dec_factor_x) = get_or_dispatch_decimal_factor(&sender_arc, &dc_read, mint_x)
            else {
                continue;
            };

            let amt_x = total_underlying_amounts[i_x] / dec_factor_x;

            for (i_y, un_y) in underlying_idxs.iter().cloned().enumerate() {
                if un_x == un_y {
                    continue;
                }

                let mint_y = &mints[i_y];

                let Some(dec_factor_y) =
                    get_or_dispatch_decimal_factor(&sender_arc, &dc_read, mint_y)
                else {
                    continue;
                };

                let amt_y = total_underlying_amounts[i_y] / dec_factor_y;

                match curve_type {
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

                        updated_ixs.insert(un_y);
                    }
                    _ => {
                        // Unsupported CurveType
                    }
                }
            }
        }

        for ix in updated_ixs {
            let update = CalculatorUpdate::NewTokenRatio(ix);
            log::trace!("Sending NewTokenRatio update for {ix:?}");
            calculator_sender.send(update).await.unwrap();
            log::trace!("Sent NewTokenRatio update for {ix:?}");
        }
    }
}
