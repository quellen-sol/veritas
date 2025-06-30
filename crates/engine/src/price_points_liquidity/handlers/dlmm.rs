use std::{
    collections::HashMap,
    sync::{atomic::AtomicBool, Arc},
};

use rust_decimal::{Decimal, MathematicalOps};
use step_ingestooor_sdk::dooot::{DlmmGlobalDooot, LPInfoUnderlyingMintVault};
use tokio::{
    sync::{mpsc::Sender, RwLock},
    time::Instant,
};
use veritas_sdk::{
    liq_relation::{relations::dlmm::DlmmBinParsed, LiqRelation},
    types::{EdgeIndiciesMap, MintIndiciesMap, WrappedMintPricingGraph},
    utils::{
        decimal_cache::DecimalCache, lp_cache::LpCache, token_balance_cache::TokenBalanceCache,
    },
};

use crate::{
    calculator::task::CalculatorUpdate,
    price_points_liquidity::task::{
        add_or_update_two_way_relation_edge, get_or_add_mint_ix, get_or_dispatch_decimals,
        get_two_way_edges_by_discriminant,
    },
};

/// DLMMs are always in the form of X/Y where X is "base" and Y is "quote"
///
/// The edge from Y -> X is NOT the `reverse` relation, since Y is expected to price X in most cases.
#[allow(clippy::unwrap_used)]
pub async fn handle_dlmm(
    dooot: DlmmGlobalDooot,
    graph: WrappedMintPricingGraph,
    lp_cache: Arc<RwLock<LpCache>>,
    decimal_cache: Arc<RwLock<DecimalCache>>,
    mint_indicies: Arc<RwLock<MintIndiciesMap>>,
    edge_indicies: Arc<RwLock<EdgeIndiciesMap>>,
    sender_arc: Sender<String>,
    token_balance_cache: Arc<RwLock<TokenBalanceCache>>,
    _calculator_sender: Sender<CalculatorUpdate>,
    _bootstrap_in_progress: Arc<AtomicBool>,
) {
    let now = Instant::now();
    let DlmmGlobalDooot {
        time,
        parts,
        pool_pubkey,
        part_index,
        ..
    } = &dooot;

    let pool_info = {
        log::trace!("Getting lp cache read lock");
        let lc_read = lp_cache.read().await;
        log::trace!("Got lp cache read lock");
        let Some(lp) = lc_read.get(pool_pubkey).cloned() else {
            // log::warn!("LP NOT FOUND IN CACHE: {pool_pubkey}");
            return;
        };

        lp
    };

    let Some(underlyings_x) = pool_info.underlyings.first() else {
        log::error!("MALFORMED DLMM: {pool_info:?}");
        return;
    };
    let Some(underlyings_y) = pool_info.underlyings.get(1) else {
        log::error!("MALFORMED DLMM: {pool_info:?}");
        return;
    };

    let LPInfoUnderlyingMintVault {
        mint: mint_x,
        vault: vault_x,
    } = underlyings_x;
    let LPInfoUnderlyingMintVault {
        mint: mint_y,
        vault: vault_y,
    } = underlyings_y;

    let (decimals_x, decimals_y) = {
        log::trace!("Getting decimal cache read lock");
        let dc_read = decimal_cache.read().await;
        log::trace!("Got decimal cache read lock");

        let Some(decimals_x) = get_or_dispatch_decimals(&sender_arc, &dc_read, mint_x) else {
            return;
        };
        let Some(decimals_y) = get_or_dispatch_decimals(&sender_arc, &dc_read, mint_y) else {
            return;
        };

        (decimals_x, decimals_y)
    };

    let Some(x_factor) = Decimal::TEN.checked_powu(decimals_x as u64) else {
        log::warn!("Math overflowed for DLMM {pool_pubkey} - {mint_x} and {mint_y}");
        return;
    };
    let Some(y_factor) = Decimal::TEN.checked_powu(decimals_y as u64) else {
        log::warn!("Math overflowed for DLMM {pool_pubkey} - {mint_x} and {mint_y}");
        return;
    };

    let (x_balance, y_balance) = {
        let (x_bal_cache_op, y_bal_cache_op) = {
            log::trace!("Getting token balance cache read lock");
            let tbc_read = token_balance_cache.read().await;
            log::trace!("Got token balance cache read lock");
            (
                tbc_read.get(vault_x).cloned(),
                tbc_read.get(vault_y).cloned(),
            )
        };

        if let (Some(x_bal_inner_val), Some(y_bal_inner_val)) = (x_bal_cache_op, y_bal_cache_op) {
            let (Some(x_vault_balance), Some(y_vault_balance)) = (x_bal_inner_val, y_bal_inner_val)
            else {
                return;
            };

            (x_vault_balance, y_vault_balance)
        } else {
            // One or more balance is missing, need to dispatch to cache that we're looking for this token account
            log::trace!("Getting token balance cache write lock");
            let mut tbc_write = token_balance_cache.write().await;
            log::trace!("Got token balance cache write lock");

            if x_bal_cache_op.is_none() {
                tbc_write.insert(vault_x.clone(), None);
            }
            if y_bal_cache_op.is_none() {
                tbc_write.insert(vault_y.clone(), None);
            }

            return;
        }
    };

    let Some(x_balance_units) = x_balance.checked_div(x_factor) else {
        log::warn!("Math overflowed for DLMM {pool_pubkey} - {mint_x} and {mint_y}");
        return;
    };

    let Some(y_balance_units) = y_balance.checked_div(y_factor) else {
        log::warn!("Math overflowed for DLMM {pool_pubkey} - {mint_x} and {mint_y}");
        return;
    };

    let (mint_x_ix, add_mint_x) =
        get_or_add_mint_ix(mint_x, graph.clone(), mint_indicies.clone()).await;
    let (mint_y_ix, add_mint_y) =
        get_or_add_mint_ix(mint_y, graph.clone(), mint_indicies.clone()).await;

    let edges = get_two_way_edges_by_discriminant(edge_indicies.clone(), pool_pubkey).await;

    if add_mint_x || add_mint_y || edges.is_none() {
        let mut bins_by_account = HashMap::new();
        bins_by_account.insert(*part_index, parts.iter().map(|p| p.into()).collect());

        let new_relation_rev = LiqRelation::Dlmm {
            amt_origin: x_balance_units,
            amt_dest: y_balance_units,
            active_bin_account: None,
            bins_by_account: bins_by_account.clone(),
            is_reverse: true,
            decimals_x,
            decimals_y,
            pool_id: pool_pubkey.to_string(),
        };

        let new_relation = LiqRelation::Dlmm {
            amt_origin: y_balance_units,
            amt_dest: x_balance_units,
            active_bin_account: None,
            bins_by_account,
            is_reverse: false,
            decimals_x,
            decimals_y,
            pool_id: pool_pubkey.to_string(),
        };

        let new_edges_res = add_or_update_two_way_relation_edge(
            mint_x_ix,
            mint_y_ix,
            edge_indicies.clone(),
            graph.clone(),
            new_relation,
            new_relation_rev,
            pool_pubkey,
            *time,
        )
        .await;

        match new_edges_res {
            Ok(_) => {}
            Err(e) => {
                log::error!("Error adding or updating two way edge for DLMM {pool_pubkey}: {e}");
            }
        }
    } else {
        let Some(edge) = edges.as_ref().and_then(|v| v.normal) else {
            log::error!("UNREACHABLE - Relation was already checked above");
            return;
        };
        let Some(edge_rev) = edges.as_ref().and_then(|v| v.reverse) else {
            log::error!("UNREACHABLE - Reverse relation was already checked above");
            return;
        };

        let new_bins_by_account: Vec<DlmmBinParsed> = parts.iter().map(|p| p.into()).collect();

        // Is the active bin in this binarray?
        let active_bin_opt = parts
            .iter()
            .enumerate()
            .find(|(_ix, bin)| bin.token_amounts.iter().all(|amt| *amt > Decimal::ZERO));

        log::trace!("Getting graph read lock");
        let g_read = graph.read().await;
        log::trace!("Got graph read lock");

        {
            let weight = g_read.edge_weight(edge).unwrap();
            log::trace!("Getting weight write lock");
            let mut w_write = weight.inner_relation.write().await;
            log::trace!("Got weight write lock");
            let LiqRelation::Dlmm {
                ref mut amt_origin,
                ref mut amt_dest,
                ref mut active_bin_account,
                ref mut bins_by_account,
                ..
            } = *w_write
            else {
                log::error!(
                    "UNREACHABLE - WEIGHT IS NOT A DLMM FOR DISCRIMINANT {}",
                    pool_pubkey
                );
                return;
            };

            *amt_origin = y_balance_units;
            *amt_dest = x_balance_units;

            if let Some((ix, _)) = active_bin_opt.as_ref() {
                active_bin_account.replace((*part_index, *ix));
            }

            bins_by_account.insert(*part_index, new_bins_by_account.clone());
        }

        {
            let weight_rev = g_read.edge_weight(edge_rev).unwrap();

            log::trace!("Getting weight rev write lock");
            let mut w_rev_write = weight_rev.inner_relation.write().await;
            log::trace!("Got weight rev write lock");
            let LiqRelation::Dlmm {
                amt_origin: ref mut amt_origin_rev,
                amt_dest: ref mut amt_dest_rev,
                active_bin_account: ref mut active_bin_rev,
                bins_by_account: ref mut bins_by_account_rev,
                ..
            } = *w_rev_write
            else {
                log::error!(
                    "UNREACHABLE - WEIGHT IS NOT A DLMM FOR DISCRIMINANT {}",
                    pool_pubkey
                );
                return;
            };

            *amt_origin_rev = x_balance_units;
            *amt_dest_rev = y_balance_units;

            bins_by_account_rev.insert(*part_index, new_bins_by_account);

            if let Some((ix, _)) = active_bin_opt.as_ref() {
                active_bin_rev.replace((*part_index, *ix));
            }
        }

        log::debug!("handle_dlmm took {:?}", now.elapsed());
    }
}
