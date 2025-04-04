use std::{
    collections::HashMap,
    sync::{atomic::AtomicBool, Arc},
};

use petgraph::graph::EdgeIndex;
use rust_decimal::Decimal;
use step_ingestooor_sdk::dooot::{DlmmGlobalDooot, Dooot, LPInfoUnderlyingMintVault};
use tokio::sync::{mpsc::Sender, RwLock};
use veritas_sdk::{
    liq_relation::LiqRelation,
    ppl_graph::graph::WrappedMintPricingGraph,
    utils::{
        decimal_cache::DecimalCache, lp_cache::LpCache, token_balance_cache::TokenBalanceCache,
    },
};

use crate::{
    calculator::task::CalculatorUpdate,
    price_points_liquidity::task::{
        add_or_update_relation_edge, get_edge_by_discriminant, get_or_add_mint_ix,
        get_or_dispatch_decimals, EdgeIndiciesMap, MintIndiciesMap,
    },
};

use super::utils::send_update_to_calculator;

pub async fn handle_dlmm(
    dooot: DlmmGlobalDooot,
    graph: WrappedMintPricingGraph,
    lp_cache: Arc<RwLock<LpCache>>,
    decimal_cache: Arc<RwLock<DecimalCache>>,
    mint_indicies: Arc<RwLock<MintIndiciesMap>>,
    edge_indicies: Arc<RwLock<EdgeIndiciesMap>>,
    sender_arc: Sender<String>,
    token_balance_cache: Arc<RwLock<TokenBalanceCache>>,
    calculator_sender: Sender<CalculatorUpdate>,
    bootstrap_in_progress: Arc<AtomicBool>,
) {
    let DlmmGlobalDooot {
        time,
        parts,
        pool_pubkey,
        ..
    } = &dooot;

    let pool_info = {
        let lc_read = lp_cache.read().await;
        let Some(lp) = lc_read.get(pool_pubkey).cloned() else {
            return;
        };

        lp
    };

    let Some(underlyings_x) = pool_info.underlyings.get(0) else {
        log::error!("MALFORMED DLMM DOOOT: {dooot:?}");
        return;
    };
    let Some(underlyings_y) = pool_info.underlyings.get(1) else {
        log::error!("MALFORMED DLMM DOOOT: {dooot:?}");
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

    let (x_balance, y_balance) = {
        let tbc_read = token_balance_cache.read().await;
        let x_bal_cache_op = tbc_read.get(mint_x).cloned();
        let y_bal_cache_op = tbc_read.get(mint_y).cloned();

        if let (Some(x_bal_inner_val), Some(y_bal_inner_val)) = (x_bal_cache_op, y_bal_cache_op) {
            let (Some(x_vault_balance), Some(y_vault_balance)) = (x_bal_inner_val, y_bal_inner_val)
            else {
                return;
            };

            log::info!("Success getting two balances from vaults");

            (x_vault_balance, y_vault_balance)
        } else {
            // One or more balance is missing, need to dispatch to cache that we're looking for this token account
            drop(tbc_read);
            let mut tbc_write = token_balance_cache.write().await;

            if x_bal_cache_op.is_none() {
                tbc_write.insert(vault_x.clone(), None);
            }
            if y_bal_cache_op.is_none() {
                tbc_write.insert(vault_y.clone(), None);
            }

            return;
        }
    };

    let (mut mint_x_ix, mut mint_y_ix) = {
        let mi_read = mint_indicies.read().await;
        let mint_x_ix = mi_read.get(mint_x).cloned();
        let mint_y_ix = mi_read.get(mint_y).cloned();

        (mint_x_ix, mint_y_ix)
    };

    // Do we need to add these to graph?
    let add_mint_x = mint_x_ix.is_none();
    let add_mint_y = mint_y_ix.is_none();

    if add_mint_x || add_mint_y {
        let (x_decimals, y_decimals) = {
            let dc_read = decimal_cache.read().await;

            let Some(x_decimals) = get_or_dispatch_decimals(&sender_arc, &dc_read, mint_x) else {
                return;
            };
            let Some(y_decimals) = get_or_dispatch_decimals(&sender_arc, &dc_read, mint_y) else {
                return;
            };

            (x_decimals, y_decimals)
        };

        let mut g_write = graph.write().await;
        let mut mi_write = mint_indicies.write().await;

        if add_mint_x {
            mint_x_ix = get_or_add_mint_ix(mint_x, &mut g_write, &mut mi_write).into();
        }

        if add_mint_y {
            mint_y_ix = get_or_add_mint_ix(mint_y, &mut g_write, &mut mi_write).into();
        }

        let new_relation = LiqRelation::Dlmm {
            amt_origin: x_balance,
            amt_dest: y_balance,
            vault_x: vault_x.to_string(),
            vault_y: vault_y.to_string(),
            active_bin: None,
            bins_by_account: HashMap::new(),
            x_decimals,
            y_decimals,
            is_reverse: false,
        };

        let new_reverse_relation = LiqRelation::Dlmm {
            amt_origin: x_balance,
            amt_dest: y_balance,
            vault_x: vault_x.to_string(),
            vault_y: vault_y.to_string(),
            active_bin: None,
            bins_by_account: HashMap::new(),
            x_decimals,
            y_decimals,
            is_reverse: true,
        };

        let (Some(x_ix), Some(y_ix)) = (mint_x_ix, mint_y_ix) else {
            log::error!("UNREACHABLE - Both indicies should have been set just now");
            return;
        };

        let mut ei_write = edge_indicies.write().await;

        let new_edge = add_or_update_relation_edge(
            x_ix,
            y_ix,
            &mut ei_write,
            &mut g_write,
            new_relation,
            &pool_pubkey,
            *time,
        )
        .await;

        let new_edge = match new_edge {
            Ok(ix) => ix,
            Err(e) => {
                log::error!("Error adding or updating edge for DLMM {pool_pubkey}: {e}");
                return;
            }
        };

        let new_reverse_edge = add_or_update_relation_edge(
            y_ix,
            x_ix,
            &mut ei_write,
            &mut g_write,
            new_reverse_relation,
            &pool_pubkey,
            *time,
        )
        .await;

        let new_reverse_edge = match new_reverse_edge {
            Ok(ix) => ix,
            Err(e) => {
                log::error!("Error adding or updating edge for DLMM {pool_pubkey}: {e}");
                return;
            }
        };

        send_update_to_calculator(
            CalculatorUpdate::NewTokenRatio(y_ix, new_edge),
            &calculator_sender,
            &bootstrap_in_progress,
        )
        .await;
        send_update_to_calculator(
            CalculatorUpdate::NewTokenRatio(x_ix, new_reverse_edge),
            &calculator_sender,
            &bootstrap_in_progress,
        )
        .await;
    } else {
        // Only need to update the relation in the edge
        let (Some(x_ix), Some(y_ix)) = (mint_x_ix, mint_y_ix) else {
            log::error!("UNREACHABLE - Both indicies should have been set already. Checked above");
            return;
        };

        let g_read = graph.read().await;
        let ei_read = edge_indicies.read().await;

        // Index by bin direction?
    }
}
