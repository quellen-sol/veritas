use std::sync::Arc;

use rust_decimal::{prelude::FromPrimitive, Decimal, MathematicalOps};
use step_ingestooor_sdk::dooot::{CurveType, MintUnderlyingsGlobalDooot};
use tokio::sync::{mpsc::Sender, RwLock};
use veritas_sdk::{
    liq_relation::LiqRelation,
    ppl_graph::graph::MintPricingGraph,
    utils::{decimal_cache::DecimalCache, lp_cache::LpCache},
};

use crate::price_points_liquidity::task::{
    add_or_update_two_way_relation_edge, get_or_add_mint_ix, get_or_dispatch_decimals,
    EdgeIndiciesMap, MintIndiciesMap,
};

pub async fn handle_amm_lp(
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

    let mint_a = &mints[0];
    let mint_b = &mints[1];

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

    let (mint_a_ix, mint_b_ix) = {
        log::trace!("Getting mint indicies read lock");
        let mi_read = mint_indicies.read().await;
        log::trace!("Got mint indicies read lock");
        let mint_a_ix = mi_read.get(mint_a).cloned();
        let mint_b_ix = mi_read.get(mint_b).cloned();

        (mint_a_ix, mint_b_ix)
    };

    let mint_a_ix = match mint_a_ix {
        Some(ix) => ix,
        None => get_or_add_mint_ix(mint_a, graph.clone(), mint_indicies.clone()).await,
    };

    let mint_b_ix = match mint_b_ix {
        Some(ix) => ix,
        None => get_or_add_mint_ix(mint_b, graph.clone(), mint_indicies.clone()).await,
    };

    let Some(relation) = build_mu_relation(
        &mu_dooot,
        lp_cache.clone(),
        parent_mint,
        mint_a,
        mint_b,
        decimals_a,
        decimals_b,
        true,
        false,
    )
    .await
    else {
        return;
    };

    let relation_rev = relation.reversed();

    match add_or_update_two_way_relation_edge(
        mint_a_ix,
        mint_b_ix,
        edge_indicies,
        graph,
        relation,
        relation_rev,
        parent_mint,
        *time,
    )
    .await
    {
        Ok(_) => {}
        Err(e) => {
            log::error!("Error adding or updating relation edge for {mint_a} -> {mint_b}: {e}");
        }
    };
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
        log::trace!("Getting lp cache read lock");
        let curve_type = lp_cache
            .read()
            .await
            .get(discriminant_id)
            .cloned()?
            .curve_type;
        log::trace!("Got lp cache read lock");

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
