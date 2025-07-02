use std::sync::{mpsc::SyncSender, Arc, RwLock};

use rust_decimal::{Decimal, MathematicalOps};
use step_ingestooor_sdk::dooot::{CurveType, MintUnderlyingsGlobalDooot};
use veritas_sdk::{
    liq_relation::LiqRelation,
    types::{EdgeIndiciesMap, MintIndiciesMap, MintPricingGraph},
    utils::{decimal_cache::DecimalCache, lp_cache::LpCache},
};

use crate::price_points_liquidity::task::{
    add_or_update_two_way_relation_edge, get_or_add_mint_ix, get_or_dispatch_decimals,
};

pub fn handle_amm_lp(
    mu_dooot: MintUnderlyingsGlobalDooot,
    lp_cache: Arc<RwLock<LpCache>>,
    graph: Arc<RwLock<MintPricingGraph>>,
    decimal_cache: Arc<RwLock<DecimalCache>>,
    cache_updator_sender: SyncSender<String>,
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
        let dc_read = decimal_cache
            .read()
            .expect("Decimal cache read lock poisoned");
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

    let mint_a_ix = get_or_add_mint_ix(mint_a, graph.clone(), mint_indicies.clone()).0;
    let mint_b_ix = get_or_add_mint_ix(mint_b, graph.clone(), mint_indicies.clone()).0;

    let Some(relation) = build_mu_relation(
        &mu_dooot,
        lp_cache.clone(),
        parent_mint,
        mint_a,
        mint_b,
        decimals_a,
        decimals_b,
    ) else {
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
    ) {
        Ok(_) => {}
        Err(e) => {
            log::error!("Error adding or updating relation edge for {mint_a} -> {mint_b}: {e}");
        }
    };
}

fn build_mu_relation(
    mu_dooot: &MintUnderlyingsGlobalDooot,
    lp_cache: Arc<RwLock<LpCache>>,
    discriminant_id: &str,
    mint_a: &str,
    mint_b: &str,
    decimals_a: u8,
    decimals_b: u8,
) -> Option<LiqRelation> {
    log::trace!("Getting lp cache read lock");
    let curve_type = lp_cache
        .read()
        .expect("LP cache read lock poisoned")
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
            let relation = LiqRelation::CpLp {
                amt_origin: amt_b_units,
                amt_dest: amt_a_units,
                pool_id: discriminant_id.to_string(),
            };
            Some(relation)
        }
        _ => None,
    }
}
