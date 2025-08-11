use std::sync::{mpsc::SyncSender, Arc, RwLock};

use rust_decimal::{prelude::FromPrimitive, Decimal, MathematicalOps};
use step_ingestooor_sdk::dooot::MintUnderlyingsGlobalDooot;
use veritas_sdk::{
    liq_relation::LiqRelation,
    types::{EdgeIndiciesMap, MintIndiciesMap, MintPricingGraph},
    utils::decimal_cache::DecimalCache,
};

use crate::price_points_liquidity::task::{
    add_or_update_relation_edge, get_or_add_mint_ix, get_or_dispatch_decimals,
};

pub fn handle_fixed(
    mu_dooot: MintUnderlyingsGlobalDooot,
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
        mints_qty_per_one_parent,
        // platform_program_pubkey,
        // total_underlying_amounts,
        ..
    } = &mu_dooot;

    let underlying_mint = &mints[0];

    let (decimals_parent, decimals_underlying) = {
        let dc_read = decimal_cache
            .read()
            .expect("Decimal cache read lock poisoned");

        let Some(decimals_parent) =
            get_or_dispatch_decimals(&cache_updator_sender, &dc_read, parent_mint)
        else {
            return;
        };

        let Some(decimals_underlying) =
            get_or_dispatch_decimals(&cache_updator_sender, &dc_read, underlying_mint)
        else {
            return;
        };

        (decimals_parent, decimals_underlying)
    };

    let mint_parent_ix = get_or_add_mint_ix(parent_mint, graph.clone(), mint_indicies.clone()).0;
    let mint_underlying_ix =
        get_or_add_mint_ix(underlying_mint, graph.clone(), mint_indicies.clone()).0;

    let Some(decimal_factor) =
        Decimal::TEN.checked_powi(decimals_parent as i64 - decimals_underlying as i64)
    else {
        log::error!("Could not compute decimal factor for {decimals_parent}, {decimals_underlying}: {mu_dooot:?}");
        return;
    };

    let ratio = mints_qty_per_one_parent[0];
    let Some(ratio) = Decimal::from_f64(ratio) else {
        log::error!("Could not convert {ratio} to Decimal");
        return;
    };

    let Some(ratio) = ratio.checked_mul(decimal_factor) else {
        log::error!("Math overflow while deriving unit ratio for Fixed {ratio} * {decimal_factor}");
        return;
    };

    let relation = LiqRelation::Fixed {
        amt_per_parent: ratio,
    };

    let update_res = add_or_update_relation_edge(
        mint_underlying_ix,
        mint_parent_ix,
        edge_indicies.clone(),
        graph.clone(),
        relation,
        parent_mint,
        *time,
        false,
    );

    match update_res {
        Ok(_) => {}
        Err(e) => {
            log::error!("Error adding or updating Fixed relation: {e}");
        }
    }

    // SPECIAL CASE: If the program id is meteora vaults, we need to add a FixedRef relation
    // if platform_program_pubkey == "24Uqj9JCLxUeoC3hGfh5W3s9FM9uCHDS2SG3LYwBpyTi" {
    //     // Meteora Vaults
    //     let Some(amt_parent_per_dest) = Decimal::ONE.checked_div(ratio) else {
    //         return;
    //     };

    //     let amt_parent = total_underlying_amounts[0];

    //     let relation = LiqRelation::FixedRef {
    //         amt_parent_per_dest,
    //         amt_parent,
    //     };

    //     let update_res = add_or_update_relation_edge(
    //         mint_parent_ix,
    //         mint_underlying_ix,
    //         edge_indicies,
    //         graph,
    //         relation,
    //         parent_mint,
    //         *time,
    //         true,
    //     );

    //     match update_res {
    //         Ok(_) => {}
    //         Err(e) => {
    //             log::error!("Error adding or updating FixedRef relation: {e}");
    //         }
    //     }
    // }
}
