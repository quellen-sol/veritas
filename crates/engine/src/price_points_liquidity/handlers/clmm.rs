use anyhow::Result;
use std::{
    collections::HashMap,
    sync::{atomic::AtomicBool, Arc},
};

use chrono::NaiveDateTime;
use rust_decimal::{Decimal, MathematicalOps};
use step_ingestooor_sdk::dooot::{ClmmTick, Dooot, LPInfoUnderlyingMintVault};
use tokio::sync::{mpsc::Sender, RwLock};
use veritas_sdk::{
    liq_relation::{relations::clmm::ClmmTickParsed, LiqRelation},
    ppl_graph::graph::WrappedMintPricingGraph,
    utils::{
        decimal_cache::DecimalCache, lp_cache::LpCache, token_balance_cache::TokenBalanceCache,
    },
};

use crate::{
    calculator::task::CalculatorUpdate,
    price_points_liquidity::task::{
        add_or_update_two_way_relation_edge, get_edge_by_discriminant, get_or_add_mint_ix,
        get_or_dispatch_decimals, EdgeIndiciesMap, MintIndiciesMap,
    },
};

enum UpdateRelationCbParams<'a> {
    ClmmGlobal {
        time: NaiveDateTime,
        pool_pubkey: &'a str,
        current_price: &'a str,
        current_tick_index: i32,
        tick_spacing: i32,
    },
    ClmmTickGlobal {
        start_tick_index: i32,
        ticks: &'a [ClmmTick],
        time: NaiveDateTime,
        pool_pubkey: &'a str,
    },
}

impl<'a> UpdateRelationCbParams<'a> {
    fn extract_clmm_params(dooot: &'a Dooot) -> Option<Self> {
        match dooot {
            Dooot::ClmmGlobal(dooot) => Some(Self::ClmmGlobal {
                current_price: &dooot.current_price,
                time: dooot.time,
                pool_pubkey: &dooot.pool_pubkey,
                current_tick_index: dooot.current_tick_index,
                tick_spacing: dooot.tick_spacing as i32,
            }),
            Dooot::ClmmTickGlobal(dooot) => Some(Self::ClmmTickGlobal {
                start_tick_index: dooot.tick_index,
                ticks: &dooot.ticks,
                time: dooot.time,
                pool_pubkey: &dooot.pool_pubkey,
            }),
            _ => None,
        }
    }

    fn extract_pool_pubkey(&self) -> &str {
        match self {
            Self::ClmmGlobal { pool_pubkey, .. } => pool_pubkey,
            Self::ClmmTickGlobal { pool_pubkey, .. } => pool_pubkey,
        }
    }

    fn extract_time(&self) -> NaiveDateTime {
        match self {
            Self::ClmmGlobal { time, .. } => *time,
            Self::ClmmTickGlobal { time, .. } => *time,
        }
    }
}

#[allow(clippy::unwrap_used)]
pub async fn handle_clmm(
    dooot: Dooot,
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
    let Some(params) = UpdateRelationCbParams::extract_clmm_params(&dooot) else {
        return;
    };

    let pool_pubkey = params.extract_pool_pubkey();
    let time = params.extract_time();

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

    let Some(underlyings_a) = pool_info.underlyings.first() else {
        log::error!("MALFORMED CLMM: {pool_info:?}");
        return;
    };

    let Some(underlyings_b) = pool_info.underlyings.get(1) else {
        log::error!("MALFORMED CLMM: {pool_info:?}");
        return;
    };

    let LPInfoUnderlyingMintVault {
        mint: mint_a,
        vault: vault_a,
    } = underlyings_a;
    let LPInfoUnderlyingMintVault {
        mint: mint_b,
        vault: vault_b,
    } = underlyings_b;

    let (decimals_a, decimals_b) = {
        log::trace!("Getting decimal cache read lock");
        let dc_read = decimal_cache.read().await;
        log::trace!("Got decimal cache read lock");

        let Some(decimals_a) = get_or_dispatch_decimals(&sender_arc, &dc_read, mint_a) else {
            return;
        };
        let Some(decimals_b) = get_or_dispatch_decimals(&sender_arc, &dc_read, mint_b) else {
            return;
        };

        (decimals_a, decimals_b)
    };

    let Some(a_factor) = Decimal::TEN.checked_powu(decimals_a as u64) else {
        log::warn!("Math overflowed for CLMM {pool_pubkey} - {mint_a} and {mint_b}");
        return;
    };
    let Some(b_factor) = Decimal::TEN.checked_powu(decimals_b as u64) else {
        log::warn!("Math overflowed for CLMM {pool_pubkey} - {mint_a} and {mint_b}");
        return;
    };

    let (a_balance, b_balance) = {
        let (a_bal_cache_op, b_bal_cache_op) = {
            log::trace!("Getting token balance cache read lock");
            let tbc_read = token_balance_cache.read().await;
            log::trace!("Got token balance cache read lock");
            (
                tbc_read.get(vault_a).cloned(),
                tbc_read.get(vault_b).cloned(),
            )
        };

        if let (Some(a_bal_inner_val), Some(b_bal_inner_val)) = (a_bal_cache_op, b_bal_cache_op) {
            let (Some(a_vault_balance), Some(b_vault_balance)) = (a_bal_inner_val, b_bal_inner_val)
            else {
                return;
            };

            (a_vault_balance, b_vault_balance)
        } else {
            // One or more balance is missing, need to dispatch to cache that we're looking for this token account
            log::trace!("Getting token balance cache write lock");
            let mut tbc_write = token_balance_cache.write().await;
            log::trace!("Got token balance cache write lock");

            if a_bal_cache_op.is_none() {
                tbc_write.insert(vault_a.clone(), None);
            }
            if b_bal_cache_op.is_none() {
                tbc_write.insert(vault_b.clone(), None);
            }

            return;
        }
    };

    let Some(a_balance_units) = a_balance.checked_div(a_factor) else {
        log::warn!("Math overflowed for CLMM {pool_pubkey} - {mint_a} and {mint_b}");
        return;
    };

    let Some(b_balance_units) = b_balance.checked_div(b_factor) else {
        log::warn!("Math overflowed for CLMM {pool_pubkey} - {mint_a} and {mint_b}");
        return;
    };

    let (mint_a_ix, add_mint_a) =
        get_or_add_mint_ix(mint_a, graph.clone(), mint_indicies.clone()).await;
    let (mint_b_ix, add_mint_b) =
        get_or_add_mint_ix(mint_b, graph.clone(), mint_indicies.clone()).await;
    let edge_ix = get_edge_by_discriminant(
        mint_a_ix,
        mint_b_ix,
        graph.clone(),
        edge_indicies.clone(),
        pool_pubkey,
    )
    .await;
    let edge_ix_rev = get_edge_by_discriminant(
        mint_b_ix,
        mint_a_ix,
        graph.clone(),
        edge_indicies.clone(),
        pool_pubkey,
    )
    .await;

    if add_mint_a || add_mint_b || edge_ix.is_none() || edge_ix_rev.is_none() {
        let (new_relation, new_relation_rev) = match params {
            UpdateRelationCbParams::ClmmGlobal {
                current_price,
                current_tick_index,
                tick_spacing,
                ..
            } => {
                let Ok(current_price_x64) = current_price.parse() else {
                    log::warn!("Could not convert current price to u128 for CLMM {pool_pubkey} - {mint_a} and {mint_b}");
                    return;
                };

                let new_relation = LiqRelation::Clmm {
                    amt_origin: b_balance_units,
                    amt_dest: a_balance_units,
                    current_price_x64: Some(current_price_x64),
                    current_tick_index: Some(current_tick_index),
                    tick_spacing: Some(tick_spacing),
                    decimals_a,
                    decimals_b,
                    is_reverse: false,
                    pool_id: pool_pubkey.to_string(),
                    ticks_by_account: HashMap::new(),
                };

                let new_relation_rev = LiqRelation::Clmm {
                    amt_origin: a_balance_units,
                    amt_dest: b_balance_units,
                    current_price_x64: Some(current_price_x64),
                    current_tick_index: Some(current_tick_index),
                    tick_spacing: Some(tick_spacing),
                    decimals_a,
                    decimals_b,
                    is_reverse: true,
                    pool_id: pool_pubkey.to_string(),
                    ticks_by_account: HashMap::new(),
                };

                (new_relation, new_relation_rev)
            }
            UpdateRelationCbParams::ClmmTickGlobal { .. } => {
                let ticks_by_account = HashMap::new();

                let new_relation = LiqRelation::Clmm {
                    amt_origin: b_balance_units,
                    amt_dest: a_balance_units,
                    current_price_x64: None,
                    current_tick_index: None,
                    tick_spacing: None,
                    decimals_a,
                    decimals_b,
                    is_reverse: false,
                    pool_id: pool_pubkey.to_string(),
                    ticks_by_account: ticks_by_account.clone(),
                };

                let new_relation_rev = LiqRelation::Clmm {
                    amt_origin: a_balance_units,
                    amt_dest: b_balance_units,
                    current_price_x64: None,
                    current_tick_index: None,
                    tick_spacing: None,
                    decimals_a,
                    decimals_b,
                    is_reverse: true,
                    pool_id: pool_pubkey.to_string(),
                    ticks_by_account,
                };

                (new_relation, new_relation_rev)
            }
        };

        let new_edges_res = add_or_update_two_way_relation_edge(
            mint_a_ix,
            mint_b_ix,
            edge_indicies.clone(),
            graph.clone(),
            new_relation,
            new_relation_rev,
            pool_pubkey,
            time,
        )
        .await;

        match new_edges_res {
            Ok(_) => {}
            Err(e) => {
                log::error!("Error adding or updating two way edge for CLMM {pool_pubkey}: {e}");
            }
        }
    } else {
        let Some(relation) = edge_ix else {
            log::error!("UNREACHABLE - Relation was already checked above");
            return;
        };
        let Some(relation_rev) = edge_ix_rev else {
            log::error!("UNREACHABLE - Reverse relation was already checked above");
            return;
        };

        log::trace!("Getting graph read lock");
        let g_read = graph.read().await;
        log::trace!("Got graph read lock");
        let weight = g_read.edge_weight(relation).unwrap();
        let weight_rev = g_read.edge_weight(relation_rev).unwrap();

        {
            log::trace!("Getting inner relation write lock");
            let mut w_write = weight.inner_relation.write().await;
            log::trace!("Got inner relation write lock");

            let LiqRelation::Clmm {
                ref mut amt_origin,
                ref mut amt_dest,
                ref mut current_price_x64,
                ref mut ticks_by_account,
                ref mut current_tick_index,
                ref mut tick_spacing,
                ..
            } = *w_write
            else {
                log::error!(
                    "UNREACHABLE - WEIGHT IS NOT A CLMM FOR DISCRIMINANT {}",
                    pool_pubkey
                );
                return;
            };

            *amt_origin = b_balance_units;
            *amt_dest = a_balance_units;

            match params {
                UpdateRelationCbParams::ClmmGlobal {
                    current_price,
                    current_tick_index: tick_idx_dooot,
                    tick_spacing: spacing_dooot,
                    ..
                } => {
                    let Ok(curr_price_parsed) = current_price.parse::<u128>() else {
                        log::error!("Could not convert current price to u128 for CLMM {pool_pubkey} - {mint_a} and {mint_b}");
                        return;
                    };

                    current_price_x64.replace(curr_price_parsed);
                    current_tick_index.replace(tick_idx_dooot);
                    tick_spacing.replace(spacing_dooot);
                }
                UpdateRelationCbParams::ClmmTickGlobal {
                    start_tick_index,
                    ticks,
                    ..
                } => {
                    let Some(tick_spacing) = tick_spacing else {
                        // Can't calculate tick indicies without tick spacing
                        return;
                    };

                    let ticks_parsed = ticks.iter().map(|tick| tick.try_into()).enumerate();

                    for (i, tick) in ticks_parsed {
                        let Ok(tick): Result<ClmmTickParsed> = tick else {
                            continue;
                        };

                        let this_idx = start_tick_index + (*tick_spacing * i as i32);
                        ticks_by_account.insert(this_idx, tick.clone());
                    }
                }
            }
        }

        {
            log::trace!("Getting inner relation write lock");
            let mut w_rev_write = weight_rev.inner_relation.write().await;
            log::trace!("Got inner relation write lock");

            let LiqRelation::Clmm {
                amt_origin: ref mut amt_origin_rev,
                amt_dest: ref mut amt_dest_rev,
                current_price_x64: ref mut current_price_x64_rev,
                ticks_by_account: ref mut ticks_by_account_rev,
                current_tick_index: ref mut current_tick_index_rev,
                tick_spacing: ref mut tick_spacing_rev,
                ..
            } = *w_rev_write
            else {
                log::error!(
                    "UNREACHABLE - WEIGHT IS NOT A CLMM FOR DISCRIMINANT {}",
                    pool_pubkey
                );
                return;
            };

            *amt_origin_rev = a_balance_units;
            *amt_dest_rev = b_balance_units;

            match params {
                UpdateRelationCbParams::ClmmGlobal {
                    current_price,
                    current_tick_index: tick_idx_dooot,
                    tick_spacing: spacing_dooot,
                    ..
                } => {
                    let Ok(curr_price_parsed) = current_price.parse::<u128>() else {
                        log::error!("Could not convert current price to u128 for CLMM {pool_pubkey} - {mint_a} and {mint_b}");
                        return;
                    };

                    current_price_x64_rev.replace(curr_price_parsed);
                    current_tick_index_rev.replace(tick_idx_dooot);
                    tick_spacing_rev.replace(spacing_dooot);
                }
                UpdateRelationCbParams::ClmmTickGlobal {
                    start_tick_index,
                    ticks,
                    ..
                } => {
                    let Some(tick_spacing) = tick_spacing_rev else {
                        // Can't calculate tick indicies without tick spacing
                        return;
                    };

                    let ticks_parsed = ticks.iter().map(|tick| tick.try_into()).enumerate();

                    for (i, tick) in ticks_parsed {
                        let Ok(tick): Result<ClmmTickParsed> = tick else {
                            continue;
                        };

                        let this_idx = start_tick_index + (*tick_spacing * i as i32);
                        ticks_by_account_rev.insert(this_idx, tick);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use step_ingestooor_sdk::dooot::{ClmmGlobalDooot, CurveType};
    use veritas_sdk::utils::lp_cache::LiquidityPool;

    use crate::price_points_liquidity::handlers::utils::build_test_handler_state;

    use super::*;

    #[tokio::test]
    async fn test_clmm_handler() {
        let state = build_test_handler_state();

        let pool_id = "9RqDTfwCx2SgxsvKpspQHc38HUo3B6hRd3oR9JR966Ps";
        let mint_a = "2u1tszSeqZ3qBWF3uNGPFc8TzMk2tdiwknnRMWGWjGWH";
        let mint_b = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

        let lp = LiquidityPool {
            curve_type: CurveType::ConcentratedLiquidity,
            underlyings: vec![
                LPInfoUnderlyingMintVault {
                    mint: "2u1tszSeqZ3qBWF3uNGPFc8TzMk2tdiwknnRMWGWjGWH".to_string(),
                    vault: "6j9UtMmzmWuLu45XXmdUXN3NJBdiicxxoBEex8jUs3j6".to_string(),
                },
                LPInfoUnderlyingMintVault {
                    mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".to_string(),
                    vault: "5Sokmb48nt8aH8TnnkrAcVea4SdRqGU3qTxhRFvTHJyn".to_string(),
                },
            ],
        };
        state.lp_cache.write().await.insert(pool_id.to_string(), lp);

        state.token_balance_cache.write().await.insert(
            "6j9UtMmzmWuLu45XXmdUXN3NJBdiicxxoBEex8jUs3j6".to_string(),
            Some(9086579393165u64.into()),
        );
        state.token_balance_cache.write().await.insert(
            "5Sokmb48nt8aH8TnnkrAcVea4SdRqGU3qTxhRFvTHJyn".to_string(),
            Some(10945387511691u64.into()),
        );

        state
            .decimal_cache
            .write()
            .await
            .insert(mint_a.to_string(), 6);
        state
            .decimal_cache
            .write()
            .await
            .insert(mint_b.to_string(), 6);

        let dooot = Dooot::ClmmGlobal(ClmmGlobalDooot {
            time: Utc::now().naive_utc(),
            pool_pubkey: pool_id.to_string(),
            current_price: "18450129097944736781".into(),
            current_tick_index: 3,
            tick_spacing: 1,
            total_liquidity_shares: None,
        });

        handle_clmm(
            dooot,
            state.graph.clone(),
            state.lp_cache,
            state.decimal_cache,
            state.mint_indicies.clone(),
            state.edge_indicies.clone(),
            state.sender_arc,
            state.token_balance_cache,
            state.calculator_sender,
            state.bootstrap_in_progress,
        )
        .await;

        let mi_read = state.mint_indicies.read().await;

        let mint_a_ix = mi_read.get(mint_a).unwrap();
        let mint_b_ix = mi_read.get(mint_b).unwrap();

        let relation = get_edge_by_discriminant(
            *mint_a_ix,
            *mint_b_ix,
            state.graph.clone(),
            state.edge_indicies.clone(),
            pool_id,
        )
        .await;
        let relation_rev = get_edge_by_discriminant(
            *mint_b_ix,
            *mint_a_ix,
            state.graph.clone(),
            state.edge_indicies.clone(),
            pool_id,
        )
        .await;

        assert!(relation.is_some());
        assert!(relation_rev.is_some());
    }
}
