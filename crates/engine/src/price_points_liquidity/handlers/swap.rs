use std::collections::HashSet;
use std::sync::{mpsc::SyncSender, Arc, RwLock};

use chrono::Utc;
use rust_decimal::{Decimal, MathematicalOps};
use step_ingestooor_sdk::dooot::{Dooot, SwapEventDooot, TokenPriceGlobalDooot};
use veritas_sdk::ppl_graph::utils::get_price_by_mint;
use veritas_sdk::types::{MintIndiciesMap, WrappedMintPricingGraph};
use veritas_sdk::utils::checked_math::clamp_to_scale;
use veritas_sdk::utils::decimal_cache::DecimalCache;

use crate::price_points_liquidity::task::get_or_add_mint_ix;

#[allow(clippy::unwrap_used)]
pub fn handle_swap_event(
    swap: SwapEventDooot,
    graph: WrappedMintPricingGraph,
    mint_indicies: Arc<RwLock<MintIndiciesMap>>,
    decimal_cache: Arc<RwLock<DecimalCache>>,
    oracle_mint_set: Arc<HashSet<String>>,
    price_sender: SyncSender<Dooot>,
) {
    let SwapEventDooot {
        in_amount,
        in_mint_pubkey,
        out_amount,
        out_mint_pubkey,
        program_pubkey,
        ..
    } = &swap;

    if program_pubkey != "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4" {
        return;
    }

    let (is_valid_swap, in_mint_is_oracle, out_mint_is_oracle) = {
        let in_mint_is_oracle = oracle_mint_set.contains(in_mint_pubkey.as_str());
        let out_mint_is_oracle = oracle_mint_set.contains(out_mint_pubkey.as_str());

        // Swap should be between one oracle mint, and one non-oracle mint
        let is_valid_swap = (in_mint_is_oracle && !out_mint_is_oracle)
            || (!in_mint_is_oracle && out_mint_is_oracle);
        (is_valid_swap, in_mint_is_oracle, out_mint_is_oracle)
    };

    if !is_valid_swap {
        return;
    }

    let (in_decimals, out_decimals) = {
        let dc_read = decimal_cache
            .read()
            .expect("Decimal cache read lock poisoned");
        let in_decimals = dc_read.get(in_mint_pubkey.as_str()).cloned();
        let out_decimals = dc_read.get(out_mint_pubkey.as_str()).cloned();

        (in_decimals, out_decimals)
    };

    let (Some(in_decimals), Some(out_decimals)) = (in_decimals, out_decimals) else {
        return;
    };

    let (
        mint_to_get_price,
        mint_to_set_price,
        df_numerator,
        df_denominator,
        ratio_numerator,
        ratio_denominator,
    ) = if in_mint_is_oracle {
        (
            in_mint_pubkey,
            out_mint_pubkey,
            out_decimals,
            in_decimals,
            in_amount,
            out_amount,
        )
    } else if out_mint_is_oracle {
        (
            out_mint_pubkey,
            in_mint_pubkey,
            in_decimals,
            out_decimals,
            out_amount,
            in_amount,
        )
    } else {
        log::warn!("UNREACHABLE - Invalid swap, check logic above");
        return;
    };

    let Some(token_ratio) = ratio_numerator.checked_div(*ratio_denominator) else {
        return;
    };

    let Some(decimal_factor) =
        Decimal::TEN.checked_powi((df_numerator as i64) - (df_denominator as i64))
    else {
        return;
    };

    let oracle_mint_price = {
        let mi_read = mint_indicies
            .read()
            .expect("Mint indicies read lock poisoned");
        let g_read = graph.read().expect("Graph read lock poisoned");
        get_price_by_mint(&g_read, &mi_read, mint_to_get_price)
    };

    let Some(oracle_mint_price) = oracle_mint_price else {
        return;
    };

    let Some(final_price) = token_ratio
        .checked_mul(decimal_factor)
        .and_then(|x| x.checked_mul(oracle_mint_price))
        .and_then(|p| clamp_to_scale(&p))
    else {
        return;
    };

    if final_price.is_zero() {
        return;
    }

    // Add the mint for the sake of debugging using endpoint
    get_or_add_mint_ix(mint_to_set_price, graph, mint_indicies);

    let price_dooot = Dooot::TokenPriceGlobal(TokenPriceGlobalDooot {
        deleted: false,
        mint: mint_to_set_price.clone(),
        price_usd: final_price,
        time: Utc::now().naive_utc(),
    });

    price_sender.send(price_dooot).unwrap();
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, RwLock};

    use rust_decimal::Decimal;
    use step_ingestooor_sdk::dooot::{Dooot, SwapEventDooot, TokenPriceGlobalDooot};
    use veritas_sdk::ppl_graph::graph::{MintNode, USDPriceWithSource};
    use veritas_sdk::types::{MintIndiciesMap, MintPricingGraph};
    use veritas_sdk::utils::decimal_cache::DecimalCache;

    use crate::price_points_liquidity::handlers::swap::handle_swap_event;

    #[test]
    fn test_swap_event() {
        let in_mint = "IN_MINT".to_string();
        let out_mint = "OUT_MINT".to_string();
        let swap_dooot = SwapEventDooot {
            in_amount: 10.into(),
            out_amount: 10.into(),
            in_mint_pubkey: in_mint.clone(),
            out_mint_pubkey: out_mint.clone(),
            program_pubkey: "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4".to_string(),
            ..Default::default()
        };

        let graph = Arc::new(RwLock::new(MintPricingGraph::new()));

        let in_ix = {
            let mut graph_write = graph.write().unwrap();
            graph_write.add_node(MintNode {
                cached_fixed_relation: RwLock::new(None),
                dirty: false,
                mint: in_mint.clone(),
                non_vertex_relations: RwLock::new(HashMap::new()),
                usd_price: RwLock::new(Some(USDPriceWithSource::Oracle(Decimal::from(2)))),
            })
        };

        let mint_indicies = Arc::new(RwLock::new(MintIndiciesMap::new()));
        {
            let mut mint_indicies_write = mint_indicies.write().unwrap();
            mint_indicies_write.insert(in_mint.clone(), in_ix);
        }

        let decimal_cache = Arc::new(RwLock::new(DecimalCache::new()));
        {
            let mut decimal_cache_write = decimal_cache.write().unwrap();
            decimal_cache_write.insert(in_mint.clone(), 6);
            decimal_cache_write.insert(out_mint.clone(), 6);
        }

        let mut oracle_mints = HashSet::new();
        oracle_mints.insert(in_mint.clone());

        let oracle_mint_set = Arc::new(oracle_mints);
        let (price_sender, price_receiver) = std::sync::mpsc::sync_channel::<Dooot>(10);

        handle_swap_event(
            swap_dooot,
            graph,
            mint_indicies,
            decimal_cache,
            oracle_mint_set,
            price_sender,
        );

        let price_dooot = price_receiver.try_recv().unwrap();

        let Dooot::TokenPriceGlobal(price_dooot) = price_dooot else {
            panic!("Should get price dooot");
        };

        let TokenPriceGlobalDooot {
            mint, price_usd, ..
        } = &price_dooot;

        assert_eq!(mint, &out_mint);
        assert_eq!(price_usd, &Decimal::from(2));
    }
}
