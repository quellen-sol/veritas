use std::collections::HashMap;

use rust_decimal::{Decimal, MathematicalOps};
use step_ingestooor_sdk::dooot::DLMMPart;

use crate::ppl_graph::structs::{LiqAmount, LiqLevels};

pub fn get_dlmm_price(
    decimals_x: u8,
    decimals_y: u8,
    usd_price_origin: &Decimal,
    bins_by_account: &HashMap<String, Vec<DLMMPart>>,
    active_bin: &Option<(String, usize)>,
    is_reverse: bool,
) -> Option<Decimal> {
    let (bin_arr_pk, ix) = active_bin.as_ref()?;
    let bin_arr = bins_by_account.get(bin_arr_pk)?;
    let bin = &bin_arr[*ix];

    let x_factor = Decimal::TEN.powu(decimals_x as u64);
    let y_factor = Decimal::TEN.powu(decimals_y as u64);

    let x_units = bin.token_amounts[0].checked_div(x_factor)?;
    let y_units = bin.token_amounts[1].checked_div(y_factor)?;

    let price = x_units.checked_div(y_units)?;
    let price = if is_reverse {
        Decimal::ONE.checked_div(price)?
    } else {
        price
    };

    let final_price = price.checked_mul(*usd_price_origin)?;

    Some(final_price)
}

pub fn get_dlmm_liq_levels(
    _amt_a: &Decimal,
    _amt_b: &Decimal,
    _tokens_per_sol: &Decimal,
) -> Option<LiqLevels> {
    Some(LiqLevels::ZERO)
}

pub fn get_dlmm_liquidity(
    amt_a: &Decimal,
    amt_b: &Decimal,
    price_source_usd: Decimal,
    price_dest_usd: Decimal,
) -> Option<LiqAmount> {
    let liq_origin = amt_a.checked_mul(price_source_usd)?;
    let liq_dest = amt_b.checked_mul(price_dest_usd)?;
    // Just allow to max out. One less failure point
    let total_liq = liq_origin.saturating_add(liq_dest);

    Some(LiqAmount::Amount(total_liq))
}
