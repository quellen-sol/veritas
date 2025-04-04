use rust_decimal::Decimal;

use crate::ppl_graph::structs::{LiqAmount, LiqLevels};

pub fn get_dlmm_price(
    amt_origin: &Decimal,
    amt_dest: &Decimal,
    usd_price_origin: &Decimal,
) -> Option<Decimal> {
    None
}

pub fn get_dlmm_liq_levels(
    amt_a: &Decimal,
    amt_b: &Decimal,
    tokens_per_sol: &Decimal,
) -> Option<LiqLevels> {
    None
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
