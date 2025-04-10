use rust_decimal::Decimal;

use crate::ppl_graph::structs::{LiqAmount, LiqLevels};

pub fn get_fixed_price(amt_per_parent: &Decimal, usd_price_origin: &Decimal) -> Option<Decimal> {
    amt_per_parent.checked_mul(*usd_price_origin)
}

pub const fn get_fixed_liq_levels() -> Option<LiqLevels> {
    Some(LiqLevels::ZERO)
}

pub const fn get_fixed_liquidity() -> Option<LiqAmount> {
    Some(LiqAmount::Inf)
}
