use rust_decimal::Decimal;

use crate::ppl_graph::structs::LiqAmount;

pub fn get_fixed_ref_price(
    usd_price_origin: &Decimal,
    amt_parent_per_dest: &Decimal,
) -> Option<Decimal> {
    usd_price_origin.checked_mul(*amt_parent_per_dest)
}

pub fn get_fixed_ref_liquidity(
    amt_parent: &Decimal,
    price_source_usd: &Decimal,
) -> Option<LiqAmount> {
    Some(LiqAmount::Amount(
        price_source_usd.checked_mul(*amt_parent)?,
    ))
}
