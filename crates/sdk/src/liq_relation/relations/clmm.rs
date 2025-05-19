use std::collections::HashMap;

use rust_decimal::{Decimal, MathematicalOps};
use serde::{Deserialize, Serialize};

use crate::ppl_graph::structs::LiqAmount;

pub type ClmmTickMap = HashMap<i32, ClmmTickParsed>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClmmTickParsed {}

// For precision. Tone down this value if we suspect this is overflowing price calcs
const SCALE_FACTOR: u128 = 1_000_000;

const SF_SQUARED_LO_BYTES: [u8; 4] = [0, 16, 165, 212];
const SF_SQUARED_MID_BYTES: [u8; 4] = [232, 0, 0, 0];
const SCALE_FACTOR_DECIMAL_SQUARED: Decimal = Decimal::from_parts(
    u32::from_le_bytes(SF_SQUARED_LO_BYTES),
    u32::from_le_bytes(SF_SQUARED_MID_BYTES),
    0,
    false,
    0,
);

pub fn get_clmm_price(
    sqrt_price_x64: u128,
    usd_per_origin_units: &Decimal,
    decimals_a: u8,
    decimals_b: u8,
    is_reverse: bool,
) -> Option<Decimal> {
    let sqrt_price_x64_scaled = sqrt_price_x64.checked_mul(SCALE_FACTOR)?;
    let sqrt_price_scaled = sqrt_price_x64_scaled >> 64;
    let sqrt_price_scaled_decimal = Decimal::from(sqrt_price_scaled);

    let price_scaled_decimal = sqrt_price_scaled_decimal.checked_powu(2)?;
    // Units atoms B per atoms token A
    let mut price_decimal = price_scaled_decimal.checked_div(SCALE_FACTOR_DECIMAL_SQUARED)?;

    let decimal_factor = if !is_reverse {
        // "Normal" case, B prices A (e.g., SOL/USDC, USDC [b] would price SOL [a])
        Decimal::from(10).checked_powi(decimals_a as i64 - decimals_b as i64)?
    } else {
        // Reverse case, flip the price
        price_decimal = Decimal::ONE.checked_div(price_decimal)?;
        Decimal::from(10).checked_powi(decimals_b as i64 - decimals_a as i64)?
    };

    let price_units = price_decimal.checked_mul(decimal_factor)?;

    price_units.checked_mul(*usd_per_origin_units)
}

pub fn get_clmm_liquidity(
    amt_source: &Decimal,
    amt_dest: &Decimal,
    price_source_usd: Decimal,
    price_dest_usd: Decimal,
) -> Option<LiqAmount> {
    let liq_origin = amt_source.checked_mul(price_source_usd)?;
    let liq_dest = amt_dest.checked_mul(price_dest_usd)?;
    // Just allow to max out. One less failure point
    let total_liq = liq_origin.saturating_add(liq_dest);

    Some(LiqAmount::Amount(total_liq))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clmm_price() {
        // Taken from SOL/USDC CLMM on Orca
        let current_price_x64 = 7494844777637029369;
        let usd_per_origin_units = Decimal::from(1);
        let decimals_a = 9;
        let decimals_b = 6;
        let is_reverse = false;

        let price = get_clmm_price(
            current_price_x64,
            &usd_per_origin_units,
            decimals_a,
            decimals_b,
            is_reverse,
        )
        .unwrap();

        println!("price: {price:?}");
    }
}
