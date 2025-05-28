use std::collections::HashMap;

use rust_decimal::{prelude::FromPrimitive, Decimal, MathematicalOps};
use serde::{Deserialize, Serialize};
use step_ingestooor_sdk::dooot::ClmmTick;

use crate::ppl_graph::structs::{LiqAmount, LiqLevels};

pub type ClmmTickMap = HashMap<i32, Vec<ClmmTickParsed>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClmmTickParsed {
    pub liquidity_gross: u128,
    pub liquidity_net: i128,
    pub fee_growth_outside_a: u128,
    pub fee_growth_outside_b: u128,
}

impl TryFrom<&ClmmTick> for ClmmTickParsed {
    type Error = anyhow::Error;

    fn try_from(tick: &ClmmTick) -> Result<Self, Self::Error> {
        Ok(Self {
            liquidity_gross: tick.liquidity_gross.parse()?,
            liquidity_net: tick.liquidity_net.parse()?,
            fee_growth_outside_a: tick.fee_growth_outside_a.parse()?,
            fee_growth_outside_b: tick.fee_growth_outside_b.parse()?,
        })
    }
}

// For precision. Tone down this value if we suspect this is overflowing price calcs
const SCALE_FACTOR: u128 = 1_000_000;
const _SCALE_FACTOR_DECIMAL: Decimal = Decimal::from_parts(SCALE_FACTOR as u32, 0, 0, false, 0);

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
    sqrt_price_x64: &Option<u128>,
    usd_per_origin_units: &Decimal,
    decimals_a: u8,
    decimals_b: u8,
    is_reverse: bool,
) -> Option<Decimal> {
    let Some(sqrt_price_x64) = sqrt_price_x64 else {
        return None;
    };

    let sqrt_price_x64_scaled = sqrt_price_x64.checked_mul(SCALE_FACTOR)?;
    let sqrt_price_scaled = sqrt_price_x64_scaled >> 64;
    let sqrt_price_scaled_decimal = Decimal::from(sqrt_price_scaled);

    let price_scaled_decimal = sqrt_price_scaled_decimal.checked_powu(2)?;
    // Atoms B per atoms token A
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

/// `current_tick_index` must be the actual current tick index, not the tickarraystart index
pub fn get_clmm_liq_levels(
    ticks_by_index: &ClmmTickMap,
    current_tick_index: Option<i32>,
    tick_spacing: Option<i32>,
    origin_tokens_per_sol: &Decimal,
    is_reverse: bool,
    _decimals_a: u8,
    _decimals_b: u8,
) -> Option<LiqLevels> {
    let mut current_tick_index = current_tick_index?;
    let tick_spacing = tick_spacing?;
    let tick_step = if is_reverse { -1 } else { 1 };
    // Do this to avoid worrying about different tick arrays lengths per protocol
    let ticks_per_array = {
        let (_, example_array) = ticks_by_index.iter().next()?;
        example_array.len() as i32
    };
    let space_factor = ticks_per_array.checked_mul(tick_spacing)?;
    let tick_array_start_idx = get_tick_array_start_idx(current_tick_index, space_factor)?;
    let mut current_tick_array = ticks_by_index.get(&tick_array_start_idx)?;
    let mut offset_in_array = current_tick_index - tick_array_start_idx;
    let mut tick = current_tick_array.get(offset_in_array as usize)?;
    let current_price = Decimal::from_f32(sqrt_price_from_idx(current_tick_index)?.powi(2))?;
    // let decimal_factor = if is_reverse {
    //     Decimal::TEN.checked_powu(decimals_a as u64)?
    // } else {
    //     Decimal::TEN.checked_powu(decimals_b as u64)?
    // };

    let mut one_sol_tokens = origin_tokens_per_sol.floor();
    let mut one_sol_price_after = Decimal::ZERO;
    let mut ten_sol_tokens = one_sol_tokens.checked_mul(Decimal::TEN)?;
    let mut ten_sol_price_after = Decimal::ZERO;
    let mut thousand_sol_tokens = one_sol_tokens.checked_mul(Decimal::ONE_THOUSAND)?;
    let mut thousand_sol_price_after = Decimal::ZERO;

    let mut one_sol_price_set = false;
    let mut ten_sol_price_set = false;
    let mut thousand_sol_price_set = false;

    while thousand_sol_tokens > Decimal::ZERO {
        let amount_in = if one_sol_tokens > Decimal::ZERO {
            one_sol_tokens
        } else if ten_sol_tokens > Decimal::ZERO {
            ten_sol_tokens
        } else if thousand_sol_tokens > Decimal::ZERO {
            thousand_sol_tokens
        } else {
            break;
        };

        let (used, used_all_tick) = tick_swap(
            amount_in,
            current_tick_index,
            tick,
            tick_spacing,
            is_reverse,
        )?;

        if used <= Decimal::ZERO {
            break;
        }

        if used_all_tick {
            let next_arr_idx = offset_in_array + tick_step;

            if !is_reverse && next_arr_idx >= ticks_per_array {
                offset_in_array = 0;
                let Some(next_tick_index) =
                    next_tick_array_start_idx(current_tick_index, ticks_per_array, is_reverse)
                else {
                    break;
                };
                current_tick_index = next_tick_index;
            } else if is_reverse && next_arr_idx <= 0 {
                offset_in_array = ticks_per_array - 1;
                let Some(next_tick_index) =
                    next_tick_array_start_idx(current_tick_index, ticks_per_array, is_reverse)
                else {
                    break;
                };
                current_tick_index = next_tick_index;
            } else {
                offset_in_array = next_arr_idx;
            }

            let Some(new_tick_array) = ticks_by_index.get(&current_tick_index) else {
                break;
            };

            let Some(new_tick) = current_tick_array.get(offset_in_array as usize) else {
                break;
            };

            current_tick_array = new_tick_array;
            tick = new_tick;
        } else {
            // Could not use up the entire tick, so set impact to current price
            if amount_in == one_sol_tokens {
                let price = sqrt_price_from_idx(current_tick_index)?.powi(2);
                one_sol_price_after = Decimal::from_f32(price)?;
                one_sol_price_set = true;
                one_sol_tokens = Decimal::ZERO;
            } else if amount_in == ten_sol_tokens {
                let price = sqrt_price_from_idx(current_tick_index)?.powi(2);
                ten_sol_price_after = Decimal::from_f32(price)?;
                ten_sol_price_set = true;
                ten_sol_tokens = Decimal::ZERO;
            } else if amount_in == thousand_sol_tokens {
                let price = sqrt_price_from_idx(current_tick_index)?.powi(2);
                thousand_sol_price_after = Decimal::from_f32(price)?;
                thousand_sol_price_set = true;
                thousand_sol_tokens = Decimal::ZERO;
            }

            continue;
        };

        one_sol_tokens = one_sol_tokens.checked_sub(used)?.max(Decimal::ZERO);
        ten_sol_tokens = ten_sol_tokens.checked_sub(used)?.max(Decimal::ZERO);
        thousand_sol_tokens = thousand_sol_tokens.checked_sub(used)?.max(Decimal::ZERO);

        if !one_sol_price_set && one_sol_tokens <= Decimal::ZERO {
            let price = sqrt_price_from_idx(current_tick_index)?.powi(2);
            one_sol_price_after = Decimal::from_f32(price)?;
            one_sol_price_set = true;
        }

        if !ten_sol_price_set && ten_sol_tokens <= Decimal::ZERO {
            let price = sqrt_price_from_idx(current_tick_index)?.powi(2);
            ten_sol_price_after = Decimal::from_f32(price)?;
            ten_sol_price_set = true;
        }

        if !thousand_sol_price_set && thousand_sol_tokens <= Decimal::ZERO {
            let price = sqrt_price_from_idx(current_tick_index)?.powi(2);
            thousand_sol_price_after = Decimal::from_f32(price)?;
            thousand_sol_price_set = true;
        }
    }

    let one_sol_price_change = one_sol_price_after
        .checked_div(current_price)
        .and_then(|p| p.checked_sub(Decimal::ONE));
    let ten_sol_price_change = ten_sol_price_after
        .checked_div(current_price)
        .and_then(|p| p.checked_sub(Decimal::ONE));
    let thousand_sol_price_change = thousand_sol_price_after
        .checked_div(current_price)
        .and_then(|p| p.checked_sub(Decimal::ONE));

    Some(LiqLevels {
        one_sol_depth: one_sol_price_change,
        ten_sol_depth: ten_sol_price_change,
        thousand_sol_depth: thousand_sol_price_change,
    })
}

/// See https://dev.orca.so/Architecture%20Overview/Understanding%20Tick%20Arrays
fn get_tick_array_start_idx(tick_index: i32, space_factor: i32) -> Option<i32> {
    // These operations do not cancel out one another bc integer math. This is intended
    tick_index
        .checked_div(space_factor)?
        .checked_mul(space_factor)
}

fn next_tick_array_start_idx(tick_index: i32, space_factor: i32, is_reverse: bool) -> Option<i32> {
    let step = if is_reverse { -1 } else { 1 };

    let this_tick_array_start = get_tick_array_start_idx(tick_index, space_factor)?;

    this_tick_array_start.checked_add(space_factor * step)
}

/// Returns (amount_used, used_all)
fn tick_swap(
    amount_in: Decimal,
    this_tick_index: i32,
    tick: &ClmmTickParsed,
    tick_spacing: i32,
    is_reverse: bool,
) -> Option<(Decimal, bool)> {
    let step = if is_reverse { -1 } else { 1 };
    let this_sqrt_price = sqrt_price_from_idx(this_tick_index)?;
    let next_sqrt_price = sqrt_price_from_idx(this_tick_index + (tick_spacing * step))?;

    let (lower, upper) = if this_sqrt_price > next_sqrt_price {
        (next_sqrt_price, this_sqrt_price)
    } else {
        (this_sqrt_price, next_sqrt_price)
    };

    // is_reverse = swap in a, get b
    // !is_reverse = swap in b, get a

    let delta_param_in_token = if is_reverse {
        (1.0 / upper) - (1.0 / lower)
        // upper - lower
    } else {
        upper - lower
    };

    let delta_in = delta_param_in_token * tick.liquidity_gross as f32;
    let delta_in_decimal = Decimal::from_f32(delta_in)?;
    let used = delta_in_decimal.min(amount_in);
    let used_all_tick = used == delta_in_decimal;

    Some((used, used_all_tick))
}

fn sqrt_price_from_idx(tick_index: i32) -> Option<f32> {
    let result = 1.0001f32.powf(tick_index as f32).sqrt();

    if result.is_infinite() || result.is_nan() {
        return None;
    }

    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_array_start_idx() {
        let tick_index = 32;
        let ticks_per_array = 88;
        let tick_spacing = 8;
        let space_factor = tick_spacing * ticks_per_array;

        let expected = 0;
        let actual = get_tick_array_start_idx(tick_index, space_factor).unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn tick_array_start_idx_2() {
        let tick_index = -200;
        let ticks_per_array = 88;
        let tick_spacing = 2;
        let space_factor = tick_spacing * ticks_per_array;

        let expected = -176;
        let actual = get_tick_array_start_idx(tick_index, space_factor).unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn tick_array_start_idx_3() {
        let tick_index = 696 + 8;
        let ticks_per_array = 88;
        let tick_spacing = 8;
        let space_factor = tick_spacing * ticks_per_array;

        let expected = space_factor;
        let actual = get_tick_array_start_idx(tick_index, space_factor).unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn clmm_price() {
        // Taken from SOL/USDC CLMM on Orca
        let current_price_x64 = 7494844777637029369;
        let usd_per_origin_units = Decimal::from(1);
        let decimals_a = 9;
        let decimals_b = 6;
        let is_reverse = false;

        let price = get_clmm_price(
            &Some(current_price_x64),
            &usd_per_origin_units,
            decimals_a,
            decimals_b,
            is_reverse,
        )
        .unwrap();

        println!("price: {price:?}");
    }

    #[test]
    fn tick_swap_quote() {
        // https://www.orca.so/pools/8sm62ee94Y3vvYkgaE519f1YgmRu89UNy9gDCqP9gZJo
        // STEP/USDC pool
        let tick = ClmmTickParsed {
            fee_growth_outside_a: 0,
            fee_growth_outside_b: 0,
            liquidity_gross: 42070156853, // 30 STEP
            liquidity_net: 0,
        };

        let tick_spacing = 128;
        let start_idx = -101376;
        let arr_idx = 55;
        let tick_idx = start_idx - ((88 - arr_idx) * tick_spacing);
        let price = sqrt_price_from_idx(tick_idx).unwrap().powi(2);
        println!("tick_idx: {}", tick_idx);
        println!("price: {}", price);
        let (used, used_all_tick) = tick_swap(
            Decimal::from(30000000000u64),
            tick_idx,
            &tick,
            tick_spacing,
            true,
        )
        .unwrap();

        println!("used: {used:?}");
        println!("used_all_tick: {used_all_tick:?}");
    }
}
