use std::collections::HashMap;

use primitive_types::U256;
use rust_decimal::{prelude::FromPrimitive, Decimal, MathematicalOps};
use serde::{Deserialize, Serialize};
use step_ingestooor_sdk::dooot::ClmmTick;

use crate::{
    ppl_graph::structs::{LiqAmount, LiqLevels},
    utils::traits::u256_helper::StepU256Helper,
};

/// The minimum sqrt price for a whirlpool.
pub const MIN_SQRT_PRICE: u128 = 4295048016;
/// The maximum sqrt price for a whirlpool.
pub const MAX_SQRT_PRICE: u128 = 79226673515401279992447579055;

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
const SCALE_FACTOR_U128: u128 = 1_000_000;

const SF_SQUARED_LO_BYTES: [u8; 4] = [0, 16, 165, 212];
const SF_SQUARED_MID_BYTES: [u8; 4] = [232, 0, 0, 0];
const SCALE_FACTOR_DECIMAL_SQUARED: Decimal = Decimal::from_parts(
    u32::from_le_bytes(SF_SQUARED_LO_BYTES),
    u32::from_le_bytes(SF_SQUARED_MID_BYTES),
    0,
    false,
    0,
);

const SCALE_FACTOR_F64: f64 = 1_000_000.0;
const SCALE_FACTOR_F64_SQUARED: f64 = SCALE_FACTOR_F64 * SCALE_FACTOR_F64;
const SCALE_FACTOR_U128_SQAURED: u128 = SCALE_FACTOR_U128 * SCALE_FACTOR_U128;

pub fn get_clmm_price(
    sqrt_price_x64: &Option<u128>,
    usd_per_origin_units: &Decimal,
    decimals_a: u8,
    decimals_b: u8,
    is_reverse: bool,
) -> Option<Decimal> {
    let sqrt_price_x64 = sqrt_price_x64.as_ref()?;

    let price_units = sqrt_price_to_decimal_price(*sqrt_price_x64, decimals_a, decimals_b)?;

    let final_price = price_units.checked_mul(*usd_per_origin_units)?;

    if is_reverse {
        Decimal::ONE.checked_div(final_price)
    } else {
        Some(final_price)
    }
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
    decimals_a: u8,
    decimals_b: u8,
) -> Option<LiqLevels> {
    let tick_spacing = tick_spacing?;
    let mut current_tick_index = current_tick_index?;
    let ticks_per_array = { ticks_by_index.iter().next()?.1.len().try_into().ok()? };
    let space_factor = tick_spacing * ticks_per_array;
    let array_offset = get_tick_array_offset(current_tick_index, tick_spacing, ticks_per_array);
    let mut current_tick = ticks_by_index.get(&current_tick_index)?.get(array_offset)?;
    let mut current_sqrt_price = sqrt_price_from_tick_index(current_tick_index)?;
    let current_pool_decimal_price =
        sqrt_price_to_decimal_price(current_sqrt_price, decimals_a, decimals_b)?;
    let sqrt_price_limit = if is_reverse {
        MIN_SQRT_PRICE
    } else {
        MAX_SQRT_PRICE
    };
    let decimal_factor = if is_reverse {
        Decimal::from(10).checked_powi(decimals_a as i64)?
    } else {
        Decimal::from(10).checked_powi(decimals_b as i64)?
    };

    let mut one_sol_tokens: u64 = origin_tokens_per_sol
        .checked_mul(decimal_factor)?
        .floor()
        .try_into()
        .ok()?;
    let mut ten_sol_tokens = one_sol_tokens.checked_mul(10)?;
    let mut thousand_sol_tokens = ten_sol_tokens.checked_mul(100)?;

    let mut one_sol_price_after = None;
    let mut ten_sol_price_after = None;
    let mut thousand_sol_price_after = None;

    let mut one_sol_price_set = false;
    let mut ten_sol_price_set = false;

    'outer: while thousand_sol_tokens > 0 && sqrt_price_limit != current_sqrt_price {
        let mut amount_to_use = if one_sol_tokens > 0 {
            one_sol_tokens
        } else if ten_sol_tokens > 0 {
            ten_sol_tokens
        } else {
            thousand_sol_tokens
        };

        let next_tick_index =
            get_very_next_tick_index(current_tick_index, tick_spacing, is_reverse)?;
        let next_start_index = get_tick_array_start_idx(next_tick_index, space_factor)?;

        let next_array_offset =
            get_tick_array_offset(next_tick_index, tick_spacing, ticks_per_array);
        let next_tick = ticks_by_index
            .get(&next_start_index)
            .and_then(|v| v.get(next_array_offset));

        let next_tick_sqrt_price = sqrt_price_from_tick_index(next_tick_index)?;

        let target_sqrt_price = if is_reverse {
            next_tick_sqrt_price.max(sqrt_price_limit)
        } else {
            next_tick_sqrt_price.min(sqrt_price_limit)
        };

        'inner: loop {
            let (amount_used, next_sqrt_price) = tick_swap_step(
                amount_to_use,
                current_tick.liquidity_gross,
                current_sqrt_price,
                target_sqrt_price,
                is_reverse,
            )?;

            one_sol_tokens = one_sol_tokens.saturating_sub(amount_used);
            ten_sol_tokens = ten_sol_tokens.saturating_sub(amount_used);
            thousand_sol_tokens = thousand_sol_tokens.saturating_sub(amount_used);
            amount_to_use = amount_to_use.saturating_sub(amount_used);

            if next_sqrt_price == next_tick_sqrt_price {
                let Some(next_tick) = next_tick else {
                    break 'outer;
                };

                current_tick = next_tick;
                current_tick_index = next_tick_index;
            } else {
                current_tick_index = sqrt_price_to_tick_index(next_sqrt_price);
            }
            current_sqrt_price = next_sqrt_price;

            if !one_sol_price_set && one_sol_tokens == 0 {
                one_sol_price_after =
                    sqrt_price_to_decimal_price(current_sqrt_price, decimals_a, decimals_b);
                one_sol_price_set = true;
            }

            if !ten_sol_price_set && ten_sol_tokens == 0 {
                ten_sol_price_after =
                    sqrt_price_to_decimal_price(current_sqrt_price, decimals_a, decimals_b);
                ten_sol_price_set = true;
            }

            if thousand_sol_tokens == 0 {
                thousand_sol_price_after =
                    sqrt_price_to_decimal_price(current_sqrt_price, decimals_a, decimals_b);
                break 'outer;
            }

            if amount_to_use == 0 || current_sqrt_price == target_sqrt_price {
                break 'inner;
            }
        }
    }

    let one_sol_price_impact = one_sol_price_after.and_then(|p| {
        p.checked_div(current_pool_decimal_price)?
            .checked_sub(Decimal::ONE)
    });
    let ten_sol_price_impact = ten_sol_price_after.and_then(|p| {
        p.checked_div(current_pool_decimal_price)?
            .checked_sub(Decimal::ONE)
    });
    let thousand_sol_price_impact = thousand_sol_price_after.and_then(|p| {
        p.checked_div(current_pool_decimal_price)?
            .checked_sub(Decimal::ONE)
    });

    Some(LiqLevels {
        one_sol_depth: one_sol_price_impact,
        ten_sol_depth: ten_sol_price_impact,
        thousand_sol_depth: thousand_sol_price_impact,
    })
}

/// See https://dev.orca.so/Architecture%20Overview/Understanding%20Tick%20Arrays
fn get_tick_array_start_idx(tick_index: i32, space_factor: i32) -> Option<i32> {
    // These operations do not cancel out one another bc integer math. This is intended
    tick_index
        .checked_div(space_factor)?
        .checked_mul(space_factor)
}

fn get_tick_array_offset(tick_index: i32, tick_spacing: i32, ticks_per_array: i32) -> usize {
    ((tick_index / tick_spacing) % ticks_per_array).unsigned_abs() as usize
}

fn get_very_next_tick_index(
    current_tick_index: i32,
    tick_spacing: i32,
    is_reverse: bool,
) -> Option<i32> {
    let step = if is_reverse { -1 } else { 1 };

    (tick_spacing * step).checked_add(current_tick_index)
}

/// Returns (amount_used, next_sqrt_price)
fn tick_swap_step(
    amount_remaining: u64,
    current_liq: u128,
    current_sqrt_price: u128,
    final_sqrt_price: u128,
    is_reverse: bool,
) -> Option<(u64, u128)> {
    let initial_delta = get_liq_delta(
        current_sqrt_price,
        final_sqrt_price,
        current_liq,
        is_reverse,
    )?;

    let overflowed = matches!(initial_delta, U64DeltaAmt::ExceedsMax);
    let amount_calculated = amount_remaining;

    let next_sqrt_price = if !overflowed && initial_delta.extract_value() <= amount_calculated {
        final_sqrt_price
    } else {
        get_new_price_from_amount_in(
            amount_calculated,
            current_sqrt_price,
            current_liq,
            is_reverse,
        )?
    };

    let is_max_swap = next_sqrt_price == final_sqrt_price;

    let amount_fixed_delta = if !is_max_swap || overflowed {
        get_liq_delta(current_sqrt_price, next_sqrt_price, current_liq, is_reverse)?
    } else {
        initial_delta
    };

    Some((amount_fixed_delta.extract_value(), next_sqrt_price))
}

const U64_MAX_U256: U256 = U256([u64::MAX, 0, 0, 0]);
enum U64DeltaAmt {
    Valid(u64),
    ExceedsMax,
}

impl U64DeltaAmt {
    pub fn extract_value(&self) -> u64 {
        match self {
            U64DeltaAmt::Valid(v) => *v,
            U64DeltaAmt::ExceedsMax => u64::MAX,
        }
    }
}

fn get_liq_delta(
    current_sqrt_price: u128,
    next_sqrt_price: u128,
    liquidity: u128,
    is_reverse: bool,
) -> Option<U64DeltaAmt> {
    let (lower, upper) = if current_sqrt_price > next_sqrt_price {
        (next_sqrt_price, current_sqrt_price)
    } else {
        (current_sqrt_price, next_sqrt_price)
    };
    let diff = upper.checked_sub(lower)?;

    if is_reverse {
        let numerator = U256::from(liquidity)
            .checked_mul(U256::from(diff))?
            .checked_shift_word_left()?;
        let denom = U256::from(upper).checked_mul(U256::from(lower))?;
        let result = numerator.checked_div(denom)?;
        if result > U64_MAX_U256 {
            return Some(U64DeltaAmt::ExceedsMax);
        }

        Some(U64DeltaAmt::Valid(result.as_u64()))
    } else {
        let delta = liquidity.checked_mul(diff)?;
        let token_amt = delta.checked_shr(64)?.try_into().ok()?;

        Some(U64DeltaAmt::Valid(token_amt))
    }
}

fn get_new_price_from_amount_in(
    amount_in: u64,
    current_sqrt_price: u128,
    liquidity: u128,
    is_reverse: bool,
) -> Option<u128> {
    if is_reverse {
        let price_u256 = U256::from(current_sqrt_price);
        let liq_u256 = U256::from(liquidity);
        let product = price_u256.checked_mul(U256::from(amount_in))?;
        let numerator = liq_u256
            .checked_mul(price_u256)?
            .checked_shift_word_left()?;
        let liq_shifted = liq_u256.shift_word_left();
        let denom = liq_shifted.checked_add(product)?;

        numerator.checked_div(denom)?.try_into().ok()
    } else {
        let amount_shifted = (amount_in as u128) << 64;
        let delta = amount_shifted.checked_div(liquidity)?;
        current_sqrt_price.checked_add(delta)
    }
}

fn sqrt_price_from_tick_index(tick: i32) -> Option<u128> {
    let t = 1.0001f64.powf(tick as f64).sqrt();
    let t_scaled = t * SCALE_FACTOR_F64_SQUARED;
    if t_scaled.is_infinite() || t_scaled.is_nan() {
        return None;
    }
    let t_scaled_int = t_scaled as u128;
    let t_scaled_int_shifted = t_scaled_int.checked_shl(64)?;
    let t_int_shifted = t_scaled_int_shifted.checked_div(SCALE_FACTOR_U128_SQAURED)?;

    Some(t_int_shifted)
}

/// Returns price in atoms of token A per atoms of token B
fn price_from_tick_index(tick: i32) -> Option<Decimal> {
    let f64_price = 1.0001f64.powf(tick as f64);
    Decimal::from_f64(f64_price)
}

const LOG_B_2_X32: i128 = 59543866431248i128;
const BIT_PRECISION: u32 = 14;
const LOG_B_P_ERR_MARGIN_LOWER_X64: i128 = 184467440737095516i128; // 0.01
const LOG_B_P_ERR_MARGIN_UPPER_X64: i128 = 15793534762490258745i128; // 2^-precision / log_2_b + 0.01

pub fn sqrt_price_to_tick_index(sqrt_price: u128) -> i32 {
    let sqrt_price_x64: u128 = sqrt_price;
    // Determine log_b(sqrt_ratio). First by calculating integer portion (msb)
    let msb: u32 = 128 - sqrt_price_x64.leading_zeros() - 1;
    let log2p_integer_x32 = (msb as i128 - 64) << 32;

    // get fractional value (r/2^msb), msb always > 128
    // We begin the iteration from bit 63 (0.5 in Q64.64)
    let mut bit: i128 = 0x8000_0000_0000_0000i128;
    let mut precision = 0;
    let mut log2p_fraction_x64 = 0;

    // Log2 iterative approximation for the fractional part
    // Go through each 2^(j) bit where j < 64 in a Q64.64 number
    // Append current bit value to fraction result if r^2 Q2.126 is more than 2
    let mut r = if msb >= 64 {
        sqrt_price_x64 >> (msb - 63)
    } else {
        sqrt_price_x64 << (63 - msb)
    };

    while bit > 0 && precision < BIT_PRECISION {
        r *= r;
        let is_r_more_than_two = r >> 127_u32;
        r >>= 63 + is_r_more_than_two;
        log2p_fraction_x64 += bit * is_r_more_than_two as i128;
        bit >>= 1;
        precision += 1;
    }

    let log2p_fraction_x32 = log2p_fraction_x64 >> 32;
    let log2p_x32 = log2p_integer_x32 + log2p_fraction_x32;

    // Transform from base 2 to base b
    let logbp_x64 = log2p_x32 * LOG_B_2_X32;

    // Derive tick_low & high estimate. Adjust with the possibility of under-estimating by 2^precision_bits/log_2(b) + 0.01 error margin.
    let tick_low: i32 = ((logbp_x64 - LOG_B_P_ERR_MARGIN_LOWER_X64) >> 64) as i32;
    let tick_high: i32 = ((logbp_x64 + LOG_B_P_ERR_MARGIN_UPPER_X64) >> 64) as i32;

    if tick_low == tick_high {
        tick_low
    } else {
        // If our estimation for tick_high returns a lower sqrt_price than the input
        // then the actual tick_high has to be higher than than tick_high.
        // Otherwise, the actual value is between tick_low & tick_high, so a floor value
        // (tick_low) is returned
        let actual_tick_high_sqrt_price_x64: u128 = sqrt_price_from_tick_index(tick_high).unwrap();
        if actual_tick_high_sqrt_price_x64 <= sqrt_price_x64 {
            tick_high
        } else {
            tick_low
        }
    }
}

fn sqrt_price_to_decimal_price(
    sqrt_price: u128,
    decimals_a: u8,
    decimals_b: u8,
) -> Option<Decimal> {
    let sqrt_price_x64_scaled = sqrt_price.checked_mul(SCALE_FACTOR_U128)?;
    let sqrt_price_scaled = sqrt_price_x64_scaled >> 64;
    let sqrt_price_scaled_decimal = Decimal::from(sqrt_price_scaled);

    let price_scaled_decimal = sqrt_price_scaled_decimal.checked_powu(2)?;
    // Atoms B per atoms token A
    let price_decimal = price_scaled_decimal.checked_div(SCALE_FACTOR_DECIMAL_SQUARED)?;

    // "Normal" case, B prices A (e.g., SOL/USDC, USDC [b] would price SOL [a])
    let decimal_factor = Decimal::from(10).checked_powi(decimals_a as i64 - decimals_b as i64)?;

    let price_units = price_decimal.checked_mul(decimal_factor)?;

    Some(price_units)
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
    fn tick_array_offset() {
        let index_pos = 356;
        let index_neg = -356;
        let ticks_per_array = 88;
        let tick_spacing = 4;
        let expected_pos = 1;
        let expected_neg = 1;

        let actual_pos = get_tick_array_offset(index_pos, tick_spacing, ticks_per_array);
        let actual_neg = get_tick_array_offset(index_neg, tick_spacing, ticks_per_array);

        assert_eq!(actual_pos, expected_pos);
        assert_eq!(actual_neg, expected_neg);
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
    fn delta_b_to_a() {
        // https://www.orca.so/pools/8sm62ee94Y3vvYkgaE519f1YgmRu89UNy9gDCqP9gZJo
        // STEP/USDC pool
        let is_reverse = false;
        let liq = 42070156853u128; // 30 STEP
        let sqrt_price = 165962079459892856u128; // 12.5 STEP/USDC
        let tick_index = -94223;
        let next_sqrt_price = sqrt_price_from_tick_index(
            get_very_next_tick_index(tick_index, 128, is_reverse).unwrap(),
        )
        .unwrap();
        let amount_in = 2000000u64; // 2 USDC, but ~2.4 should use up all liq

        let token_delta = get_liq_delta(sqrt_price, next_sqrt_price, liq, is_reverse)
            .unwrap()
            .extract_value();

        let new_price =
            get_new_price_from_amount_in(amount_in, sqrt_price, liq, is_reverse).unwrap();

        let used_all_tokens = token_delta <= amount_in;
        let price_exceeded_next = new_price > next_sqrt_price;

        println!("Liq Delta: {token_delta}");
        println!("Price Delta: {new_price}");
        println!("Used all tokens: {used_all_tokens}");
        println!("Price exceeded next: {price_exceeded_next}");
    }

    #[test]
    fn delta_a_to_b() {
        // https://www.orca.so/pools/8sm62ee94Y3vvYkgaE519f1YgmRu89UNy9gDCqP9gZJo
        // STEP/USDC pool
        let is_reverse = true; // trading STEP for USDC
        let liq = 42070156853u128; // ~30 STEP
        let tick_index = -94223;
        let sqrt_price = sqrt_price_from_tick_index(tick_index).unwrap();
        let next_sqrt_price = sqrt_price_from_tick_index(
            get_very_next_tick_index(tick_index, 128, is_reverse).unwrap(),
        )
        .unwrap();
        let amount_in = 5000000000u64;

        let token_delta = get_liq_delta(sqrt_price, next_sqrt_price, liq, is_reverse)
            .unwrap()
            .extract_value();

        let new_price =
            get_new_price_from_amount_in(amount_in, sqrt_price, liq, is_reverse).unwrap();

        let used_all_tokens = token_delta <= amount_in;
        let price_exceeded_next = new_price > next_sqrt_price;

        println!("Liq Delta: {token_delta}");
        println!("Price Delta: {new_price}");
        println!("Used all tokens: {used_all_tokens}");
        println!("Price exceeded next: {price_exceeded_next}");
    }

    #[test]
    fn full_swap() {
        // https://www.orca.so/pools/Czfq3xZZDmsdGdUyrNLtRhGc47cXcZtLG4crryfu44zE
        // SOL/USDC pool
        // let tick1 = ClmmTickParsed {
        //     fee_growth_outside_a: 0,
        //     fee_growth_outside_b: 0,
        //     liquidity_gross: 1,
        //     liquidity_net: 0,
        // };
        let tick1 = ClmmTickParsed {
            fee_growth_outside_a: 0,
            fee_growth_outside_b: 0,
            liquidity_gross: 125999621768,
            liquidity_net: 0,
        };
        let tick2 = ClmmTickParsed {
            fee_growth_outside_a: 0,
            fee_growth_outside_b: 0,
            liquidity_gross: 666655118046,
            liquidity_net: 0,
        };
        let tick3 = ClmmTickParsed {
            fee_growth_outside_a: 0,
            fee_growth_outside_b: 0,
            liquidity_gross: 452727587020,
            liquidity_net: 0,
        };
        let tick4 = ClmmTickParsed {
            fee_growth_outside_a: 0,
            fee_growth_outside_b: 0,
            liquidity_gross: 87502502639,
            liquidity_net: 0,
        };
        let mut tick_map = ClmmTickMap::new();
        tick_map.insert(-16544, vec![tick1, tick2, tick3, tick4]);

        let liq_levels = get_clmm_liq_levels(
            &tick_map,
            Some(-16544),
            Some(4),
            &Decimal::from(1), // USDC price
            false,
            9,
            6,
        )
        .unwrap();

        println!("Liq levels: {liq_levels:?}");
    }
}
