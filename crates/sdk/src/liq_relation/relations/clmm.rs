use std::collections::HashMap;

use primitive_types::U256;
use rust_decimal::{prelude::FromPrimitive, Decimal, MathematicalOps};
use serde::{Deserialize, Serialize};
use step_ingestooor_sdk::dooot::ClmmTick;

use crate::{
    ppl_graph::structs::{LiqAmount, LiqLevels},
    utils::traits::u256_helper::StepU256Helper,
};

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
    let Some(sqrt_price_x64) = sqrt_price_x64 else {
        return None;
    };

    let sqrt_price_x64_scaled = sqrt_price_x64.checked_mul(SCALE_FACTOR_U128)?;
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
    decimals_a: u8,
    decimals_b: u8,
) -> Option<LiqLevels> {
    let mut current_tick_index = current_tick_index?;
    let tick_spacing = tick_spacing?;
    // Do this to avoid worrying about different tick arrays lengths per protocol
    let ticks_per_array = {
        let (_, example_array) = ticks_by_index.iter().next()?;
        example_array.len() as i32
    };
    let space_factor = ticks_per_array.checked_mul(tick_spacing)?;
    println!("space_factor: {space_factor}");
    let tick_array_start_idx = get_tick_array_start_idx(current_tick_index, space_factor)?;
    println!("tick_array_start_idx: {tick_array_start_idx}");
    let mut current_tick_array = ticks_by_index.get(&tick_array_start_idx)?;
    println!("current_tick_array exists");
    let offset_in_array =
        get_tick_array_offset(current_tick_index, tick_array_start_idx, tick_spacing);
    println!("offset_in_array: {offset_in_array}");
    let mut tick = current_tick_array.get(offset_in_array)?;
    println!("tick exists");
    let current_price = price_from_tick_index(current_tick_index)?;
    println!("current_price: {current_price}");
    let decimal_factor = if is_reverse {
        Decimal::TEN.checked_powu(decimals_a as u64)?
    } else {
        Decimal::TEN.checked_powu(decimals_b as u64)?
    };
    println!("decimal_factor: {decimal_factor}");

    let mut one_sol_tokens: u64 = origin_tokens_per_sol
        .checked_mul(decimal_factor)?
        .floor()
        .try_into()
        .ok()?;
    println!("one_sol_tokens: {one_sol_tokens}");
    let mut ten_sol_tokens = one_sol_tokens.checked_mul(10)?;
    println!("ten_sol_tokens: {ten_sol_tokens}");
    let mut thousand_sol_tokens = one_sol_tokens.checked_mul(1_000)?;
    println!("thousand_sol_tokens: {thousand_sol_tokens}");

    let mut one_sol_price_after: Option<Decimal> = None;
    let mut ten_sol_price_after = None;
    let mut thousand_sol_price_after = None;

    let mut one_sol_price_set = false;
    let mut ten_sol_price_set = false;
    let mut thousand_sol_price_set = false;

    while thousand_sol_tokens > 0 {
        let amount_in = if one_sol_tokens > 0 {
            one_sol_tokens
        } else if ten_sol_tokens > 0 {
            ten_sol_tokens
        } else {
            thousand_sol_tokens
        };

        let (used, used_all_tick) = tick_swap(
            amount_in,
            current_tick_index,
            tick,
            tick_spacing,
            is_reverse,
        )?;
        println!("used: {used}");

        if used == 0 {
            println!("used == 0, breaking");
            break;
        }

        if used_all_tick {
            let next_tick_index =
                get_very_next_tick_index(current_tick_index, tick_spacing, is_reverse)?;
            let next_tick_array_start_idx =
                get_tick_array_start_idx(next_tick_index, space_factor)?;
            let next_offset_in_array =
                get_tick_array_offset(next_tick_index, next_tick_array_start_idx, tick_spacing);

            let Some(next_tick_array) = ticks_by_index.get(&current_tick_index) else {
                println!("new_tick_array returned None");
                break;
            };

            let Some(next_tick) = current_tick_array.get(next_offset_in_array as usize) else {
                println!("new_tick returned None");
                break;
            };

            current_tick_index = next_tick_index;
            current_tick_array = next_tick_array;
            tick = next_tick;
        } else {
            // Could not use up the entire tick, so set impact to current price
            if amount_in == one_sol_tokens && !one_sol_price_set {
                println!("amount_in == one_sol_tokens");
                one_sol_price_after = Some(price_from_tick_index(current_tick_index)?);
                println!("one_sol_price_after: {:?}", one_sol_price_after);
                one_sol_price_set = true;
                one_sol_tokens = 0;
            } else if amount_in == ten_sol_tokens && !ten_sol_price_set {
                println!("amount_in == ten_sol_tokens");
                ten_sol_price_after = Some(price_from_tick_index(current_tick_index)?);
                println!("ten_sol_price_after: {:?}", ten_sol_price_after);
                ten_sol_price_set = true;
                ten_sol_tokens = 0;
            } else if amount_in == thousand_sol_tokens && !thousand_sol_price_set {
                println!("amount_in == thousand_sol_tokens");
                thousand_sol_price_after = Some(price_from_tick_index(current_tick_index)?);
                println!("thousand_sol_price_after: {:?}", thousand_sol_price_after);
                thousand_sol_price_set = true;
                thousand_sol_tokens = 0;
            }

            continue;
        };

        println!("one_sol_tokens pre: {one_sol_tokens}");
        println!("ten_sol_tokens pre: {ten_sol_tokens}");
        println!("thousand_sol_tokens pre: {thousand_sol_tokens}");

        one_sol_tokens = one_sol_tokens.saturating_sub(used);
        ten_sol_tokens = ten_sol_tokens.saturating_sub(used);
        thousand_sol_tokens = thousand_sol_tokens.saturating_sub(used);

        println!("one_sol_tokens post: {one_sol_tokens}");
        println!("ten_sol_tokens post: {ten_sol_tokens}");
        println!("thousand_sol_tokens post: {thousand_sol_tokens}");

        if !one_sol_price_set && one_sol_tokens == 0 {
            one_sol_price_after = Some(price_from_tick_index(current_tick_index)?);
            println!("one_sol_price_after: {:?}", one_sol_price_after);
            one_sol_price_set = true;
        }

        if !ten_sol_price_set && ten_sol_tokens == 0 {
            ten_sol_price_after = Some(price_from_tick_index(current_tick_index)?);
            println!("ten_sol_price_after: {:?}", ten_sol_price_after);
            ten_sol_price_set = true;
        }

        if !thousand_sol_price_set && thousand_sol_tokens == 0 {
            thousand_sol_price_after = Some(price_from_tick_index(current_tick_index)?);
            println!("thousand_sol_price_after: {:?}", thousand_sol_price_after);
            thousand_sol_price_set = true;
        }
    }

    let one_sol_price_change = one_sol_price_after.and_then(|p| {
        p.checked_div(current_price)
            .and_then(|p| p.checked_sub(Decimal::ONE))
    });
    let ten_sol_price_change = ten_sol_price_after.and_then(|p| {
        p.checked_div(current_price)
            .and_then(|p| p.checked_sub(Decimal::ONE))
    });
    let thousand_sol_price_change = thousand_sol_price_after.and_then(|p| {
        p.checked_div(current_price)
            .and_then(|p| p.checked_sub(Decimal::ONE))
    });

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

/// Returns (amount_used, used_all)
fn tick_swap(
    amount_in: u64,
    this_tick_index: i32,
    tick: &ClmmTickParsed,
    tick_spacing: i32,
    is_reverse: bool,
) -> Option<(u64, bool)> {
    let this_sqrt_price = sqrt_price_from_tick_index(this_tick_index)?;
    let next_tick_index = get_very_next_tick_index(this_tick_index, tick_spacing, is_reverse)?;
    let next_sqrt_price = sqrt_price_from_tick_index(next_tick_index)?;

    // Max tokens in that can be used to get to next tick
    let max_in = get_liq_delta(
        this_sqrt_price,
        next_sqrt_price,
        tick.liquidity_gross,
        is_reverse,
    )?
    .extract_value();
    // Absolute amount of tokens used
    let absolute_used = amount_in.min(max_in);

    Some((absolute_used, absolute_used == max_in))
}

const U64_MAX_U256: U256 = U256([u64::MAX, 0, 0, 0]);
enum U64DeltaAmt {
    Valid(u64),
    ExceedsMax,
}

impl U64DeltaAmt {
    pub fn extract_value(self) -> u64 {
        match self {
            U64DeltaAmt::Valid(v) => v,
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
        let liq_shifted = U256::from(liquidity) << 64;
        let diff_u256 = U256::from(diff);
        let denom = U256::from(upper).checked_mul(U256::from(lower))?;
        let result = liq_shifted.checked_mul(diff_u256)?.checked_div(denom)?;
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
        let tick1 = ClmmTickParsed {
            fee_growth_outside_a: 0,
            fee_growth_outside_b: 0,
            liquidity_gross: 1,
            liquidity_net: 0,
        };
        // let tick1 = ClmmTickParsed {
        //     fee_growth_outside_a: 0,
        //     fee_growth_outside_b: 0,
        //     liquidity_gross: 125999621768,
        //     liquidity_net: 0,
        // };
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
