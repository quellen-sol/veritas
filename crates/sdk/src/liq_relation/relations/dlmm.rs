use std::collections::HashMap;

use rust_decimal::{prelude::ToPrimitive, Decimal, MathematicalOps};
use serde::{Deserialize, Serialize};
use step_ingestooor_sdk::dooot::DLMMPart;

use crate::ppl_graph::structs::{LiqAmount, LiqLevels};

pub type DlmmBinMap = HashMap<i32, Vec<DlmmBinParsed>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlmmBinParsed {
    pub price: u128,
    pub token_amounts: [Decimal; 2],
}

impl From<&DLMMPart> for DlmmBinParsed {
    fn from(part: &DLMMPart) -> Self {
        Self {
            price: part.price.parse::<u128>().unwrap(),
            token_amounts: [part.token_amounts[0], part.token_amounts[1]],
        }
    }
}

// For precision. Tone down this value if we suspect this is overflowing price calcs
const SCALE_FACTOR: u128 = 1_000_000;
const SCALE_FACTOR_DECIMAL: Decimal = Decimal::from_parts(SCALE_FACTOR as u32, 0, 0, false, 0);

pub fn get_dlmm_price(
    decimals_x: u8,
    decimals_y: u8,
    usd_per_origin_units: &Decimal,
    bins_by_account: &DlmmBinMap,
    active_bin: &Option<(i32, usize)>,
    is_reverse: bool,
) -> Option<Decimal> {
    let (bin_arr_ix, vec_ix) = active_bin.as_ref()?;
    let bin_arr = bins_by_account.get(bin_arr_ix)?;
    let bin = &bin_arr[*vec_ix];
    let price = bin.price;

    if !is_reverse {
        // X PER Y (Y is origin node)
        let x_per_y_atoms = (SCALE_FACTOR << 64).checked_div(price)?;
        let decimal_factor = Decimal::TEN.checked_powu(decimals_x as u64)?;
        let x_per_y_units = Decimal::from(x_per_y_atoms)
            .checked_div(decimal_factor)?
            .checked_div(SCALE_FACTOR_DECIMAL)?;
        let usd_per_x_units = usd_per_origin_units.checked_div(x_per_y_units)?;

        Some(usd_per_x_units)
    } else {
        // Y PER X (X is origin node)
        let y_per_x_atoms = (price.checked_mul(SCALE_FACTOR)?) >> 64;
        let decimal_factor = Decimal::TEN.checked_powu(decimals_y as u64)?;
        let y_per_x_units = Decimal::from(y_per_x_atoms)
            .checked_div(decimal_factor)?
            .checked_div(SCALE_FACTOR_DECIMAL)?;
        let usd_per_y_units = usd_per_origin_units.checked_div(y_per_x_units)?;

        Some(usd_per_y_units)
    }
}

pub fn get_dlmm_liq_levels(
    bins_by_account: &DlmmBinMap,
    active_binarray: &Option<(i32, usize)>,
    origin_tokens_per_sol: &Decimal,
    is_reverse: bool,
    decimals_x: u8,
    decimals_y: u8,
) -> Option<LiqLevels> {
    let (active_bin_arr_ix, active_bin_vec_ix) = active_binarray.as_ref()?;
    let bin_side_ix = is_reverse as usize;
    let step: i32 = if is_reverse { -1 } else { 1 };
    let decimal_factor = if is_reverse {
        Decimal::TEN.checked_powu(decimals_x as u64)?
    } else {
        Decimal::TEN.checked_powu(decimals_y as u64)?
    };

    let mut one_sol_tokens = (*origin_tokens_per_sol * decimal_factor).floor();
    let mut ten_sol_tokens = origin_tokens_per_sol.checked_mul(Decimal::TEN)?;
    let mut thousand_sol_tokens = origin_tokens_per_sol.checked_mul(Decimal::ONE_THOUSAND)?;

    let mut bin_vec_ix = *active_bin_vec_ix;
    let mut binarray_ix = *active_bin_arr_ix;
    let mut curr_binarray = bins_by_account.get(&binarray_ix)?;
    let mut curr_bin = curr_binarray.get(bin_vec_ix)?.clone();
    while thousand_sol_tokens > Decimal::ZERO {
        let amount_in = if one_sol_tokens > Decimal::ZERO {
            one_sol_tokens
        } else if ten_sol_tokens > Decimal::ZERO {
            ten_sol_tokens
        } else {
            thousand_sol_tokens
        };

        let Some(used) = bin_swap(amount_in, &mut curr_bin, is_reverse) else {
            return None;
        };

        one_sol_tokens = one_sol_tokens.saturating_sub(used);
        ten_sol_tokens = ten_sol_tokens.saturating_sub(used);
        thousand_sol_tokens = thousand_sol_tokens.saturating_sub(used);

        let holdings = curr_bin.token_amounts[bin_side_ix];
        if holdings <= Decimal::ZERO {
            if bin_vec_ix == 0 || bin_vec_ix == 69 {
                binarray_ix += step;
                bin_vec_ix = 0;

                let Some(next_binarray) = bins_by_account.get(&binarray_ix) else {
                    // Out of range
                    break;
                };

                curr_binarray = next_binarray;
            } else if is_reverse {
                bin_vec_ix -= 1;
            } else {
                bin_vec_ix += 1;
            }

            let Some(next_bin) = curr_binarray.get(bin_vec_ix) else {
                log::error!("UNREACHABLE - Should stay within [0, 69] bin range");
                break;
            };
            curr_bin = next_bin.clone();
        }
    }

    // let one_sol_depth = if one_sol_tokens <= Decimal::ZERO {

    // } else {
    //     None
    // };

    Some(LiqLevels::ZERO)
}

/// Returns amount_in_consumed
#[allow(clippy::unwrap_used)]
fn bin_swap(amt_in: Decimal, bin: &mut DlmmBinParsed, rev_dir: bool) -> Option<Decimal> {
    let bin_price = bin.price;
    let amt_128: u128 = amt_in.floor().to_u128()?;
    let x_amt = bin.token_amounts[0].to_u128()?;
    let y_amt = bin.token_amounts[1].to_u128()?;

    if rev_dir {
        // Swap in X, get Y (swap_for_y = true)
        let max_in = (x_amt << 64) / bin_price;
        let actual_in = max_in.min(amt_128);
        let amount_out = (actual_in * bin_price) >> 64;
        let max_out = y_amt;
        let actual_out: Decimal = amount_out.min(max_out).into();
        let actual_in_decimal = actual_in.into();

        bin.token_amounts[1] -= actual_out;
        bin.token_amounts[0] += actual_in_decimal;

        Some(actual_in_decimal)
    } else {
        // Swap in Y, get X (swap_for_y = false)
        let max_in = (y_amt * bin_price) >> 64;
        let actual_in = max_in.min(amt_128);
        let amount_out = (actual_in << 64) / bin_price;
        let max_out = x_amt;
        let actual_out: Decimal = amount_out.min(max_out).into();
        let actual_in_decimal = actual_in.into();

        bin.token_amounts[0] -= actual_out;
        bin.token_amounts[1] += actual_in_decimal;

        Some(actual_in_decimal)
    }
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

#[cfg(test)]
mod tests {}
