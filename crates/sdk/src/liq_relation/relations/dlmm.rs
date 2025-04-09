use std::collections::HashMap;

use rust_decimal::{prelude::ToPrimitive, Decimal, MathematicalOps};
use serde::{Deserialize, Serialize};
use step_ingestooor_sdk::dooot::DLMMPart;
use tokio::time::Instant;

use crate::ppl_graph::structs::{LiqAmount, LiqLevels};

pub type DlmmBinMap = HashMap<i32, Vec<DlmmBinParsed>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlmmBinParsed {
    pub price: u128,
    pub token_amounts: [Decimal; 2],
}

impl DlmmBinParsed {
    pub fn get_price(&self, is_reverse: bool) -> Option<Decimal> {
        if is_reverse {
            let y_per_x_atoms = self.price.checked_mul(SCALE_FACTOR)? >> 64;
            let final_price = Decimal::from(y_per_x_atoms).checked_div(SCALE_FACTOR_DECIMAL)?;
            Some(final_price)
        } else {
            let x_per_y_atoms = (SCALE_FACTOR << 64).checked_div(self.price)?;
            let final_price = Decimal::from(x_per_y_atoms).checked_div(SCALE_FACTOR_DECIMAL)?;
            Some(final_price)
        }
    }
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

    if is_reverse {
        // Y PER X (X is origin node)
        let y_per_x_atoms = (price.checked_mul(SCALE_FACTOR)?) >> 64;
        let decimal_factor = Decimal::TEN.checked_powi(decimals_x as i64 - decimals_y as i64)?;
        let y_per_x_units = Decimal::from(y_per_x_atoms)
            .checked_div(SCALE_FACTOR_DECIMAL)?
            .checked_mul(decimal_factor)?;
        let usd_per_y_units = usd_per_origin_units.checked_div(y_per_x_units)?;

        Some(usd_per_y_units)
    } else {
        // X PER Y (Y is origin node)
        let x_per_y_atoms = (SCALE_FACTOR << 64).checked_div(price)?;
        let decimal_factor = Decimal::TEN.checked_powi(decimals_y as i64 - decimals_x as i64)?;
        let x_per_y_units = Decimal::from(x_per_y_atoms)
            .checked_div(SCALE_FACTOR_DECIMAL)?
            .checked_mul(decimal_factor)?;
        let usd_per_x_units = usd_per_origin_units.checked_div(x_per_y_units)?;

        Some(usd_per_x_units)
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
    let now = Instant::now();
    let (active_bin_arr_ix, active_bin_vec_ix) = active_binarray.as_ref()?;
    let bin_side_ix = is_reverse as usize;
    let step: i32 = if is_reverse { -1 } else { 1 };
    let decimal_factor = if is_reverse {
        Decimal::TEN.checked_powu(decimals_x as u64)?
    } else {
        Decimal::TEN.checked_powu(decimals_y as u64)?
    };

    let mut one_sol_tokens = (*origin_tokens_per_sol * decimal_factor).floor();
    let mut ten_sol_tokens = one_sol_tokens.checked_mul(Decimal::TEN)?;
    let mut thousand_sol_tokens = one_sol_tokens.checked_mul(Decimal::ONE_THOUSAND)?;

    let mut bin_vec_ix = *active_bin_vec_ix;
    let mut binarray_ix = *active_bin_arr_ix;
    let mut curr_binarray = bins_by_account.get(&binarray_ix)?;
    let mut curr_bin = curr_binarray.get(bin_vec_ix)?.clone();
    let pool_price = curr_bin.get_price(is_reverse)?;

    let mut one_sol_price_after: Option<Decimal> = None;
    let mut one_sol_price_set = false;
    let mut ten_sol_price_after: Option<Decimal> = None;
    let mut ten_sol_price_set = false;
    let mut thousand_sol_price_after: Option<Decimal> = None;
    let mut thousand_sol_price_set = false;

    while thousand_sol_tokens > Decimal::ZERO {
        let amount_in = if one_sol_tokens > Decimal::ZERO {
            one_sol_tokens
        } else if ten_sol_tokens > Decimal::ZERO {
            ten_sol_tokens
        } else {
            thousand_sol_tokens
        };

        let used = bin_swap(amount_in, &mut curr_bin, is_reverse)?;
        let holdings = curr_bin.token_amounts[bin_side_ix];

        if used == Decimal::ZERO && holdings != Decimal::ZERO {
            log::trace!("Used is 0, but holdings are not (Bin: {curr_bin:?}). Breaking");
            break;
        }

        one_sol_tokens = one_sol_tokens.saturating_sub(used).max(Decimal::ZERO);
        ten_sol_tokens = ten_sol_tokens.saturating_sub(used).max(Decimal::ZERO);
        thousand_sol_tokens = thousand_sol_tokens.saturating_sub(used).max(Decimal::ZERO);

        if !one_sol_price_set && one_sol_tokens <= Decimal::ZERO {
            one_sol_price_after = curr_bin.get_price(is_reverse);
            one_sol_price_set = true;
        }

        if !ten_sol_price_set && ten_sol_tokens <= Decimal::ZERO {
            ten_sol_price_after = curr_bin.get_price(is_reverse);
            ten_sol_price_set = true;
        }

        if !thousand_sol_price_set && thousand_sol_tokens <= Decimal::ZERO {
            thousand_sol_price_after = curr_bin.get_price(is_reverse);
            thousand_sol_price_set = true;
        }

        
        if holdings <= Decimal::ZERO {
            if (bin_vec_ix == 0 && is_reverse) || bin_vec_ix >= 69 {
                binarray_ix += step;
                bin_vec_ix = if is_reverse { 69 } else { 0 };

                let Some(next_binarray) = bins_by_account.get(&binarray_ix) else {
                    // Out of range or not enough info
                    break;
                };

                curr_binarray = next_binarray;
            } else if is_reverse {
                bin_vec_ix -= 1;
            } else {
                bin_vec_ix += 1;
            }

            let Some(next_bin) = curr_binarray.get(bin_vec_ix) else {
                log::error!(
                    "UNREACHABLE - Should stay within [0, 69] bin range, tried index {bin_vec_ix}"
                );
                break;
            };
            curr_bin = next_bin.clone();
        }
    }

    let one_sol_price_change =
        one_sol_price_after.and_then(|p| p.checked_div(pool_price)?.checked_sub(Decimal::ONE));
    let ten_sol_price_change =
        ten_sol_price_after.and_then(|p| p.checked_div(pool_price)?.checked_sub(Decimal::ONE));
    let thousand_sol_price_change =
        thousand_sol_price_after.and_then(|p| p.checked_div(pool_price)?.checked_sub(Decimal::ONE));

    let liq_levels = LiqLevels {
        one_sol_depth: one_sol_price_change,
        ten_sol_depth: ten_sol_price_change,
        thousand_sol_depth: thousand_sol_price_change,
    };

    log::debug!("get_dlmm_liq_levels took {:?}", now.elapsed());

    Some(liq_levels)
}

/// Returns amount_in_consumed
#[allow(clippy::unwrap_used)]
fn bin_swap(amt_in: Decimal, bin: &mut DlmmBinParsed, rev_dir: bool) -> Option<Decimal> {
    let bin_price = bin.price;
    let amt_128: u128 = amt_in.floor().to_u128()?;
    let x_amt = bin.token_amounts[0].to_u128()?;
    let y_amt = bin.token_amounts[1].to_u128()?;
    let holdings = &mut bin.token_amounts;

    if rev_dir {
        // Swap in X, get Y (swap_for_y = true)
        let max_in = (y_amt << 64) / bin_price;
        if max_in == 0 {
            // y_amt is likely 1 or more by only a few atoms, so just give up the rest, sort of like a round up
            holdings[1] = Decimal::ZERO;
            return Some(Decimal::ZERO);
        }
        let actual_in = max_in.min(amt_128);
        let amount_out = (actual_in * bin_price) >> 64;
        let max_out = y_amt;
        let actual_out: Decimal = amount_out.min(max_out).into();
        let actual_in_decimal = actual_in.into();

        holdings[1] -= actual_out;
        holdings[0] += actual_in_decimal;

        Some(actual_in_decimal)
    } else {
        // Swap in Y, get X (swap_for_y = false)
        let max_in = (x_amt * bin_price) >> 64;
        if max_in == 0 {
            holdings[0] = Decimal::ZERO;
            // x_amt is likely 1 or more by only a few atoms, so just give up the rest, sort of like a round up
            return Some(Decimal::ZERO);
        }

        let actual_in = max_in.min(amt_128);
        let amount_out = (actual_in << 64) / bin_price;
        let max_out = x_amt;
        let actual_out: Decimal = amount_out.min(max_out).into();
        let actual_in_decimal = actual_in.into();

        holdings[0] -= actual_out;
        holdings[1] += actual_in_decimal;

        Some(actual_in_decimal)
    }
}

pub fn get_dlmm_liquidity(
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
    use super::{get_dlmm_liq_levels, DlmmBinMap, DlmmBinParsed};

    #[test]
    fn liq_levels_calc() {
        let bin1 = DlmmBinParsed {
            price: 12395918212259458082,
            token_amounts: [205149870204u64.into(), 1728451075u64.into()],
        };

        let bin2 = DlmmBinParsed {
            price: 12550867189912701309,
            token_amounts: [206118815125u64.into(), 0.into()],
        };

        let mut map = DlmmBinMap::new();
        map.insert(-1, vec![bin1, bin2]);
        let active_bin = Some((-1, 0));
        let origin_tokens_per_sol = 1950.into();
        let reversed = false;
        let decimals_x = 6;
        let decimals_y = 6;

        get_dlmm_liq_levels(
            &map,
            &active_bin,
            &origin_tokens_per_sol,
            reversed,
            decimals_x,
            decimals_y,
        );
    }
}
