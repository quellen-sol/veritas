use rust_decimal::Decimal;

use crate::ppl_graph::structs::{LiqAmount, LiqLevels};

pub fn get_cplp_price(
    amt_origin: &Decimal,
    amt_dest: &Decimal,
    usd_price_origin: &Decimal,
) -> Option<Decimal> {
    amt_origin
        .checked_div(*amt_dest)?
        .checked_mul(*usd_price_origin)
}

pub fn get_cplp_liq_levels(
    amt_a: &Decimal,
    amt_b: &Decimal,
    tokens_per_sol: &Decimal,
) -> Option<LiqLevels> {
    let current_price_a = amt_a.checked_div(*amt_b)?;
    let product = amt_a.checked_mul(*amt_b)?;

    let tokens_amt_one_sol = tokens_per_sol;
    let tokens_amt_ten_sol = tokens_amt_one_sol.checked_mul(Decimal::from(10))?;
    let tokens_amt_thousand_sol = tokens_amt_one_sol.checked_mul(Decimal::from(1000))?;

    // Calc thousands info first, so if it overflows,
    // we can assume the rest is borked if values got as high as 2^96, and return early
    let post_a_thousand_sol = amt_a.checked_add(tokens_amt_thousand_sol)?;
    let post_b_thousand_sol = product.checked_div(post_a_thousand_sol)?;
    let price_thousand_sol = post_a_thousand_sol.checked_div(post_b_thousand_sol)?;

    let post_a_ten_sol = amt_a.checked_add(tokens_amt_ten_sol)?;
    let post_b_ten_sol = product.checked_div(post_a_ten_sol)?;
    let price_ten_sol = post_a_ten_sol.checked_div(post_b_ten_sol)?;

    let post_a_one_sol = amt_a.checked_add(*tokens_amt_one_sol)?;
    let post_b_one_sol = product.checked_div(post_a_one_sol)?;
    let price_one_sol = post_a_one_sol.checked_div(post_b_one_sol)?;

    let one_sol_pct_change = price_one_sol
        .checked_div(current_price_a)?
        .checked_sub(Decimal::ONE)?;
    let ten_sol_pct_change = price_ten_sol
        .checked_div(current_price_a)?
        .checked_sub(Decimal::ONE)?;
    let thousand_sol_pct_change = price_thousand_sol
        .checked_div(current_price_a)?
        .checked_sub(Decimal::ONE)?;

    Some(LiqLevels {
        one_sol_depth: one_sol_pct_change,
        ten_sol_depth: ten_sol_pct_change,
        thousand_sol_depth: thousand_sol_pct_change,
    })
}

pub fn get_cplp_liquidity(
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
