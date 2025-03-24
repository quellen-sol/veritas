use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::constants::ONE_PERCENT;

#[derive(Debug)]
pub struct LiqLevels {
    /// pct change
    pub one_sol_depth: Decimal,
    /// pct change
    pub ten_sol_depth: Decimal,
    /// pct change
    pub thousand_sol_depth: Decimal,
}

impl LiqLevels {
    pub const ZERO: Self = Self {
        one_sol_depth: Decimal::ZERO,
        ten_sol_depth: Decimal::ZERO,
        thousand_sol_depth: Decimal::ZERO,
    };

    /// Determines if the liq levels are acceptable for a given relation,
    /// and should be used for pricing
    ///
    /// As of writing, current determination should be that
    /// 10 SOL (~$1,270 atm) should not have an impact of 1% or greater
    ///
    /// This is completely arbitrary and subject to change
    pub fn acceptable(&self) -> bool {
        self.ten_sol_depth.abs() < ONE_PERCENT
    }
}

#[derive(Serialize, Deserialize)]
pub enum LiqAmount {
    Amount(Decimal),
    /// Used by `Fixed` relations, that will ALWAYS take prescidence when calculating price
    Inf,
}

/// Each variant should contain minimum information to calculate price, liquidity, and liq levels
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum LiqRelation {
    /// Constant Product LP
    CpLp {
        /// Expressed in UNITS
        amt_origin: Decimal,
        /// Expressed in UNITS
        amt_dest: Decimal,
    },
    /// Fixed ratio of parent to underlying, e.g., STEP -> xSTEP
    Fixed { amt_per_parent: Decimal },
    // /// CLOBs
    // Clob,
    // /// DLMMs
    // Dlmm,
    // /// CLMMs
    // Clmm,
}

impl LiqRelation {
    /// Returns `None` if unable to calculate the price of this relation (through overflows, divs by 0, etc)
    #[inline]
    pub fn get_price(&self, usd_price_origin: Decimal) -> Option<Decimal> {
        match self {
            LiqRelation::CpLp {
                amt_origin,
                amt_dest,
                ..
            } => amt_origin
                .checked_div(*amt_dest)?
                .checked_mul(usd_price_origin),
            LiqRelation::Fixed { amt_per_parent } => amt_per_parent.checked_mul(usd_price_origin),
        }
    }

    #[inline]
    pub fn get_liq_levels(&self, tokens_per_sol: Decimal) -> Option<LiqLevels> {
        match self {
            LiqRelation::CpLp {
                amt_origin: amt_a,
                amt_dest: amt_b,
            } => {
                let current_price_a = amt_a.checked_div(*amt_b)?;
                let product = amt_a.checked_mul(*amt_b)?;

                let tokens_amt_one_sol = tokens_per_sol;
                let tokens_amt_ten_sol = tokens_amt_one_sol.checked_mul(Decimal::from(10))?;
                let tokens_amt_thousand_sol =
                    tokens_amt_one_sol.checked_mul(Decimal::from(1000))?;

                // Calc thousands info first, so if it overflows,
                // we can assume the rest is borked if values got as high as 2^96, and return early
                let post_a_thousand_sol = amt_a.checked_add(tokens_amt_thousand_sol)?;
                let post_b_thousand_sol = product.checked_div(post_a_thousand_sol)?;
                let price_thousand_sol = post_a_thousand_sol.checked_div(post_b_thousand_sol)?;

                let post_a_ten_sol = amt_a.checked_add(tokens_amt_ten_sol)?;
                let post_b_ten_sol = product.checked_div(post_a_ten_sol)?;
                let price_ten_sol = post_a_ten_sol.checked_div(post_b_ten_sol)?;

                let post_a_one_sol = amt_a.checked_add(tokens_amt_one_sol)?;
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
            LiqRelation::Fixed { .. } => Some(LiqLevels::ZERO),
        }
    }

    /// Returns `None` if unable to calculate liquidity of this relation (through overflows, divs by 0, etc)
    #[inline]
    pub fn get_liquidity(
        &self,
        price_source_usd: Decimal,
        price_dest_usd: Decimal,
    ) -> Option<LiqAmount> {
        match self {
            LiqRelation::CpLp {
                amt_origin,
                amt_dest,
                ..
            } => {
                let liq_origin = amt_origin.checked_mul(price_source_usd)?;
                let liq_dest = amt_dest.checked_mul(price_dest_usd)?;
                // Just allow to max out. One less failure point
                let total_liq = liq_origin.saturating_add(liq_dest);

                Some(LiqAmount::Amount(total_liq))
            }
            LiqRelation::Fixed { .. } => Some(LiqAmount::Inf),
        }
    }
}
