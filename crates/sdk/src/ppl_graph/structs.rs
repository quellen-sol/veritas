use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
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
    #[inline]
    pub fn acceptable(&self, max_price_impact: &Decimal) -> bool {
        self.ten_sol_depth.abs() < *max_price_impact
    }
}

#[derive(Serialize, Deserialize)]
pub enum LiqAmount {
    Amount(Decimal),
    /// Used by `Fixed` relations, that will ALWAYS take prescidence when calculating price
    Inf,
}
