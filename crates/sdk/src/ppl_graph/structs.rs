use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct LiqLevels {
    /// pct change, `None` is infinite change, and therefore unacceptable
    pub one_sol_depth: Option<Decimal>,
    /// pct change, `None` is infinite change, and therefore unacceptable
    pub ten_sol_depth: Option<Decimal>,
    /// pct change, `None` is infinite change, and therefore unacceptable
    pub thousand_sol_depth: Option<Decimal>,
}

impl LiqLevels {
    pub const ZERO: Self = Self {
        one_sol_depth: Some(Decimal::ZERO),
        ten_sol_depth: Some(Decimal::ZERO),
        thousand_sol_depth: Some(Decimal::ZERO),
    };

    pub const INFINITE: Self = Self {
        one_sol_depth: None,
        ten_sol_depth: None,
        thousand_sol_depth: None,
    };

    /// Determines if the liq levels are acceptable for a given relation,
    /// and should be used for pricing
    ///
    /// As of writing, current determination should be that
    /// 10 SOL (~$1,670 atm) should not have an impact of 25% or greater
    ///
    /// This is completely arbitrary and subject to change
    #[inline]
    pub fn acceptable(&self, max_price_impact: &Decimal) -> bool {
        self.ten_sol_depth
            .is_some_and(|depth| depth < *max_price_impact)
    }
}

#[derive(Serialize, Deserialize)]
pub enum LiqAmount {
    Amount(Decimal),
    /// Used by `Fixed` relations, that will ALWAYS take precedence when calculating price
    Inf,
}
