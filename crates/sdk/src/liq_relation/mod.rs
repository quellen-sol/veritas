use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::ppl_graph::structs::{LiqAmount, LiqLevels};

use relations::{
    cplp::{get_cplp_liq_levels, get_cplp_liquidity, get_cplp_price},
    fixed::{get_fixed_liq_levels, get_fixed_liquidity, get_fixed_price},
};

pub mod relations;

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
    // /// DLMMs
    // Dlmm,
    // /// CLMMs
    // Clmm,
    // /// CLOBs
    // Clob,
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
            } => get_cplp_price(amt_origin, amt_dest, &usd_price_origin),
            LiqRelation::Fixed { amt_per_parent } => {
                get_fixed_price(amt_per_parent, &usd_price_origin)
            }
        }
    }

    /// `tokens_per_sol` is tokens_a_per_sol (aka source of the incoming relation)
    #[inline]
    pub fn get_liq_levels(&self, tokens_per_sol: Decimal) -> Option<LiqLevels> {
        match self {
            LiqRelation::CpLp {
                amt_origin: amt_a,
                amt_dest: amt_b,
            } => get_cplp_liq_levels(amt_a, amt_b, &tokens_per_sol),
            LiqRelation::Fixed { .. } => get_fixed_liq_levels(),
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
            } => get_cplp_liquidity(amt_origin, amt_dest, price_source_usd, price_dest_usd),
            LiqRelation::Fixed { .. } => get_fixed_liquidity(),
        }
    }
}
