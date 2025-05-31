use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::ppl_graph::structs::{LiqAmount, LiqLevels};

use relations::{
    clmm::{get_clmm_liq_levels, get_clmm_liquidity, get_clmm_price, ClmmTickMap},
    cplp::{get_cplp_liq_levels, get_cplp_liquidity, get_cplp_price},
    dlmm::{get_dlmm_liq_levels, get_dlmm_liquidity, get_dlmm_price, DlmmBinMap},
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
    /// DLMMs
    Dlmm {
        /// Expressed in UNITS
        amt_origin: Decimal,
        /// Expressed in UNITS
        amt_dest: Decimal,
        decimals_x: u8,
        decimals_y: u8,
        /// (bin_arr_ix, bin_index_in_vec)
        active_bin_account: Option<(i32, usize)>,
        #[serde(skip)]
        bins_by_account: DlmmBinMap,
        is_reverse: bool,
        pool_id: String,
    },
    /// CLMMs
    Clmm {
        amt_origin: Decimal,
        amt_dest: Decimal,
        decimals_a: u8,
        decimals_b: u8,
        #[serde(skip)]
        ticks_by_account: ClmmTickMap,
        current_price_x64: Option<u128>,
        current_tick_index: Option<i32>,
        tick_spacing: Option<i32>,
        is_reverse: bool,
        pool_id: String,
    },
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
            LiqRelation::Dlmm {
                decimals_x,
                decimals_y,
                bins_by_account,
                active_bin_account,
                is_reverse,
                ..
            } => get_dlmm_price(
                *decimals_x,
                *decimals_y,
                &usd_price_origin,
                bins_by_account,
                active_bin_account,
                *is_reverse,
            ),
            LiqRelation::Clmm {
                decimals_a,
                decimals_b,
                current_price_x64,
                is_reverse,
                ..
            } => get_clmm_price(
                current_price_x64,
                &usd_price_origin,
                *decimals_a,
                *decimals_b,
                *is_reverse,
            ),
        }
    }

    /// `tokens_per_sol` is tokens_a_per_sol (aka source of the incoming relation) IN UNITS
    #[inline]
    pub fn get_liq_levels(&self, tokens_per_sol: Decimal) -> Option<LiqLevels> {
        match self {
            LiqRelation::CpLp {
                amt_origin: amt_a,
                amt_dest: amt_b,
            } => get_cplp_liq_levels(amt_a, amt_b, &tokens_per_sol),
            LiqRelation::Fixed { .. } => get_fixed_liq_levels(),
            LiqRelation::Dlmm {
                bins_by_account,
                active_bin_account,
                is_reverse,
                decimals_x,
                decimals_y,
                ..
            } => get_dlmm_liq_levels(
                bins_by_account,
                active_bin_account,
                &tokens_per_sol,
                *is_reverse,
                *decimals_x,
                *decimals_y,
            ),
            LiqRelation::Clmm {
                ticks_by_account,
                is_reverse,
                current_tick_index,
                current_price_x64,
                tick_spacing,
                decimals_a,
                decimals_b,
                ..
            } => get_clmm_liq_levels(
                ticks_by_account,
                *current_tick_index,
                *current_price_x64,
                *tick_spacing,
                &tokens_per_sol,
                *is_reverse,
                *decimals_a,
                *decimals_b,
            ),
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
            LiqRelation::Dlmm {
                amt_origin: amt_a,
                amt_dest: amt_b,
                ..
            } => get_dlmm_liquidity(amt_a, amt_b, price_source_usd, price_dest_usd),
            LiqRelation::Clmm {
                amt_origin,
                amt_dest,
                ..
            } => get_clmm_liquidity(amt_origin, amt_dest, price_source_usd, price_dest_usd),
        }
    }
}
