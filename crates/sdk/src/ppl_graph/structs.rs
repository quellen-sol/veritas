use rust_decimal::Decimal;

/// When calling `get_price`, this enum describes which token's price to return
pub enum TokenTarget {
    Origin,
    Destination,
}

#[derive(Debug, Default)]
pub struct LiqLevels {
    pub one_sol_depth: Decimal,
    pub ten_sol_depth: Decimal,
    pub thousand_sol_depth: Decimal,
}

pub enum LiqAmount {
    Amount(Decimal),
    /// Used by `Fixed` relations, that will ALWAYS take prescidence when calculating price
    Inf,
}

/// Each variant should contain minimum information to calculate price, liquidity, and liq levels
#[derive(Debug)]
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

    // #[inline]
    // pub fn get_liq_levels(&self) -> LiqLevels {
    //     match self {
    //         LiqRelationEnum::CpLp {
    //             amt_origin: amt_a,
    //             amt_dest: amt_b,
    //             ..
    //         } => {}
    //     }
    //
    //     LiqLevels::default()
    // }

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
