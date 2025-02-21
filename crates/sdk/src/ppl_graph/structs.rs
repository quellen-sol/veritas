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

/// Each variant should contain enough information to calculate the price, liquidity, and liq levels
pub enum LiqRelationEnum {
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

impl LiqRelationEnum {
    #[inline]
    pub fn get_price(&self, usd_price_origin: Decimal) -> Decimal {
        match self {
            LiqRelationEnum::CpLp {
                amt_origin,
                amt_dest,
                ..
            } => (amt_origin / amt_dest) * usd_price_origin,
            LiqRelationEnum::Fixed { amt_per_parent } => usd_price_origin * amt_per_parent,
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

    #[inline]
    pub fn get_liquidity(&self, price_source_usd: Decimal, price_dest_usd: Decimal) -> LiqAmount {
        match self {
            LiqRelationEnum::CpLp {
                amt_origin,
                amt_dest,
                ..
            } => LiqAmount::Amount(amt_origin * price_source_usd + amt_dest * price_dest_usd),
            LiqRelationEnum::Fixed { .. } => LiqAmount::Inf,
        }
    }
}
