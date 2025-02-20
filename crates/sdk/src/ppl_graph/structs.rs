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

// pub trait LiqRelationTrait {
//     fn get_price(&self, target: TokenTarget, usd_price_other: Decimal) -> Decimal;
//     fn get_liq_levels(&self) -> LiqLevels;
//     fn get_liquidity(&self) -> Decimal;
// }

/// Each variant should contain enough information to calculate the price, liquidity, and liq levels
pub enum LiqRelationEnum {
    /// Constant Product LP
    CpLp {
        /// Expressed in UNITS
        amt_origin: Decimal,
        /// Expressed in UNITS
        amt_dest: Decimal,
    },
    // /// Fixed ratio of parent to underlying, e.g., STEP -> xSTEP
    // Fixed {
    //     amt_per_parent: f64,
    //     underlying_amt: u64,
    // },
    // /// CLOBs
    // Clob,
    // /// DLMMs
    // Dlmm,
    // /// CLMMs
    // Clmm,
}

impl LiqRelationEnum {
    pub fn get_price(&self, target: TokenTarget, usd_price_other: Decimal) -> Decimal {
        match self {
            LiqRelationEnum::CpLp {
                amt_origin,
                amt_dest,
                ..
            } => {
                let int_price = (amt_dest / amt_origin) * usd_price_other;

                match target {
                    TokenTarget::Origin => Decimal::ONE / int_price,
                    TokenTarget::Destination => int_price,
                }
            }
        }
    }

    pub fn get_liq_levels(&self) -> LiqLevels {
        match self {
            LiqRelationEnum::CpLp {
                amt_origin: amt_a,
                amt_dest: amt_b,
                ..
            } => {}
        }

        LiqLevels::default()
    }

    pub fn get_liquidity(&self, price_source_usd: Decimal, price_dest_usd: Decimal) -> Decimal {
        match self {
            LiqRelationEnum::CpLp {
                amt_origin,
                amt_dest,
                ..
            } => amt_origin * price_source_usd + amt_dest * price_dest_usd,
        }
    }
}
