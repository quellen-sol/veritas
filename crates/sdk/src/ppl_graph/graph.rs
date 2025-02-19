use anyhow::{Context, Result};
use std::sync::Arc;

use chrono::NaiveDateTime;
use petgraph::{Directed, Graph};
use rust_decimal::Decimal;
use tokio::sync::RwLock;

use super::structs::LiqRelationEnum;

pub type MintPricingGraph = Graph<Arc<RwLock<MintNode>>, MintEdge, Directed>;
pub type WrappedMintPricingGraph = Arc<RwLock<MintPricingGraph>>;

pub const EDGE_SIZE: usize = std::mem::size_of::<MintEdge>();
pub const NODE_SIZE: usize = std::mem::size_of::<MintNode>();

#[cfg_attr(not(feature = "debug-graph"), derive(Debug))]
pub struct MintNode {
    pub mint: String,
    pub usd_price: Option<USDPriceWithSource>,
}

#[cfg(feature = "debug-graph")]
impl std::fmt::Debug for MintNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let price = self
            .usd_price
            .as_ref()
            .map(|p| p.extract_price())
            .unwrap_or(&Decimal::ZERO);
        write!(f, "{:.5} {:.4}", self.mint, price)
    }
}

pub struct MintEdge {
    pub id: String,
    pub last_updated: RwLock<NaiveDateTime>,
    pub inner_relation: RwLock<LiqRelationEnum>,
}

#[cfg_attr(not(feature = "debug-graph"), derive(Debug))]
pub struct MintLiquidity {
    pub mints: Vec<String>,
    pub liquidity: Vec<Decimal>,
}

#[cfg(feature = "debug-graph")]
impl std::fmt::Debug for MintLiquidity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.liquidity)
    }
}

impl MintLiquidity {
    pub fn get_liq_for_mint(&self, mint: &str) -> Result<&Decimal> {
        let mut opt = None;
        for (idx, c_mint) in self.mints.iter().enumerate() {
            if c_mint != mint {
                continue;
            }

            opt = self.liquidity.get(idx);
            break;
        }

        opt.context(format!(
            "Cannot find liquidity for {mint} in {self:?}. Must be malformed MintUnderlying!"
        ))
    }
}

#[derive(Debug, Clone)]
pub enum USDPriceWithSource {
    Oracle(Decimal),
    /// Priced via related nodes on the graph
    Relation(Decimal),
}

impl USDPriceWithSource {
    #[inline]
    pub fn extract_price(&self) -> &Decimal {
        match self {
            USDPriceWithSource::Oracle(p) => p,
            USDPriceWithSource::Relation(p) => p,
        }
    }

    #[inline]
    pub fn is_oracle(&self) -> bool {
        matches!(self, Self::Oracle(_))
    }
}
