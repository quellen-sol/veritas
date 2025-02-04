use std::sync::Arc;

use chrono::NaiveDateTime;
use petgraph::{Directed, Graph};
use rust_decimal::Decimal;
use tokio::sync::RwLock;

pub type MintPricingGraph = Graph<MintNode, Arc<RwLock<MintEdge>>, Directed>;
pub type WrappedMintPricingGraph = Arc<RwLock<MintPricingGraph>>;

#[cfg_attr(not(feature = "debug-graph"), derive(Debug))]
pub struct MintNode {
    pub mint: String,
}

#[cfg(feature = "debug-graph")]
impl std::fmt::Debug for MintNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:.4}", self.mint)
    }
}

pub const NODE_SIZE: usize = std::mem::size_of::<MintNode>();

#[cfg_attr(not(feature = "debug-graph"), derive(Debug))]
#[derive(Default)]
pub struct MintEdge {
    pub this_per_that: Option<Decimal>,
    pub market: Option<String>,
    pub liquidity: Option<MintLiquidity>,
    pub last_updated: NaiveDateTime,
}

#[cfg(feature = "debug-graph")]
impl std::fmt::Debug for MintEdge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.this_per_that)
    }
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

pub enum RelationSource {
    /// Relation was created from a swap/fill/new mintunderlying, etc
    Market,
    /// Relation was created from an oracle
    Oracle,
}

pub const EDGE_SIZE: usize = std::mem::size_of::<MintEdge>();

impl MintEdge {
    pub fn new(
        ratio: Option<Decimal>,
        liquidity: Option<MintLiquidity>,
        market: Option<String>,
        last_updated: NaiveDateTime,
    ) -> Self {
        Self {
            this_per_that: ratio,
            liquidity,
            market,
            last_updated,
        }
    }
}
