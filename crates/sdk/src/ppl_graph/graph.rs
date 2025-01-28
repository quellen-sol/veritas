use std::sync::Arc;

use chrono::NaiveDateTime;
use petgraph::Graph;
use rust_decimal::Decimal;
use tokio::sync::RwLock;

pub type MintPricingGraph = Graph<MintNode, Arc<RwLock<MintEdge>>>;

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
pub struct MintEdge {
    pub this_per_that: Option<Decimal>,
    pub market: Option<String>,
    pub liquidity: Option<u128>,
    pub last_updated: NaiveDateTime,
}

#[cfg(feature = "debug-graph")]
impl std::fmt::Debug for MintEdge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.this_per_that)
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
        liquidity: Option<u128>,
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
