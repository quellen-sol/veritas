use anyhow::{Context, Result};
use std::sync::Arc;

use chrono::NaiveDateTime;
use petgraph::{Directed, Graph};
use rust_decimal::Decimal;
use tokio::sync::{Mutex, RwLock};

use super::structs::LiqRelationEnum;

pub type MintPricingGraph = Graph<MintNode, MintEdge, Directed>;
pub type WrappedMintPricingGraph = Arc<RwLock<MintPricingGraph>>;

pub const EDGE_SIZE: usize = std::mem::size_of::<MintEdge>();
pub const NODE_SIZE: usize = std::mem::size_of::<MintNode>();

#[cfg_attr(not(feature = "debug-graph"), derive(Debug))]
pub struct MintNode {
    pub mint: String,
    pub usd_price: RwLock<Option<USDPriceWithSource>>,
}

pub struct MintEdge {
    pub id: String,
    pub dirty: bool,
    pub last_updated: RwLock<NaiveDateTime>,
    pub inner_relation: RwLock<LiqRelationEnum>,
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
