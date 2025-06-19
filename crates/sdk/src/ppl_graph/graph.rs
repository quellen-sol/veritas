use std::{collections::HashMap, fmt::Debug, sync::Arc};

use chrono::NaiveDateTime;
use petgraph::{graph::NodeIndex, Directed, Graph};
use rust_decimal::Decimal;
use tokio::sync::RwLock;

use crate::liq_relation::LiqRelation;

pub type MintGraphNodeIndexType = u32;

pub type MintPricingGraph = Graph<MintNode, MintEdge, Directed, MintGraphNodeIndexType>;
pub type WrappedMintPricingGraph = Arc<RwLock<MintPricingGraph>>;

pub const NODE_SIZE: usize = std::mem::size_of::<MintNode>();
pub const EDGE_SIZE: usize = std::mem::size_of::<MintEdge>();

#[cfg_attr(not(feature = "debug-graph"), derive(Debug))]
pub struct MintNode {
    pub mint: String,
    pub usd_price: RwLock<Option<USDPriceWithSource>>,
    /// Relations that are not represented as edges in the graph, e.g., (BTC, ETH, USDC, USDT) -> CRT.
    ///
    /// Relations like these would require a hypertree, which is not supported by petgraph.
    /// We store them here instead.
    ///
    /// `HashMap<K = Market ID/Discriminant, V = Relation>`
    pub non_vertex_relations: RwLock<Option<HashMap<String, LiqRelation>>>,
}

#[cfg(feature = "debug-graph")]
impl Debug for MintNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:.5}\n{:?}", self.mint, self.usd_price)
    }
}

#[cfg_attr(not(feature = "debug-graph"), derive(Debug))]
pub struct MintEdge {
    pub id: String,
    pub dirty: bool,
    pub last_updated: RwLock<NaiveDateTime>,
    pub inner_relation: RwLock<LiqRelation>,
}

#[cfg(feature = "debug-graph")]
impl Debug for MintEdge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:.5}\n{:?}", self.id, self.inner_relation)
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
