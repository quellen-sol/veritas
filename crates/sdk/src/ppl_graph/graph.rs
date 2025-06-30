use std::{collections::HashMap, fmt::Debug};

use chrono::NaiveDateTime;
use petgraph::graph::EdgeIndex;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::liq_relation::LiqRelation;

pub const NODE_SIZE: usize = std::mem::size_of::<MintNode>();
pub const EDGE_SIZE: usize = std::mem::size_of::<MintEdge>();

#[cfg_attr(not(feature = "debug-graph"), derive(Debug))]
pub struct MintNode {
    pub mint: String,
    pub usd_price: RwLock<Option<USDPriceWithSource>>,
    /// If this node has a `Fixed` relation pointing to it (meaning it takes absolute precedence over other relations),
    /// we cache the edge index here to avoid having to traverse the graph to find it
    pub cached_fixed_relation: RwLock<Option<EdgeIndex>>,
    /// Relations that are not represented as edges in the graph, e.g., (BTC, ETH, USDC, USDT) -> CRT.
    ///
    /// Relations like these would require a hypertree, which is not supported by petgraph.
    /// We store them here instead.
    ///
    /// `HashMap<K = Market ID/Discriminant, V = Relation>`
    pub non_vertex_relations: RwLock<HashMap<String, RwLock<LiqRelation>>>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "source")]
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
