use std::{collections::HashMap, fmt::Debug, sync::RwLock};

use chrono::NaiveDateTime;
use petgraph::graph::EdgeIndex;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    liq_relation::LiqRelation,
    ppl_graph::structs::{LiqAmount, LiqLevels},
};

pub const NODE_SIZE: usize = std::mem::size_of::<MintNode>();
pub const EDGE_SIZE: usize = std::mem::size_of::<MintEdge>();

#[cfg_attr(not(feature = "debug-graph"), derive(Debug))]
pub struct MintNode {
    pub mint: String,
    pub dirty: bool,
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

impl Clone for MintNode {
    fn clone(&self) -> Self {
        let non_vertex_relations = RwLock::new(
            self.non_vertex_relations
                .read()
                .expect("Non vertex relations read lock poisoned")
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        RwLock::new(v.read().expect("Relation read lock poisoned").clone()),
                    )
                })
                .collect(),
        );

        MintNode {
            mint: self.mint.clone(),
            dirty: self.dirty,
            usd_price: RwLock::new(
                self.usd_price
                    .read()
                    .expect("Price read lock poisoned")
                    .clone(),
            ),
            cached_fixed_relation: RwLock::new(
                *self
                    .cached_fixed_relation
                    .read()
                    .expect("Fixed relation read lock poisoned"),
            ),
            non_vertex_relations,
        }
    }
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
    pub dirty: RwLock<bool>,
    pub cached_price_and_liq: RwLock<Option<PriceAndLiqInfo>>,
    pub last_updated: RwLock<NaiveDateTime>,
    pub inner_relation: RwLock<LiqRelation>,
}

impl Clone for MintEdge {
    fn clone(&self) -> Self {
        MintEdge {
            id: self.id.clone(),
            dirty: RwLock::new(*self.dirty.read().expect("Dirty read lock poisoned")),
            cached_price_and_liq: RwLock::new(
                self.cached_price_and_liq
                    .read()
                    .expect("Cached price and liq read lock poisoned")
                    .clone(),
            ),
            last_updated: RwLock::new(
                *self
                    .last_updated
                    .read()
                    .expect("Last updated read lock poisoned"),
            ),
            inner_relation: RwLock::new(
                self.inner_relation
                    .read()
                    .expect("Inner relation read lock poisoned")
                    .clone(),
            ),
        }
    }
}

#[cfg(feature = "debug-graph")]
impl Debug for MintEdge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:.5}\n{:?}", self.id, self.inner_relation)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceAndLiqInfo {
    pub price: Option<Decimal>,
    pub liq: Option<LiqAmount>,
    pub liq_levels: Option<LiqLevels>,
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
