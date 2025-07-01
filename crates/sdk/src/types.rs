use std::{collections::HashMap, sync::Arc};

use petgraph::{
    graph::{EdgeIndex, NodeIndex},
    Directed, Graph,
};
use tokio::sync::RwLock;

use crate::ppl_graph::graph::{MintEdge, MintNode};

pub type MintGraphNodeIndexType = u32;
pub type MintPricingGraph = Graph<MintNode, MintEdge, Directed, MintGraphNodeIndexType>;
pub type WrappedMintPricingGraph = Arc<RwLock<MintPricingGraph>>;

pub type MintIndiciesMap = HashMap<String, NodeIndex>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EdgeIndexMapValue {
    pub normal: Option<EdgeIndex>,
    pub reverse: Option<EdgeIndex>,
}
pub type EdgeIndiciesMap = HashMap<String, EdgeIndexMapValue>; // Given one discriminant (market), we should only have max 2 relations (A -> B, and B -> A)
