use petgraph::graph::NodeIndex;
use rust_decimal::Decimal;

use crate::types::{MintIndiciesMap, MintPricingGraph};

#[inline]
pub async fn get_price_by_node_idx(graph: &MintPricingGraph, node: NodeIndex) -> Option<Decimal> {
    let w = graph.node_weight(node)?;
    let p_r = w.usd_price.read().await;

    p_r.as_ref().map(|p| *p.extract_price())
}

pub async fn get_price_by_mint(
    graph: &MintPricingGraph,
    mint_indicies: &MintIndiciesMap,
    mint: &str,
) -> Option<Decimal> {
    let mint_ix = mint_indicies.get(mint)?;
    get_price_by_node_idx(graph, *mint_ix).await
}
