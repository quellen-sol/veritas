use petgraph::graph::NodeIndex;
use rust_decimal::Decimal;

use crate::types::{MintIndiciesMap, MintPricingGraph};

#[inline]
pub async fn get_price_by_node_idx(graph: &MintPricingGraph, node: NodeIndex) -> Option<Decimal> {
    graph
        .node_weight(node)?
        .usd_price
        .read()
        .await
        .as_ref()
        .map(|p| *p.extract_price())
}

pub async fn get_price_by_mint(
    graph: &MintPricingGraph,
    mint_indicies: &MintIndiciesMap,
    mint: &str,
) -> Option<Decimal> {
    let mint_ix = mint_indicies.get(mint)?;
    get_price_by_node_idx(graph, *mint_ix).await
}
