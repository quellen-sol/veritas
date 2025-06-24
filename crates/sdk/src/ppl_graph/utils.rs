use petgraph::graph::NodeIndex;
use rust_decimal::Decimal;

use super::graph::MintPricingGraph;

#[inline]
pub async fn get_price_by_node_idx(graph: &MintPricingGraph, node: NodeIndex) -> Option<Decimal> {
    let w = graph.node_weight(node)?;
    let p_r = w
        .usd_price
        .read()
        .await
        .as_ref()
        .map(|p| *p.extract_price());

    p_r
}
