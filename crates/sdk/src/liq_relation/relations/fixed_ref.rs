use petgraph::graph::EdgeIndex;
use rust_decimal::Decimal;

use crate::{
    ppl_graph::{
        structs::{LiqAmount, LiqLevels},
        utils::get_price_by_node_idx,
    },
    types::MintPricingGraph,
};

pub fn get_fixed_ref_price(
    amt_parent_per_dest: &Decimal,
    market_ref: &EdgeIndex,
    graph: &MintPricingGraph,
) -> Option<Decimal> {
    let (a, _) = graph.edge_endpoints(*market_ref)?;
    let price_a = get_price_by_node_idx(graph, a)?;
    let relation_price = graph
        .edge_weight(*market_ref)?
        .inner_relation
        .read()
        .expect("Relation read lock poisoned")
        .get_price(price_a, graph)?;

    amt_parent_per_dest.checked_mul(relation_price)
}

pub fn get_fixed_ref_liq_levels(
    market_ref: &EdgeIndex,
    graph: &MintPricingGraph,
    sol_index: &Option<Decimal>,
) -> Option<LiqLevels> {
    let (a, _) = graph.edge_endpoints(*market_ref)?;
    let price_a = get_price_by_node_idx(graph, a)?;
    let tokens_a_per_sol = sol_index.and_then(|sol_index| sol_index.checked_div(price_a))?;

    let relation_liq_levels = graph
        .edge_weight(*market_ref)?
        .inner_relation
        .read()
        .expect("Relation read lock poisoned")
        .get_liq_levels(tokens_a_per_sol, sol_index, graph)?;

    Some(relation_liq_levels)
}

pub fn get_fixed_ref_liquidity(
    market_ref: &EdgeIndex,
    graph: &MintPricingGraph,
) -> Option<LiqAmount> {
    let (a, _) = graph.edge_endpoints(*market_ref)?;
    let price_a = get_price_by_node_idx(graph, a)?;

    let relation_liquidity = graph
        .edge_weight(*market_ref)?
        .inner_relation
        .read()
        .expect("Relation read lock poisoned")
        .get_liquidity(price_a, Decimal::ZERO, graph)?;

    Some(relation_liquidity)
}
