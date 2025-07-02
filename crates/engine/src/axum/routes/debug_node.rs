use std::{collections::HashMap, str::FromStr, sync::Arc};

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use itertools::Itertools;
use rust_decimal::Decimal;
use veritas_sdk::{
    api::types::{NodeInfo, NodeRelationInfo, RelationWithLiq},
    ppl_graph::utils::get_price_by_node_idx,
};

use crate::axum::task::VeritasServerState;

fn option_is_valid(opt: &Option<&String>) -> bool {
    match opt {
        None => false,
        Some(v) if *v == "false" => false,
        _ => true,
    }
}

pub async fn debug_node_info(
    State(state): State<Arc<VeritasServerState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<NodeInfo>, StatusCode> {
    let mint = params.get("mint").ok_or(StatusCode::BAD_REQUEST)?;
    let only_incoming = option_is_valid(&params.get("only_incoming"));
    let only_outgoing = option_is_valid(&params.get("only_outgoing"));
    let only_acceptable = option_is_valid(&params.get("only_acceptable"));
    let only_non_vertex = option_is_valid(&params.get("only_non_vertex"));
    let price_impact = params
        .get("custom_price_impact")
        .map(|v| Decimal::from_str(v))
        .transpose()
        .map_err(|_| StatusCode::BAD_REQUEST)?
        .unwrap_or(state.max_price_impact);

    let mint_ix = {
        let mi_read = state
            .mint_indicies
            .read()
            .expect("Mint indicies read lock poisoned");
        mi_read.get(mint).cloned().ok_or(StatusCode::NOT_FOUND)?
    };

    let g_read = state.graph.read().expect("Graph read lock poisoned");
    let sol_price = *state
        .sol_price_index
        .read()
        .expect("Sol price index read lock poisoned");
    let this_price = get_price_by_node_idx(&g_read, mint_ix);

    let mut node_info = NodeInfo {
        mint: mint.clone(),
        calculated_price: this_price,
        neighbors: vec![],
        non_vertex_relations: vec![],
    };

    // All non-vertex edges
    if !only_non_vertex {
        let this_node_weight = g_read.node_weight(mint_ix).ok_or(StatusCode::NOT_FOUND)?;
        let non_vertex_relations = this_node_weight
            .non_vertex_relations
            .read()
            .expect("Non vertex relations read lock poisoned");
        for (_, relation) in non_vertex_relations.iter() {
            let relation = relation
                .read()
                .expect("Relation read lock poisoned")
                .clone();

            let liquidity_amount = relation.get_liquidity(Decimal::ZERO, Decimal::ZERO);
            let liquidity_levels = relation.get_liq_levels(Decimal::ZERO);
            let derived_price = relation.get_price(Decimal::ZERO, &g_read);

            let relation_with_liq = RelationWithLiq {
                relation,
                liquidity_amount,
                liquidity_levels,
                derived_price,
            };

            node_info.non_vertex_relations.push(relation_with_liq);
        }
    }

    for neighbor in g_read.neighbors_undirected(mint_ix).unique() {
        let Some(neigh_weight) = g_read.node_weight(neighbor) else {
            log::error!("UNREACHABLE - {neighbor:?} should exist in graph??");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        };

        let neighbor_mint = &neigh_weight.mint;

        let mut relation_info = NodeRelationInfo {
            mint: neighbor_mint.clone(),
            incoming_relations: vec![],
            outgoing_relations: vec![],
        };

        // All outgoing edges
        if !only_incoming {
            for edge in g_read.edges_connecting(mint_ix, neighbor) {
                let e_weight = edge.weight();
                let relation = e_weight
                    .inner_relation
                    .read()
                    .expect("Relation read lock poisoned")
                    .clone();
                let calc_res = if let Some(p) = this_price {
                    let this_tokens_per_sol = sol_price.and_then(|s| s.checked_div(p));
                    let liq = relation.get_liquidity(p, Decimal::ZERO);
                    let levels = this_tokens_per_sol.and_then(|tps| relation.get_liq_levels(tps));
                    let derived = relation.get_price(p, &g_read);

                    Some((liq, levels, derived))
                } else {
                    None
                };

                let mut relation_with_liq = RelationWithLiq {
                    relation,
                    liquidity_amount: None,
                    liquidity_levels: None,
                    derived_price: None,
                };

                if let Some((liquidity_amount, liquidity_levels, derived_price)) = calc_res {
                    relation_with_liq.liquidity_amount = liquidity_amount;
                    relation_with_liq.liquidity_levels = liquidity_levels;
                    relation_with_liq.derived_price = derived_price;
                };

                if only_acceptable
                    && !relation_with_liq
                        .liquidity_levels
                        .as_ref()
                        .is_some_and(|l| l.acceptable(&price_impact))
                {
                    continue;
                }

                relation_info.outgoing_relations.push(relation_with_liq);
            }
        }

        // All incoming edges
        if !only_outgoing {
            for edge in g_read.edges_connecting(neighbor, mint_ix) {
                let e_weight = edge.weight();
                let relation = e_weight
                    .inner_relation
                    .read()
                    .expect("Relation read lock poisoned")
                    .clone();
                let price_neighbor = get_price_by_node_idx(&g_read, neighbor);
                let liquidity_amount =
                    price_neighbor.and_then(|p| relation.get_liquidity(p, Decimal::ZERO));
                let derived_price = if let Some(p) = price_neighbor {
                    relation.get_price(p, &g_read)
                } else {
                    None
                };
                let liquidity_levels = price_neighbor.and_then(|p| {
                    let sol_price = sol_price?;
                    let tokens_per_sol = sol_price.checked_div(p)?;
                    relation.get_liq_levels(tokens_per_sol)
                });

                if only_acceptable
                    && !liquidity_levels
                        .as_ref()
                        .is_some_and(|l| l.acceptable(&price_impact))
                {
                    continue;
                }

                relation_info.incoming_relations.push(RelationWithLiq {
                    relation,
                    liquidity_amount,
                    liquidity_levels,
                    derived_price,
                });
            }
        }

        node_info.neighbors.push(relation_info);
    }

    Ok(Json(node_info))
}
