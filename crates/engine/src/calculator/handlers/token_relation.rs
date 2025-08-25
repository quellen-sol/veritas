use std::{
    collections::{HashSet, VecDeque},
    sync::{mpsc::SyncSender, Arc, RwLock},
};

use itertools::Itertools;
use petgraph::{
    graph::{EdgeIndex, NodeIndex},
    visit::EdgeRef,
    Direction,
};
use rust_decimal::Decimal;
use step_ingestooor_sdk::dooot::Dooot;
use veritas_sdk::types::MintPricingGraph;

use crate::calculator::algo::bfs_recalculate;

pub fn handle_token_relation_update(
    graph: Arc<RwLock<MintPricingGraph>>,
    token: NodeIndex,
    updated_edge: EdgeIndex,
    dooot_tx: SyncSender<Dooot>,
    oracle_mint_set: &HashSet<String>,
    sol_index: Arc<RwLock<Option<Decimal>>>,
    max_price_impact: &Decimal,
) {
    let mut g_write = graph.write().expect("Graph write lock poisoned");
    let mut g_scan_copy = g_write.clone();

    clean_downstream_from(token, &mut g_write);

    let mut visited = HashSet::with_capacity(g_write.node_count());

    let Some((src, _)) = g_write.edge_endpoints(updated_edge) else {
        return;
    };

    drop(g_write);

    // Do not consider the source token of this relation
    visited.insert(src);

    let sol_index = sol_index
        .read()
        .expect("Sol index read lock poisoned")
        .as_ref()
        .cloned();

    let recalc_result = bfs_recalculate(
        &mut g_scan_copy,
        token,
        &mut visited,
        dooot_tx.clone(),
        oracle_mint_set,
        &sol_index,
        max_price_impact,
        true,
    );

    match recalc_result {
        Ok(_) => {}
        Err(e) => {
            log::error!("Error during BFS recalculation for NewTokenRatio update: {e}");
        }
    }
}

fn clean_downstream_from(start: NodeIndex, graph: &mut MintPricingGraph) {
    let mut visited = HashSet::with_capacity(graph.node_count());
    let mut queue = VecDeque::with_capacity(graph.node_count());
    queue.push_back(start);

    while let Some(node) = queue.pop_front() {
        visited.insert(node);

        let Some(node_weight_mut) = graph.node_weight_mut(node) else {
            log::error!("UNREACHABLE - NodeIndex {node:?} should always exist");
            return;
        };

        node_weight_mut.dirty = false;

        for edge in graph
            .edges_directed(node, Direction::Outgoing)
            .unique_by(|e| e.id())
        {
            let edge_weight = edge.weight();
            *edge_weight
                .dirty
                .write()
                .expect("Dirty write lock poisoned") = false;
        }

        for neighbor in graph.neighbors_directed(node, Direction::Outgoing) {
            if visited.contains(&neighbor) {
                continue;
            }

            queue.push_back(neighbor);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use petgraph::Graph;
    use rust_decimal::Decimal;
    use std::collections::HashMap;
    use veritas_sdk::liq_relation::LiqRelation;
    use veritas_sdk::ppl_graph::graph::{MintEdge, MintNode, USDPriceWithSource};

    fn create_test_node(mint: &str, dirty: bool) -> MintNode {
        MintNode {
            mint: mint.to_string(),
            dirty,
            usd_price: RwLock::new(Some(USDPriceWithSource::Oracle(Decimal::ONE))),
            cached_fixed_relation: RwLock::new(None),
            non_vertex_relations: RwLock::new(HashMap::new()),
        }
    }

    fn create_test_edge(id: &str, dirty: bool) -> MintEdge {
        MintEdge {
            id: id.to_string(),
            dirty: RwLock::new(dirty),
            cached_price_and_liq: RwLock::new(None),
            last_updated: RwLock::new(Utc::now().naive_utc()),
            inner_relation: RwLock::new(LiqRelation::Fixed {
                amt_per_parent: Decimal::ONE,
            }),
        }
    }

    #[test]
    fn test_clean_downstream_from() {
        // Create a graph with nodes: C -> A -> B
        // All nodes start dirty, after cleaning from A, only A and B should be clean
        let mut graph = Graph::new();

        // Add nodes
        let node_a = graph.add_node(create_test_node("A", true));
        let node_b = graph.add_node(create_test_node("B", true));
        let node_c = graph.add_node(create_test_node("C", true));

        // Add edges: C -> A, A -> B
        let _edge_ca = graph.add_edge(node_c, node_a, create_test_edge("C->A", true));
        let _edge_ab = graph.add_edge(node_a, node_b, create_test_edge("A->B", true));

        // Verify initial state - all nodes should be dirty
        assert!(graph.node_weight(node_a).unwrap().dirty);
        assert!(graph.node_weight(node_b).unwrap().dirty);
        assert!(graph.node_weight(node_c).unwrap().dirty);

        // Clean downstream from A
        clean_downstream_from(node_a, &mut graph);

        // Verify results:
        // - A should be clean (starting node)
        // - B should be clean (downstream from A)
        // - C should remain dirty (upstream from A, not downstream)
        assert!(
            !graph.node_weight(node_a).unwrap().dirty,
            "Node A should be clean"
        );
        assert!(
            !graph.node_weight(node_b).unwrap().dirty,
            "Node B should be clean"
        );
        assert!(
            graph.node_weight(node_c).unwrap().dirty,
            "Node C should remain dirty"
        );

        // Verify edges are also cleaned appropriately
        // Edge A->B should be clean (outgoing from A)
        // Edge C->A should remain dirty (not outgoing from A or B)
        let edge_ab = graph.find_edge(node_a, node_b).unwrap();
        let edge_ca = graph.find_edge(node_c, node_a).unwrap();

        assert!(
            !*graph.edge_weight(edge_ab).unwrap().dirty.read().unwrap(),
            "Edge A->B should be clean"
        );
        assert!(
            *graph.edge_weight(edge_ca).unwrap().dirty.read().unwrap(),
            "Edge C->A should remain dirty"
        );
    }
}
