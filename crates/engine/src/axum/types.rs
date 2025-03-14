use serde::{Deserialize, Serialize};
use veritas_sdk::ppl_graph::structs::LiqRelation;

#[derive(Deserialize, Serialize)]
pub struct NodeInfo {
    pub mint: String,
    pub neighbors: Vec<NodeRelationInfo>,
}

#[derive(Deserialize, Serialize)]
pub struct NodeRelationInfo {
    pub mint: String,
    pub incoming_relations: Vec<LiqRelation>,
    pub outgoing_relations: Vec<LiqRelation>,
}
