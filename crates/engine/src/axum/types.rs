use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use veritas_sdk::ppl_graph::structs::{LiqAmount, LiqRelation};

#[derive(Deserialize, Serialize)]
pub struct NodeInfo {
    pub mint: String,
    pub calculated_price: Option<Decimal>,
    pub neighbors: Vec<NodeRelationInfo>,
}

#[derive(Deserialize, Serialize)]
pub struct NodeRelationInfo {
    pub mint: String,
    pub incoming_relations: Vec<RelationWithLiq>,
    pub outgoing_relations: Vec<RelationWithLiq>,
}

#[derive(Deserialize, Serialize)]
pub struct RelationWithLiq {
    pub relation: LiqRelation,
    pub liquidity: Option<LiqAmount>,
    pub price: Option<Decimal>,
}
