use rust_decimal::Decimal;
use serde::Serialize;

use crate::liq_relation::LiqRelation;
use crate::ppl_graph::structs::{LiqAmount, LiqLevels};

#[derive(Serialize)]
pub struct NodeInfo {
    pub mint: String,
    pub calculated_price: Option<Decimal>,
    pub non_vertex_relations: Vec<RelationWithLiq>,
    pub neighbors: Vec<NodeRelationInfo>,
}

#[derive(Serialize)]
pub struct NodeRelationInfo {
    pub mint: String,
    pub incoming_relations: Vec<RelationWithLiq>,
    pub outgoing_relations: Vec<RelationWithLiq>,
}

#[derive(Serialize)]
pub struct RelationWithLiq {
    pub relation: LiqRelation,
    pub liquidity_amount: Option<LiqAmount>,
    pub liquidity_levels: Option<LiqLevels>,
    pub derived_price: Option<Decimal>,
}
