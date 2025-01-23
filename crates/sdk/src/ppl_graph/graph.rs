use std::sync::Arc;

use chrono::NaiveDateTime;
use petgraph::Graph;
use rust_decimal::Decimal;
use tokio::sync::Mutex;

pub type MintPricingGraph = Graph<MintNode, Arc<Mutex<MintEdge>>>;

#[derive(Debug)]
pub struct MintNode {
    pub mint: String,
    pub market: String,
    pub last_updated: NaiveDateTime,
}

#[derive(Debug, Default)]
pub struct MintEdge {
    pub this_per_that: Option<Decimal>,
    pub liquidity: Option<u128>,
}

impl MintEdge {
    pub fn new(ratio: Option<Decimal>, liquidity: Option<u128>) -> Self {
        Self {
            this_per_that: ratio,
            liquidity,
        }
    }
}
