use petgraph::Graph;
use rust_decimal::Decimal;
use tokio::sync::Mutex;

pub type MintPricingGraph = Graph<MintNode, Mutex<MintEdge>>;

#[derive(Debug)]
pub struct MintNode {
    pub mint: String,
    pub market: String,
}

#[derive(Debug, Default)]
pub struct MintEdge {
    pub this_per_that: Option<Decimal>,
    pub liquidity: Option<u128>,
}

impl MintEdge {
    pub fn new_mutex(ratio: Option<Decimal>, liquidity: Option<u128>) -> Mutex<Self> {
        Mutex::new(Self {
            this_per_that: ratio,
            liquidity,
        })
    }

    pub fn new_mutex_default() -> Mutex<Self> {
        Mutex::new(Self::default())
    }
}
