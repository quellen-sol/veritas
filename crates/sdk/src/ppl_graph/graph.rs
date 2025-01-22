use petgraph::Graph;
use tokio::sync::Mutex;

pub type MintPricingGraph = Graph<MintNode, Mutex<MintEdge>>;

pub struct MintNode {
    pub mint: String,
    pub market: String,
}

pub struct MintEdge {
    pub this_per_that: Option<f64>,
    pub liquidity: Option<u128>,
}

impl MintEdge {
    pub fn new(ratio: Option<f64>, liquidity: Option<u128>) -> Mutex<Self> {
        Mutex::new(Self {
            this_per_that: ratio,
            liquidity,
        })
    }
}
