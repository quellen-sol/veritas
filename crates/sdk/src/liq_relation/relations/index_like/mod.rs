use rust_decimal::Decimal;

use crate::{
    liq_relation::{relations::index_like::carrot::get_carrot_price, IndexPart},
    types::MintPricingGraph,
};

pub mod carrot;

pub async fn get_index_like_price(
    graph: &MintPricingGraph,
    market_id: &str,
    parts: &[IndexPart],
    decimals_parent: u8,
) -> Option<Decimal> {
    match market_id {
        "CRTx1JouZhzSU6XytsE42UQraoGqiHgxabocVfARTy2s" => {
            // Carrot "Index"
            get_carrot_price(graph, decimals_parent, parts).await
        }
        _ => None,
    }
}
