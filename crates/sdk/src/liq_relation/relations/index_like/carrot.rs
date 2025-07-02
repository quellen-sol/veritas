use std::ops::Neg;

use petgraph::graph::NodeIndex;
use rust_decimal::{Decimal, MathematicalOps};

use crate::{
    liq_relation::IndexPart, ppl_graph::utils::get_price_by_node_idx, types::MintPricingGraph,
};

pub fn get_carrot_price(
    graph: &MintPricingGraph,
    decimals_parent: u8,
    parts: &[IndexPart],
) -> Option<Decimal> {
    let mut cm_sum = Decimal::ZERO;

    for part in parts {
        // As of writing, this is a safe cast
        let usd_price = get_price_by_node_idx(graph, NodeIndex::new(part.node_idx as usize))?;

        let decimal_factor = Decimal::TEN.checked_powi((part.decimals as i64).neg())?;
        cm_sum += part
            .ratio
            .checked_mul(decimal_factor)?
            .checked_mul(usd_price)?;
    }

    let dec_factor_parent = Decimal::TEN.checked_powi(decimals_parent as i64)?;

    let final_crt_price = cm_sum.checked_mul(dec_factor_parent)?;

    Some(final_crt_price)
}
