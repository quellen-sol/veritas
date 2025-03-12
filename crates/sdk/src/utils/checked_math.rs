use rust_decimal::{prelude::FromPrimitive, Decimal};

const PCT_DIFF_TOLERANCE: f64 = 0.001; // 0.1%

pub fn checked_pct_diff(old: &Decimal, new: &Decimal) -> Option<Decimal> {
    Some(new.checked_sub(*old)?.checked_div(*old)?.abs())
}

#[allow(clippy::unwrap_used)]
pub fn is_significant_change(old: &Decimal, new: &Decimal) -> bool {
    checked_pct_diff(old, new).is_some_and(|d| d > Decimal::from_f64(PCT_DIFF_TOLERANCE).unwrap())
}
