use rust_decimal::{prelude::FromPrimitive, Decimal};

const PCT_DIFF_TOLERANCE: f64 = 0.001; // 0.1%

pub fn checked_pct_diff(old: &Decimal, new: &Decimal) -> Option<Decimal> {
    Some(new.checked_sub(*old)?.checked_div(*old)?.abs())
}

#[allow(clippy::unwrap_used)]
pub fn is_significant_change(old: &Decimal, new: &Decimal) -> bool {
    checked_pct_diff(old, new).is_some_and(|d| d > Decimal::from_f64(PCT_DIFF_TOLERANCE).unwrap())
}

/// Try to make this Decimal fit our CH table spec
pub fn clamp_to_scale(value: &Decimal) -> Decimal {
    value.normalize().trunc_with_scale(9)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use rust_decimal::Decimal;

    use super::clamp_to_scale;

    #[test]
    fn clamping() {
        let value = Decimal::from_str("302.92938638770920892670435169").unwrap();
        let clamped = clamp_to_scale(&value);

        assert_eq!(clamped, Decimal::from_str("302.929386387").unwrap())
    }
}
