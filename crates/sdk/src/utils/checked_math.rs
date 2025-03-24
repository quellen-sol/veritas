use rust_decimal::Decimal;

use crate::constants::POINT_ONE_PERCENT;

#[inline]
pub fn checked_pct_diff(old: &Decimal, new: &Decimal) -> Option<Decimal> {
    Some(new.checked_sub(*old)?.checked_div(*old)?.abs())
}

#[allow(clippy::unwrap_used)]
#[inline]
pub fn is_significant_change(old: &Decimal, new: &Decimal) -> bool {
    checked_pct_diff(old, new).is_some_and(|d| d > POINT_ONE_PERCENT)
}

/// Try to make this Decimal fit our CH table spec
#[inline]
pub fn clamp_to_scale(value: &Decimal) -> Decimal {
    value.normalize().trunc_with_scale(6)
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

        assert_eq!(clamped, Decimal::from_str("302.929386").unwrap())
    }
}
