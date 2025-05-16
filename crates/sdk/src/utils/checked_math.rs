use rust_decimal::Decimal;

use crate::constants::POINT_ONE_PERCENT;

#[inline]
pub fn checked_pct_diff(old: &Decimal, new: &Decimal) -> Option<Decimal> {
    Some(new.checked_div(*old)?.checked_sub(Decimal::ONE)?.abs())
}

#[allow(clippy::unwrap_used)]
#[inline]
pub fn is_significant_change(old: &Decimal, new: &Decimal) -> bool {
    checked_pct_diff(old, new).is_some_and(|d| d > POINT_ONE_PERCENT)
}

/// Try to make this Decimal fit our CH table spec (Decimal(18,9))
#[inline]
pub fn clamp_to_scale(value: &Decimal) -> Decimal {
    let normalized = value.normalize();
    let mantissa = normalized.mantissa().abs();
    let mut scale = normalized.scale();

    // Cannot take log10 of 0
    if mantissa == 0 {
        return normalized;
    }

    let digits = (mantissa as f64).log10().floor() as u32 + 1;

    // If total digits exceed 18, reduce scale to fit
    if digits > 18 {
        scale = scale.saturating_sub(digits - 18);
    }

    scale = scale.min(9u32);

    normalized.trunc_with_scale(scale)
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

    #[test]
    fn clamping_digits() {
        let value = Decimal::from_str("302929386387709.20892670435169").unwrap();
        let clamped = clamp_to_scale(&value);

        assert_eq!(clamped, Decimal::from_str("302929386387709.208").unwrap())
    }
}
