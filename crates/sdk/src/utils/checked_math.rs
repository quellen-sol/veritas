use rust_decimal::Decimal;

pub fn checked_pct_diff(old: &Decimal, new: &Decimal) -> Option<Decimal> {
    new.checked_sub(*old)?.checked_div(*old)
}
