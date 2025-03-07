use std::collections::HashMap;

use rust_decimal::Decimal;

// K = Mint, V = Price
pub type OraclePriceCache = HashMap<String, Decimal>;
