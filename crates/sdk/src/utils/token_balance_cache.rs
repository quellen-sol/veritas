use std::collections::HashMap;

use rust_decimal::Decimal;

pub type TokenBalanceCache = HashMap<String, Option<Decimal>>;
