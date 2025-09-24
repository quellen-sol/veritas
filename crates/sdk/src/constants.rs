use rust_decimal::Decimal;

pub const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
pub const USDT_MINT: &str = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";
pub const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
pub const STEP_MINT: &str = "StepAscQoEioFxxWGnh2sLBDFp9d8rvKz2Yp39iDpyT";
pub const EMPTY_PUBKEY: &str = "11111111111111111111111111111111";

pub const SOL_FEED_ACCOUNT_ID: &str = "7UVimffxr9ow1uXYxsr4LHAcV58mLzhmwaeKvJ1pjLiE";
pub const USDC_FEED_ACCOUNT_ID: &str = "Dpw1EAVrSB1ibxiDQyTAW6Zip3J4Btk2x4SgApQCeFbX";
pub const USDT_FEED_ACCOUNT_ID: &str = "HT2PLQBcG5EiCcNSaMHAjSgd9F98ecpATbk4Sk5oYuM";

pub const ORACLE_FEED_MAP_PAIRS: [(&str, &str); 3] = [
    (USDC_FEED_ACCOUNT_ID, USDC_MINT),
    (SOL_FEED_ACCOUNT_ID, WSOL_MINT),
    (USDT_FEED_ACCOUNT_ID, USDT_MINT),
];

pub const SWAP_LIMIT_EXEMPT_TOKENS: &[&str] = &[STEP_MINT];

/// 0.001
pub const POINT_ONE_PERCENT: Decimal = Decimal::from_parts(1, 0, 0, false, 3);
pub const MAX_SENSIBLE_PRICE_USD: Decimal = Decimal::from_parts(175_000, 0, 0, false, 0);
/// 100 USD atm
pub const MIN_SWAP_VOLUME_USD: Decimal = Decimal::from_parts(100, 0, 0, false, 0);
/// 5 USD atm
pub const MIN_EXEMPT_SWAP_VOLUME_USD: Decimal = Decimal::from_parts(5, 0, 0, false, 0);
