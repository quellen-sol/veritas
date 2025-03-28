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

/// 0.001
pub const POINT_ONE_PERCENT: Decimal = Decimal::from_parts(1, 0, 0, false, 3);
