pub const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
pub const USDT_MINT: &str = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";
pub const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
pub const STEP_MINT: &str = "StepAscQoEioFxxWGnh2sLBDFp9d8rvKz2Yp39iDpyT";

pub const SOL_FEED_ACCOUNT_ID: &str = "7UVimffxr9ow1uXYxsr4LHAcV58mLzhmwaeKvJ1pjLiE";
pub const USDC_FEED_ACCOUNT_ID: &str = "Dpw1EAVrSB1ibxiDQyTAW6Zip3J4Btk2x4SgApQCeFbX";

pub const ORACLE_FEED_MAP_PAIRS: [(&str, &str); 2] = [
    (USDC_FEED_ACCOUNT_ID, USDC_MINT),
    (SOL_FEED_ACCOUNT_ID, WSOL_MINT),
];
