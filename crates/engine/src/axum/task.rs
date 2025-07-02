use std::{
    collections::HashSet,
    sync::{atomic::AtomicBool, Arc, RwLock},
};

use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Router,
};
use rust_decimal::Decimal;
use tokio::task::JoinHandle;
use veritas_sdk::{
    types::{MintIndiciesMap, WrappedMintPricingGraph},
    utils::{
        decimal_cache::DecimalCache, lp_cache::LpCache, token_balance_cache::TokenBalanceCache,
    },
};

use crate::axum::routes::{
    balance_cache::get_balance_cache_token, debug_node::debug_node_info,
    decimal_cache::get_decimal_cache_token, diagnose::diagnose_node, force_recalc::force_recalc,
    lp_cache::get_lp_cache_pool, node_info::get_node_info, stats::get_stats,
    toggle_calculation::toggle_calculation, toggle_ingestion::toggle_ingestion,
};

pub struct VeritasServerState {
    pub bootstrap_in_progress: Arc<AtomicBool>,
    pub graph: WrappedMintPricingGraph,
    pub mint_indicies: Arc<RwLock<MintIndiciesMap>>,
    pub sol_price_index: Arc<RwLock<Option<Decimal>>>,
    pub lp_cache: Arc<RwLock<LpCache>>,
    pub decimal_cache: Arc<RwLock<DecimalCache>>,
    pub token_balance_cache: Arc<RwLock<TokenBalanceCache>>,
    pub max_price_impact: Decimal,
    pub paused_ingestion: Arc<AtomicBool>,
    pub paused_calculation: Arc<AtomicBool>,
    pub oracle_mint_set: HashSet<String>,
}

pub fn spawn_axum_server(
    bootstrap_in_progress: Arc<AtomicBool>,
    graph: WrappedMintPricingGraph,
    mint_indicies: Arc<RwLock<MintIndiciesMap>>,
    sol_price_index: Arc<RwLock<Option<Decimal>>>,
    lp_cache: Arc<RwLock<LpCache>>,
    decimal_cache: Arc<RwLock<DecimalCache>>,
    token_balance_cache: Arc<RwLock<TokenBalanceCache>>,
    max_price_impact: Decimal,
    paused_ingestion: Arc<AtomicBool>,
    paused_calculation: Arc<AtomicBool>,
    oracle_mint_set: HashSet<String>,
) -> JoinHandle<()> {
    tokio::spawn(
        #[allow(clippy::unwrap_used)]
        async move {
            let state = Arc::new(VeritasServerState {
                bootstrap_in_progress,
                graph,
                mint_indicies,
                sol_price_index,
                lp_cache,
                decimal_cache,
                token_balance_cache,
                max_price_impact,
                paused_ingestion,
                paused_calculation,
                oracle_mint_set,
            });

            let app = Router::new()
                .route("/healthcheck", get(handle_healthcheck))
                .route("/debug-node", get(debug_node_info))
                .route("/lp-cache", get(get_lp_cache_pool))
                .route("/decimal-cache", get(get_decimal_cache_token))
                .route("/balance-cache", get(get_balance_cache_token))
                .route("/toggle-ingestion", post(toggle_ingestion))
                .route("/toggle-calculation", post(toggle_calculation))
                .route("/stats", get(get_stats))
                .route("/force-recalc", post(force_recalc))
                .route("/diagnose-mint", get(diagnose_node))
                .route("/node-info", get(get_node_info))
                .with_state(state);

            let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
            axum::serve(listener, app).await.unwrap()
        },
    )
}

async fn handle_healthcheck(State(state): State<Arc<VeritasServerState>>) -> StatusCode {
    if state
        .bootstrap_in_progress
        .load(std::sync::atomic::Ordering::Relaxed)
    {
        // 503
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        // 200
        StatusCode::OK
    }
}
