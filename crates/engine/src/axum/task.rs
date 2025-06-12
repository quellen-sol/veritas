use std::sync::{atomic::AtomicBool, Arc};

use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Router,
};
use rust_decimal::Decimal;
use tokio::{sync::RwLock, task::JoinHandle};
use veritas_sdk::{
    ppl_graph::graph::WrappedMintPricingGraph,
    utils::{
        decimal_cache::DecimalCache, lp_cache::LpCache, token_balance_cache::TokenBalanceCache,
    },
};

use crate::{
    axum::routes::{
        balance_cache::get_balance_cache_token, debug_node::debug_node_info,
        decimal_cache::get_decimal_cache_token, lp_cache::get_lp_cache_pool,
        toggle_calculation::toggle_calculation, toggle_ingestion::toggle_ingestion,
    },
    price_points_liquidity::task::MintIndiciesMap,
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
            });

            let app = Router::new()
                .route("/healthcheck", get(handle_healthcheck))
                .route("/debug-node", get(debug_node_info))
                .route("/lp-cache", get(get_lp_cache_pool))
                .route("/decimal-cache", get(get_decimal_cache_token))
                .route("/balance-cache", get(get_balance_cache_token))
                .route("/toggle-ingestion", post(toggle_ingestion))
                .route("/toggle-calculation", post(toggle_calculation))
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
