use std::sync::{atomic::AtomicBool, Arc};

use axum::{extract::State, http::StatusCode, routing::get, Router};
use tokio::task::JoinHandle;

struct VeritasServerState {
    bootstrap_in_progress: Arc<AtomicBool>,
}

pub fn spawn_axum_server(bootstrap_in_progress: Arc<AtomicBool>) -> JoinHandle<()> {
    tokio::spawn(
        #[allow(clippy::unwrap_used)]
        async move {
            let state = Arc::new(VeritasServerState {
                bootstrap_in_progress,
            });

            let app = Router::new()
                .route("/healthcheck", get(handle_healthcheck))
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
