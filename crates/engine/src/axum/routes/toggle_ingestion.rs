use std::sync::{atomic::Ordering, Arc};

use axum::{extract::State, Json};
use serde::Serialize;

use crate::axum::task::VeritasServerState;

#[derive(Serialize)]
pub struct ToggleIngestionResponse {
    pub ingesting: bool,
}

pub async fn toggle_ingestion(
    State(state): State<Arc<VeritasServerState>>,
) -> Json<ToggleIngestionResponse> {
    let ingesting = state.paused_ingestion.fetch_not(Ordering::Relaxed);
    let response = ToggleIngestionResponse { ingesting };
    Json(response)
}
