use std::sync::{atomic::Ordering, Arc};

use axum::{extract::State, Json};
use serde::Serialize;

use crate::axum::task::VeritasServerState;

#[derive(Serialize)]
pub struct ToggleCalculationResponse {
    pub calculating: bool,
}

pub async fn toggle_calculation(
    State(state): State<Arc<VeritasServerState>>,
) -> Json<ToggleCalculationResponse> {
    let calculating = state.paused_calculation.fetch_not(Ordering::Relaxed);
    let response = ToggleCalculationResponse { calculating };
    Json(response)
}
