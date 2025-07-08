use std::sync::Arc;

use axum::{extract::State, http::StatusCode, Json};
use serde::Deserialize;

use crate::axum::task::VeritasServerState;

#[derive(Deserialize)]
pub struct RefetchMetadataBody {
    pub mints: Vec<String>,
}

pub async fn refetch_metadata(
    State(state): State<Arc<VeritasServerState>>,
    Json(body): Json<RefetchMetadataBody>,
) -> StatusCode {
    StatusCode::OK
}
