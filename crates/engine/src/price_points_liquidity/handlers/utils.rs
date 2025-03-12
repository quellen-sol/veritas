use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::mpsc::Sender;

use crate::calculator::task::CalculatorUpdate;

#[inline]
#[allow(clippy::unwrap_used)]
pub async fn send_update_to_calculator(
    update: CalculatorUpdate,
    calculator_sender: &Sender<CalculatorUpdate>,
    bootstrap_in_progress: &AtomicBool,
) {
    if bootstrap_in_progress.load(Ordering::Relaxed) {
        return;
    }

    calculator_sender
        .send(update)
        .await
        .inspect_err(|e| log::error!("Error sending CalculatorUpdate: {e}"))
        .unwrap();
}
