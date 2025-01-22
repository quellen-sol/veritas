use anyhow::Result;
use step_ingestooor_sdk::dooot::Dooot;
use tokio::{sync::mpsc::Receiver, task::JoinHandle};

pub async fn spawn_price_points_liquidity_task(mut msg_rx: Receiver<Dooot>) -> Result<JoinHandle<()>> {
    log::info!("Spawning price points liquidity task (PPL)");
    let task = tokio::spawn(async move {
        while let Some(dooot) = msg_rx.recv().await {
            log::info!("Received dooot: {:?}", dooot);
            // ... TODO: Implement PPL
        }

        log::warn!("Price points liquidity task (PPL) shutting down. Channel closed.");
    });

    Ok(task)
}
