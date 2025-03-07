use anyhow::Result;
use std::{sync::Arc, time::Duration};

use clickhouse::{query::RowCursor, Row};
use serde::Deserialize;
use tokio::sync::{mpsc::Receiver, RwLock};
use veritas_sdk::utils::decimal_cache::DecimalCache;

#[derive(Deserialize, Row)]
pub struct DecimalResult {
    mint_pk: String,
    decimals: Option<u8>,
}

/// Two tasks are spawned here:
/// 1. Receiver task, which receives requests and adds them to a buffer
/// 2. Query task, which wakes up when the buffer has reached the batch size or when a timeout has occurred, and queries the decimals
///
/// No `Arc` for the clickhouse client, since we can clone it and use a pool
#[allow(clippy::unwrap_used)]
pub async fn spawn_ch_cache_updator_task(
    decimals_cache: Arc<RwLock<DecimalCache>>,
    clickhouse_client: clickhouse::Client,
    mut req_rx: Receiver<String>,
    cache_updator_batch_size: usize,
) {
    let requests_lock = Arc::new(RwLock::new(Vec::with_capacity(cache_updator_batch_size)));

    let (wake_channel_tx, mut wake_channel_rx) = tokio::sync::mpsc::channel::<()>(1);

    // Receiver task
    let receiver_lock = requests_lock.clone();
    let receiver_task = tokio::spawn(async move {
        while let Some(mint) = req_rx.recv().await {
            let mut requests = receiver_lock.write().await;
            requests.push(mint);

            if requests.len() >= cache_updator_batch_size {
                wake_channel_tx.send(()).await.unwrap();
            }
        }
    });

    // Query task
    let query_task_lock = requests_lock.clone();
    let query_task = tokio::spawn(async move {
        loop {
            let recv_fut = wake_channel_rx.recv();

            match tokio::time::timeout(Duration::from_millis(100), recv_fut).await {
                Ok(None) => {
                    // Poisoned channel
                    panic!("Batching Wakeup Channel is poisoned!");
                }
                Err(_) | Ok(Some(_)) => {
                    // Query the decimals
                    let mints = {
                        let mut lock = query_task_lock.write().await;

                        // Take the Vec out of the lock, and leaves an empty Vec in its place
                        std::mem::replace(&mut *lock, Vec::with_capacity(cache_updator_batch_size))
                    };

                    if mints.is_empty() {
                        continue;
                    }

                    match query_decimals(&clickhouse_client, &mints).await {
                        Ok(result) => {
                            let Some(mut cursor) = result else {
                                continue;
                            };

                            let mut dc_write = decimals_cache.write().await;
                            loop {
                                match cursor.next().await {
                                    Ok(Some(item)) => {
                                        if let Some(decimals) = item.decimals {
                                            dc_write.insert(item.mint_pk, decimals);
                                        }
                                    }
                                    Err(e) => {
                                        log::error!("Error fetching batch of decimals: {e:?}");
                                        break;
                                    }
                                    Ok(None) => {
                                        break;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            log::error!("Error fetching batch of decimals: {e:?}");
                        }
                    }
                }
            }
        }
    });

    tokio::select! {
        _ = query_task => {
            log::warn!("Cache Updator query task exited!");
        }
        _ = receiver_task => {
            log::warn!("Cache Updator receiver task exited!");
        }
    }
}

#[inline]
pub async fn query_decimals(
    clickhouse_client: &clickhouse::Client,
    batch: &[String],
) -> Result<Option<RowCursor<DecimalResult>>> {
    if batch.is_empty() {
        return Ok(None);
    }

    let query = clickhouse_client
        .query(
            "
                SELECT
                    base58Encode(mint) as mint_pk,
                    anyLastMerge(decimals) as decimals
                FROM lookup_mint_info
                WHERE mint IN (
                    SELECT * FROM arrayMap(x -> base58Decode(x), ?)
                )
                GROUP BY mint
            ",
        )
        .bind(batch);

    Ok(Some(query.fetch::<DecimalResult>()?))
}
