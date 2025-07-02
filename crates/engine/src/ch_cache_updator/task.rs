use anyhow::Result;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::Receiver,
        Arc, RwLock,
    },
    time::Duration,
};

use clickhouse::{query::RowCursor, Row};
use serde::Deserialize;
use tokio::task::JoinHandle;
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
pub fn spawn_ch_cache_updator_tasks(
    decimals_cache: Arc<RwLock<DecimalCache>>,
    clickhouse_client: clickhouse::Client,
    req_rx: Receiver<String>,
    cache_updator_batch_size: usize,
    bootstrap_in_progress: Arc<AtomicBool>,
) -> [JoinHandle<()>; 2] {
    let requests_lock = Arc::new(RwLock::new(Vec::with_capacity(cache_updator_batch_size)));

    let (wake_channel_tx, mut wake_channel_rx) = tokio::sync::mpsc::channel::<()>(1);

    // Receiver task
    let receiver_request_lock = requests_lock.clone();
    let receiver_task = tokio::spawn(async move {
        while let Ok(mint) = req_rx.recv() {
            if bootstrap_in_progress.load(Ordering::Relaxed) {
                // Do not process graph updates while bootstrapping,
                // We already have all the decimals that are possible to have at this point
                continue;
            }

            let amt_reqs = {
                let mut requests = receiver_request_lock
                    .write()
                    .expect("Requests write lock poisoned");
                requests.push(mint);
                requests.len()
            };

            if amt_reqs >= cache_updator_batch_size {
                wake_channel_tx.send(()).await.unwrap();
            }
        }
    });

    // Query task
    let query_request_lock = requests_lock.clone();
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
                        let mut requests = query_request_lock
                            .write()
                            .expect("Query request lock poisoned");

                        if requests.is_empty() {
                            // Don't perform the mem replacement
                            continue;
                        }

                        // Take the Vec out of the lock, and leaves an empty Vec in its place
                        std::mem::replace(
                            &mut *requests,
                            Vec::with_capacity(cache_updator_batch_size),
                        )
                    };

                    match query_decimals(&clickhouse_client, &mints).await {
                        Ok(result) => {
                            let Some(mut cursor) = result else {
                                continue;
                            };

                            let mut results = Vec::with_capacity(mints.len());

                            loop {
                                match cursor.next().await {
                                    Ok(Some(item)) => {
                                        if let Some(decimals) = item.decimals {
                                            results.push((item.mint_pk.clone(), decimals));
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

                            let mut dc_write = decimals_cache
                                .write()
                                .expect("Decimals cache write lock poisoned");

                            for (mint, decimals) in results {
                                dc_write.insert(mint, decimals);
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

    [receiver_task, query_task]
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
                    argMaxMerge(decimals) as decimals
                FROM lookup_mint_info
                WHERE mint IN (
                    SELECT arrayJoin(arrayMap(x -> base58Decode(x), ?))
                )
                GROUP BY mint
            ",
        )
        .bind(batch);

    Ok(Some(query.fetch::<DecimalResult>()?))
}
