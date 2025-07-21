use std::{
    sync::{mpsc::SyncSender, Arc},
    time::Duration,
};

use anyhow::Result;
use axum::{extract::State, http::StatusCode, Json};
use chrono::Utc;
use clickhouse::Row;
use futures::{stream::FuturesUnordered, StreamExt};
use serde::Deserialize;
use step_ingestooor_sdk::{
    dooot::{Dooot, MintInfoDooot},
    utils::step_utils::StepUtils,
};

use crate::axum::task::VeritasServerState;

#[derive(Deserialize)]
pub struct RefetchMetadataBody {
    pub mints: Vec<String>,
}

#[derive(Deserialize, Row, Debug)]
pub struct FetchTokensResp {
    pub mint: String,
    pub metadata_uri: Option<String>,
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct MetaplexJSONMetadata {
    pub name: Option<String>,
    pub symbol: Option<String>,
    pub description: Option<String>,
    pub image: Option<String>,
}

impl MetaplexJSONMetadata {
    pub fn trimmed(self) -> Self {
        MetaplexJSONMetadata {
            name: self.name.as_ref().map(|s| trim_padding_bytes(s)),
            symbol: self.symbol.as_ref().map(|s| trim_padding_bytes(s)),
            description: self.description.as_ref().map(|s| trim_padding_bytes(s)),
            image: self.image.as_ref().map(|s| trim_padding_bytes(s)),
        }
    }
}

#[inline]
fn trim_padding_bytes(string: &str) -> String {
    string.replace('\0', "")
}

pub async fn refetch_metadata(
    State(state): State<Arc<VeritasServerState>>,
    Json(body): Json<RefetchMetadataBody>,
) -> Result<StatusCode, StatusCode> {
    let mints = body.mints;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|e| {
            log::error!("Failed to build reqwest client {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let clickhouse = state.clickhouse_client.clone();
    let dooot_sender = state.dooot_publisher_sender.clone();

    match refetch_metadata_for_mints(&mints, &state.step_utils, client, clickhouse, dooot_sender)
        .await
    {
        Ok(_) => Ok(StatusCode::OK),
        Err(e) => {
            log::error!("Failed to refetch metadata: {e:?}");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

pub async fn refetch_metadata_for_mints(
    mints: &[String],
    step_utils: &StepUtils,
    client: reqwest::Client,
    clickhouse: clickhouse::Client,
    dooot_sender: SyncSender<Dooot>,
) -> Result<()> {
    // For now, just fetch from clickhouse
    let res = clickhouse
        .query(
            "
                WITH mints AS (
                    SELECT arrayJoin(?) AS mint
                )
                SELECT
                    mint,
                    fnSF_getTokenMetadataUri(mint) as metadata_uri
                FROM mints
            ",
        )
        .bind(mints)
        .fetch_all::<FetchTokensResp>()
        .await?;

    let mut futs = FuturesUnordered::new();

    for token in res {
        log::info!("{token:?}");
        let Some(uri) = token.metadata_uri else {
            // TODO: Fetch from onchain
            log::info!("No meta uri for {}", token.mint);
            continue;
        };

        if uri.is_empty() {
            log::info!("Empty meta uri for {}", token.mint);
            continue;
        }

        let new_uri = step_utils.transform_ipfs_url(&uri).unwrap_or(uri);

        if reqwest::Url::parse(&new_uri).is_err() {
            log::info!("Bad url for {}: {new_uri}", token.mint);
            continue;
        }

        let client = client.clone();
        let fut = async move {
            let resp = client.get(new_uri).send().await;
            (token.mint, resp)
        };

        futs.push(fut);
    }

    let time = Utc::now().naive_utc();

    while let Some((mint, res)) = futs.next().await {
        let res = match res {
            Ok(res) => res,
            Err(e) => {
                log::error!("Failed to fetch metadata for {mint}: {e:?}");
                continue;
            }
        };

        let Ok(meta) = res
            .json::<MetaplexJSONMetadata>()
            .await
            .map(|b| b.trimmed())
        else {
            log::error!("Failed to fetch metadata for {mint}");
            continue;
        };

        let dooot = Dooot::MintInfo(MintInfoDooot {
            time,
            mint,
            description: meta.description,
            logouri: meta.image,
            metadata_fetch_success: Some(time),
            name: meta.name,
            symbol: meta.symbol,
            ..Default::default()
        });

        log::info!("{dooot:?}");

        dooot_sender.send(dooot)?;
    }

    Ok(())
}
