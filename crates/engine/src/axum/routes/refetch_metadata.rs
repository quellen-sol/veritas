use std::{
    sync::{mpsc::SyncSender, Arc},
    time::Duration,
};

use anyhow::Result;
use axum::{extract::State, http::StatusCode, Json};
use chrono::Utc;
use clickhouse::Row;
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

#[derive(Deserialize, Row)]
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
    pub fn as_trimmed(self) -> Self {
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
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| {
            log::error!("Failed to build reqwest client {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let clickhouse = state.clickhouse_client.clone();
    let dooot_sender = state.dooot_publisher_sender.clone();

    match refetch_metadata_for_mint(&mints, &state.step_utils, client, clickhouse, dooot_sender)
        .await
    {
        Ok(_) => Ok(StatusCode::OK),
        Err(e) => {
            log::error!("Failed to refetch metadata: {e:?}");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

pub async fn refetch_metadata_for_mint(
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
                WITH decoded_mints AS (
                    SELECT arrayJoin(arrayMap(x -> base58Decode(x), ?)) AS mint_pk
                )
                SELECT
                    base58Encode(mint_pk) AS mint,
                    fnSF_getTokenMetadataUri(mint) as metadata_uri
                FROM decoded_mints
            ",
        )
        .bind(mints)
        .fetch_all::<FetchTokensResp>()
        .await?;

    let mut futs = Vec::new();

    for token in res {
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

        let client = client.clone();
        let fut = async move {
            let resp = client.get(new_uri).send().await;
            (token.mint, resp)
        };

        futs.push(fut);
    }

    let final_all = futures::future::join_all(futs).await;

    let time = Utc::now().naive_utc();

    for (mint, res) in final_all {
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
            .map(|b| b.as_trimmed())
        else {
            log::error!("Failed to fetch metadata for {mint}");
            continue;
        };

        dooot_sender.send(Dooot::MintInfo(MintInfoDooot {
            time,
            mint,
            description: meta.description,
            logouri: meta.image,
            metadata_fetch_success: Some(time),
            name: meta.name,
            symbol: meta.symbol,
            ..Default::default()
        }))?;
    }

    Ok(())
}
