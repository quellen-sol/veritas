use std::sync::Arc;

use anyhow::Result;
use chrono::NaiveDateTime;
use clickhouse::Row;
use rust_decimal::Decimal;
use serde::Deserialize;
use step_ingestooor_sdk::dooot::{Dooot, DoootTrait, MintUnderlyingsGlobalDooot};
use tokio::sync::mpsc::Sender;

#[derive(Deserialize, Row)]
pub struct MintUnderlyingBootstrapRow {
    #[serde(
        deserialize_with = "step_ingestooor_sdk::serde::ch_naive_date_time::deserialize_ch_naive_date_time"
    )]
    time: NaiveDateTime,
    mint: String,
    platform_program_pubkey: String,
    mints: Vec<String>,
    mints_qty_per_one_parent: Vec<f64>,
    total_underlying_amounts: Vec<u64>,
}

impl From<MintUnderlyingBootstrapRow> for Dooot {
    fn from(val: MintUnderlyingBootstrapRow) -> Self {
        let total_underlying_amounts = val
            .total_underlying_amounts
            .iter()
            .map(|x| Decimal::from(*x))
            .collect();

        Dooot::MintUnderlyingsGlobal(MintUnderlyingsGlobalDooot {
            time: val.time,
            mint_pubkey: val.mint,
            mints: val.mints,
            mints_qty_per_one_parent: val.mints_qty_per_one_parent,
            total_underlying_amounts,
            platform_program_pubkey: val.platform_program_pubkey,
            discriminant_id: "".to_string(),
        })
    }
}

const MINT_UNDERLYINGS_GLOBAL_DOOOTS_QUERY: &str = "
    SELECT
        time,
        base58Encode(mint_pubkey) as mint,
        base58Encode(platform_program_pubkey) as platform_program_pubkey,
        arrayMap(x -> base58Encode(x), mints) as mints,
        mints_qty_per_one_parent,
        total_underlying_amounts
    FROM current_mint_underlyings_global_by_mint
";

pub async fn bootstrap_graph(
    clickhouse_client: clickhouse::Client,
    dooot_tx: Arc<Sender<Dooot>>,
) -> Result<()> {
    load_and_send_dooots::<MintUnderlyingBootstrapRow, MintUnderlyingsGlobalDooot>(
        MINT_UNDERLYINGS_GLOBAL_DOOOTS_QUERY,
        "MintUnderlyingsGlobal",
        clickhouse_client.clone(),
        dooot_tx.clone(),
    )
    .await?;

    // TODO: Add other Dooots (Clmm, Dlmm)

    Ok(())
}

pub async fn load_and_send_dooots<'a, I: Deserialize<'a> + Row + Into<Dooot>, D: DoootTrait>(
    sql_query: &str,
    dooot_name: &str,
    clickhouse_client: clickhouse::Client,
    dooot_tx: Arc<Sender<Dooot>>,
) -> Result<()> {
    log::info!("Loading current Dooots from Clickhouse...");

    let mut cursor = clickhouse_client.query(sql_query).fetch::<I>()?;

    let mut count: usize = 0;

    while let Some(row) = cursor.next().await? {
        dooot_tx.send(row.into()).await?;
        count += 1;
    }

    log::info!("Loaded {count} {dooot_name} Dooots from Clickhouse");

    Ok(())
}
