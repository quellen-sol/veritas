use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use anyhow::Result;
use chrono::NaiveDateTime;
use clickhouse::Row;
use rust_decimal::Decimal;
use serde::Deserialize;
use step_ingestooor_sdk::dooot::{
    DLMMPart, DlmmGlobalDooot, Dooot, DoootTrait, MintUnderlyingsGlobalDooot,
};
use tokio::{sync::mpsc::Sender, time::Instant};

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

#[derive(Deserialize, Row)]
pub struct DlmmGlobalBootstrapRow {
    #[serde(
        deserialize_with = "step_ingestooor_sdk::serde::ch_naive_date_time::deserialize_ch_naive_date_time"
    )]
    time: NaiveDateTime,
    pool_pubkey: String,
    parts_account_pubkey: String,
    part_index: i32,
    parts: Vec<(u128, Vec<u64>, Vec<u64>, u128)>,
}

impl From<DlmmGlobalBootstrapRow> for Dooot {
    fn from(value: DlmmGlobalBootstrapRow) -> Self {
        let parsed_parts = value
            .parts
            .iter()
            .map(|p| {
                let shares = p.0.to_string();
                let token_amounts = p.1.iter().map(|x| Decimal::from(*x)).collect();
                let fee_amounts = p.2.iter().map(|x| Decimal::from(*x)).collect();
                let price = p.3.to_string();

                DLMMPart {
                    shares,
                    token_amounts,
                    fee_amounts,
                    price,
                }
            })
            .collect();

        Dooot::DlmmGlobal(DlmmGlobalDooot {
            time: value.time,
            pool_pubkey: value.pool_pubkey,
            parts_account_pubkey: value.parts_account_pubkey,
            part_index: value.part_index,
            parts: parsed_parts,
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

const DLMM_GLOBAL_DOOOTS_QUERT: &str = "
    SELECT
        time,
        base58Encode(pool_pubkey) AS pool_pubkey,
        base58Encode(parts_account_pubkey) AS parts_account_pubkey,
        part_index,
        parts
    FROM current_dlmm_global_by_pool_parts
";

pub async fn bootstrap_graph(
    clickhouse_client: clickhouse::Client,
    dooot_tx: Sender<Dooot>,
    bootstrap_in_progress: Arc<AtomicBool>,
) -> Result<()> {
    log::info!("Bootstrapping the graph with current Clickhouse data...");

    bootstrap_in_progress.store(true, Ordering::Relaxed);

    load_and_send_dooots::<MintUnderlyingBootstrapRow, MintUnderlyingsGlobalDooot>(
        MINT_UNDERLYINGS_GLOBAL_DOOOTS_QUERY,
        "MintUnderlyingsGlobal",
        clickhouse_client.clone(),
        dooot_tx.clone(),
    )
    .await?;

    load_and_send_dooots::<DlmmGlobalBootstrapRow, DlmmGlobalDooot>(
        DLMM_GLOBAL_DOOOTS_QUERT,
        "DlmmGlobal",
        clickhouse_client.clone(),
        dooot_tx.clone(),
    )
    .await?;

    // TODO: Add other Dooots (Clmm)

    log::info!("Bootstrap complete");

    Ok(())
}

pub async fn load_and_send_dooots<'a, I: Deserialize<'a> + Row + Into<Dooot>, D: DoootTrait>(
    sql_query: &str,
    dooot_name: &str,
    clickhouse_client: clickhouse::Client,
    dooot_tx: Sender<Dooot>,
) -> Result<()> {
    log::info!("Loading current {dooot_name} Dooots from Clickhouse...");

    let mut cursor = clickhouse_client.query(sql_query).fetch::<I>()?;

    let mut count: usize = 0;
    let mut now = Instant::now();

    while let Some(row) = cursor.next().await? {
        dooot_tx.send(row.into()).await?;
        count += 1;

        if count % 10000 == 0 {
            log::debug!(
                "Loaded {count} {dooot_name} Dooots from Clickhouse in {:?}",
                now.elapsed()
            );
            now = Instant::now();
        }
    }

    log::info!("Loaded {count} {dooot_name} Dooots from Clickhouse");

    Ok(())
}
