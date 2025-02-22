use std::{collections::HashMap, sync::Arc};

use amqp::AMQPManager;
use anyhow::Result;
use calculator::task::{spawn_calculator_task, CalculatorUpdate};
use ch_cache_updator::task::spawn_ch_cache_updator_task;
use clap::Parser;
use price_points_liquidity::task::spawn_price_points_liquidity_task;
use step_ingestooor_sdk::dooot::Dooot;
use tokio::sync::RwLock;
use veritas_sdk::{
    constants::ORACLE_FEED_MAP_PAIRS,
    ppl_graph::graph::MintPricingGraph,
    utils::{
        decimal_cache::build_decimal_cache, lp_cache::build_lp_cache,
        oracle_cache::OraclePriceCache,
    },
};

mod amqp;
mod calculator;
mod ch_cache_updator;
mod price_points_liquidity;

#[derive(Parser)]
pub struct Args {
    #[clap(flatten)]
    pub amqp: AMQPArgs,

    #[clap(flatten)]
    pub clickhouse: ClickhouseArgs,

    #[clap(long, env)]
    pub enable_db_writes: bool,

    #[clap(long, env, default_value = "10")]
    pub max_calculator_subtasks: u8,

    #[clap(long, env, default_value = "10")]
    pub max_ppl_subtasks: u8,
}

#[derive(Parser)]
pub struct AMQPArgs {
    #[clap(long, env)]
    pub amqp_url: String,

    #[clap(long, env)]
    pub amqp_debug_user: Option<String>,

    #[clap(long, env)]
    pub amqp_prefetch: u16,

    #[clap(long, env)]
    pub ingestooor_dooot_exchange: String,
}

#[derive(Parser)]
pub struct ClickhouseArgs {
    #[clap(long, env)]
    pub clickhouse_url: String,

    #[clap(long, env)]
    pub clickhouse_user: String,

    #[clap(long, env)]
    pub clickhouse_password: Option<String>,

    #[clap(long, env)]
    pub clickhouse_database: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    env_logger::init();

    let args = Args::parse();

    // Connect to clickhouse
    let ClickhouseArgs {
        clickhouse_user,
        clickhouse_url,
        clickhouse_password,
        clickhouse_database,
    } = &args.clickhouse;

    let mut clickhouse_client = clickhouse::Client::default()
        .with_url(clickhouse_url)
        .with_user(clickhouse_user)
        .with_database(clickhouse_database);

    if let Some(password) = clickhouse_password {
        clickhouse_client = clickhouse_client.with_password(password);
    }

    let clickhouse_client = Arc::new(clickhouse_client);
    log::info!("Created Clickhouse client");

    let decimal_cache = build_decimal_cache(clickhouse_client.clone()).await?;
    let decimal_cache = Arc::new(RwLock::new(decimal_cache));

    let lp_cache = build_lp_cache(clickhouse_client.clone()).await?;
    let lp_cache = Arc::new(RwLock::new(lp_cache));

    let oracle_feed_map: Arc<HashMap<String, String>> = Arc::new(
        ORACLE_FEED_MAP_PAIRS
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<HashMap<String, String>>(),
    );
    let oracle_cache = Arc::new(RwLock::new(OraclePriceCache::new()));

    // Connect to amqp
    let AMQPArgs {
        amqp_url,
        ingestooor_dooot_exchange,
        amqp_debug_user,
        amqp_prefetch,
    } = args.amqp;
    let amqp_manager = Arc::new(
        AMQPManager::new(
            amqp_url,
            ingestooor_dooot_exchange,
            amqp_debug_user,
            amqp_prefetch,
            args.enable_db_writes,
        )
        .await?,
    );
    amqp_manager.set_prefetch().await?;
    amqp_manager.assert_amqp_topology().await?;

    // CHANNELS for tasks
    let (ch_cache_updator_req_tx, ch_cache_updator_req_rx) =
        tokio::sync::mpsc::channel::<String>(10000);
    let (calculator_sender, calculator_receiver) =
        tokio::sync::mpsc::channel::<CalculatorUpdate>(1000);
    let (amqp_dooot_tx, amqp_dooot_rx) = tokio::sync::mpsc::channel::<Dooot>(2000);
    let (publish_dooot_tx, publish_dooot_rx) = tokio::sync::mpsc::channel::<Dooot>(2000);

    let mint_price_graph = Arc::new(RwLock::new(MintPricingGraph::new()));

    let amqp_task = amqp_manager.spawn_amqp_listener(amqp_dooot_tx).await?;

    let ppl_task = spawn_price_points_liquidity_task(
        amqp_dooot_rx,
        mint_price_graph.clone(),
        calculator_sender,
        decimal_cache.clone(),
        lp_cache.clone(),
        oracle_cache.clone(),
        oracle_feed_map.clone(),
        args.max_ppl_subtasks,
        ch_cache_updator_req_tx,
    )?;

    let dooot_publisher_task = amqp_manager.spawn_dooot_publisher(publish_dooot_rx).await?;

    let calculator_task = spawn_calculator_task(
        calculator_receiver,
        clickhouse_client.clone(),
        mint_price_graph.clone(),
        decimal_cache.clone(),
        lp_cache.clone(),
        oracle_cache.clone(),
        oracle_feed_map.clone(),
        Arc::new(publish_dooot_tx),
        args.max_calculator_subtasks,
    );

    let ch_cache_updator_task = spawn_ch_cache_updator_task(
        decimal_cache.clone(),
        clickhouse_client.clone(),
        ch_cache_updator_req_rx,
    );

    tokio::select! {
        _ = amqp_task => {
            log::warn!("AMQP task exited");
        }
        _ = ppl_task => {
            log::warn!("PPL task exited");
        }
        _ = dooot_publisher_task => {
            log::warn!("Dooot publisher task exited");
        }
        _ = calculator_task => {
            log::warn!("Calculator task exited");
        }
        _ = ch_cache_updator_task => {
            log::warn!("CH cache updator task exited");
        }
    }

    Ok(())
}
