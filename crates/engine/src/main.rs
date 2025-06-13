use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use amqp::AMQPManager;
use anyhow::{Context, Result};
use axum::task::spawn_axum_server;
use calculator::task::{spawn_calculator_task, CalculatorUpdate};
use ch_cache_updator::task::spawn_ch_cache_updator_tasks;
use clap::Parser;
use price_points_liquidity::task::{spawn_price_points_liquidity_task, MintIndiciesMap};
use rust_decimal::{prelude::FromPrimitive, Decimal};
use step_ingestooor_sdk::dooot::Dooot;
use tokio::sync::RwLock;
use veritas_sdk::{
    constants::ORACLE_FEED_MAP_PAIRS,
    ppl_graph::{bootstrap::bootstrap_graph, graph::MintPricingGraph},
    utils::{
        decimal_cache::build_decimal_cache, lp_cache::build_lp_cache,
        token_balance_cache::build_token_balance_cache,
    },
};

mod amqp;
mod axum;
mod calculator;
mod ch_cache_updator;
mod price_points_liquidity;
mod reaper;

#[derive(Parser)]
pub struct Args {
    #[clap(flatten)]
    pub amqp: AMQPArgs,

    #[clap(flatten)]
    pub clickhouse: ClickhouseArgs,

    #[clap(long, env)]
    pub enable_db_writes: bool,

    #[clap(long, env, default_value = "20")]
    pub max_calculator_subtasks: u8,

    #[clap(long, env, default_value = "20")]
    pub max_ppl_subtasks: u8,

    #[clap(long, env, default_value = "10000")]
    pub cache_updator_buffer_size: usize,

    #[clap(long, env, default_value = "1000")]
    pub calculator_update_buffer_size: usize,

    #[clap(long, env, default_value = "2000")]
    pub ppl_dooot_buffer_size: usize,

    #[clap(long, env, default_value = "2000")]
    pub dooot_publisher_buffer_size: usize,

    #[clap(long, env, default_value = "10000")]
    pub cache_updator_batch_size: usize,

    #[clap(long, env, default_value = "false")]
    pub skip_bootstrap: bool,

    #[clap(long, env, default_value = "0.25")]
    pub max_price_impact: f64,
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

    let mint_price_graph = Arc::new(RwLock::new(MintPricingGraph::new()));
    let sol_price_index = Arc::new(RwLock::new(None));

    // Only to be used if we never *remove* nodes from the graph
    // See https://docs.rs/petgraph/latest/petgraph/graph/struct.Graph.html#graph-indices
    let mint_indicies = Arc::new(RwLock::new(MintIndiciesMap::new()));

    // Start serving the axum server as early as possible
    let bootstrap_in_progress = Arc::new(AtomicBool::new(true));
    let axum_server_task = spawn_axum_server(
        bootstrap_in_progress.clone(),
        mint_price_graph.clone(),
        mint_indicies.clone(),
        sol_price_index.clone(),
    );

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

    log::info!("Created Clickhouse client");

    let reaper_task = tokio::spawn(reaper::reaper_task(
        mint_price_graph.clone(),
        clickhouse_client.clone(),
    ));

    let decimal_cache = build_decimal_cache(&clickhouse_client).await?;
    let decimal_cache = Arc::new(RwLock::new(decimal_cache));

    let lp_cache = build_lp_cache(&clickhouse_client).await?;
    let lp_cache = Arc::new(RwLock::new(lp_cache));

    let token_balance_cache = build_token_balance_cache(&clickhouse_client).await?;
    let token_balance_cache = Arc::new(RwLock::new(token_balance_cache));

    let oracle_feed_map: Arc<HashMap<String, String>> = Arc::new(
        ORACLE_FEED_MAP_PAIRS
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<HashMap<String, String>>(),
    );
    let oracle_mint_set = ORACLE_FEED_MAP_PAIRS
        .iter()
        .map(|(_, v)| v.to_string())
        .collect::<HashSet<String>>();

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

    // CHANNELS for tasks
    log::info!("Creating channels with the following buffer sizes...");
    log::info!(
        "PPL dooot buffer size (AMQP -> PPL): {}",
        args.ppl_dooot_buffer_size
    );
    log::info!(
        "Cache updator buffer size (PPL -> CU): {}",
        args.cache_updator_buffer_size
    );
    log::info!(
        "Calculator update buffer size (PPL -> CS): {}",
        args.calculator_update_buffer_size
    );
    log::info!(
        "Dooot publisher buffer size (CS -> DP): {}",
        args.dooot_publisher_buffer_size
    );
    let (amqp_dooot_tx, amqp_dooot_rx) =
        tokio::sync::mpsc::channel::<Dooot>(args.ppl_dooot_buffer_size);
    let (ch_cache_updator_req_tx, ch_cache_updator_req_rx) =
        tokio::sync::mpsc::channel::<String>(args.cache_updator_buffer_size);
    let (calculator_sender, calculator_receiver) =
        tokio::sync::mpsc::channel::<CalculatorUpdate>(args.calculator_update_buffer_size);
    let (publish_dooot_tx, publish_dooot_rx) =
        tokio::sync::mpsc::channel::<Dooot>(args.dooot_publisher_buffer_size);

    // "DP" or "Dooot Publisher" Task
    let dooot_publisher_task = amqp_manager.spawn_dooot_publisher(publish_dooot_rx).await;

    let max_price_impact =
        Decimal::from_f64(args.max_price_impact).context("Invalid max price impact")?;

    // "CS" or "Calculator" Task
    let calculator_task = spawn_calculator_task(
        calculator_receiver,
        mint_price_graph.clone(),
        publish_dooot_tx.clone(),
        args.max_calculator_subtasks,
        bootstrap_in_progress.clone(),
        oracle_mint_set,
        sol_price_index,
        max_price_impact,
    );

    // "CU" or "Cache Updator" Task
    let [ch_cache_updator_receiver_task, ch_cache_updator_query_task] =
        spawn_ch_cache_updator_tasks(
            decimal_cache.clone(),
            clickhouse_client.clone(),
            ch_cache_updator_req_rx,
            args.cache_updator_batch_size,
            bootstrap_in_progress.clone(),
        );

    // "PPL" or "Price Points Liquidity" Task
    let ppl_task = spawn_price_points_liquidity_task(
        amqp_dooot_rx,
        mint_price_graph.clone(),
        calculator_sender,
        decimal_cache.clone(),
        lp_cache.clone(),
        oracle_feed_map.clone(),
        args.max_ppl_subtasks,
        ch_cache_updator_req_tx,
        bootstrap_in_progress.clone(),
        mint_indicies.clone(),
        publish_dooot_tx.clone(),
        token_balance_cache.clone(),
    )?;

    // PPL (+CU) -> CS -> DP thread pipeline now set up, note that AMQP is missing.
    // We'll use this incomplete pipeline to bootstrap the graph, and then attach the AMQP task for normal operation.

    // Bootstrap the graph, sending Dooots through the AMQP Sender to act as though we're receiving them from the AMQP listener
    if !args.skip_bootstrap {
        let amqp_dooot_tx_bootstrap_copy = amqp_dooot_tx.clone();
        bootstrap_graph(
            clickhouse_client.clone(),
            amqp_dooot_tx_bootstrap_copy,
            bootstrap_in_progress.clone(),
        )
        .await?;

        let g_read = mint_price_graph.read().await;
        let nodes = g_read.node_count();
        let edges = g_read.edge_count();

        log::info!("Pricing Graph contains {nodes} nodes and {edges} edges post-bootstrap");
    } else {
        log::info!("Skipping bootstrap (flag set)");
    }

    // Wait 3 seconds to ensure that the graph is fully bootstrapped
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    bootstrap_in_progress.store(false, Ordering::Relaxed);

    // Spawn the AMQP listener **after** the bootstrap, so that we don't get flooded with new Dooots during the bootstrap
    // Completing the pipeline AMQP -> PPL (+CU) -> CS -> DP
    let amqp_task = amqp_manager.spawn_amqp_listener(amqp_dooot_tx).await?;

    tokio::select! {
        e = amqp_task => {
            log::warn!("AMQP task exited: {:?}", e);
        }
        e = ppl_task => {
            log::warn!("PPL task exited: {:?}", e);
        }
        e = ch_cache_updator_receiver_task => {
            log::warn!("CH cache updator receiver task exited: {:?}", e);
        }
        e = ch_cache_updator_query_task => {
            log::warn!("CH cache updator query task exited: {:?}", e);
        }
        e = calculator_task => {
            log::warn!("Calculator update task exited: {:?}", e);
        }
        e = dooot_publisher_task => {
            log::warn!("Dooot publisher task exited: {:?}", e);
        }
        e = axum_server_task => {
            log::warn!("Axum server task exited: {:?}", e);
        }
        e = reaper_task => {
            log::warn!("Reaper task exited: {:?}", e);
        }
    }

    log::warn!("Shutting down...");

    Ok(())
}
