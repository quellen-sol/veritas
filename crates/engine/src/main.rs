use amqp::AMQPManager;
use anyhow::Result;
use clap::Parser;
use lapin::message::Delivery;
use price_points_liquidity::task::spawn_price_points_liquidity_task;
use step_ingestooor_sdk::dooot::Dooot;

mod amqp;
mod price_points_liquidity;

#[derive(Parser)]
pub struct Args {
    #[clap(flatten)]
    pub amqp: AMQPArgs,

    #[clap(flatten)]
    pub clickhouse: ClickhouseArgs,

    #[clap(long, env)]
    pub enable_db_writes: bool,
}

#[derive(Parser)]
pub struct AMQPArgs {
    #[clap(long, env)]
    pub amqp_url: String,

    #[clap(long, env)]
    pub amqp_debug_user: Option<String>,

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

    let mut tasks = vec![];

    // Connect to amqp
    let AMQPArgs {
        amqp_url,
        ingestooor_dooot_exchange,
        amqp_debug_user,
    } = args.amqp;
    let amqp_manager =
        AMQPManager::new(amqp_url, ingestooor_dooot_exchange, amqp_debug_user).await?;
    amqp_manager.assert_amqp_topology().await?;

    let (d_tx, d_rx) = tokio::sync::mpsc::channel::<Dooot>(2000);

    let amqp_task = amqp_manager.spawn_amqp_listener(d_tx).await?;
    tasks.push(amqp_task);

    // Connect to clickhouse
    let _client = if args.enable_db_writes {
        let ClickhouseArgs {
            clickhouse_user,
            clickhouse_url,
            clickhouse_password,
            clickhouse_database,
        } = &args.clickhouse;

        let mut client = clickhouse::Client::default()
            .with_url(clickhouse_url)
            .with_user(clickhouse_user)
            .with_database(clickhouse_database);

        if let Some(password) = clickhouse_password {
            client = client.with_password(password);
        }

        log::info!("Created Clickhouse client");

        Some(client)
    } else {
        None
    };

    let ppl_task = spawn_price_points_liquidity_task(d_rx).await?;
    tasks.push(ppl_task);

    // Wait for all tasks to finish
    futures::future::join_all(tasks).await;

    Ok(())
}
