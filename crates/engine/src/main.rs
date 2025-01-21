use amqp::AMQPManager;
use anyhow::Result;
use clap::Parser;

mod amqp;

#[derive(Parser)]
pub struct Args {
    #[clap(flatten)]
    pub amqp: AMQPArgs,

    #[clap(flatten)]
    pub clickhouse: ClickhouseArgs,

    #[clap(long, env, default_value_t = false)]
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
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    env_logger::init();
    let args = Args::parse();

    let mut tasks = vec![];

    // Connect to amqp
    let amqp_manager = AMQPManager::new(
        args.amqp.amqp_url,
        args.amqp.ingestooor_dooot_exchange,
        args.amqp.amqp_debug_user,
    )
    .await?;
    amqp_manager.assert_amqp_topology().await?;
    let amqp_task = amqp_manager.spawn_amqp_listener().await?;
    tasks.push(amqp_task);

    // Wait for all tasks to finish
    futures::future::join_all(tasks).await;

    Ok(())
}