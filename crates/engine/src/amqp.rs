use anyhow::Result;
use futures::StreamExt;
use lapin::{
    options::{
        BasicAckOptions, BasicConsumeOptions, ExchangeDeclareOptions, QueueBindOptions,
        QueueDeclareOptions,
    },
    types::{AMQPValue, FieldTable},
    Channel, Connection, ConnectionProperties,
};
use tokio::task::JoinHandle;

pub struct AMQPManager {
    channel: Channel,
    dooot_exchange: String,
    queue_name: String,
    dlx_name: String,
    dlq_name: String,
    is_debug: bool,
}

impl AMQPManager {
    pub async fn new(
        url: String,
        dooot_exchange: String,
        debug_user: Option<String>,
    ) -> Result<Self> {
        let client = Connection::connect(&url, ConnectionProperties::default()).await?;
        let channel = client.create_channel().await?;

        let mut is_debug = false;
        let mut queue_name = String::from("veritas.dooot");
        let mut dlx_name = String::from("veritas.dooot.dead-letter");
        let mut dlq_name = dlx_name.clone();
        if let Some(user) = debug_user {
            is_debug = true;
            queue_name.push_str(format!(".debug.{user}").as_str());
            dlx_name.push_str(format!(".debug.{user}").as_str());
            dlq_name.push_str(format!(".debug.{user}").as_str());
        }

        Ok(Self {
            channel,
            dooot_exchange,
            queue_name,
            dlx_name,
            dlq_name,
            is_debug,
        })
    }

    pub async fn spawn_amqp_listener(&self) -> Result<JoinHandle<()>> {
        let mut consumer = self
            .channel
            .basic_consume(
                &self.queue_name,
                "veritas-dooot",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;

        let handle = tokio::spawn(async move {
            while let Some(delivery) = consumer.next().await {
                match delivery {
                    Ok(delivery) => {
                        log::info!("Received message: {:?}", delivery);
                        delivery.ack(BasicAckOptions::default()).await.unwrap();
                    }
                    Err(e) => {
                        panic!("Error receiving message: {:?}", e);
                    }
                }
            }
        });

        Ok(handle)
    }

    pub async fn assert_amqp_topology(&self) -> Result<()> {
        log::info!("Asserting AMQP topology");

        log::info!("Declaring DLX {}", self.dlx_name);
        // Declare DLX
        self.channel
            .exchange_declare(
                &self.dlx_name,
                lapin::ExchangeKind::Topic,
                ExchangeDeclareOptions {
                    auto_delete: self.is_debug,
                    durable: !self.is_debug,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;

        log::info!("Declaring DLQ {}", self.dlq_name);
        // Declare DLQ
        self.channel
            .queue_declare(
                &self.dlq_name,
                QueueDeclareOptions {
                    auto_delete: self.is_debug,
                    durable: !self.is_debug,
                    exclusive: self.is_debug,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;

        log::info!("Binding DLX {} -> DLQ {}", self.dlx_name, self.dlq_name);
        // Bind DLX -> DLQ
        self.channel
            .queue_bind(
                &self.dlq_name,
                &self.dlx_name,
                "#",
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await?;

        log::info!("Declaring Veritas Dooot queue {}", self.queue_name);
        // Declare Veritas Dooot queue
        let mut queue_args = FieldTable::default();
        queue_args.insert(
            "x-dead-letter-exchange".into(),
            AMQPValue::LongString(self.dlx_name.clone().into()),
        );
        self.channel
            .queue_declare(
                &self.queue_name,
                QueueDeclareOptions {
                    auto_delete: self.is_debug,
                    exclusive: self.is_debug,
                    durable: !self.is_debug,
                    ..Default::default()
                },
                queue_args,
            )
            .await?;

        log::info!("Binding Veritas Dooot queue to dooot exchange");
        // Bind Veritas Dooot queue to dooot exchange
        self.channel
            .queue_bind(
                &self.queue_name,
                &self.dooot_exchange,
                "#",
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await?;

        Ok(())
    }
}
