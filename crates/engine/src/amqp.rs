use anyhow::{Context, Result};
use futures::StreamExt;
use lapin::{
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions,
        BasicQosOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
    },
    types::{AMQPValue, FieldTable},
    BasicProperties, Channel, Connection, ConnectionProperties,
};
use step_ingestooor_sdk::dooot::Dooot;
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};
pub struct AMQPManager {
    channel: Channel,
    dooot_exchange: String,
    queue_name: String,
    dlx_name: String,
    dlq_name: String,
    is_debug: bool,
    prefetch: u16,
    db_writes: bool,
}

impl AMQPManager {
    pub async fn new(
        url: String,
        dooot_exchange: String,
        debug_user: Option<String>,
        prefetch: u16,
        db_writes: bool,
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
            prefetch,
            db_writes,
        })
    }

    pub async fn set_prefetch(&self) -> Result<()> {
        self.channel
            .basic_qos(self.prefetch, BasicQosOptions::default())
            .await?;

        Ok(())
    }

    pub async fn spawn_amqp_listener(&self, msg_tx: Sender<Dooot>) -> Result<JoinHandle<()>> {
        let mut consumer = self
            .channel
            .basic_consume(
                &self.queue_name,
                &self.queue_name,
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;

        log::info!("Spawning AMQP consumer");
        let handle = tokio::spawn(
            #[allow(clippy::unwrap_used)]
            async move {
                while let Some(delivery) = consumer.next().await {
                    match delivery {
                        Ok(delivery) => {
                            // log::info!("Received message: {:?}", delivery);
                            let data = &delivery.data;
                            let dooots = data
                                .split(|b| *b == b'\n')
                                .map(serde_json::from_slice)
                                .collect::<Result<Vec<Dooot>, _>>();
                            match dooots {
                                Ok(dooots) => {
                                    for dooot in dooots {
                                        msg_tx.send(dooot).await.unwrap();
                                    }
                                    delivery.ack(BasicAckOptions::default()).await.unwrap();
                                }
                                Err(e) => {
                                    log::error!("Error parsing dooot: {:?}", e);
                                    delivery.nack(BasicNackOptions::default()).await.unwrap();
                                }
                            }
                        }
                        Err(e) => {
                            panic!("Error receiving message: {:?}", e);
                        }
                    }
                }

                log::warn!("AMQP listener shutting down. Consumer stream finished.");
            },
        );

        Ok(handle)
    }

    #[allow(clippy::unwrap_used)]
    pub async fn spawn_dooot_publisher(
        &self,
        mut dooot_tx: Receiver<Dooot>,
    ) -> Result<JoinHandle<()>> {
        let db_writes = self.db_writes;
        let channel = self.channel.clone();
        let dooot_exchange = self.dooot_exchange.clone();

        let handle = tokio::spawn(async move {
            while let Some(dooot) = dooot_tx.recv().await {
                if !db_writes {
                    continue;
                }

                let payload = serde_json::to_string(&dooot).unwrap().into_bytes();
                channel
                    .basic_publish(
                        &dooot_exchange,
                        "TokenPriceGlobal",
                        BasicPublishOptions::default(),
                        &payload,
                        BasicProperties::default(),
                    )
                    .await
                    .context("Error publishing dooot")
                    .unwrap();
            }
        });

        Ok(handle)
    }

    pub async fn assert_amqp_topology(&self) -> Result<()> {
        log::info!("Asserting AMQP topology...");

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
