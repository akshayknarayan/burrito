//! Kafka wrapper for Bertha.

use bertha::ChunnelConnection;
use color_eyre::eyre::{eyre, Report, WrapErr};
use rdkafka::{
    admin::AdminClient,
    consumer::{stream_consumer::StreamConsumer, Consumer},
    producer::{future_producer::FutureProducer, FutureRecord},
    ClientConfig,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tracing::{debug_span, trace};
use tracing_futures::Instrument;

pub async fn make_topic(addr: &str, name: &str) -> Result<(), Report> {
    let topic = rdkafka::admin::NewTopic::new(name, 10, rdkafka::admin::TopicReplication::Fixed(1));
    let client: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", addr)
        .create()?;
    client
        .create_topics(
            std::iter::once(&topic),
            &rdkafka::admin::AdminOptions::default(),
        )
        .await
        .wrap_err("create_topics failed")?
        .pop()
        .unwrap()
        .map_err(|(s, x)| {
            let r: Report = x.into();
            r.wrap_err(eyre!("Topic creation failed: {:?}", s))
        })?;
    Ok(())
}

pub async fn delete_topic(addr: &str, name: &str) -> Result<(), Report> {
    let client: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", addr)
        .create()?;
    client
        .delete_topics(&[name], &rdkafka::admin::AdminOptions::default())
        .await
        .wrap_err("delete_topics failed")?
        .pop()
        .unwrap()
        .map_err(|(s, x)| {
            let r: Report = x.into();
            r.wrap_err(eyre!("Topic deletion failed: {:?}", s))
        })?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct KafkaAddr {
    pub topic_id: String,
    pub group: Option<String>, // group = partition?
}

#[derive(Clone)]
pub struct KafkaChunnel {
    producer: FutureProducer,
    consumer: Arc<StreamConsumer>,
}

impl std::fmt::Debug for KafkaChunnel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_tuple("KafkaChunnel").finish()
    }
}

fn default_consumer_cfg(addr: &str) -> ClientConfig {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", addr);
    cfg.set("group.id", gen_resource_id())
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "smallest")
        .set("enable.partition.eof", "false");
    cfg
}

fn default_producer_cfg(addr: &str) -> ClientConfig {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", addr);
    cfg
}

impl KafkaChunnel {
    pub fn new(addr: &str) -> Result<Self, Report> {
        Self::new_with_cfg(default_producer_cfg(addr), default_consumer_cfg(addr))
    }

    pub fn new_with_batch_size(addr: &str, batch_size_bytes: usize) -> Result<Self, Report> {
        let mut cfg = default_producer_cfg(addr);
        cfg.set("batch.size", batch_size_bytes.to_string());
        tracing::debug!(?batch_size_bytes, "kafka producer config");
        Self::new_with_cfg(cfg, default_consumer_cfg(addr))
    }

    pub fn new_with_cfg(
        producer_cfg: ClientConfig,
        consumer_cfg: ClientConfig,
    ) -> Result<Self, Report> {
        tracing::debug!(consumer_group_id = ?&consumer_cfg.get("group.id"), "making KafkaChunnel");
        Ok(KafkaChunnel {
            producer: producer_cfg.create()?,
            consumer: Arc::new(consumer_cfg.create()?),
        })
    }

    pub fn listen(&self, topics: &[&str]) -> Result<(), Report> {
        self.consumer.subscribe(topics)?;
        Ok(())
    }
}

impl ChunnelConnection for KafkaChunnel {
    type Data = (KafkaAddr, String);

    fn send(
        &self,
        (KafkaAddr { topic_id, group }, body): Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        let producer = self.producer.clone();
        Box::pin(async move {
            let fr = FutureRecord {
                topic: &topic_id,
                partition: None,
                payload: Some(&body),
                key: group.as_ref(),
                timestamp: Some(time()),
                headers: None,
            };

            trace!(?topic_id, "sending message");
            match producer.send(fr, rdkafka::util::Timeout::Never).await {
                Ok((_, _)) => Ok(()),
                Err((e, _)) => Err::<_, Report>(e.into()).wrap_err("kafka send"),
            }
        })
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        let consumer = Arc::clone(&self.consumer);
        Box::pin(
            async move {
                let sub = consumer.subscription()?;
                let topics: Vec<_> = sub
                    .elements()
                    .into_iter()
                    .map(|s| s.topic().to_owned())
                    .collect();
                trace!(?topics, "receiving");
                use rdkafka::message::Message;
                trace!("waiting on kafka stream");
                let msg = consumer.recv().await.wrap_err("kafka recv stream")?;
                let key = if let Some(Ok(s)) = msg.key_view::<str>() {
                    Some(s.to_owned())
                } else {
                    None
                };
                let body = if let Some(Ok(s)) = msg.payload_view::<str>() {
                    s.to_owned()
                } else {
                    String::new()
                };

                let topic_id = msg.topic().to_owned();
                trace!(?topic_id, "got msg");
                Ok((
                    KafkaAddr {
                        topic_id,
                        group: key,
                    },
                    body,
                ))
            }
            .instrument(debug_span!("kafka_recv")),
        )
    }
}

fn time() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as _
}

fn gen_resource_id() -> String {
    use rand::Rng;
    let rng = rand::thread_rng();
    "k".chars()
        .chain(rng.sample_iter(&rand::distributions::Alphanumeric).take(10))
        .collect()
}

#[cfg(test)]
mod test {
    use super::{delete_topic, gen_resource_id, make_topic};
    use super::{KafkaAddr, KafkaChunnel};
    use bertha::ChunnelConnection;
    use color_eyre::eyre::WrapErr;
    use color_eyre::Report;
    use tracing::info;
    use tracing_error::ErrorLayer;
    use tracing_futures::Instrument;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    #[ignore]
    #[test]
    fn kafka_send_recv() {
        // relies on Kafka running
        let kafka_addr =
            std::env::var("KAFKA_SERVER").unwrap_or_else(|_| "localhost:9092".to_owned());
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        color_eyre::install().unwrap_or(());

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                let topic_name = gen_resource_id();
                info!(?topic_name, ?kafka_addr, "making topic");
                make_topic(&kafka_addr, &topic_name).await?;

                let ch = KafkaChunnel::new(&kafka_addr)?;
                ch.listen(&[&topic_name])?;
                ch.send((
                    KafkaAddr {
                        topic_id: topic_name.clone(),
                        group: None,
                    },
                    "test message".to_string(),
                ))
                .await
                .wrap_err("kafka queue send")?;
                info!("sent");
                let (_, msg) = ch.recv().await.wrap_err("kafka queue recv")?;
                info!("received");
                assert_eq!(&msg, "test message");

                info!(?topic_name, "deleting topic");
                delete_topic(&kafka_addr, &topic_name).await?;
                info!("done");
                Ok::<_, Report>(())
            }
            .instrument(tracing::info_span!("kafka_send_recv")),
        )
        .unwrap();
    }
}
