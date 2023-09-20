//! Kafka wrapper for Bertha.

use bertha::{Chunnel, ChunnelConnection, Negotiate};
use color_eyre::eyre::{eyre, Report, WrapErr};
use futures_util::future::{ready, Ready};
use futures_util::StreamExt;
use queue_steer::{
    MessageQueueAddr, MessageQueueCaps, MessageQueueOrdering, MessageQueueReliability,
};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::{
    admin::AdminClient,
    consumer::{stream_consumer::StreamConsumer, Consumer},
    producer::{future_producer::FutureProducer, FutureRecord},
    ClientConfig,
};
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tracing::{debug_span, trace};
use tracing_futures::Instrument;

pub async fn make_topics(addr: String, topics: Vec<String>) -> Result<(), Report> {
    let topics: Vec<_> = topics
        .iter()
        .map(|name| {
            rdkafka::admin::NewTopic::new(
                name.as_str(),
                10,
                rdkafka::admin::TopicReplication::Fixed(1),
            )
        })
        .collect();
    let client: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", addr)
        .create()?;
    client
        .create_topics(topics.iter(), &rdkafka::admin::AdminOptions::default())
        .await
        .wrap_err("create_topics failed")?
        .into_iter()
        .map(|r| match r {
            Ok(_) | Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => Ok(()),
            Err((t, err)) => {
                Err(eyre!(err)).wrap_err_with(|| eyre!("Failed to create topic {:?}", t))
            }
        })
        .collect()
}

pub async fn delete_topic(addr: String, name: Vec<String>) -> Result<(), Report> {
    let client: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", addr)
        .create()?;
    let name_str: Vec<_> = name.iter().map(|s| s.as_str()).collect();
    client
        .delete_topics(&name_str, &rdkafka::admin::AdminOptions::default())
        .await
        .wrap_err("delete_topics failed")?
        .into_iter()
        .map(|r| match r {
            Ok(_) | Err((_, RDKafkaErrorCode::UnknownTopic)) => Ok(()),
            Err((t, err)) => {
                Err(eyre!(err)).wrap_err_with(|| eyre!("Failed to delete topic {:?}", t))
            }
        })
        .collect()
}

#[derive(Clone)]
pub struct KafkaChunnel {
    addr: String,
    topics: Vec<String>,
    consumer_group: Option<String>,
    producer_cfg: Option<ClientConfig>,
    consumer_cfg: Option<ClientConfig>,
}

impl Debug for KafkaChunnel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaChunnel")
            .field("recv_topics", &self.topics)
            .finish()
    }
}

impl KafkaChunnel {
    /// recv_topics: (topic, Option<consumer_group_id>)
    pub fn new<'a>(
        addr: &str,
        recv_topics: impl IntoIterator<Item = &'a str>,
        consumer_group_id: Option<&str>,
    ) -> Self {
        Self {
            addr: addr.to_owned(),
            topics: recv_topics.into_iter().map(|s| s.to_owned()).collect(),
            consumer_group: consumer_group_id.map(|s| s.to_owned()),
            producer_cfg: None,
            consumer_cfg: None,
        }
    }

    pub fn send_cfg(self, cfg: ClientConfig) -> Self {
        Self {
            producer_cfg: Some(cfg),
            ..self
        }
    }

    pub fn recv_cfg(self, cfg: ClientConfig) -> Self {
        Self {
            consumer_cfg: Some(cfg),
            ..self
        }
    }
}

impl<InC> Chunnel<InC> for KafkaChunnel {
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = KafkaConn;
    type Error = Report;

    fn connect_wrap(&mut self, _: InC) -> Self::Future {
        ready((|| {
            let mut producer_cfg = self
                .producer_cfg
                .clone()
                .unwrap_or_else(|| default_producer_cfg(&self.addr));
            producer_cfg.set("bootstrap.servers", &self.addr);
            let mut consumer_cfg = self.consumer_cfg.clone().unwrap_or_else(|| {
                default_consumer_cfg(&self.addr, self.consumer_group.as_ref().map(|s| s.as_str()))
            });
            consumer_cfg.set("bootstrap.servers", &self.addr);
            let cn = KafkaConn::new_with_cfg(producer_cfg, consumer_cfg)?;
            let t: Vec<&str> = self.topics.iter().map(|s| s.as_str()).collect();
            cn.listen(&t[..])?;
            tracing::info!(?t, ?self.addr,  "new kafka connection");
            Ok(cn)
        })())
    }
}

impl Negotiate for KafkaChunnel {
    type Capability = MessageQueueCaps;

    fn guid() -> u64 {
        0x982a6af1c899fe15
    }

    fn capabilities() -> Vec<Self::Capability> {
        vec![MessageQueueCaps {
            ordering: MessageQueueOrdering::Ordered,
            reliability: MessageQueueReliability::AtMostOnce,
        }]
    }
}

#[derive(Clone)]
pub struct KafkaConn {
    producer: FutureProducer,
    consumer: Arc<StreamConsumer>,
}

impl std::fmt::Debug for KafkaConn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_tuple("KafkaConn").finish()
    }
}

fn default_consumer_cfg(addr: &str, consumer_group_id: Option<&str>) -> ClientConfig {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", addr);
    cfg.set(
        "group.id",
        consumer_group_id
            .map(str::to_owned)
            .unwrap_or_else(gen_resource_id),
    )
    .set("enable.auto.commit", "false")
    .set("auto.offset.reset", "smallest")
    .set("enable.partition.eof", "false")
    .set("socket.nagle.disable", "true");
    cfg
}

fn default_producer_cfg(addr: &str) -> ClientConfig {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", addr)
        .set("socket.nagle.disable", "true");
    cfg
}

impl KafkaConn {
    pub fn new(addr: &str) -> Result<Self, Report> {
        Self::new_with_cfg(default_producer_cfg(addr), default_consumer_cfg(addr, None))
    }

    pub fn new_with_batch_size(addr: &str, batch_size_bytes: usize) -> Result<Self, Report> {
        let mut cfg = default_producer_cfg(addr);
        cfg.set("batch.size", batch_size_bytes.to_string());
        tracing::debug!(?batch_size_bytes, "kafka producer config");
        Self::new_with_cfg(cfg, default_consumer_cfg(addr, None))
    }

    pub fn new_with_cfg(
        producer_cfg: ClientConfig,
        consumer_cfg: ClientConfig,
    ) -> Result<Self, Report> {
        tracing::debug!(consumer_group_id = ?&consumer_cfg.get("group.id"), "making KafkaConn");
        Ok(KafkaConn {
            producer: producer_cfg.create()?,
            consumer: Arc::new(consumer_cfg.create()?),
        })
    }

    pub fn listen(&self, topics: &[&str]) -> Result<(), Report> {
        self.consumer.subscribe(topics)?;
        Ok(())
    }
}

impl ChunnelConnection for KafkaConn {
    type Data = (MessageQueueAddr, String);

    fn send<'cn, B>(
        &'cn self,
        burst: B,
        //: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'cn>>
    where
        B: IntoIterator<Item = Self::Data> + Send + 'cn,
        <B as IntoIterator>::IntoIter: Send,
    {
        Box::pin(async move {
            // producer.send() uses internal buffering, so it is not expected to be necessary
            // to await the sends in this loop concurrently.
            for (MessageQueueAddr { topic_id, group }, body) in burst {
                let fr = FutureRecord {
                    topic: &topic_id,
                    partition: None,
                    payload: Some(&body),
                    key: group.as_ref(),
                    timestamp: Some(time()),
                    headers: None,
                };

                trace!(?topic_id, "sending message");
                match {
                    match self.producer.send_result(fr) {
                        Ok(f) => f.await,
                        Err((e, _)) => {
                            return Err::<_, Report>(e.into())
                                .wrap_err("kafka internal queue is full")
                        }
                    }
                } {
                    Ok(Ok((_, _))) => (),
                    Err(_) => {
                        // we won't cancel the future without cancelling this future.
                        unreachable!()
                    }
                    Ok(Err((e, _))) => {
                        return Err::<_, Report>(e.into()).wrap_err("kafka error on send");
                    }
                }
            }

            Ok(())
        })
    }

    fn recv<'cn, 'buf>(
        &'cn self,
        msgs_buf: &'buf mut [Option<Self::Data>],
    ) -> Pin<Box<dyn Future<Output = Result<&'buf mut [Option<Self::Data>], Report>> + Send + 'cn>>
    where
        'buf: 'cn,
    {
        Box::pin(
            async move {
                let sub = self.consumer.subscription()?;
                let topics: Vec<_> = sub
                    .elements()
                    .into_iter()
                    .map(|s| s.topic().to_owned())
                    .collect();
                trace!(?topics, "receiving");
                use rdkafka::message::Message;
                let kafka_stream = self.consumer.stream();

                let mut slot_idx = 0;
                for (msg, slot) in kafka_stream
                    .ready_chunks(msgs_buf.len())
                    .next()
                    .await
                    .expect("MessageStream never returns None")
                    .into_iter()
                    .zip(msgs_buf.iter_mut())
                {
                    let msg = msg?;
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
                    *slot = Some((
                        MessageQueueAddr {
                            topic_id,
                            group: key,
                        },
                        body,
                    ));
                    slot_idx += 1;
                }

                trace!(num_msgs = ?slot_idx, "got msgs");
                Ok(&mut msgs_buf[..slot_idx])
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
        .chain(
            rng.sample_iter(&rand::distributions::Alphanumeric)
                .map(Into::into)
                .take(10),
        )
        .collect()
}

#[cfg(test)]
mod test {
    use super::{delete_topic, gen_resource_id, make_topic, KafkaConn};
    use bertha::ChunnelConnection;
    use color_eyre::eyre::WrapErr;
    use color_eyre::Report;
    use queue_steer::MessageQueueAddr;
    use tracing::info;
    use tracing_error::ErrorLayer;
    use tracing_futures::Instrument;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    #[ignore]
    #[test]
    fn kafka_send_recv() {
        // relies on Kafka running. To start:
        // 1. wget https://downloads.apache.org/kafka/3.5.1/kafka_2.13-3.5.1.tgz
        // 2. tar -xzf kafka_2.13-3.5.1.tgz
        // 3. ./kafka_2.13-3.5.1/bin/zookeeper-server-start.sh ./kafka_2.13-3.5.1/config/zookeeper.properties
        // 4. ./kafka_2.13-3.5.1/bin/kafka-server-start.sh ./kafka_2.13-3.5.1/config/server.properties

        let kafka_addr =
            std::env::var("KAFKA_SERVER").unwrap_or_else(|_| "localhost:9092".to_owned());
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::filter::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let d = tracing::Dispatch::new(subscriber);
        d.init();
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

                let ch = KafkaConn::new(&kafka_addr)?;
                ch.listen(&[&topic_name])?;
                ch.send(std::iter::once((
                    MessageQueueAddr {
                        topic_id: topic_name.clone(),
                        group: None,
                    },
                    "test message".to_string(),
                )))
                .await
                .wrap_err("kafka queue send")?;
                info!("sent");
                let mut slot = [None];
                let msgs = ch.recv(&mut slot[..]).await.wrap_err("kafka queue recv")?;
                let (_, msg) = msgs[0].take().unwrap();
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
