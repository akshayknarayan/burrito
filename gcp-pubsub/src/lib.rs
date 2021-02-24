//! An GCP PubSub wrapper for Bertha, allowing it to be used as transport for chunnels.
//! This can't impl `Chunnel` because it can't wrap another transport; it is the transport.
//!
//! Chunnel data type = (String, String) -> (queue URL, msg_string)

use bertha::ChunnelConnection;
use color_eyre::eyre::{bail, eyre, Report, WrapErr};
use google_cloud::pubsub::{Client, PublishMessage, Subscription, SubscriptionConfig};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

#[derive(Clone)]
pub struct OrderedPubSubChunnel {
    inner: PubSubChunnel,
    ordering_key: String,
}

impl OrderedPubSubChunnel {
    pub async fn new<'a>(
        ps_client: Client,
        recv_topics: impl IntoIterator<Item = &'a str>,
    ) -> Result<Self, Report> {
        let subscriptions = make_subscriptions(ps_client.clone(), recv_topics, true).await?;
        let ordering_key = gen_resource_id();

        Ok(OrderedPubSubChunnel {
            inner: PubSubChunnel {
                ps_client,
                subscriptions,
            },
            ordering_key,
        })
    }

    pub async fn cleanup(&mut self) -> Result<(), Report> {
        self.inner.cleanup().await
    }
}

impl ChunnelConnection for OrderedPubSubChunnel {
    type Data = (String, String);

    fn send(
        &self,
        (topic_id, body): Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        let mut client = self.inner.ps_client.clone();
        let ordering_key = self.ordering_key.clone();
        Box::pin(async move {
            let topic = client
                .topic(&topic_id)
                .await
                .wrap_err(eyre!("get topic {:?}", &topic_id))?;
            if topic.is_none() {
                bail!("Topic not found: {:?}", &topic_id);
            }

            let mut topic = topic.unwrap(); // checked above
            let publish_msg = PublishMessage::from(body.as_bytes()).with_ordering_key(ordering_key);
            topic
                .publish::<_, &'_ [u8]>(publish_msg)
                .await
                .wrap_err(eyre!("Send on topic: {:?}", &topic_id))
        })
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        self.inner.recv()
    }
}

#[derive(Clone)]
pub struct PubSubChunnel {
    ps_client: Client,
    subscriptions: HashMap<String, Subscription>,
}

impl PubSubChunnel {
    pub async fn new<'a>(
        ps_client: Client,
        recv_topics: impl IntoIterator<Item = &'a str>,
    ) -> Result<Self, Report> {
        let subscriptions = make_subscriptions(ps_client.clone(), recv_topics, false).await?;
        Ok(PubSubChunnel {
            ps_client,
            subscriptions,
        })
    }

    // no async drop :'(
    pub async fn cleanup(&mut self) -> Result<(), Report> {
        futures_util::future::try_join_all(self.subscriptions.drain().map(|(_, sub)| sub.delete()))
            .await
            .map(|_| ())
            .map_err(Into::into)
    }
}

impl ChunnelConnection for PubSubChunnel {
    type Data = (String, String);

    fn send(
        &self,
        (topic_id, body): Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        let mut client = self.ps_client.clone();
        Box::pin(async move {
            // TODO cache topic handles?
            // This call basically just checks the topic exists, which could be done without the
            // call by directly calling Topic::new?
            let topic = client
                .topic(&topic_id)
                .await
                .wrap_err(eyre!("get topic {:?}", &topic_id))?;
            if topic.is_none() {
                bail!("Topic not found: {:?}", &topic_id);
            }

            let mut topic = topic.unwrap(); // checked above
            topic
                .publish(body.as_bytes())
                .await
                .wrap_err(eyre!("Send on topic: {:?}", &topic_id))
        })
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        let mut subs = self.subscriptions.clone();
        Box::pin(async move {
            let futs = subs.iter_mut().map(|(topic, sub)| {
                Box::pin(async move {
                    let msg = sub
                        .receive()
                        .await
                        .ok_or_else(|| eyre!("Should not receive None"))?;
                    Ok::<_, Report>((topic.clone(), msg))
                })
            });

            let ((topic, mut msg), _) = futures_util::future::select_ok(futs).await?;
            msg.ack()
                .await
                .wrap_err(eyre!("ACKing message {:?}", msg.id()))?;
            let body = std::str::from_utf8(msg.data())?.to_owned();
            Ok((topic, body))
        })
    }
}

async fn make_subscriptions<'a>(
    ps_client: Client,
    topics: impl IntoIterator<Item = &'a str>,
    with_ordering: bool,
) -> Result<HashMap<String, Subscription>, Report> {
    Ok(
        futures_util::future::try_join_all(topics.into_iter().map(|topic_id| {
            let mut ps_client = ps_client.clone();
            async move {
                let topic = ps_client
                    .topic(topic_id)
                    .await
                    .wrap_err(eyre!("get topic {:?}", &topic_id))?;
                if topic.is_none() {
                    bail!("Topic not found: {:?}", &topic_id);
                }

                let mut topic = topic.unwrap(); // checked above
                let sub_id = gen_resource_id();
                let sub_conf =
                    SubscriptionConfig::default().ack_deadline(chrono::Duration::seconds(15));
                let sub_conf = if with_ordering {
                    sub_conf.enable_message_ordering()
                } else {
                    sub_conf
                };

                Ok((
                    topic.id().to_owned(),
                    topic
                        .create_subscription(&sub_id, sub_conf)
                        .await
                        .wrap_err(eyre!("create_subscription {:?}", &topic_id))?,
                ))
            }
        }))
        .await?
        .into_iter()
        .collect(),
    )
}

fn gen_resource_id() -> String {
    use rand::Rng;
    let rng = rand::thread_rng();
    "b".chars()
        .chain(rng.sample_iter(&rand::distributions::Alphanumeric).take(10))
        .collect()
}

#[cfg(test)]
mod test {
    use super::{OrderedPubSubChunnel, PubSubChunnel};
    use bertha::ChunnelConnection;
    use color_eyre::{
        eyre::{ensure, WrapErr},
        Report,
    };
    use google_cloud::pubsub::Client;
    use tracing::info;
    use tracing_error::ErrorLayer;
    use tracing_futures::Instrument;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    #[test]
    fn pubsub_ordered() {
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
                let project_name =
                    std::env::var("GCLOUD_PROJECT_NAME").wrap_err("GCLOUD_PROJECT_NAME env var")?;
                // this test assumes "my-topic" already exists.
                const TEST_TOPIC_URL: &'static str = "my-topic";
                let gcloud_client = Client::new(project_name.clone())
                    .await
                    .wrap_err("make client")?;
                let mut rch = OrderedPubSubChunnel::new(gcloud_client, vec![TEST_TOPIC_URL])
                    .await
                    .wrap_err("making chunnel")?;
                let gcloud_client = Client::new(project_name.clone())
                    .await
                    .wrap_err("make client")?;
                let mut sch1 = OrderedPubSubChunnel::new(gcloud_client, vec![])
                    .await
                    .wrap_err("making chunnel")?;
                let gcloud_client = Client::new(project_name.clone())
                    .await
                    .wrap_err("make client")?;
                let mut sch2 = OrderedPubSubChunnel::new(gcloud_client, vec![])
                    .await
                    .wrap_err("making chunnel")?;

                const A1: &'static str = "message A1";
                const A2: &'static str = "message A2";
                const B1: &'static str = "message B1";
                const B2: &'static str = "message B2";

                sch1.send((TEST_TOPIC_URL.to_string(), A1.to_string()))
                    .await
                    .wrap_err("send")?;
                sch2.send((TEST_TOPIC_URL.to_string(), B1.to_string()))
                    .await
                    .wrap_err("send")?;
                sch1.send((TEST_TOPIC_URL.to_string(), A2.to_string()))
                    .await
                    .wrap_err("send")?;
                sch2.send((TEST_TOPIC_URL.to_string(), B2.to_string()))
                    .await
                    .wrap_err("send")?;

                let (q, msg1) = rch.recv().await.wrap_err("recv")?;
                assert_eq!(q, TEST_TOPIC_URL);
                info!(?msg1, "recv msg");

                let (q, msg2) = rch.recv().await.wrap_err("recv")?;
                assert_eq!(q, TEST_TOPIC_URL);
                info!(?msg2, "recv msg");

                let (q, msg3) = rch.recv().await.wrap_err("recv")?;
                assert_eq!(q, TEST_TOPIC_URL);
                info!(?msg3, "recv msg");

                let (q, msg4) = rch.recv().await.wrap_err("recv")?;
                assert_eq!(q, TEST_TOPIC_URL);
                info!(?msg4, "recv msg");

                let valid_orders = [
                    [A1, A2, B1, B2],
                    [A1, B1, A2, B2],
                    [A1, B1, B2, A2],
                    [B1, B2, A1, A2],
                    [B1, A1, A2, B2],
                    [B1, A1, B2, A2],
                ];

                rch.cleanup().await?;
                sch1.cleanup().await?;
                sch2.cleanup().await?;

                ensure!(
                    valid_orders
                        .iter()
                        .any(|o| &[&msg1, &msg2, &msg3, &msg4] == o),
                    "invalid ordering"
                );

                Ok::<_, Report>(())
            }
            .instrument(tracing::info_span!("pubsub_send_recv")),
        )
        .unwrap();
    }

    #[test]
    fn pubsub_send_recv() {
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
                let project_name =
                    std::env::var("GCLOUD_PROJECT_NAME").wrap_err("GCLOUD_PROJECT_NAME env var")?;
                let gcloud_client = Client::new(project_name).await.wrap_err("make client")?;
                // this test assumes "my-topic" already exists.
                const TEST_TOPIC_URL: &'static str = "my-topic";
                let mut ch = PubSubChunnel::new(gcloud_client, vec![TEST_TOPIC_URL])
                    .await
                    .wrap_err("making chunnel")?;

                ch.send((TEST_TOPIC_URL.to_string(), "test message".to_string()))
                    .await
                    .wrap_err("send")?;
                let (q, msg) = ch.recv().await.wrap_err("recv")?;
                assert_eq!(q, TEST_TOPIC_URL);
                assert_eq!(&msg, "test message");
                ch.cleanup().await?;
                Ok::<_, Report>(())
            }
            .instrument(tracing::info_span!("pubsub_send_recv")),
        )
        .unwrap();
    }
}
