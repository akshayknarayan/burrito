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

/// Builder for [`Client`].
#[derive(Debug)]
pub struct GcpCreds {
    project_name: Result<String, Report>,
    creds: Result<google_cloud::authorize::ApplicationCredentials, Report>,
}

impl Default for GcpCreds {
    fn default() -> Self {
        GcpCreds {
            project_name: Err(eyre!("Must supply project name")),
            creds: Err(eyre!("Must supply GCP credentials")),
        }
    }
}

impl GcpCreds {
    /// Reads the environment variables `GOOGLE_APPLICATION_CREDENTIALS` and `GCP_PROJECT_NAME`.
    pub fn with_env_vars(self) -> Self {
        self.creds_path_env().project_name_env()
    }

    /// Reads `GOOGLE_APPLICATION_CREDENTIALS`.
    pub fn creds_path_env(self) -> Self {
        let creds = std::env::var("GOOGLE_APPLICATION_CREDENTIALS")
            .wrap_err("expected GOOGLE_APPLICATION_CREDENTIALS env var")
            .and_then(|p| std::fs::File::open(p).map_err(Into::into))
            .and_then(|f| serde_json::from_reader(f).map_err(Into::into));
        Self { creds, ..self }
    }

    pub fn with_creds_path(self, path: impl AsRef<std::path::Path>) -> Self {
        let creds = std::fs::File::open(path)
            .map_err(Into::into)
            .and_then(|f| serde_json::from_reader(f).map_err(Into::into));
        Self { creds, ..self }
    }

    /// Reads `GCP_PROJECT_NAME`.
    pub fn project_name_env(self) -> Self {
        GcpCreds {
            project_name: std::env::var("GCP_PROJECT_NAME")
                .wrap_err("expected GCP_PROJECT_NAME env var"),
            ..self
        }
    }

    pub fn with_project_name(self, project_name: impl Into<String>) -> Self {
        Self {
            project_name: Ok(project_name.into()),
            ..self
        }
    }

    pub async fn finish(self) -> Result<Client, Report> {
        Client::from_credentials(self.project_name?, self.creds?)
            .await
            .map_err(Into::into)
    }
}

/// Get a Google cloud PubSub client.
///
/// Requires the environment variables `GCP_PROJECT_NAME` and `GOOGLE_APPLICATION_CREDENTIALS` to be set.
pub async fn default_gcloud_client() -> Result<Client, Report> {
    GcpCreds::default().with_env_vars().finish().await
}

pub async fn make_topic(client: &mut Client, name: String) -> Result<String, Report> {
    Ok(client
        .create_topic(&name, Default::default())
        .await?
        .id()
        .to_owned())
}

pub async fn delete_topic(client: &mut Client, name: String) -> Result<(), Report> {
    client
        .topic(&name)
        .await?
        .ok_or_else(|| eyre!("Topic not found"))?
        .delete()
        .await
        .map_err(Into::into)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PubSubAddr {
    pub topic_id: String,
    pub group: Option<String>,
}

impl From<(String, String)> for PubSubAddr {
    fn from((topic_id, group): (String, String)) -> Self {
        PubSubAddr {
            topic_id,
            group: Some(group),
        }
    }
}

#[derive(Clone, Debug)]
pub struct OrderedPubSubChunnel {
    inner: PubSubChunnel,
}

impl OrderedPubSubChunnel {
    pub async fn new<'a>(
        ps_client: Client,
        recv_topics: impl IntoIterator<Item = &'a str>,
    ) -> Result<Self, Report> {
        let subscriptions = make_subscriptions(ps_client.clone(), recv_topics, true).await?;

        Ok(OrderedPubSubChunnel {
            inner: PubSubChunnel {
                ps_client,
                subscriptions,
            },
        })
    }

    pub async fn cleanup(&mut self) -> Result<(), Report> {
        self.inner.cleanup().await
    }
}

impl ChunnelConnection for OrderedPubSubChunnel {
    type Data = (PubSubAddr, String);

    fn send(
        &self,
        (
            PubSubAddr {
                topic_id,
                group: ordering_key,
            },
            body,
        ): Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        let mut client = self.inner.ps_client.clone();
        Box::pin(async move {
            let ordering_key =
                ordering_key.ok_or(eyre!("Ordered send must include ordering group"))?;
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

impl std::fmt::Debug for PubSubChunnel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PubSubChunnel").finish()
    }
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
    type Data = (PubSubAddr, String);

    fn send(
        &self,
        (PubSubAddr { topic_id, .. }, body): Self::Data,
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

            let ((topic_id, mut msg), _) = futures_util::future::select_ok(futs).await?;
            msg.ack()
                .await
                .wrap_err(eyre!("ACKing message {:?}", msg.id()))?;
            let body = std::string::String::from_utf8(msg.take_data())?;
            // ordering_key is empty string if none was set
            let group = msg.take_ordering_key();
            let group = if group.is_empty() { None } else { Some(group) };
            Ok((PubSubAddr { topic_id, group }, body))
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
    use super::{OrderedPubSubChunnel, PubSubAddr, PubSubChunnel};
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
                const TEST_TOPIC_URL: &str = "my-topic1";
                let gcloud_client = Client::new(project_name.clone())
                    .await
                    .wrap_err("make client")?;
                let mut rch = OrderedPubSubChunnel::new(gcloud_client, vec![TEST_TOPIC_URL])
                    .await
                    .wrap_err("making chunnel")?;
                let gcloud_client = Client::new(project_name.clone())
                    .await
                    .wrap_err("make client")?;
                let mut sch = OrderedPubSubChunnel::new(gcloud_client, vec![])
                    .await
                    .wrap_err("making chunnel")?;

                const GROUP_A: &str = "A";
                const A1: &str = "message A1";
                const A2: &str = "message A2";
                const GROUP_B: &str = "B";
                const B1: &str = "message B1";
                const B2: &str = "message B2";

                let addr_a: PubSubAddr = (TEST_TOPIC_URL.to_string(), GROUP_A.to_string()).into();
                let addr_b: PubSubAddr = (TEST_TOPIC_URL.to_string(), GROUP_B.to_string()).into();

                sch.send((addr_a.clone(), A1.to_string()))
                    .await
                    .wrap_err("send")?;
                sch.send((addr_b.clone(), B1.to_string()))
                    .await
                    .wrap_err("send")?;
                sch.send((addr_a, A2.to_string())).await.wrap_err("send")?;
                sch.send((addr_b, B2.to_string())).await.wrap_err("send")?;

                let (a, msg1) = rch.recv().await.wrap_err("recv")?;
                assert_eq!(a.topic_id, TEST_TOPIC_URL);
                info!(?msg1, "recv msg");

                let (a, msg2) = rch.recv().await.wrap_err("recv")?;
                assert_eq!(a.topic_id, TEST_TOPIC_URL);
                info!(?msg2, "recv msg");

                let (a, msg3) = rch.recv().await.wrap_err("recv")?;
                assert_eq!(a.topic_id, TEST_TOPIC_URL);
                info!(?msg3, "recv msg");

                let (a, msg4) = rch.recv().await.wrap_err("recv")?;
                assert_eq!(a.topic_id, TEST_TOPIC_URL);
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
                sch.cleanup().await?;

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
                const TEST_TOPIC_URL: &str = "my-topic";
                let mut ch = PubSubChunnel::new(gcloud_client, vec![TEST_TOPIC_URL])
                    .await
                    .wrap_err("making chunnel")?;

                let a = PubSubAddr {
                    topic_id: TEST_TOPIC_URL.to_string(),
                    group: None,
                };
                ch.send((a.clone(), "test message".to_string()))
                    .await
                    .wrap_err("send")?;
                let (q, msg) = ch.recv().await.wrap_err("recv")?;
                assert_eq!(q, a);
                assert_eq!(&msg, "test message");
                ch.cleanup().await?;
                Ok::<_, Report>(())
            }
            .instrument(tracing::info_span!("pubsub_send_recv")),
        )
        .unwrap();
    }
}
