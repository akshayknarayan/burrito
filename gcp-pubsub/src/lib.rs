//! An GCP PubSub wrapper for Bertha, allowing it to be used as transport for chunnels.
//! This can't impl `Chunnel` because it can't wrap another transport; it is the transport.
//!
//! Chunnel data type = (String, String) -> (queue URL, msg_string)

use bertha::{Chunnel, ChunnelConnection, Negotiate};
use color_eyre::eyre::{ensure, eyre, Report, WrapErr};
use futures_util::future::ready;
use futures_util::stream::{FuturesUnordered, TryStreamExt};
use google_cloud::pubsub::{Client, PublishMessage, Subscription, SubscriptionConfig, Topic};
use queue_steer::{
    MessageQueueAddr, MessageQueueCaps, MessageQueueOrdering, MessageQueueReliability,
};
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tracing::{debug, trace};

/// The underlying client to Google PubSub.
pub use google_cloud::pubsub::Client as GcpClient;

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

const ORDERING_TOPIC_SUFFIX: &str = ".ord";

pub async fn make_topic(client: &mut Client, mut name: String) -> Result<String, Report> {
    ensure!(
        !name.ends_with(ORDERING_TOPIC_SUFFIX),
        "topic name cannot end with .ord"
    );
    let topic = client
        .create_topic(&name, Default::default())
        .await
        .wrap_err("create default topic");
    name.push_str(ORDERING_TOPIC_SUFFIX);
    let ord_topic = client
        .create_topic(&name, Default::default())
        .await
        .wrap_err("create topic for ordered subscriptions");
    match (topic, ord_topic) {
        (Ok(t), Ok(_)) => Ok(t.id().to_owned()),
        (Err(e), Ok(_)) | (Ok(_), Err(e)) => Err(e.into()),
        (Err(e1), Err(e2)) => Err(e1).wrap_err(e2),
    }
}

pub async fn delete_topic(client: &mut Client, mut name: String) -> Result<(), Report> {
    let t = client.topic(&name).await;
    let topic = match t {
        Ok(Some(top)) => top.delete().await,
        Ok(None) => Ok(()),
        Err(e) => Err(e),
    };

    name.push_str(ORDERING_TOPIC_SUFFIX);
    let t_ord = client.topic(&name).await;
    let ord_topic = match t_ord {
        Ok(Some(top)) => top.delete().await,
        Ok(None) => Ok(()),
        Err(e) => Err(e),
    };

    match (topic, ord_topic) {
        (Ok(_), Ok(_)) => Ok(()),
        (Err(e), Ok(_)) | (Ok(_), Err(e)) => Err(e.into()),
        (Err(e1), Err(e2)) => Err(e1).wrap_err(e2),
    }
}

#[derive(Clone)]
pub struct PubSubChunnel {
    client: Client,
    recv_topics: Vec<String>,
}

impl Debug for PubSubChunnel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PubSubChunnel")
            .field("recv_topics", &self.recv_topics)
            .finish()
    }
}

impl PubSubChunnel {
    pub fn new(client: Client, recv_topics: impl IntoIterator<Item = impl AsRef<str>>) -> Self {
        Self {
            client,
            recv_topics: recv_topics
                .into_iter()
                .map(|s| s.as_ref().to_owned())
                .collect(),
        }
    }
}

impl<InC> Chunnel<InC> for PubSubChunnel {
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Connection = PubSubConn;
    type Error = Report;

    fn connect_wrap(&mut self, _: InC) -> Self::Future {
        let client = self.client.clone();
        let recv_topics = self.recv_topics.clone();
        Box::pin(async move { Ok(PubSubConn::new(client, recv_topics).await?) })
    }
}

impl Negotiate for PubSubChunnel {
    type Capability = MessageQueueCaps;

    fn guid() -> u64 {
        0xf8269884685dc39e
    }

    fn capabilities() -> Vec<Self::Capability> {
        vec![MessageQueueCaps {
            ordering: MessageQueueOrdering::BestEffort,
            reliability: MessageQueueReliability::AtLeastOnce,
        }]
    }
}

#[derive(Clone)]
pub struct PubSubConn {
    ps_client: Client,
    subscriptions: HashMap<String, Subscription>,
    topics: Arc<Mutex<HashMap<String, Topic>>>,
}

impl std::fmt::Debug for PubSubConn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PubSubConn").finish()
    }
}

impl PubSubConn {
    pub async fn new(
        ps_client: Client,
        recv_topics: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<Self, Report> {
        let (subscriptions, topics_cached) =
            make_subscriptions(ps_client.clone(), recv_topics, false).await?;
        debug!(num_subs = ?subscriptions.len(), "returning new gcp connection");
        Ok(PubSubConn {
            ps_client,
            subscriptions,
            topics: Arc::new(Mutex::new(topics_cached)),
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

impl ChunnelConnection for PubSubConn {
    type Data = (MessageQueueAddr, String);

    fn send<'cn, B>(
        &'cn self,
        burst: B,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'cn>>
    where
        B: IntoIterator<Item = Self::Data> + Send + 'cn,
        <B as IntoIterator>::IntoIter: Send,
    {
        let mut ps_client = self.ps_client.clone();
        Box::pin(async move {
            let msgs_with_cached_topics: Vec<_> = {
                let mut topics_local = self.topics.lock().unwrap();
                burst
                    .into_iter()
                    .map(|(MessageQueueAddr { topic_id, group }, body)| {
                        (
                            topics_local
                                .get_mut(&topic_id)
                                .map(|t| (topic_id.clone(), t.clone()))
                                .ok_or(topic_id),
                            (group, body),
                        )
                    })
                    .collect()
            }; // unlock self.topics to avoid holding the lock across the await point below

            let mut new_topics: HashMap<_, _> = Default::default();
            let mut topic_batches: HashMap<_, (Topic, HashMap<_, Vec<_>>)> = Default::default();
            for (maybe_topic, (group, body)) in msgs_with_cached_topics {
                let v = match maybe_topic {
                    Ok((tname, t)) => {
                        let (_, v) = topic_batches
                            .entry(tname)
                            .or_insert((t.clone(), Default::default()));
                        v
                    }
                    Err(topic_name) => {
                        let t = ps_client
                            .topic(&topic_name)
                            .await
                            .wrap_err_with(|| eyre!("get topic {:?}", &topic_name))?
                            .ok_or_else(|| eyre!("topic not found: {:?}", &topic_name))?;
                        new_topics.insert(topic_name.clone(), t.clone());
                        let (_, v) = topic_batches
                            .entry(topic_name)
                            .or_insert((t, Default::default()));
                        v
                    }
                };

                let mut m = PublishMessage::from(body.into_bytes());
                if let Some(g) = group.as_ref() {
                    m = m.with_ordering_key(g);
                }
                v.entry(group).or_insert(Vec::new()).push(m);
            }

            for (topic_id, (mut topic, groups)) in topic_batches {
                for (_, msg_batch) in groups {
                    topic
                        .publish::<_, _, Vec<u8>>(msg_batch.into_iter())
                        .await
                        .wrap_err_with(|| eyre!("Send on topic: {:?}", &topic_id))?;
                }
            }

            {
                let mut topics_local = self.topics.lock().unwrap();
                topics_local.extend(new_topics);
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
        let mut subs = self.subscriptions.clone();
        trace!("called recv");
        Box::pin(async move {
            // first check for things we can return immediately.
            let mut remaining_slots = msgs_buf.len();
            for (topic_id, sub) in &mut subs {
                let sub_buffer_len = sub.buffer_len();
                if sub_buffer_len > 0 {
                    for mut m in sub.drain_buffer(msgs_buf.len()) {
                        m.ack()
                            .await
                            .wrap_err_with(|| eyre!("ACKing message {:?}", m.id()))?;
                        let body = std::string::String::from_utf8(m.take_data())?;
                        // ordering_key is empty string if none was set
                        let group = m.take_ordering_key();
                        let group = if group.is_empty() { None } else { Some(group) };
                        msgs_buf[msgs_buf.len() - remaining_slots] = Some((
                            MessageQueueAddr {
                                topic_id: topic_id.clone(),
                                group,
                            },
                            body,
                        ));
                        remaining_slots -= 1;
                        if remaining_slots == 0 {
                            trace!(num_msgs = ?msgs_buf.len(), "returning");
                            return Ok(&mut msgs_buf[..]);
                        }
                    }
                }
            }

            if remaining_slots < msgs_buf.len() {
                let num_msgs = msgs_buf.len() - remaining_slots;
                trace!(?num_msgs, "returning");
                return Ok(&mut msgs_buf[..num_msgs]);
            }

            // now poll everything
            let futs = subs.iter_mut().map(|(topic, sub)| {
                Box::pin(async move {
                    let sub_id = sub.id().to_owned();
                    let msgs = sub
                        .receive_multiple(remaining_slots)
                        .await
                        .wrap_err_with(|| {
                            eyre!(
                                "receive messages topic={:?} subscription={:?}",
                                topic,
                                sub_id,
                            )
                        })?;
                    Ok::<_, Report>((topic.clone(), msgs))
                })
            });

            let (topic_received_on, msgs_recvd): (_, Vec<_>) = {
                let ((topic_received_on, msgs_recvd_iter), _leftover_futs) =
                    futures_util::future::select_ok(futs).await?;
                Ok::<_, Report>((topic_received_on, msgs_recvd_iter.collect()))
            }?;
            trace!(?topic_received_on, "got messages");

            let acked: FuturesUnordered<_> = msgs_recvd
                .into_iter()
                .map(|mut m| {
                    let topic_received_on = topic_received_on.clone();
                    async move {
                        m.ack()
                            .await
                            .wrap_err_with(|| eyre!("ACKing message {:?}", m.id()))?;
                        let body = std::string::String::from_utf8(m.take_data())?;
                        // ordering_key is empty string if none was set
                        let group = m.take_ordering_key();
                        let group = if group.is_empty() { None } else { Some(group) };
                        trace!(?topic_received_on, ?group, "recvd msg");
                        Ok::<_, Report>((
                            MessageQueueAddr {
                                topic_id: topic_received_on.clone(),
                                group,
                            },
                            body,
                        ))
                    }
                })
                .collect();
            acked
                .try_for_each(|m| {
                    msgs_buf[msgs_buf.len() - remaining_slots] = Some(m);
                    remaining_slots -= 1;
                    if remaining_slots == 0 {
                        ready(Err(eyre!("")))
                    } else {
                        ready(Ok(()))
                    }
                })
                .await
                .unwrap_or(());
            let num_msgs = msgs_buf.len() - remaining_slots;
            trace!(?num_msgs, "returning");
            return Ok(&mut msgs_buf[..num_msgs]);
        })
    }
}

#[derive(Debug, Clone)]
pub struct OrderedPubSubChunnel(PubSubChunnel);
impl From<PubSubChunnel> for OrderedPubSubChunnel {
    fn from(i: PubSubChunnel) -> Self {
        Self(i)
    }
}

impl<InC> Chunnel<InC> for OrderedPubSubChunnel {
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Connection = OrderedPubSubConn;
    type Error = Report;

    fn connect_wrap(&mut self, _: InC) -> Self::Future {
        let client = self.0.client.clone();
        let recv_topics = self.0.recv_topics.clone();
        Box::pin(async move { Ok(OrderedPubSubConn::new(client, recv_topics).await?) })
    }
}

impl Negotiate for OrderedPubSubChunnel {
    type Capability = MessageQueueCaps;

    fn guid() -> u64 {
        0x8521569756866026
    }

    fn capabilities() -> Vec<Self::Capability> {
        vec![MessageQueueCaps {
            ordering: MessageQueueOrdering::Ordered,
            reliability: MessageQueueReliability::AtMostOnce,
        }]
    }
}

#[derive(Clone, Debug)]
pub struct OrderedPubSubConn {
    inner: PubSubConn,
}

impl OrderedPubSubConn {
    pub async fn new(
        ps_client: Client,
        recv_topics: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<Self, Report> {
        let recv_ordered_topics = recv_topics.into_iter().map(|t| {
            let mut s = t.as_ref().to_owned();
            s.push_str(ORDERING_TOPIC_SUFFIX);
            s
        });
        let (subscriptions, topics) =
            make_subscriptions(ps_client.clone(), recv_ordered_topics, true).await?;
        debug!(num_subs = ?subscriptions.len(), "returning new ordered gcp connection");
        Ok(OrderedPubSubConn {
            inner: PubSubConn {
                ps_client,
                subscriptions,
                topics: Arc::new(Mutex::new(topics)),
            },
        })
    }

    pub async fn convert(mut inner: PubSubConn) -> Result<Self, Report> {
        let topics: Vec<_> = inner.subscriptions.keys().cloned().collect();
        inner.cleanup().await?;
        Self::new(inner.ps_client, topics.iter().map(String::as_str)).await
    }

    pub async fn cleanup(&mut self) -> Result<(), Report> {
        self.inner.cleanup().await
    }
}

impl ChunnelConnection for OrderedPubSubConn {
    type Data = (MessageQueueAddr, String);

    fn send<'cn, B>(
        &'cn self,
        burst: B,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'cn>>
    where
        B: IntoIterator<Item = Self::Data> + Send + 'cn,
        <B as IntoIterator>::IntoIter: Send,
    {
        self.inner.send(burst.into_iter().map(
            |(
                MessageQueueAddr {
                    mut topic_id,
                    group,
                },
                s,
            )| {
                topic_id.push_str(ORDERING_TOPIC_SUFFIX);
                (MessageQueueAddr { topic_id, group }, s)
            },
        ))
    }

    fn recv<'cn, 'buf>(
        &'cn self,
        msgs_buf: &'buf mut [Option<Self::Data>],
    ) -> Pin<Box<dyn Future<Output = Result<&'buf mut [Option<Self::Data>], Report>> + Send + 'cn>>
    where
        'buf: 'cn,
    {
        Box::pin(async move {
            let ms = self.inner.recv(msgs_buf).await?;
            for (
                MessageQueueAddr {
                    ref mut topic_id, ..
                },
                _,
            ) in ms.iter_mut().map_while(Option::as_mut)
            {
                ensure!(
                    topic_id.ends_with(ORDERING_TOPIC_SUFFIX),
                    "Received on non-ordered topic"
                );

                let new_len = topic_id.len() - ORDERING_TOPIC_SUFFIX.len();
                topic_id.truncate(new_len);
            }

            Ok(ms)
        })
    }
}

async fn make_subscriptions(
    ps_client: Client,
    topics: impl IntoIterator<Item = impl AsRef<str>>,
    with_ordering: bool,
) -> Result<(HashMap<String, Subscription>, HashMap<String, Topic>), Report> {
    trace!(?with_ordering, "starting make_subscriptions");
    let mut topic_handles: HashMap<_, _> =
        futures_util::future::try_join_all(topics.into_iter().map(|topic_id| {
            let mut ps_client = ps_client.clone();
            async move {
                let topic_id = topic_id.as_ref();
                trace!(?topic_id, "get topic handle");
                let topic = ps_client
                    .topic(topic_id)
                    .await
                    .wrap_err_with(|| eyre!("get topic {:?}", &topic_id))?;
                trace!(?topic_id, "retrieved topic handle");

                Ok::<_, Report>((
                    topic_id.to_owned(),
                    topic.ok_or_else(|| eyre!("Topic not found: {:?}", &topic_id))?,
                ))
            }
        }))
        .await?
        .into_iter()
        .collect();
    trace!("done getting topic handles");

    let subs = futures_util::future::try_join_all(topic_handles.iter_mut().map(
        |(topic_id, topic)| async move {
            let sub_id = gen_resource_id();
            let sub_conf =
                SubscriptionConfig::default().ack_deadline(chrono::Duration::seconds(15));
            let sub_conf = if with_ordering {
                sub_conf.enable_message_ordering()
            } else {
                sub_conf
            };

            debug!(?topic_id, ?sub_id, "create subscription");
            Ok::<_, Report>((
                topic.id().to_owned(),
                topic
                    .create_subscription(&sub_id, sub_conf)
                    .await
                    .wrap_err_with(|| eyre!("create_subscription {:?}", &topic_id))?,
            ))
        },
    ))
    .await?
    .into_iter()
    .collect();

    Ok((subs, topic_handles))
}

fn gen_resource_id() -> String {
    use rand::Rng;
    let rng = rand::thread_rng();
    "b".chars()
        .chain(
            rng.sample_iter(&rand::distributions::Alphanumeric)
                .take(10)
                .map(|x| (x as char)),
        )
        .collect()
}

#[cfg(test)]
mod test {
    use super::{OrderedPubSubConn, PubSubConn};
    use bertha::ChunnelConnection;
    use color_eyre::{
        eyre::{ensure, WrapErr},
        Report,
    };
    use google_cloud::pubsub::Client;
    use queue_steer::MessageQueueAddr;
    use std::iter::once;
    use std::sync::Once;
    use tracing::info;
    use tracing_error::ErrorLayer;
    use tracing_futures::Instrument;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    pub static COLOR_EYRE: Once = Once::new();

    #[ignore]
    #[test]
    fn pubsub_ordered() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        COLOR_EYRE.call_once(|| color_eyre::install().unwrap_or(()));

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .unwrap();
        const TEST_TOPIC_URL: &str = "my-topic1";

        let res = rt.block_on(
            async move {
                let project_name =
                    std::env::var("GCLOUD_PROJECT_NAME").wrap_err("GCLOUD_PROJECT_NAME env var")?;
                let mut gcloud_client = Client::new(project_name.clone())
                    .await
                    .wrap_err("make client")?;

                super::make_topic(&mut gcloud_client, TEST_TOPIC_URL.to_owned())
                    .await
                    .wrap_err("make my-topic1")?;

                let mut rch = OrderedPubSubConn::new(gcloud_client.clone(), vec![TEST_TOPIC_URL])
                    .await
                    .wrap_err("making chunnel")?;
                let mut sch = OrderedPubSubConn::new(gcloud_client, vec![])
                    .await
                    .wrap_err("making chunnel")?;

                const GROUP_A: &str = "A";
                const A1: &str = "message A1";
                const A2: &str = "message A2";
                const GROUP_B: &str = "B";
                const B1: &str = "message B1";
                const B2: &str = "message B2";

                let addr_a: MessageQueueAddr =
                    (TEST_TOPIC_URL.to_string(), GROUP_A.to_string()).into();
                let addr_b: MessageQueueAddr =
                    (TEST_TOPIC_URL.to_string(), GROUP_B.to_string()).into();

                sch.send(once((addr_a.clone(), A1.to_string())))
                    .await
                    .wrap_err("send")?;
                sch.send(once((addr_b.clone(), B1.to_string())))
                    .await
                    .wrap_err("send")?;
                sch.send(once((addr_a, A2.to_string())))
                    .await
                    .wrap_err("send")?;
                sch.send(once((addr_b, B2.to_string())))
                    .await
                    .wrap_err("send")?;

                let mut slot = [None];

                let ms = rch.recv(&mut slot[..]).await.wrap_err("recv")?;
                let (a, msg1) = ms[0].take().unwrap();
                assert_eq!(a.topic_id, TEST_TOPIC_URL);
                info!(?msg1, "recv msg");

                let ms = rch.recv(&mut slot[..]).await.wrap_err("recv")?;
                let (a, msg2) = ms[0].take().unwrap();
                assert_eq!(a.topic_id, TEST_TOPIC_URL);
                info!(?msg2, "recv msg");

                let ms = rch.recv(&mut slot[..]).await.wrap_err("recv")?;
                let (a, msg3) = ms[0].take().unwrap();
                assert_eq!(a.topic_id, TEST_TOPIC_URL);
                info!(?msg3, "recv msg");

                let ms = rch.recv(&mut slot[..]).await.wrap_err("recv")?;
                let (a, msg4) = ms[0].take().unwrap();
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
        );

        rt.block_on(async move {
            let project_name =
                std::env::var("GCLOUD_PROJECT_NAME").wrap_err("GCLOUD_PROJECT_NAME env var")?;
            let mut gcloud_client = Client::new(project_name.clone())
                .await
                .wrap_err("make client")?;
            super::delete_topic(&mut gcloud_client, TEST_TOPIC_URL.to_owned())
                .await
                .wrap_err("delete my-topic1")?;
            Ok::<_, Report>(())
        })
        .unwrap();
        res.unwrap();
    }

    #[ignore]
    #[test]
    fn pubsub_send_recv() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        COLOR_EYRE.call_once(|| color_eyre::install().unwrap_or(()));

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .unwrap();
        const TEST_TOPIC_URL: &str = "my-topic";

        let res = rt.block_on(
            async move {
                let project_name =
                    std::env::var("GCLOUD_PROJECT_NAME").wrap_err("GCLOUD_PROJECT_NAME env var")?;
                let mut gcloud_client = Client::new(project_name).await.wrap_err("make client")?;
                super::make_topic(&mut gcloud_client, TEST_TOPIC_URL.to_owned())
                    .await
                    .wrap_err("make my-topic")?;

                let mut ch = PubSubConn::new(gcloud_client, vec![TEST_TOPIC_URL])
                    .await
                    .wrap_err("making chunnel")?;

                let a = MessageQueueAddr {
                    topic_id: TEST_TOPIC_URL.to_string(),
                    group: None,
                };
                ch.send(once((a.clone(), "test message".to_string())))
                    .await
                    .wrap_err("send")?;
                let mut slot = [None];
                let ms = ch.recv(&mut slot[..]).await.wrap_err("recv")?;
                let (q, msg) = ms[0].take().unwrap();
                assert_eq!(q, a);
                assert_eq!(&msg, "test message");
                ch.cleanup().await?;
                Ok::<_, Report>(())
            }
            .instrument(tracing::info_span!("pubsub_send_recv")),
        );

        rt.block_on(async move {
            let project_name =
                std::env::var("GCLOUD_PROJECT_NAME").wrap_err("GCLOUD_PROJECT_NAME env var")?;
            let mut gcloud_client = Client::new(project_name.clone())
                .await
                .wrap_err("make client")?;
            super::delete_topic(&mut gcloud_client, TEST_TOPIC_URL.to_owned())
                .await
                .wrap_err("delete my-topic1")?;
            Ok::<_, Report>(())
        })
        .unwrap();
        res.unwrap();
    }
}
