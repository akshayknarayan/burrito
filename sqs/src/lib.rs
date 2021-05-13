//! An AWS SQS wrapper for Bertha, allowing it to be used as transport for chunnels.
//! This can't impl `Chunnel` because it can't wrap another transport; it is the transport.
//!
//! Chunnel data type = (String, String) -> (queue URL, msg_string)

// TODO: publish to SNS topics, auto-create SQS queues as subscriptions
use bertha::ChunnelConnection;
use color_eyre::eyre::{ensure, eyre, Report, WrapErr};
use rusoto_sqs::{
    DeleteMessageRequest, Message, ReceiveMessageRequest, ReceiveMessageResult, SendMessageRequest,
    SendMessageResult, Sqs,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::{atomic::AtomicUsize, Arc};
use tracing::{debug_span, instrument, trace};
use tracing_futures::Instrument;

/// The underlying client type to access Sqs.
pub use rusoto_sqs::SqsClient;

#[derive(Clone)]
pub struct AwsAccess {
    key_id: Option<String>,
    key_secret: Option<String>,
    region: rusoto_core::Region,
}

impl Default for AwsAccess {
    fn default() -> Self {
        AwsAccess {
            key_id: None,
            key_secret: None,
            region: rusoto_core::Region::UsEast1,
        }
    }
}

impl AwsAccess {
    pub fn key(self, id: String, secret: String) -> Self {
        Self {
            key_id: Some(id),
            key_secret: Some(secret),
            ..self
        }
    }

    pub fn region(self, region: rusoto_core::Region) -> Self {
        Self { region, ..self }
    }

    pub fn make_client(self) -> Result<SqsClient, Report> {
        match self {
            Self {
                key_id: Some(id),
                key_secret: Some(secret),
                region,
            } => {
                let http_client = rusoto_core::request::HttpClient::new()?;
                let rusoto_client = rusoto_core::Client::new_with(
                    rusoto_credential::StaticProvider::new_minimal(id, secret),
                    http_client,
                );
                Ok(SqsClient::new_with_client(rusoto_client, region))
            }
            Self {
                key_id: None,
                key_secret: None,
                region,
            } => Ok(SqsClient::new(region)),
            _ => unreachable!(),
        }
    }
}

pub fn default_sqs_client() -> SqsClient {
    AwsAccess::default().make_client().unwrap()
}

pub fn sqs_client_from_creds(key_id: String, key_secret: String) -> Result<SqsClient, Report> {
    AwsAccess::default().key(key_id, key_secret).make_client()
}

#[instrument(skip(client))]
pub async fn make_fifo_queue(client: &SqsClient, name: String) -> Result<String, Report> {
    ensure!(name.ends_with(".fifo"), "Fifo queue name must end in .fifo");

    let rusoto_sqs::CreateQueueResult { queue_url } = client
        .create_queue(rusoto_sqs::CreateQueueRequest {
            queue_name: name,
            attributes: Some(
                std::iter::once(("FifoQueue".to_owned(), "true".to_owned())).collect(),
            ),
            ..Default::default()
        })
        .await
        .wrap_err("create_queue errored")?;
    queue_url.ok_or_else(|| eyre!("No queue URL returned"))
}

#[instrument(skip(client))]
pub async fn make_be_queue(client: &SqsClient, name: String) -> Result<String, Report> {
    let rusoto_sqs::CreateQueueResult { queue_url } = client
        .create_queue(rusoto_sqs::CreateQueueRequest {
            queue_name: name,
            ..Default::default()
        })
        .await
        .wrap_err("create_queue errored")?;
    queue_url.ok_or_else(|| eyre!("No queue URL returned"))
}

#[instrument(skip(client))]
pub async fn delete_queue(client: &SqsClient, name: String) -> Result<(), Report> {
    client
        .delete_queue(rusoto_sqs::DeleteQueueRequest { queue_url: name })
        .await?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct SqsAddr {
    pub queue_id: String,
    pub group: Option<String>,
}

impl From<(String, String)> for SqsAddr {
    fn from((queue_id, group): (String, String)) -> Self {
        SqsAddr {
            queue_id,
            group: Some(group),
        }
    }
}

#[derive(Clone, Debug)]
pub struct OrderedSqsChunnel {
    inner: SqsChunnel,
    send_ctr: Arc<AtomicUsize>,
}

impl From<SqsChunnel> for OrderedSqsChunnel {
    fn from(mut inner: SqsChunnel) -> Self {
        let fifo_urls = inner
            .recv_queue_urls
            .into_iter()
            .map(|mut s| {
                if !s.ends_with(".fifo") {
                    s.push_str(".fifo");
                    s
                } else {
                    s
                }
            })
            .collect();
        inner.recv_queue_urls = fifo_urls;

        Self {
            inner,
            send_ctr: Default::default(),
        }
    }
}

impl OrderedSqsChunnel {
    pub fn new<'a>(
        sqs_client: SqsClient,
        recv_queue_urls: impl IntoIterator<Item = &'a str>,
    ) -> Result<Self, Report> {
        let recv_queue_urls: Result<_, _> = recv_queue_urls
            .into_iter()
            .map(|s| {
                if s.ends_with(".fifo") {
                    Ok(s.to_owned())
                } else {
                    Err(eyre!("OrderedSqsChunnel must use fifo queues"))
                }
            })
            .collect();
        let recv_queue_urls = recv_queue_urls?;
        Ok(Self {
            inner: SqsChunnel {
                sqs_client,
                recv_queue_urls,
            },
            send_ctr: Default::default(),
        })
    }
}

impl ChunnelConnection for OrderedSqsChunnel {
    type Data = (SqsAddr, String);

    fn send(
        &self,
        (
            SqsAddr {
                mut queue_id,
                group,
            },
            body,
        ): Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        let sqs = self.inner.sqs_client.clone();
        let ctr = self
            .send_ctr
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async move {
            //ensure!(
            //    queue_id.ends_with(".fifo"),
            //    "Can only send to a FIFO queue with an OrderedSqsChunnel"
            //);
            if !queue_id.ends_with(".fifo") {
                queue_id.push_str(".fifo");
            }

            // message_deduplication_id is optional if cloud-side "content-based deduplication" is
            // enabled, but required if it is not.
            let mut dedup_id = format!("{}", ctr);
            base64::encode_config_buf(body[..8].as_bytes(), base64::STANDARD, &mut dedup_id);
            let SendMessageResult {
                sequence_number,
                message_id,
                ..
            } = sqs
                .send_message(SendMessageRequest {
                    message_body: body,
                    queue_url: queue_id.clone(),
                    message_group_id: group.clone(),
                    message_deduplication_id: Some(dedup_id),
                    ..Default::default()
                })
                .await
                .wrap_err(eyre!("sqs.send_message on {:?}", queue_id))?;
            trace!(
                ?message_id,
                ?sequence_number,
                ?queue_id,
                ?group,
                "sent ordered sqs message"
            );
            Ok(())
        })
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        self.inner.recv()
    }
}

#[derive(Clone)]
pub struct SqsChunnel {
    sqs_client: SqsClient,
    recv_queue_urls: Vec<String>,
}

impl std::fmt::Debug for SqsChunnel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqsChunnel")
            .field("recv_queue_urls", &self.recv_queue_urls)
            .finish()
    }
}

impl SqsChunnel {
    pub fn new<'a>(
        sqs_client: SqsClient,
        recv_queue_urls: impl IntoIterator<Item = &'a str>,
    ) -> Self {
        SqsChunnel {
            sqs_client,
            recv_queue_urls: recv_queue_urls.into_iter().map(str::to_owned).collect(),
        }
    }

    pub fn listen(&mut self, addr: &str) {
        self.recv_queue_urls.push(addr.to_owned());
    }
}

impl ChunnelConnection for SqsChunnel {
    type Data = (SqsAddr, String);

    fn send(
        &self,
        (SqsAddr { queue_id, .. }, body): Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        let sqs = self.sqs_client.clone();
        Box::pin(async move {
            let SendMessageResult {
                message_id,
                sequence_number,
                ..
            } = sqs
                .send_message(SendMessageRequest {
                    message_body: body,
                    queue_url: queue_id.clone(),
                    ..Default::default()
                })
                .await
                .wrap_err(eyre!("sqs.send_message on {:?}", queue_id))?;
            trace!(
                ?message_id,
                ?sequence_number,
                ?queue_id,
                "sent unordered sqs message"
            );
            Ok(())
        })
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        let sqs = self.sqs_client.clone();
        let recv_queue_urls = self.recv_queue_urls.clone();
        // How to receive an SQS message:
        // 1. call receive_message on the queue
        // 2. call has some timeout. if we got no message, goto 1.
        // 3. once we have the message, explicitly delete it. Otherwise, it will show up in future
        //    receive_message requests.
        Box::pin(
            async move {
                tokio::pin!(sqs);
                let mut futs = vec![];
                let (
                    queue_id,
                    Message {
                        body,
                        receipt_handle,
                        attributes,
                        ..
                    },
                ) = loop {
                    if futs.is_empty() {
                        futs = recv_queue_urls
                            .iter()
                            .map(|q_url| {
                                let qu = q_url.to_string();
                                let sqs = &sqs; // don't move sqs, we need to use it in the other concurrent reqs
                                Box::pin(async move {
                                    let resp = sqs
                                        .receive_message(ReceiveMessageRequest {
                                            max_number_of_messages: Some(1),
                                            queue_url: qu.clone(),
                                            visibility_timeout: Some(5),
                                            wait_time_seconds: Some(5),
                                            attribute_names: Some(
                                                vec!["MessageGroupId".to_owned()],
                                            ),
                                            ..Default::default()
                                        })
                                        .await
                                        .wrap_err(eyre!("sqs.receive_message on {:?}", qu))?;
                                    trace!(?resp, "sqs receive_message future completed");
                                    Ok::<_, Report>((resp, qu))
                                })
                            })
                            .collect();
                    }

                    let (
                        (
                            ReceiveMessageResult {
                                messages: recvd_message,
                            },
                            qid,
                        ),
                        leftover,
                    ) = futures_util::future::select_ok(futs).await?;

                    match recvd_message {
                        None => {
                            // try again with the rest of the futs.
                            futs = leftover;
                            trace!("receive_message returned None");
                            continue;
                        }
                        Some(msg) if msg.is_empty() => {
                            // try again with the rest of the futs.
                            futs = leftover;
                            trace!("receive_message returned empty list");
                            continue;
                        }
                        Some(msg) if msg.len() > 1 => {
                            // this shouldn't be possible, we asked for a limit of 1
                            unreachable!("SQS receive_message limit exceeded");
                        }
                        Some(mut msg) => {
                            // done. can drop leftover. if the leftover futures happen to successfully
                            // resolve, that is fine, we're not going to clear them from the queue.
                            break (qid, msg.pop().unwrap());
                        }
                    }
                };

                // delete the message that we got
                sqs.delete_message(DeleteMessageRequest {
                    queue_url: queue_id.clone(),
                    receipt_handle: receipt_handle.unwrap(),
                })
                .await
                .wrap_err(eyre!("sqs.delete_message on {:?}", queue_id))?;

                // remember that to get any attributes back you have to ask for them in the
                // request.
                trace!(?attributes, "receive_message attributes");
                //let dedup_id = attrs.get("MessageDeduplicationId");
                let from_addr = SqsAddr {
                    queue_id,
                    group: attributes
                        .and_then(|attrs| attrs.get("MessageGroupId").map(Clone::clone)),
                };
                trace!(?from_addr, "receive_message succeeded");
                Ok((from_addr, body.unwrap()))
            }
            .instrument(debug_span!("sqs_recv")),
        )
    }
}

#[cfg(test)]
mod test {
    use super::{OrderedSqsChunnel, SqsAddr, SqsChunnel};
    use bertha::ChunnelConnection;
    use color_eyre::{
        eyre::{ensure, WrapErr},
        Report,
    };
    use rusoto_sqs::SqsClient;
    use tracing::{info, info_span};
    use tracing_error::ErrorLayer;
    use tracing_futures::Instrument;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    #[ignore]
    #[test]
    fn sqs_ordered_groups() {
        // relies on SQS queue "test.fifo" being available.
        const FIFO_TEST_QUEUE_URL: &'static str =
            "https://sqs.us-east-1.amazonaws.com/413104736560/test.fifo";
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
                // each sqs client has its own http client, so make new ones
                let sqs_client = SqsClient::new(rusoto_core::Region::UsEast1);
                let rch = OrderedSqsChunnel::new(sqs_client, vec![FIFO_TEST_QUEUE_URL])?;
                let sqs_client = SqsClient::new(rusoto_core::Region::UsEast1);
                let sch = OrderedSqsChunnel::new(sqs_client, vec![])?;

                const GROUP_A: &'static str = "A";
                const A1: &'static str = "message A1";
                const A2: &'static str = "message A2";
                const GROUP_B: &'static str = "B";
                const B1: &'static str = "message B1";
                const B2: &'static str = "message B2";

                let addr_a: SqsAddr = (FIFO_TEST_QUEUE_URL.to_string(), GROUP_A.to_string()).into();
                let addr_b: SqsAddr = (FIFO_TEST_QUEUE_URL.to_string(), GROUP_B.to_string()).into();

                sch.send((addr_a.clone(), A1.to_string()))
                    .await
                    .wrap_err("sqs send")?;
                info!("sent a1");
                sch.send((addr_b.clone(), B1.to_string()))
                    .await
                    .wrap_err("sqs send")?;
                info!("sent b1");
                sch.send((addr_a.clone(), A2.to_string()))
                    .await
                    .wrap_err("sqs send")?;
                info!("sent a2");
                sch.send((addr_b.clone(), B2.to_string()))
                    .await
                    .wrap_err("sqs send")?;
                info!("sent b2");

                let (q, msg1) = rch.recv().await.wrap_err("sqs recv")?;
                assert_eq!(q.queue_id, FIFO_TEST_QUEUE_URL);
                info!(?msg1, "recvd");
                let (q, msg2) = rch.recv().await.wrap_err("sqs recv")?;
                assert_eq!(q.queue_id, FIFO_TEST_QUEUE_URL);
                info!(?msg2, "recvd");
                let (q, msg3) = rch.recv().await.wrap_err("sqs recv")?;
                assert_eq!(q.queue_id, FIFO_TEST_QUEUE_URL);
                info!(?msg3, "recvd");
                let (q, msg4) = rch.recv().await.wrap_err("sqs recv")?;
                assert_eq!(q.queue_id, FIFO_TEST_QUEUE_URL);
                info!(?msg4, "recvd");

                let valid_orders = [
                    [A1, A2, B1, B2],
                    [A1, B1, A2, B2],
                    [A1, B1, B2, A2],
                    [B1, B2, A1, A2],
                    [B1, A1, A2, B2],
                    [B1, A1, B2, A2],
                ];

                ensure!(
                    valid_orders
                        .iter()
                        .any(|o| &[&msg1, &msg2, &msg3, &msg4] == o),
                    "invalid ordering"
                );
                Ok::<_, Report>(())
            }
            .instrument(info_span!("sqs_ordered_groups")),
        )
        .unwrap();
    }

    #[ignore]
    #[test]
    fn sqs_ordering() {
        // relies on SQS queue "test1.fifo" being available.
        const FIFO_TEST_QUEUE_URL: &'static str =
            "https://sqs.us-east-1.amazonaws.com/413104736560/test1.fifo";
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
                // each sqs client has its own http client, so make new ones
                let sqs_client = SqsClient::new(rusoto_core::Region::UsEast1);
                let rch = OrderedSqsChunnel::new(sqs_client, vec![FIFO_TEST_QUEUE_URL])?;
                let sqs_client = SqsClient::new(rusoto_core::Region::UsEast1);
                let sch = OrderedSqsChunnel::new(sqs_client, vec![])?;

                const GROUP_A: &'static str = "A";
                const A1: &'static str = "message A1";
                const A2: &'static str = "message A2";
                const A3: &'static str = "message A3";
                const A4: &'static str = "message A4";

                let addr_a: SqsAddr = (FIFO_TEST_QUEUE_URL.to_string(), GROUP_A.to_string()).into();

                sch.send((addr_a.clone(), A1.to_string()))
                    .await
                    .wrap_err("sqs send")?;
                info!("sent a1");
                sch.send((addr_a.clone(), A2.to_string()))
                    .await
                    .wrap_err("sqs send")?;
                info!("sent a2");
                sch.send((addr_a.clone(), A3.to_string()))
                    .await
                    .wrap_err("sqs send")?;
                info!("sent a3");
                sch.send((addr_a.clone(), A4.to_string()))
                    .await
                    .wrap_err("sqs send")?;
                info!("sent a4");

                let (q, msg1) = rch.recv().await.wrap_err("sqs recv")?;
                assert_eq!(q.queue_id, FIFO_TEST_QUEUE_URL);
                info!(?msg1, "recvd");
                let (q, msg2) = rch.recv().await.wrap_err("sqs recv")?;
                assert_eq!(q.queue_id, FIFO_TEST_QUEUE_URL);
                info!(?msg2, "recvd");
                let (q, msg3) = rch.recv().await.wrap_err("sqs recv")?;
                assert_eq!(q.queue_id, FIFO_TEST_QUEUE_URL);
                info!(?msg3, "recvd");
                let (q, msg4) = rch.recv().await.wrap_err("sqs recv")?;
                assert_eq!(q.queue_id, FIFO_TEST_QUEUE_URL);
                info!(?msg4, "recvd");

                let valid_orders = [[A1, A2, A3, A4]];

                ensure!(
                    valid_orders
                        .iter()
                        .any(|o| &[&msg1, &msg2, &msg3, &msg4] == o),
                    "invalid ordering"
                );
                Ok::<_, Report>(())
            }
            .instrument(info_span!("sqs_ordering")),
        )
        .unwrap();
    }

    #[ignore]
    #[test]
    fn sqs_send_recv() {
        // relies on SQS queue "test" being available.
        const TEST_QUEUE_URL: &'static str =
            "https://sqs.us-east-1.amazonaws.com/413104736560/test";
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
                let sqs_client = SqsClient::new(rusoto_core::Region::UsEast1);
                let ch = SqsChunnel::new(sqs_client, vec![TEST_QUEUE_URL]);

                ch.send((
                    SqsAddr {
                        queue_id: TEST_QUEUE_URL.to_string(),
                        group: None,
                    },
                    "test message".to_string(),
                ))
                .await
                .wrap_err("sqs send")?;
                let (q, msg) = ch.recv().await.wrap_err("sqs recv")?;
                assert_eq!(q.queue_id, TEST_QUEUE_URL);
                assert_eq!(&msg, "test message");
                Ok::<_, Report>(())
            }
            .instrument(info_span!("sqs_send_recv")),
        )
        .unwrap();
    }
}
