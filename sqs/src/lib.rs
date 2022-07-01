//! An AWS SQS wrapper for Bertha, allowing it to be used as transport for chunnels.
//! This can't impl `Chunnel` because it can't wrap another transport; it is the transport.
//!
//! Chunnel data type = (String, String) -> (queue URL, msg_string)

use base64::Engine;
// TODO: publish to SNS topics, auto-create SQS queues as subscriptions
use color_eyre::eyre::{ensure, eyre, Report, WrapErr};
use rusoto_sqs::{
    DeleteMessageBatchRequest, DeleteMessageBatchRequestEntry, DeleteMessageBatchResult, Message,
    ReceiveMessageRequest, ReceiveMessageResult, SendMessageBatchRequest,
    SendMessageBatchRequestEntry, SendMessageBatchResult, Sqs,
};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::Mutex;
use tracing::{info, instrument, trace};

/// The underlying client type to access Sqs.
pub use rusoto_sqs::SqsClient;

mod besteffort;
mod fut_with_string;
use fut_with_string::FutWithString;
mod ordered;

pub use besteffort::SqsChunnel;
pub use ordered::OrderedSqsChunnel;

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
    let name = queue_url.ok_or_else(|| eyre!("No queue URL returned"))?;
    info!(?name, "created fifo queue");
    Ok(name)
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
    let name = queue_url.ok_or_else(|| eyre!("No queue URL returned"))?;
    info!(?name, "created fifo queue");
    Ok(name)
}

#[instrument(skip(client))]
pub async fn delete_queue(client: &SqsClient, name: String) -> Result<(), Report> {
    info!(?name, "deleting queue");
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

fn get_dedup(ctr: usize, body: &[u8]) -> String {
    let mut dedup_id = format!("{}", ctr);
    base64::engine::general_purpose::STANDARD_NO_PAD.encode_string(&body[..8], &mut dedup_id);
    dedup_id
}

const MESSAGE_GROUP_ID_ATTRIBUTE_NAME: &str = "MessageGroupId";

#[tracing::instrument(skip(sqs, msgs), err, level = "debug")]
async fn do_send_batch<const FIFO: bool>(
    sqs: &SqsClient,
    qid: String,
    ctr: usize,
    msgs: impl IntoIterator<Item = (SqsAddr, String)>,
) -> Result<(), Report> {
    let batch_request_entries = msgs
        .into_iter()
        .enumerate()
        .map(|(id, (SqsAddr { group, .. }, body))| {
            if FIFO {
                let dedup_id = get_dedup(ctr + id, body[..].as_bytes());
                SendMessageBatchRequestEntry {
                    message_body: body,
                    message_group_id: group,
                    message_deduplication_id: Some(dedup_id),
                    id: id.to_string(), // batch id
                    ..Default::default()
                }
            } else {
                SendMessageBatchRequestEntry {
                    message_body: body,
                    id: id.to_string(), // batch id
                    ..Default::default()
                }
            }
        })
        .collect();
    let SendMessageBatchResult { failed, successful } = sqs
        .send_message_batch(SendMessageBatchRequest {
            entries: batch_request_entries,
            queue_url: qid.clone(),
        })
        .await
        .wrap_err(eyre!("send_message_batch to {:?}", &qid))?;
    if !failed.is_empty() {
        return Err(failed.into_iter().fold(
            eyre!("one or more messages failed to send to {:?}", &qid),
            |err, failed_msg| err.wrap_err(eyre!("{:?}", failed_msg)),
        ));
    }

    trace!(?qid, batch=?successful.len(), "send sqs message batch");
    Ok(())
}

#[instrument(skip(sqs, recv_futs), err, level = "debug")]
async fn do_recv_batch<'s, 'slots>(
    sqs: &'s SqsClient,
    recv_queue_urls: &[String],
    recv_futs: &Arc<Mutex<HashMap<String, FutWithString>>>,
    recv_call_ctr: &Arc<AtomicUsize>,
    del_call_ctr: &Arc<AtomicUsize>,
    slots: &'slots mut [Option<(SqsAddr, String)>],
) -> Result<&'slots mut [Option<(SqsAddr, String)>], Report> {
    trace!("called");
    let num_slots = slots.len();
    tokio::pin!(sqs);
    let mut futs = recv_futs.lock().await;
    trace!("locked");
    let (queue_id, mut msgs) = loop {
        // refill any futures we are missing.
        for recv_q in recv_queue_urls {
            futs.entry(recv_q.clone()).or_insert_with_key(|q_url| {
                let qu = q_url.to_string();
                let sqs = sqs.clone();
                let recv_call_ctr = Arc::clone(recv_call_ctr);
                FutWithString::new(
                    qu.clone(),
                    Box::pin(async move {
                        trace!(?qu, "receive_message future starting");
                        recv_call_ctr.fetch_add(1, Ordering::SeqCst);
                        let resp = sqs
                            .receive_message(ReceiveMessageRequest {
                                max_number_of_messages: Some(num_slots as _),
                                queue_url: qu.clone(),
                                visibility_timeout: Some(5),
                                wait_time_seconds: Some(20),
                                attribute_names: Some(vec![
                                    MESSAGE_GROUP_ID_ATTRIBUTE_NAME.to_owned()
                                ]),
                                message_attribute_names: Some(vec![
                                    MESSAGE_GROUP_ID_ATTRIBUTE_NAME.to_owned(),
                                ]),
                                ..Default::default()
                            })
                            .await
                            .wrap_err(eyre!("sqs.receive_message on {:?}", qu))?;
                        trace!(
                            messages = ?resp.messages.as_ref().map(Vec::len),
                            "receive_message future completed"
                        );
                        Ok::<_, Report>((resp, qu))
                    }),
                )
            });
        }

        let f = futs.drain().map(|(_, v)| v);
        trace!(num = ?f.len(), "executing");
        // take all the futures out
        let (
            (
                ReceiveMessageResult {
                    messages: recvd_message,
                },
                qid,
            ),
            leftover,
        ) = futures_util::future::select_ok(f).await?;
        // put the leftover futures back
        futs.extend(leftover.into_iter().map(|f| (f.key().to_string(), f)));
        trace!("returned");

        match recvd_message {
            None => {
                // try again with the rest of the futs.
                // the one that completed will get replenished above.
                trace!("receive_message returned None");
                continue;
            }
            Some(msg) if msg.is_empty() => {
                // try again with the rest of the futs.
                // the one that completed will get replenished above.
                trace!("receive_message returned empty list");
                continue;
            }
            Some(msgs) => {
                // done for now.
                trace!(
                    recvd_batch_size = msgs.len(),
                    "receive_message returned batch"
                );
                break (qid, msgs);
            }
        }
    };

    // delete the batch
    let delete_message_batch_entries: Vec<_> = msgs
        .iter_mut()
        .enumerate()
        .map(|(id, msg)| {
            let receipt_handle = msg.receipt_handle.take().unwrap();
            DeleteMessageBatchRequestEntry {
                id: id.to_string(),
                receipt_handle,
            }
        })
        .collect();
    del_call_ctr.fetch_add(1, Ordering::SeqCst);
    let DeleteMessageBatchResult { failed, successful } = sqs
        .delete_message_batch(DeleteMessageBatchRequest {
            queue_url: queue_id.clone(),
            entries: delete_message_batch_entries,
        })
        .await
        .wrap_err(eyre!("sqs.delete_message_batch on {:?}", &queue_id))?;
    if !failed.is_empty() {
        return Err(failed.into_iter().fold(
            eyre!("one or more messages failed to delete on {:?}", &queue_id),
            |err, failed_msg| err.wrap_err(eyre!("{:?}", failed_msg)),
        ));
    }

    trace!(num=?successful.len(), "deleted received messages");

    let mut slot_idx = 0;
    for msg in msgs.into_iter().map(
        |Message {
             body,
             attributes,
             message_attributes,
             ..
         }| {
            let from_addr =
                SqsAddr {
                    queue_id: queue_id.clone(),
                    // try `attributes` first, then look at `message_attributes`. `attributes` is the
                    // one amazon sets for a FIFO queue, and `message_attributes` is the one we set
                    // since non-fifo queues won't give us the group id attribute
                    group: attributes
                        .and_then(|mut attrs| attrs.remove(MESSAGE_GROUP_ID_ATTRIBUTE_NAME))
                        .or_else(|| {
                            message_attributes.and_then(|mut attrs| {
                                attrs.remove(MESSAGE_GROUP_ID_ATTRIBUTE_NAME).and_then(|v| {
                                    v.string_value.and_then(|s| {
                                        if s.is_empty() {
                                            None
                                        } else {
                                            Some(s)
                                        }
                                    })
                                })
                            })
                        }),
                };

            trace!(?from_addr, "receive_message succeeded");
            (from_addr, body.unwrap())
        },
    ) {
        slots[slot_idx] = Some(msg);
        slot_idx += 1;
    }

    Ok(&mut slots[..slot_idx])
}

#[cfg(test)]
mod test {
    use super::{OrderedSqsChunnel, SqsAddr, SqsChunnel};
    use bertha::ChunnelConnection;
    use color_eyre::{
        eyre::{ensure, eyre, WrapErr},
        Report,
    };
    use rand::Rng;
    use rusoto_sqs::SqsClient;
    use tracing::{info, info_span};
    use tracing_error::ErrorLayer;
    use tracing_futures::Instrument;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    fn gen_name() -> String {
        let rng = rand::thread_rng();
        "bertha-"
            .chars()
            .chain(
                rng.sample_iter(&rand::distributions::Alphanumeric)
                    .take(10)
                    .flat_map(|x| (x as char).to_lowercase()),
            )
            .collect()
    }

    #[ignore]
    #[test]
    fn sqs_ordered_groups() {
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

        let mut queue_name = gen_name();
        queue_name.push_str(".fifo");
        let queue_name = rt
            .block_on(async move {
                let err = eyre!("Create fifo sqs queue: {:?}", &queue_name);
                let sqs_client = SqsClient::new(rusoto_core::Region::UsEast1);
                super::make_fifo_queue(&sqs_client, queue_name)
                    .await
                    .wrap_err(err)
            })
            .unwrap();
        let qn = queue_name.clone();
        let res = rt.block_on(
            async move {
                let queue_name = qn;
                // each sqs client has its own http client, so make new ones
                let sqs_client = SqsClient::new(rusoto_core::Region::UsEast1);
                let rch = OrderedSqsChunnel::new(sqs_client, vec![queue_name.as_str()])?;
                let sqs_client = SqsClient::new(rusoto_core::Region::UsEast1);
                let sch = OrderedSqsChunnel::new(sqs_client, vec![])?;

                const GROUP_A: &str = "A";
                const A1: &str = "message A1";
                const A2: &str = "message A2";
                const GROUP_B: &str = "B";
                const B1: &str = "message B1";
                const B2: &str = "message B2";

                let addr_a: SqsAddr = (queue_name.clone(), GROUP_A.to_string()).into();
                let addr_b: SqsAddr = (queue_name.clone(), GROUP_B.to_string()).into();

                let burst = [
                    (addr_a.clone(), A1.to_string()),
                    (addr_b.clone(), B1.to_string()),
                    (addr_a.clone(), A2.to_string()),
                    (addr_b.clone(), B2.to_string()),
                ];

                sch.send(burst).await.wrap_err("sqs send")?;
                info!("sent");

                let mut slots = [None, None, None, None];
                let ms = rch.recv(&mut slots).await.wrap_err("sqs recv")?;
                for s in &ms[..] {
                    if let Some((q, _)) = s {
                        assert_eq!(&q.queue_id, &queue_name);
                    }
                }

                let msg1 = ms[0].take().ok_or_else(|| eyre!("Message was None"))?.1;
                let msg2 = ms[1].take().ok_or_else(|| eyre!("Message was None"))?.1;
                let msg3 = ms[2].take().ok_or_else(|| eyre!("Message was None"))?.1;
                let msg4 = ms[3].take().ok_or_else(|| eyre!("Message was None"))?.1;

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
        );

        rt.block_on(async move {
            let sqs_client = SqsClient::new(rusoto_core::Region::UsEast1);
            super::delete_queue(&sqs_client, queue_name).await
        })
        .unwrap();
        res.unwrap()
    }

    #[ignore]
    #[test]
    fn sqs_ordering() {
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

        let mut queue_name = gen_name();
        queue_name.push_str(".fifo");
        let queue_name = rt
            .block_on(async move {
                let sqs_client = SqsClient::new(rusoto_core::Region::UsEast1);
                super::make_fifo_queue(&sqs_client, queue_name)
                    .await
                    .wrap_err("Create fifo sqs queue")
            })
            .unwrap();
        let qn = queue_name.clone();
        let res = rt.block_on(
            async move {
                let queue_name = qn;
                // each sqs client has its own http client, so make new ones
                let sqs_client = SqsClient::new(rusoto_core::Region::UsEast1);
                let rch = OrderedSqsChunnel::new(sqs_client, vec![queue_name.as_str()])?;
                let sqs_client = SqsClient::new(rusoto_core::Region::UsEast1);
                let sch = OrderedSqsChunnel::new(sqs_client, vec![])?;

                const GROUP_A: &str = "A";
                const A1: &str = "message A1";
                const A2: &str = "message A2";
                const A3: &str = "message A3";
                const A4: &str = "message A4";

                let addr_a: SqsAddr = (queue_name.to_string(), GROUP_A.to_string()).into();
                let burst = [
                    (addr_a.clone(), A1.to_string()),
                    (addr_a.clone(), A2.to_string()),
                    (addr_a.clone(), A3.to_string()),
                    (addr_a.clone(), A4.to_string()),
                ];

                sch.send(burst).await.wrap_err("sqs send")?;
                info!("sent burst");

                let mut slots = [None, None, None, None];
                let ms = rch.recv(&mut slots).await.wrap_err("sqs recv")?;
                for s in &ms[..] {
                    if let Some((q, _)) = s {
                        assert_eq!(&q.queue_id, &queue_name);
                    }
                }

                let msg1 = ms[0].take().ok_or_else(|| eyre!("Message was None"))?.1;
                let msg2 = ms[1].take().ok_or_else(|| eyre!("Message was None"))?.1;
                let msg3 = ms[2].take().ok_or_else(|| eyre!("Message was None"))?.1;
                let msg4 = ms[3].take().ok_or_else(|| eyre!("Message was None"))?.1;

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
        );

        rt.block_on(async move {
            let sqs_client = SqsClient::new(rusoto_core::Region::UsEast1);
            super::delete_queue(&sqs_client, queue_name).await
        })
        .unwrap();
        res.unwrap()
    }

    #[ignore]
    #[test]
    fn sqs_send_recv() {
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

        let queue_name = gen_name();
        let queue_name = rt
            .block_on(async move {
                let sqs_client = SqsClient::new(rusoto_core::Region::UsEast1);
                super::make_be_queue(&sqs_client, queue_name)
                    .await
                    .wrap_err("Create be sqs queue")
            })
            .unwrap();
        let qn = queue_name.clone();
        let res = rt.block_on(
            async move {
                let queue_name = qn;
                let sqs_client = SqsClient::new(rusoto_core::Region::UsEast1);
                let ch = SqsChunnel::new(sqs_client, vec![queue_name.as_str()]);

                ch.send(std::iter::once((
                    SqsAddr {
                        queue_id: queue_name.to_string(),
                        group: None,
                    },
                    "test message".to_string(),
                )))
                .await
                .wrap_err("sqs send")?;
                let mut slot = [None];
                let (q, msg) = ch.recv(&mut slot).await.wrap_err("sqs recv")?[0]
                    .take()
                    .unwrap();
                assert_eq!(q.queue_id, queue_name);
                assert_eq!(&msg, "test message");
                Ok::<_, Report>(())
            }
            .instrument(info_span!("sqs_send_recv")),
        );

        rt.block_on(async move {
            let sqs_client = SqsClient::new(rusoto_core::Region::UsEast1);
            super::delete_queue(&sqs_client, queue_name).await
        })
        .unwrap();
        res.unwrap()
    }
}
