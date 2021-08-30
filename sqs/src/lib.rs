//! An AWS SQS wrapper for Bertha, allowing it to be used as transport for chunnels.
//! This can't impl `Chunnel` because it can't wrap another transport; it is the transport.
//!
//! Chunnel data type = (String, String) -> (queue URL, msg_string)

// TODO: publish to SNS topics, auto-create SQS queues as subscriptions
use bertha::ChunnelConnection;
use color_eyre::eyre::{ensure, eyre, Report, WrapErr};
use rusoto_sqs::{
    DeleteMessageBatchRequest, DeleteMessageBatchRequestEntry, DeleteMessageBatchResult,
    DeleteMessageRequest, Message, ReceiveMessageRequest, ReceiveMessageResult,
    SendMessageBatchRequest, SendMessageBatchRequestEntry, SendMessageBatchResult,
    SendMessageRequest, SendMessageResult, Sqs,
};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex as StdMutex,
};
use tokio::sync::Mutex;
use tracing::{debug_span, info, instrument, trace};
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
            .clone()
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
                num_sqs_send_calls: Default::default(),
                num_sqs_recv_calls: Default::default(),
                num_sqs_del_calls: Default::default(),
            },
            send_ctr: Default::default(),
        })
    }
}

fn get_dedup(ctr: usize, body: &[u8]) -> String {
    let mut dedup_id = format!("{}", ctr);
    base64::encode_config_buf(&body[..8], base64::STANDARD, &mut dedup_id);
    dedup_id
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
        let ctr = self.send_ctr.fetch_add(1, Ordering::SeqCst);
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
            let dedup_id = get_dedup(ctr, &body[..].as_bytes());
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

/// OrderedSqsChunnel but with send_batch and recv_batch impls.
pub struct OrderedSqsChunnelBatch(
    pub OrderedSqsChunnel,
    // saved recv_batch futures
    Arc<Mutex<HashMap<String, FutWithString>>>,
);

impl OrderedSqsChunnelBatch {
    pub fn new(inner: OrderedSqsChunnel) -> Self {
        Self(inner, Default::default())
    }
}

impl ChunnelConnection for OrderedSqsChunnelBatch {
    type Data = (SqsAddr, String);

    fn send(
        &self,
        d: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        self.0.send(d)
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        self.0.recv()
    }

    // TODO advertise max batch size, in our case 10
    fn send_batch<'cn, D>(
        &'cn self,
        data: D,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'cn>>
    where
        D: IntoIterator<Item = Self::Data> + Send + 'cn,
        <D as IntoIterator>::IntoIter: Send,
        Self::Data: Send,
        Self: Sync,
    {
        #[tracing::instrument(skip(sqs, msgs), err, level = "trace")]
        async fn send_batch(
            sqs: &SqsClient,
            qid: String,
            ctr: usize,
            msgs: Vec<(SqsAddr, String)>,
        ) -> Result<(), Report> {
            let batch_request_entries = msgs
                .into_iter()
                .enumerate()
                .map(|(id, (SqsAddr { group, .. }, body))| {
                    let dedup_id = get_dedup(ctr + id, &body[..].as_bytes());
                    SendMessageBatchRequestEntry {
                        message_body: body,
                        message_group_id: group.clone(),
                        message_deduplication_id: Some(dedup_id),
                        id: id.to_string(), // batch id
                        ..Default::default()
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

            trace!(?qid, batch=?successful.len(), "send ordered sqs message batch");
            Ok(())
        }

        let sqs = self.0.inner.sqs_client.clone();
        let send_ctr = Arc::clone(&self.0.send_ctr);
        Box::pin(async move {
            let mut pending_batches = HashMap::new();
            let mut data = data.into_iter();
            loop {
                // group the messages by destination queue. If we fill a queue's batch (to 10),
                // stop pulling messages and send that batch, setting `batch_to_send` to `Some`
                // accordingly.  After the loop, if `batch_to_send` is None, then we couldn't fill any batches
                // and there are no more messages, so just send the residual batches and we're done.
                let mut batch_to_send = None;
                for (
                    SqsAddr {
                        mut queue_id,
                        group,
                    },
                    body,
                ) in &mut data
                {
                    if !queue_id.ends_with(".fifo") {
                        queue_id.push_str(".fifo");
                    }

                    let q_batch = pending_batches
                        .entry(queue_id.clone())
                        .or_insert(Vec::with_capacity(10));
                    q_batch.push((
                        SqsAddr {
                            queue_id: queue_id.clone(),
                            group,
                        },
                        body,
                    ));
                    if q_batch.len() == 10 {
                        batch_to_send = Some(queue_id.clone());
                        break;
                    }
                }

                if let Some(qid) = batch_to_send {
                    // we filled a batch. send it and loop back for any other messages.
                    let msgs = pending_batches.remove(&qid).unwrap(); // for loop above guarantees presence
                    let ctr = send_ctr.fetch_add(msgs.len(), Ordering::SeqCst);
                    send_batch(&sqs, qid, ctr, msgs).await?;
                } else {
                    // send the residual batches and break.
                    for (qid, msgs) in pending_batches {
                        let ctr = send_ctr.fetch_add(msgs.len(), Ordering::SeqCst);
                        send_batch(&sqs, qid, ctr, msgs).await?;
                    }

                    break;
                }
            }

            Ok(())
        })
    }

    fn recv_batch<'cn>(
        &'cn self,
        batch_size: usize,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Self::Data>, Report>> + Send + 'cn>>
    where
        Self::Data: Send,
        Self: Sync,
    {
        // first inner: OrderedSqsChunnel. second: SqsChunnel
        let sqs = self.0.inner.sqs_client.clone();
        let recv_queue_urls = self.0.inner.recv_queue_urls.clone();
        let recv_futs = Arc::clone(&self.1);
        Box::pin(do_recv_batch(
            sqs,
            recv_queue_urls,
            recv_futs,
            batch_size,
            Default::default(),
            Default::default(),
        ))
    }
}

#[derive(Clone)]
pub struct SqsChunnel {
    sqs_client: SqsClient,
    recv_queue_urls: Vec<String>,
    num_sqs_send_calls: Arc<AtomicUsize>,
    num_sqs_recv_calls: Arc<AtomicUsize>,
    num_sqs_del_calls: Arc<AtomicUsize>,
}

impl std::fmt::Debug for SqsChunnel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqsChunnel")
            .field("recv_queue_urls", &self.recv_queue_urls)
            .finish()
    }
}

impl Drop for SqsChunnel {
    fn drop(&mut self) {
        let sends = self.num_sqs_send_calls.load(Ordering::SeqCst);
        let recvs = self.num_sqs_recv_calls.load(Ordering::SeqCst);
        let dels = self.num_sqs_del_calls.load(Ordering::SeqCst);
        info!(?sends, ?recvs, ?dels, "sqschunnel call counters");
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
            num_sqs_send_calls: Default::default(),
            num_sqs_recv_calls: Default::default(),
            num_sqs_del_calls: Default::default(),
        }
    }

    pub fn listen(&mut self, addr: &str) {
        self.recv_queue_urls.push(addr.to_owned());
    }
}

const MESSAGE_GROUP_ID_ATTRIBUTE_NAME: &str = "MessageGroupId";

impl ChunnelConnection for SqsChunnel {
    type Data = (SqsAddr, String);

    fn send(
        &self,
        (SqsAddr { queue_id, group }, body): Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        let sqs = self.sqs_client.clone();
        let call_ctr = Arc::clone(&self.num_sqs_send_calls);
        Box::pin(async move {
            let attributes: HashMap<_, _> = group
                .clone()
                .into_iter()
                .map(|g| {
                    (
                        MESSAGE_GROUP_ID_ATTRIBUTE_NAME.to_owned(),
                        rusoto_sqs::MessageAttributeValue {
                            data_type: "String".to_owned(),
                            string_value: Some(g),
                            ..Default::default()
                        },
                    )
                })
                .collect();
            call_ctr.fetch_add(1, Ordering::SeqCst);
            let SendMessageResult {
                message_id,
                sequence_number,
                ..
            } = sqs
                .send_message(SendMessageRequest {
                    message_body: body,
                    queue_url: queue_id.clone(),
                    message_attributes: Some(attributes),
                    ..Default::default()
                })
                .await
                .wrap_err(eyre!("sqs.send_message on {:?}", queue_id))?;
            trace!(
                ?message_id,
                ?sequence_number,
                ?queue_id,
                ?group,
                "sent unordered sqs message"
            );
            Ok(())
        })
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        let sqs = self.sqs_client.clone();
        let recv_queue_urls = self.recv_queue_urls.clone();
        let recv_call_ctr = Arc::clone(&self.num_sqs_recv_calls);
        let del_call_ctr = Arc::clone(&self.num_sqs_del_calls);
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
                        message_attributes,
                        ..
                    },
                ) = loop {
                    if futs.is_empty() {
                        futs = recv_queue_urls
                            .iter()
                            .map(|q_url| {
                                let qu = q_url.to_string();
                                let sqs = &sqs; // don't move sqs, we need to use it in the other concurrent reqs
                                let recv_call_ctr = &recv_call_ctr;
                                Box::pin(async move {
                                    recv_call_ctr.fetch_add(1, Ordering::SeqCst);
                                    let resp = sqs
                                        .receive_message(ReceiveMessageRequest {
                                            max_number_of_messages: Some(1),
                                            queue_url: qu.clone(),
                                            visibility_timeout: Some(5),
                                            wait_time_seconds: Some(5),
                                            attribute_names: Some(vec![
                                                MESSAGE_GROUP_ID_ATTRIBUTE_NAME.to_owned(),
                                            ]),
                                            message_attribute_names: Some(vec![
                                                MESSAGE_GROUP_ID_ATTRIBUTE_NAME.to_owned(),
                                            ]),
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

                // remember that to get any attributes back you have to ask for them in the
                // request.
                trace!(
                    ?attributes,
                    ?message_attributes,
                    "receive_message attributes"
                );

                // delete the message that we got
                del_call_ctr.fetch_add(1, Ordering::SeqCst);
                sqs.delete_message(DeleteMessageRequest {
                    queue_url: queue_id.clone(),
                    receipt_handle: receipt_handle.unwrap(),
                })
                .await
                .wrap_err(eyre!("sqs.delete_message on {:?}", queue_id))?;
                //let dedup_id = attrs.get("MessageDeduplicationId");
                let from_addr = SqsAddr {
                    queue_id,
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
                Ok((from_addr, body.unwrap()))
            }
            .instrument(debug_span!("sqs_recv")),
        )
    }
}

/// SqsChunnel but with send_batch and recv_batch impls.
#[derive(Clone)]
pub struct SqsChunnelBatch {
    inner: SqsChunnel,

    pending_sends: Arc<StdMutex<HashMap<SqsAddr, (usize, Vec<(SqsAddr, String)>)>>>,
    pending_recvs: Arc<StdMutex<VecDeque<(SqsAddr, String)>>>,

    // saved recv_batch futures
    batch_recv_futs: Arc<Mutex<HashMap<String, FutWithString>>>,
}

impl std::fmt::Debug for SqsChunnelBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqsChunnelBatch")
            .field("inner", &self.inner)
            .finish()
    }
}

impl SqsChunnelBatch {
    pub fn new(inner: SqsChunnel) -> Self {
        Self {
            inner,
            pending_sends: Default::default(),
            batch_recv_futs: Default::default(),
            pending_recvs: Default::default(),
        }
    }
}

impl ChunnelConnection for SqsChunnelBatch {
    type Data = (SqsAddr, String);

    fn send(
        &self,
        (q, data): Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        enum BatchState {
            Now(Vec<(SqsAddr, String)>),
            Nagle(SqsAddr, usize),
        }

        let batch_state = {
            let mut sends = self.pending_sends.lock().unwrap();
            let (batch_ctr, batch) = sends.entry(q.clone()).or_default();
            batch.push((q.clone(), data));
            if batch.len() >= 10 {
                *batch_ctr += 1;
                let batch = std::mem::take(batch);
                BatchState::Now(batch)
            } else {
                BatchState::Nagle(q, *batch_ctr)
            }
        };

        let this = self.clone();
        Box::pin(async move {
            match batch_state {
                BatchState::Now(batch) => {
                    trace!(len = ?batch.len(), "batch send");
                    return this.send_batch(batch).await;
                }
                BatchState::Nagle(q, batch_id) => {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;

                    let batch = {
                        let mut sends = this.pending_sends.lock().unwrap();
                        let (batch_ctr, batch) = sends.get_mut(&q).unwrap();
                        if *batch_ctr != batch_id {
                            // this was already resolved
                            return Ok(());
                        } else if !batch.is_empty() {
                            *batch_ctr += 1;
                            std::mem::take(batch)
                        } else {
                            unreachable!()
                        }
                    };

                    trace!(len = ?batch.len(), "post-nagle batch send");
                    return this.send_batch(batch).await;
                }
            }
        })
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        {
            let mut rcvs = self.pending_recvs.lock().unwrap();
            if let Some(r) = rcvs.pop_front() {
                return Box::pin(futures_util::future::ready(Ok(r)));
            }
        }

        let this = self.clone();
        Box::pin(async move {
            let batch = this.recv_batch(10).await?;
            let ret = {
                let mut rcvs = this.pending_recvs.lock().unwrap();
                rcvs.extend(batch.into_iter());
                rcvs.pop_front().unwrap()
            };

            Ok(ret)
        })
    }

    // TODO advertise max batch size, in our case 10
    // XXX this implementation is very similar to the ordered one (with shared batching logic). find a way
    // to merge.
    fn send_batch<'cn, D>(
        &'cn self,
        data: D,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'cn>>
    where
        D: IntoIterator<Item = Self::Data> + Send + 'cn,
        <D as IntoIterator>::IntoIter: Send,
        Self::Data: Send,
        Self: Sync,
    {
        #[tracing::instrument(skip(sqs, msgs), err, level = "trace")]
        async fn send_batch(
            sqs: &SqsClient,
            qid: String,
            msgs: Vec<(SqsAddr, String)>,
            send_call_ctr: &Arc<AtomicUsize>,
        ) -> Result<(), Report> {
            let batch_request_entries = msgs
                .into_iter()
                .enumerate()
                .map(|(id, (SqsAddr { group, .. }, body))| {
                    let attributes: HashMap<_, _> = group
                        .clone()
                        .into_iter()
                        .map(|g| {
                            (
                                MESSAGE_GROUP_ID_ATTRIBUTE_NAME.to_owned(),
                                rusoto_sqs::MessageAttributeValue {
                                    data_type: "String".to_owned(),
                                    string_value: Some(g),
                                    ..Default::default()
                                },
                            )
                        })
                        .collect();
                    SendMessageBatchRequestEntry {
                        message_body: body,
                        message_attributes: Some(attributes),
                        id: id.to_string(), // batch id
                        ..Default::default()
                    }
                })
                .collect();
            send_call_ctr.fetch_add(1, Ordering::SeqCst);
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

        let sqs = self.inner.sqs_client.clone();
        let send_call_ctr = Arc::clone(&self.inner.num_sqs_send_calls);
        Box::pin(async move {
            let mut pending_batches = HashMap::new();
            let mut data = data.into_iter();
            loop {
                // group the messages by destination queue. If we fill a queue's batch (to 10),
                // stop pulling messages and send that batch, setting `batch_to_send` to `Some`
                // accordingly.  After the loop, if `batch_to_send` is None, then we couldn't fill any batches
                // and there are no more messages, so just send the residual batches and we're done.
                let mut batch_to_send = None;
                for (SqsAddr { queue_id, group }, body) in &mut data {
                    let q_batch = pending_batches
                        .entry(queue_id.clone())
                        .or_insert(Vec::with_capacity(10));
                    q_batch.push((
                        SqsAddr {
                            queue_id: queue_id.clone(),
                            group,
                        },
                        body,
                    ));
                    if q_batch.len() == 10 {
                        batch_to_send = Some(queue_id.clone());
                        break;
                    }
                }

                if let Some(qid) = batch_to_send {
                    // we filled a batch. send it and loop back for any other messages.
                    let msgs = pending_batches.remove(&qid).unwrap(); // for loop above guarantees presence
                    send_batch(&sqs, qid, msgs, &send_call_ctr).await?;
                } else {
                    // send the residual batches and break.
                    for (qid, msgs) in pending_batches {
                        send_batch(&sqs, qid, msgs, &send_call_ctr).await?;
                    }

                    break;
                }
            }

            Ok(())
        })
    }

    fn recv_batch<'cn>(
        &'cn self,
        batch_size: usize,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Self::Data>, Report>> + Send + 'cn>>
    where
        Self::Data: Send,
        Self: Sync,
    {
        let sqs = self.inner.sqs_client.clone();
        let recv_queue_urls = self.inner.recv_queue_urls.clone();
        let recv_futs = Arc::clone(&self.batch_recv_futs);
        let batch_size = std::cmp::min(batch_size, 10);

        let recv_call_ctr = Arc::clone(&self.inner.num_sqs_recv_calls);
        let del_call_ctr = Arc::clone(&self.inner.num_sqs_del_calls);
        Box::pin(do_recv_batch(
            sqs,
            recv_queue_urls,
            recv_futs,
            batch_size,
            recv_call_ctr,
            del_call_ctr,
        ))
    }
}

struct FutWithString {
    q: String,
    f: Pin<
        Box<dyn Future<Output = Result<(ReceiveMessageResult, String), Report>> + Send + 'static>,
    >,
}

impl FutWithString {
    fn key(&self) -> &str {
        &self.q
    }

    fn new(
        q: String,
        f: Pin<
            Box<
                dyn Future<Output = Result<(ReceiveMessageResult, String), Report>>
                    + Send
                    + 'static,
            >,
        >,
    ) -> Self {
        Self { q, f }
    }
}

use std::task::{Context, Poll};
impl Future for FutWithString {
    type Output = Result<(ReceiveMessageResult, String), Report>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::as_mut(&mut self.f).poll(cx)
    }
}

#[instrument(skip(sqs, recv_futs), err, level = "debug")]
async fn do_recv_batch(
    sqs: SqsClient,
    recv_queue_urls: Vec<String>,
    recv_futs: Arc<Mutex<HashMap<String, FutWithString>>>,
    batch_size: usize,
    recv_call_ctr: Arc<AtomicUsize>,
    del_call_ctr: Arc<AtomicUsize>,
) -> Result<Vec<(SqsAddr, String)>, Report> {
    trace!("called");
    tokio::pin!(sqs);
    let mut futs = recv_futs.lock().await;
    trace!("locked");
    let (queue_id, mut msgs) = loop {
        // refill any futures we are missing.
        for recv_q in &recv_queue_urls {
            futs.entry(recv_q.clone()).or_insert_with_key(|q_url| {
                let qu = q_url.to_string();
                let sqs = sqs.clone();
                let recv_call_ctr = Arc::clone(&recv_call_ctr);
                FutWithString::new(
                    qu.clone(),
                    Box::pin(async move {
                        trace!(?qu, "receive_message future starting");
                        recv_call_ctr.fetch_add(1, Ordering::SeqCst);
                        let resp = sqs
                            .receive_message(ReceiveMessageRequest {
                                max_number_of_messages: Some(batch_size as _),
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

    Ok(msgs
        .into_iter()
        .map(
            |Message {
                 body,
                 attributes,
                 message_attributes,
                 ..
             }| {
                let from_addr = SqsAddr {
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
        )
        .collect())
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

        let rng = rand::thread_rng();
        let mut queue_name: String = "bertha-"
            .chars()
            .chain(
                rng.sample_iter(&rand::distributions::Alphanumeric)
                    .take(10)
                    .flat_map(char::to_lowercase),
            )
            .collect();
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

                const GROUP_A: &'static str = "A";
                const A1: &'static str = "message A1";
                const A2: &'static str = "message A2";
                const GROUP_B: &'static str = "B";
                const B1: &'static str = "message B1";
                const B2: &'static str = "message B2";

                let addr_a: SqsAddr = (queue_name.clone(), GROUP_A.to_string()).into();
                let addr_b: SqsAddr = (queue_name.clone(), GROUP_B.to_string()).into();

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
                assert_eq!(&q.queue_id, &queue_name);
                info!(?msg1, "recvd");
                let (q, msg2) = rch.recv().await.wrap_err("sqs recv")?;
                assert_eq!(&q.queue_id, &queue_name);
                info!(?msg2, "recvd");
                let (q, msg3) = rch.recv().await.wrap_err("sqs recv")?;
                assert_eq!(&q.queue_id, &queue_name);
                info!(?msg3, "recvd");
                let (q, msg4) = rch.recv().await.wrap_err("sqs recv")?;
                assert_eq!(&q.queue_id, &queue_name);
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

        let rng = rand::thread_rng();
        let mut queue_name: String = "bertha-"
            .chars()
            .chain(
                rng.sample_iter(&rand::distributions::Alphanumeric)
                    .take(10)
                    .flat_map(char::to_lowercase),
            )
            .collect();
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

                const GROUP_A: &'static str = "A";
                const A1: &'static str = "message A1";
                const A2: &'static str = "message A2";
                const A3: &'static str = "message A3";
                const A4: &'static str = "message A4";

                let addr_a: SqsAddr = (queue_name.to_string(), GROUP_A.to_string()).into();

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
                assert_eq!(q.queue_id, queue_name);
                info!(?msg1, "recvd");
                let (q, msg2) = rch.recv().await.wrap_err("sqs recv")?;
                assert_eq!(q.queue_id, queue_name);
                info!(?msg2, "recvd");
                let (q, msg3) = rch.recv().await.wrap_err("sqs recv")?;
                assert_eq!(q.queue_id, queue_name);
                info!(?msg3, "recvd");
                let (q, msg4) = rch.recv().await.wrap_err("sqs recv")?;
                assert_eq!(q.queue_id, queue_name);
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

        let rng = rand::thread_rng();
        let queue_name: String = "bertha-"
            .chars()
            .chain(
                rng.sample_iter(&rand::distributions::Alphanumeric)
                    .take(10)
                    .flat_map(char::to_lowercase),
            )
            .collect();
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

                ch.send((
                    SqsAddr {
                        queue_id: queue_name.to_string(),
                        group: None,
                    },
                    "test message".to_string(),
                ))
                .await
                .wrap_err("sqs send")?;
                let (q, msg) = ch.recv().await.wrap_err("sqs recv")?;
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

    #[ignore]
    #[test]
    fn sqs_batch() {
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

        let rng = rand::thread_rng();
        let queue_name: String = "bertha-"
            .chars()
            .chain(
                rng.sample_iter(&rand::distributions::Alphanumeric)
                    .take(10)
                    .flat_map(char::to_lowercase),
            )
            .collect();
        let queue_name = rt
            .block_on(async move {
                let sqs_client = SqsClient::new(rusoto_core::Region::UsEast1);
                info!(?queue_name, "make queue");
                super::make_be_queue(&sqs_client, queue_name)
                    .await
                    .wrap_err("Create be sqs queue")
            })
            .unwrap();
        info!(?queue_name, "made queue");
        let qn = queue_name.clone();
        let res = rt.block_on(
            async move {
                let queue_name = qn;
                let sqs_client = SqsClient::new(rusoto_core::Region::UsEast1);
                let ch = SqsChunnel::new(sqs_client, vec![queue_name.as_str()]);

                info!("sending 20 messages");
                ch.send_batch(
                    std::iter::repeat((
                        SqsAddr {
                            queue_id: queue_name.to_string(),
                            group: None,
                        },
                        "test message".to_string(),
                    ))
                    .take(20),
                )
                .await
                .wrap_err("sqs send")?;

                let mut recvd = 0;
                while recvd < 20 {
                    info!("calling recv_batch");
                    let msgs = ch.recv_batch(10).await.wrap_err("sqs recv")?;
                    let l = msgs.len();
                    for (q, msg) in msgs {
                        assert_eq!(q.queue_id, queue_name);
                        assert_eq!(&msg, "test message");
                    }
                    recvd += l;
                }

                Ok::<_, Report>(())
            }
            .instrument(info_span!("sqs_batch")),
        );

        rt.block_on(async move {
            let sqs_client = SqsClient::new(rusoto_core::Region::UsEast1);
            super::delete_queue(&sqs_client, queue_name).await
        })
        .unwrap();
        res.unwrap()
    }
}
