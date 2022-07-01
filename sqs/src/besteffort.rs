use crate::{do_recv_batch, do_send_batch, fut_with_string::FutWithString, SqsAddr};
use bertha::ChunnelConnection;
use color_eyre::eyre::Report;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::Mutex;
use tracing::{info, trace};

/// The underlying client type to access Sqs.
pub use rusoto_sqs::SqsClient;

/// SqsChunnel
#[derive(Clone)]
pub struct SqsChunnel {
    pub(crate) sqs_client: SqsClient,
    pub(crate) recv_queue_urls: Vec<String>,

    // saved recv_batch futures
    pub(crate) batch_recv_futs: Arc<Mutex<HashMap<String, FutWithString>>>,

    pub(crate) num_sqs_send_calls: Arc<AtomicUsize>,
    pub(crate) num_sqs_recv_calls: Arc<AtomicUsize>,
    pub(crate) num_sqs_del_calls: Arc<AtomicUsize>,
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
            batch_recv_futs: Default::default(),
            num_sqs_send_calls: Default::default(),
            num_sqs_recv_calls: Default::default(),
            num_sqs_del_calls: Default::default(),
        }
    }

    pub fn listen(&mut self, addr: &str) {
        self.recv_queue_urls.push(addr.to_owned());
    }
}

impl ChunnelConnection for SqsChunnel {
    type Data = (SqsAddr, String);

    fn send<'cn, B>(
        &'cn self,
        burst: B,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'cn>>
    where
        B: IntoIterator<Item = Self::Data> + Send + 'cn,
        <B as IntoIterator>::IntoIter: Send,
    {
        Box::pin(async move {
            let mut pending_batches = HashMap::new();
            let mut burst = burst.into_iter();
            loop {
                // group the messages by destination queue. If we fill a queue's batch (to 10),
                // stop pulling messages and send that batch, setting `batch_to_send` to `Some`
                // accordingly.  After the loop, if `batch_to_send` is None, then we couldn't fill any batches
                // and there are no more messages, so just send the residual batches and we're done.
                let mut batch_to_send = None;
                for (SqsAddr { queue_id, group }, body) in &mut burst {
                    let q_batch = pending_batches
                        .entry(queue_id.clone())
                        .or_insert_with(|| Vec::with_capacity(10));
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
                    let ctr = self.num_sqs_send_calls.fetch_add(1, Ordering::SeqCst);
                    trace!(?qid, ?ctr, batch_size=?msgs.len(), "sending batch");
                    do_send_batch::<false>(&self.sqs_client, qid, ctr, msgs).await?;
                } else {
                    // send the residual batches and break.
                    for (qid, msgs) in pending_batches {
                        let ctr = self.num_sqs_send_calls.fetch_add(1, Ordering::SeqCst);
                        trace!(?qid, ?ctr, batch_size=?msgs.len(), "sending batch");
                        do_send_batch::<false>(&self.sqs_client, qid, ctr, msgs).await?;
                    }

                    break;
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
        Box::pin(do_recv_batch(
            &self.sqs_client,
            &self.recv_queue_urls[..],
            &self.batch_recv_futs,
            &self.num_sqs_recv_calls,
            &self.num_sqs_del_calls,
            msgs_buf,
        ))
    }
}
