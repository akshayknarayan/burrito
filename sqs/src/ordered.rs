use crate::{besteffort::SqsChunnel, do_recv_batch, do_send_batch, SqsAddr};
use bertha::ChunnelConnection;
use color_eyre::eyre::{eyre, Report};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use tracing::trace;

/// The underlying client type to access Sqs.
pub use rusoto_sqs::SqsClient;

#[derive(Clone, Debug)]
pub struct OrderedSqsChunnel {
    inner: SqsChunnel,
}

impl From<SqsChunnel> for OrderedSqsChunnel {
    fn from(mut inner: SqsChunnel) -> Self {
        for s in &mut inner.recv_queue_urls {
            if !s.ends_with(".fifo") {
                s.push_str(".fifo");
            }
        }

        Self { inner }
    }
}

impl OrderedSqsChunnel {
    pub fn new<'a>(
        sqs_client: SqsClient,
        recv_queue_urls: impl IntoIterator<Item = &'a str>,
    ) -> Result<Self, Report> {
        let recv_queue_urls: Result<Vec<_>, _> = recv_queue_urls
            .into_iter()
            .map(|s| {
                if s.ends_with(".fifo") {
                    Ok(s)
                } else {
                    Err(eyre!("OrderedSqsChunnel must use fifo queues"))
                }
            })
            .collect();
        let recv_queue_urls = recv_queue_urls?;
        let inner = SqsChunnel::new(sqs_client, recv_queue_urls);
        Ok(Self { inner })
    }
}

impl ChunnelConnection for OrderedSqsChunnel {
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
                for (
                    SqsAddr {
                        mut queue_id,
                        group,
                    },
                    body,
                ) in &mut burst
                {
                    if !queue_id.ends_with(".fifo") {
                        queue_id.push_str(".fifo");
                    }

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
                    let ctr = self.inner.num_sqs_send_calls.fetch_add(1, Ordering::SeqCst);
                    trace!(?qid, ?ctr, batch_size=?msgs.len(), "sending ordered batch");
                    do_send_batch::<true>(&self.inner.sqs_client, qid, ctr, msgs).await?;
                } else {
                    // send the residual batches and break.
                    for (qid, msgs) in pending_batches {
                        let ctr = self.inner.num_sqs_send_calls.fetch_add(1, Ordering::SeqCst);
                        trace!(?qid, ?ctr, batch_size=?msgs.len(), "sending ordered batch");
                        do_send_batch::<true>(&self.inner.sqs_client, qid, ctr, msgs).await?;
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
            &self.inner.sqs_client,
            &self.inner.recv_queue_urls[..],
            &self.inner.batch_recv_futs,
            &self.inner.num_sqs_recv_calls,
            &self.inner.num_sqs_del_calls,
            msgs_buf,
        ))
    }
}
