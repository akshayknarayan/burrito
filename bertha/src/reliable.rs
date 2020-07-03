//! Chunnel implementing reliability.

use crate::{Chunnel, Endedness, Scope};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::convert::TryInto;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::sync::RwLock;
use tracing::{debug, info};
use tracing_futures::Instrument;

#[derive(Default)]
struct ReliabilityState {
    inflight: HashMap<u32, (Vec<u8>, Option<oneshot::Sender<Result<(), eyre::Report>>>)>, // list of inflight seqs
    pending_payload: VecDeque<(u32, Vec<u8>)>, // payloads we have received that are waiting for a recv() call
}

#[derive(Default)]
pub struct Reliability<C> {
    inner: Arc<C>,
    state: Arc<RwLock<ReliabilityState>>,
}

impl<Cx> Reliability<Cx> {
    pub fn with_context<C>(self, cx: C) -> Reliability<C> {
        Reliability {
            inner: Arc::new(cx),
            state: Default::default(),
        }
    }
}

impl<C> Clone for Reliability<C> {
    fn clone(&self) -> Self {
        Reliability {
            inner: Arc::clone(&self.inner),
            state: Arc::clone(&self.state),
        }
    }
}

impl<C> Chunnel for Reliability<C>
where
    C: Chunnel<Data = Vec<u8>> + Send + Sync + 'static,
{
    type Data = (u32, Vec<u8>); // a tag and its data.

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + Sync>> {
        if data.1.is_empty() {
            return Box::pin(async move { Ok(()) });
        }

        // TODO check/arm timer
        // TODO retx on timer expiry

        let state = self.state.clone();
        let inner = self.inner.clone();
        Box::pin(
            async move {
                let (s, r) = oneshot::channel();
                let (seq, data) = data;
                let buf = {
                    let mut state = state.write().await;
                    let mut buf = seq.to_be_bytes().to_vec();
                    buf.extend(&data);
                    state.inflight.insert(seq, (data, Some(s)));
                    info!(inflight = state.inflight.len(), "sent");
                    buf
                };
                inner.send(buf).await?;
                futures_util::future::select(
                    Box::pin(async move {
                        r.await??;
                        Ok::<_, eyre::Report>(())
                    }),
                    Box::pin(
                        async move {
                            loop {
                                let mut seq = match inner.recv().await {
                                    Ok(s) => s,
                                    Err(e) => {
                                        debug!(err = ?e, "Error from inner chunnel");
                                        continue;
                                    }
                                };
                                // pop off the seqno
                                let data = seq.split_off(4);
                                let seq = u32::from_be_bytes(seq[0..4].try_into().unwrap());

                                if data.is_empty() {
                                    // it was an ack
                                    let mut st = state.write().await;
                                    info!(seq = ?seq, "got ack");
                                    if let Some((_, Some(s))) = st.inflight.remove(&seq) {
                                        s.send(Ok(())).unwrap_or_else(|_| {
                                            debug!(seq = ?seq, "discarding oneshot send");
                                        });
                                    }
                                } else {
                                    // send an ack
                                    if let Err(e) = inner.send(seq.to_be_bytes().to_vec()).await {
                                        debug!(err = ?e, "Error from inner chunnel");
                                    }

                                    info!(seq = ?seq, len=data.len(), "got payload, sent ack");
                                    {
                                        let mut st = state.write().await;
                                        st.pending_payload.push_back((seq, data));
                                    }
                                }
                            }
                        }
                        .instrument(tracing::info_span!("send_select_recv", seq=?seq)),
                    ),
                )
                .await
                .factor_first()
                .0
            }
            .instrument(tracing::debug_span!("send")),
        )
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + Sync>> {
        let inner = self.inner.clone();
        let state = self.state.clone();

        Box::pin(
            async move {
                {
                    let st = state.read().await;
                    if !st.pending_payload.is_empty() {
                        std::mem::drop(st);
                        let mut st = state.write().await;
                        return Ok(st.pending_payload.pop_front().unwrap());
                    }
                }

                loop {
                    info!("call inner recv");
                    let mut seq = inner.recv().await?;
                    // pop off the seqno
                    let data = seq.split_off(4);
                    let seq = u32::from_be_bytes(seq[0..4].try_into().unwrap());

                    if data.is_empty() {
                        // it was an ack
                        let mut st = state.write().await;
                        info!(seq = ?seq, "got ack");
                        if let Some((_, Some(s))) = st.inflight.remove(&seq) {
                            s.send(Ok(())).unwrap_or_else(|_| {
                                debug!(seq = ?seq, "discarding oneshot send");
                            });
                        }

                        continue;
                    } else {
                        // send an ack
                        inner.send(seq.to_be_bytes().to_vec()).await?;
                        info!(seq = ?seq, len=data.len(), "got payload, sent ack");
                    }

                    return Ok((seq, data));
                }
            }
            .instrument(tracing::debug_span!("recv")),
        )
    }

    fn scope(&self) -> Scope {
        Scope::Application
    }

    fn endedness(&self) -> Endedness {
        Endedness::Both // we add a header
    }

    fn implementation_priority(&self) -> usize {
        1
    }
}

#[cfg(test)]
mod test {
    use super::Reliability;
    use crate::chan_transport::Chan;
    use crate::{Chunnel, Connector};
    use futures_util::StreamExt;
    use tracing::info;
    use tracing_futures::Instrument;

    #[test]
    fn no_drops() {
        let _guard = tracing_subscriber::fmt().try_init();

        let msgs = vec![(0, vec![0u8; 10]), (1, vec![1u8; 10]), (2, vec![2u8; 10])];

        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                let mut t = Chan::new();
                let mut rcv = t.listen(()).await;
                let rcv_cn = rcv.next().await.unwrap();
                let rcv_ch = Reliability::<()>::default().with_context(rcv_cn);

                let snd = t.connect(()).await;
                let snd_ch = Reliability::<()>::default().with_context(snd);

                // recv side
                tokio::spawn(
                    async move {
                        info!("starting receiver");
                        loop {
                            let m = rcv_ch.recv().await.unwrap();
                            info!(m = ?m, "rcvd");
                        }
                    }
                    .instrument(tracing::info_span!("receiver")),
                );

                futures_util::future::join_all(msgs.into_iter().map(|m| {
                    info!(m = ?m, "sending");
                    snd_ch.send(m)
                }))
                .await
                .into_iter()
                .collect::<Result<(), _>>()
                .unwrap();

                info!("done");
            }
            .instrument(tracing::info_span!("no_drops")),
        );
    }
}
