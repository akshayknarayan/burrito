//! Chunnel implementing reliability.
//!
//! Takes as Data a `(u32, Vec<u8>)`, where the `u32` is a unique tag corresponding to a data
//! segment, the `Vec<u8>`.

use crate::{Chunnel, ChunnelConnection, Context, InheritChunnel};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::convert::TryInto;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use tokio::sync::RwLock;
use tracing::{debug, trace};
use tracing_futures::Instrument;

pub struct ReliabilityChunnel<C> {
    inner: Arc<C>,
    timeout: Duration,
}

impl<Cx> From<Cx> for ReliabilityChunnel<Cx> {
    fn from(cx: Cx) -> ReliabilityChunnel<Cx> {
        ReliabilityChunnel {
            inner: Arc::new(cx),
            timeout: Duration::from_millis(100),
        }
    }
}

impl<Cx> ReliabilityChunnel<Cx> {
    pub fn set_timeout(&mut self, to: Duration) -> &mut Self {
        self.timeout = to;
        self
    }
}

impl<C> Context for ReliabilityChunnel<C> {
    type ChunnelType = C;

    fn context(&self) -> &Self::ChunnelType {
        &self.inner
    }

    fn context_mut(&mut self) -> Option<&mut Self::ChunnelType> {
        Arc::get_mut(&mut self.inner)
    }
}

impl<C> InheritChunnel for ReliabilityChunnel<C>
where
    C: Chunnel,
    C::Connection: ChunnelConnection<Data = Vec<u8>> + Send + Sync + 'static,
{
    type Connection = Reliability<C::Connection>;
    type Config = Duration;

    fn get_config(&mut self) -> Self::Config {
        self.timeout
    }

    fn make_connection(
        cx: <<Self as Context>::ChunnelType as Chunnel>::Connection,
        cfg: Self::Config,
    ) -> Self::Connection {
        let mut c = Reliability::from(cx);
        c.set_timeout(cfg);
        c
    }
}

#[derive(Default)]
struct ReliabilityState {
    inflight: HashMap<u32, (Vec<u8>, Option<oneshot::Sender<Result<(), eyre::Report>>>)>, // list of inflight seqs
    retx_tracker: BTreeMap<Instant, u32>,
    last_timeout: Option<Instant>,
    pending_payload: VecDeque<(u32, Vec<u8>)>, // payloads we have received that are waiting for a recv() call
}

pub struct Reliability<C> {
    timeout: Duration,
    inner: Arc<C>,
    state: Arc<RwLock<ReliabilityState>>,
}

impl<C> Reliability<C> {
    pub fn set_timeout(&mut self, to: Duration) -> &mut Self {
        self.timeout = to;
        self
    }
}

impl<C: Default> Default for Reliability<C> {
    fn default() -> Self {
        Self {
            timeout: Duration::from_millis(100),
            inner: Default::default(),
            state: Default::default(),
        }
    }
}

impl<Cx> From<Cx> for Reliability<Cx> {
    fn from(cx: Cx) -> Reliability<Cx> {
        Reliability {
            inner: Arc::new(cx),
            timeout: Duration::from_millis(100),
            state: Default::default(),
        }
    }
}

impl<C> Clone for Reliability<C> {
    fn clone(&self) -> Self {
        Reliability {
            timeout: self.timeout,
            inner: Arc::clone(&self.inner),
            state: Arc::clone(&self.state),
        }
    }
}

impl<C> Context for Reliability<C> {
    type ChunnelType = C;

    fn context(&self) -> &Self::ChunnelType {
        &self.inner
    }

    fn context_mut(&mut self) -> Option<&mut Self::ChunnelType> {
        Arc::get_mut(&mut self.inner)
    }
}

impl<C> ChunnelConnection for Reliability<C>
where
    C: ChunnelConnection<Data = Vec<u8>> + Send + Sync + 'static,
{
    type Data = (u32, Vec<u8>); // a tag and its data.

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + Sync>> {
        if data.1.is_empty() {
            return Box::pin(async move { Ok(()) });
        }

        let seq = data.0;

        let mut state = Arc::clone(&self.state);
        let mut inner = Arc::clone(&self.inner);
        let timeout = self.timeout;
        Box::pin(
            async move {
                let (s, mut r) = oneshot::channel();
                let (seq, data) = data;
                let buf = {
                    let mut state = state.write().await;
                    let mut buf = seq.to_be_bytes().to_vec();
                    buf.extend(&data);
                    state.inflight.insert(seq, (buf.clone(), Some(s)));
                    state.retx_tracker.insert(Instant::now(), seq);
                    trace!(inflight = state.inflight.len(), seq = ?seq, "sent");
                    buf
                };

                inner.send(buf).await?;
                let state1 = Arc::clone(&state);
                let inner1 = Arc::clone(&inner);
                futures_util::future::select(
                    Box::pin(
                        async move {
                            let state = state1;
                            let inner = inner1;
                            loop {
                                tokio::select!(
                                    done = &mut r => {
                                        done??;
                                        return Ok::<_, eyre::Report>(());
                                    }
                                    _ = tokio::time::delay_for(timeout) => {
                                        // manage timeouts
                                        let mut need_retx = false;
                                        {
                                            let st = state.read().await;
                                            match st.last_timeout {
                                                None => need_retx = true,
                                                Some(then) if then.elapsed() > timeout => {
                                                    // might need to retransmit things.
                                                    need_retx = true;
                                                }
                                                _ => (),
                                            }
                                        } // end read lock on state


                                        if need_retx {
                                            debug!("checking retransmissions");
                                            let expired = {
                                                let mut st = state.write().await;
                                                let cutoff = Instant::now() - timeout;
                                                let mut unexpired = st.retx_tracker.split_off(&cutoff);
                                                std::mem::swap(&mut unexpired, &mut st.retx_tracker);
                                                unexpired
                                            };

                                            for (_, seq) in expired {
                                                let st = state.read().await;
                                                let d = if let Some((d, _)) = st.inflight.get(&seq) {
                                                    debug!(seq = ?seq, "retransmitting");
                                                    Some(d.clone())
                                                } else { None };
                                                // avoid holding lock across the await
                                                std::mem::drop(st);

                                                if let Some(d) = d {
                                                    inner.send(d.clone()).await?;
                                                    let mut st = state.write().await;
                                                    st.retx_tracker.insert(Instant::now(), seq);
                                                }
                                            }

                                            let mut st = state.write().await;
                                            st.last_timeout = Some(Instant::now());
                                        }
                                    }
                                );
                            }
                        }
                        .instrument(tracing::debug_span!("send_retx_loop")),
                    ),
                    Box::pin(
                        async move {
                            loop {
                                let r = do_recv(&mut inner, &mut state).await?;
                                let mut st = state.write().await;
                                st.pending_payload.push_back(r);
                            }
                        }
                        .instrument(tracing::debug_span!("send_select_recv")),
                    ),
                )
                .await
                .factor_first()
                .0
            }
            .instrument(tracing::debug_span!("send", seq = ?seq)),
        )
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + Sync>> {
        let mut state = Arc::clone(&self.state);
        let mut inner = Arc::clone(&self.inner);

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

                do_recv(&mut inner, &mut state).await
            }
            .instrument(tracing::debug_span!("recv")),
        )
    }
}

async fn do_recv<C>(
    inner: &mut Arc<C>,
    state: &mut Arc<RwLock<ReliabilityState>>,
) -> Result<(u32, Vec<u8>), eyre::Report>
where
    C: ChunnelConnection<Data = Vec<u8>> + Send + Sync + 'static,
{
    loop {
        trace!("call inner recv");
        let mut seq = inner.recv().await?;
        // pop off the seqno
        let data = seq.split_off(4);
        let seq = u32::from_be_bytes(seq[0..4].try_into().unwrap());

        if data.is_empty() {
            // it was an ack
            let mut st = state.write().await;
            trace!(seq = ?seq, "got ack");
            if let Some((_, Some(s))) = st.inflight.remove(&seq) {
                s.send(Ok(())).unwrap_or_else(|_| {
                    debug!(seq = ?seq, "discarding oneshot send");
                });
            }

            continue;
        } else {
            // send an ack
            inner.send(seq.to_be_bytes().to_vec()).await?;
            trace!(seq = ?seq, len=data.len(), "got payload, sent ack");
        }

        return Ok((seq, data));
    }
}

#[cfg(test)]
mod test {
    use super::{Reliability, ReliabilityChunnel};
    use crate::chan_transport::Chan;
    use crate::{Chunnel, ChunnelConnection};
    use futures_util::StreamExt;
    use tracing::{debug, info};
    use tracing_futures::Instrument;

    async fn do_transmit<C>(
        snd_ch: Reliability<C>,
        rcv_ch: Reliability<C>,
        msgs: Vec<(u32, Vec<u8>)>,
    ) where
        C: ChunnelConnection<Data = Vec<u8>> + Send + Sync + 'static,
    {
        // recv side
        tokio::spawn(
            async move {
                info!("starting receiver");
                loop {
                    let m = rcv_ch.recv().await.unwrap();
                    debug!(m = ?m, "rcvd");
                }
            }
            .instrument(tracing::debug_span!("receiver")),
        );

        futures_util::future::join_all(msgs.into_iter().map(|m| {
            debug!(m = ?m, "sending");
            snd_ch.send(m)
        }))
        .await
        .into_iter()
        .collect::<Result<(), _>>()
        .unwrap();

        info!("done");
    }

    #[test]
    fn no_drops() {
        let _guard = tracing_subscriber::fmt::try_init();
        let msgs = vec![(0, vec![0u8; 10]), (1, vec![1u8; 10]), (2, vec![2u8; 10])];

        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_time()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                let (srv, cln) = Chan::default().split();

                let mut rcv = ReliabilityChunnel::from(srv);
                let mut rcv = rcv.listen(()).await;
                let rcv = rcv.next().await.unwrap();

                let mut snd = ReliabilityChunnel::from(cln);
                let snd = snd.connect(()).await;

                do_transmit(snd, rcv, msgs).await;
            }
            .instrument(tracing::info_span!("no_drops")),
        );
    }

    #[test]
    fn drop_2() {
        let _guard = tracing_subscriber::fmt::try_init();
        let msgs = vec![(0, vec![0u8; 10]), (1, vec![1u8; 10]), (2, vec![2u8; 10])];

        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_time()
            .build()
            .unwrap();

        use std::sync::{atomic::AtomicUsize, Arc};

        rt.block_on(
            async move {
                let mut t = Chan::default();
                let ctr: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
                t.link_conditions(move |x| {
                    let c = ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if c != 2 {
                        x
                    } else {
                        None
                    }
                });
                let (srv, cln) = t.split();

                let mut rcv = ReliabilityChunnel::from(srv);
                let mut rcv = rcv.listen(()).await;
                let rcv = rcv.next().await.unwrap();

                let mut snd = ReliabilityChunnel::from(cln);
                let snd = snd.connect(()).await;

                do_transmit(snd, rcv, msgs).await;
            }
            .instrument(tracing::info_span!("drop_2")),
        );
    }
}
