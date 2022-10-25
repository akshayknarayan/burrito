use bertha::{
    reliable::ReliabilityNeg,
    util::{MsgId, Nothing},
    Chunnel, ChunnelConnection, Negotiate,
};
use color_eyre::eyre::{eyre, Report, WrapErr};
use flume::{Receiver, Sender};
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::time::Duration;
use tokio::sync::oneshot;

/// Reliable transmission of request/response pairs.
///
/// Assumptions:
/// 1. Data segments have an id (the `MsgId` trait)
/// 2. Each request will generate a single response, and that response data segment will have a
///    matching id.
///
/// These assumptions allow us to forego seqno generation and explicit acks.  As a result, however,
/// this will only work on the client side, since we assume that `send`s and `recv`s are paired and
/// that the `send` is first. Waiting on a `recv` without a prior `send` will block forever.
#[derive(Debug, Clone)]
pub struct KvReliabilityChunnel {
    timeout: Duration,
}

impl Negotiate for KvReliabilityChunnel {
    type Capability = ReliabilityNeg;

    fn guid() -> u64 {
        0xa84943c6f0ce1b78
    }

    fn capabilities() -> Vec<Self::Capability> {
        vec![ReliabilityNeg::Reliability, ReliabilityNeg::Ordering]
    }
}

impl Default for KvReliabilityChunnel {
    fn default() -> Self {
        KvReliabilityChunnel {
            timeout: Duration::from_millis(500),
        }
    }
}

impl KvReliabilityChunnel {
    pub fn set_timeout(&mut self, to: Duration) -> &mut Self {
        self.timeout = to;
        self
    }
}

impl<A, InC, D> Chunnel<InC> for KvReliabilityChunnel
where
    InC: ChunnelConnection<Data = (A, D)> + Send + Sync + 'static,
    A: Clone + Eq + Hash + std::fmt::Debug + Send + Sync + 'static,
    D: MsgId + Clone + Send + Sync + 'static,
{
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Connection = KvReliability<InC, A, D>;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        let to = self.timeout;
        Box::pin(async move {
            let r = KvReliability::new(cn, to);
            Ok(r)
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct KvReliabilityServerChunnel(Nothing<ReliabilityNeg>);

impl Negotiate for KvReliabilityServerChunnel {
    type Capability = ReliabilityNeg;

    fn guid() -> u64 {
        0xa84943c6f0ce1b78
    }

    fn capabilities() -> Vec<Self::Capability> {
        vec![ReliabilityNeg::Reliability, ReliabilityNeg::Ordering]
    }
}

impl<D, InC> Chunnel<InC> for KvReliabilityServerChunnel
where
    InC: ChunnelConnection<Data = D> + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Future = <Nothing<ReliabilityNeg> as Chunnel<InC>>::Future;
    type Connection = <Nothing<ReliabilityNeg> as Chunnel<InC>>::Connection;
    type Error = <Nothing<ReliabilityNeg> as Chunnel<InC>>::Error;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        self.0.connect_wrap(cn)
    }
}

pub struct KvReliability<C, A, D> {
    inner: C,
    timeout: Duration,
    sends: dashmap::DashMap<usize, oneshot::Sender<()>>,
    signal_recv: Sender<(A, D)>,
    recvs: Receiver<(A, D)>,
}

impl<C, A, D> KvReliability<C, A, D> {
    fn new(inner: C, timeout: Duration) -> Self {
        let (s, r) = flume::bounded(100);
        KvReliability {
            inner,
            timeout,
            sends: Default::default(),
            signal_recv: s,
            recvs: r,
        }
    }
}

impl<A, D, C> ChunnelConnection for KvReliability<C, A, D>
where
    C: ChunnelConnection<Data = (A, D)> + Send + Sync + 'static,
    A: Clone + Send + Sync + 'static,
    D: MsgId + Clone + Send + Sync + 'static,
{
    type Data = (A, D);

    fn send<'cn, B>(
        &'cn self,
        burst: B,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'cn>>
    where
        B: IntoIterator<Item = Self::Data> + Send + 'cn,
        <B as IntoIterator>::IntoIter: Send,
    {
        use futures_util::TryStreamExt;
        Box::pin(async move {
            let mut batch_pkts = Vec::new();
            let mut slots = Vec::new();
            let batch_completions = futures_util::stream::FuturesUnordered::new();
            for pkt in burst {
                let (s, r) = oneshot::channel();
                let msg_id = pkt.1.id();
                self.sends.insert(msg_id, s);
                batch_pkts.push(pkt);
                batch_completions.push(r);
                slots.push(None);
            }

            self.inner.send(batch_pkts.clone()).await?;

            let mut done = batch_completions.try_collect();
            loop {
                tokio::select!(
                    ms_res = self.inner.recv(&mut slots) => {
                        let ms = ms_res.wrap_err("kvreliability recv acks")?;
                        for resp in ms.into_iter().map_while(Option::take) {
                            let recv_msg_id = resp.1.id();
                            // careful about blocking here, could deadlock
                            self.signal_recv.send_async(resp).await.map_err(|_| ()).expect("receiver won't hang up");
                            let send_ch = if let Some(s) = self.sends.remove(&recv_msg_id) {
                                s
                            } else { continue; };
                            send_ch.1.send(()).map_err(|_| ()).expect("send done notification failed");
                        }
                    }
                    r = &mut done => {
                        r?;
                        return Ok(());
                    }
                    _ = tokio::time::sleep(self.timeout) => {
                        self.inner.send(batch_pkts.clone()).await?;
                    }
                );
            }
        })
    }

    fn recv<'cn, 'buf>(
        &'cn self,
        msgs_buf: &'buf mut [Option<Self::Data>],
    ) -> Pin<Box<dyn Future<Output = Result<&'buf mut [Option<Self::Data>], Report>> + Send + 'cn>>
    where
        'buf: 'cn,
    {
        Box::pin(async move {
            msgs_buf[0] = Some(
                self.recvs
                    .recv_async()
                    .await
                    .wrap_err(eyre!("All senders dropped"))?,
            );
            let mut slot_idx = 1;
            if slot_idx >= msgs_buf.len() {
                return Ok(msgs_buf);
            }

            while let Ok(m) = self.recvs.try_recv() {
                msgs_buf[slot_idx] = Some(m);
                slot_idx += 1;

                if slot_idx >= msgs_buf.len() {
                    break;
                }
            }

            Ok(msgs_buf)
        })
    }
}

#[cfg(test)]
mod test {
    use super::KvReliabilityChunnel;
    use crate::tests::COLOR_EYRE;
    use bertha::{
        chan_transport::Chan, Chunnel, ChunnelConnection, ChunnelConnector, ChunnelListener,
    };
    use futures_util::StreamExt;
    use tracing::{debug, info};
    use tracing_error::ErrorLayer;
    use tracing_futures::Instrument;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    #[derive(Clone, Copy, Debug)]
    struct Msg {
        field: usize,
    }

    impl bertha::util::MsgId for Msg {
        fn id(&self) -> usize {
            self.field
        }
    }

    #[test]
    fn drop_2() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        COLOR_EYRE.call_once(|| color_eyre::install().unwrap_or(()));
        let msgs: Vec<_> = (0..10).map(|field| ((), Msg { field })).collect();

        let rt = tokio::runtime::Builder::new_current_thread()
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

                let (mut srv, mut cln) = t.split();

                let mut rcv_st = srv.listen(()).await.unwrap();
                let rcv = rcv_st.next().await.unwrap().unwrap();

                let mut l = KvReliabilityChunnel::default();
                let cln = cln.connect(()).await.unwrap();
                let snd = l.connect_wrap(cln).await.unwrap();

                // recv side
                tokio::spawn(
                    async move {
                        info!("starting receiver");
                        let mut slots = [None, None, None, None];
                        loop {
                            let ms = rcv.recv(&mut slots).await.unwrap();
                            debug!(?ms, "rcvd");
                            rcv.send(ms.into_iter().map_while(Option::take))
                                .await
                                .unwrap();
                        }
                    }
                    .instrument(tracing::info_span!("receiver")),
                );

                tokio::time::sleep(std::time::Duration::from_millis(100)).await;

                let mut slot = [None];
                for m in msgs {
                    debug!(?m, "sending");
                    snd.send(std::iter::once(m)).await.unwrap();
                    debug!(?m, "getting response");
                    let ms = snd.recv(&mut slot).await.unwrap();
                    let m: ((), Msg) = ms[0].take().unwrap();
                    debug!(?m, "done");
                }

                info!("done");
            }
            .instrument(tracing::info_span!("drop_2")),
        );
    }
}
