use bertha::{util::MsgId, Chunnel, ChunnelConnection, Negotiate};
use color_eyre::eyre;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{trace, trace_span};
use tracing_futures::Instrument;

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
    type Capability = ();
    fn capabilities() -> Vec<Self::Capability> {
        vec![]
    }
}

impl Default for KvReliabilityChunnel {
    fn default() -> Self {
        KvReliabilityChunnel {
            timeout: Duration::from_millis(100),
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

pub struct KvReliability<C, A, D> {
    inner: Arc<C>,
    timeout: Duration,
    sends: Arc<dashmap::DashMap<usize, oneshot::Sender<()>>>,
    signal_recv: mpsc::Sender<(A, D)>,
    recvs: Arc<Mutex<mpsc::Receiver<(A, D)>>>,
}

impl<C, A, D> KvReliability<C, A, D> {
    fn new(inner: C, timeout: Duration) -> Self {
        let (s, r) = mpsc::channel(1000);
        KvReliability {
            inner: Arc::new(inner),
            timeout,
            sends: Default::default(),
            signal_recv: s,
            recvs: Arc::new(Mutex::new(r)),
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

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>> {
        let inner = Arc::clone(&self.inner);
        let msg_id = data.1.id();
        let (s, mut r) = oneshot::channel();
        let to = self.timeout;
        let signal_recv = self.signal_recv.clone();

        // state map id -> chan sender
        self.sends.insert(msg_id, s);
        let sends = Arc::clone(&self.sends);
        Box::pin(async move {
            trace!("start");
            inner.send(data.clone()).await?;
            loop {
                tokio::select!(
                    resp = inner.recv() => {
                        let resp = resp?;

                        // careful about blocking here, could deadlock
                        let recv_msg_id = resp.1.id();
                        trace!(id = ?recv_msg_id, "recv");
                        signal_recv.send(resp).await.map_err(|_| ()).expect("receiver won't hang up");

                        let send_ch = if let Some(s) = sends.remove(&msg_id) {
                            s
                        } else { continue; };

                        if recv_msg_id == msg_id {
                            return Ok(());
                        } else {
                            send_ch.1.send(()).map_err(|_| ()).expect("send done notification failed");
                        }
                    }
                    _ = &mut r => {
                        trace!("signaled");
                        return Ok(());
                    }
                    _ = tokio::time::sleep(to) => {
                        trace!("retx");
                        inner.send(data.clone()).await?;
                    }
                );
            }
        }.instrument(trace_span!("send", ?msg_id)))
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>> {
        // return next signaled recv
        let r = Arc::clone(&self.recvs);
        Box::pin(async move { Ok(r.lock().await.recv().await.unwrap()) })
    }
}

#[cfg(test)]
mod test {
    use super::KvReliabilityChunnel;
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
        color_eyre::install().unwrap_or_else(|_| ());
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
                        loop {
                            let m: ((), Msg) = rcv.recv().await.unwrap();
                            debug!(?m, "rcvd");
                            rcv.send(m).await.unwrap();
                        }
                    }
                    .instrument(tracing::info_span!("receiver")),
                );

                tokio::time::sleep(std::time::Duration::from_millis(100)).await;

                for m in msgs {
                    debug!(?m, "sending");
                    snd.send(m).await.unwrap();
                    debug!(?m, "getting response");
                    let m: ((), Msg) = snd.recv().await.unwrap();
                    debug!(?m, "done");
                }

                info!("done");
            }
            .instrument(tracing::info_span!("drop_2")),
        );
    }
}
