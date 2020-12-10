use bertha::{util::MsgId, ChunnelConnection, Client, Negotiate, Serve};
use color_eyre::eyre;
use futures_util::{
    future::{ready, Ready},
    stream::{Stream, TryStreamExt},
};
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, Mutex};

/// Reliable transmission of request/response pairs.
///
/// Assumptions:
/// 1. Data segments have an id (the `MsgId` trait)
/// 2. Each request will generate a single response, and that response data segment will have a matching id.
///
/// These assumptions allow us to forego seqno generation and sending acks.
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

impl<A, InS, InC, InE, D> Serve<InS> for KvReliabilityChunnel
where
    InS: Stream<Item = Result<InC, InE>> + Send + 'static,
    InC: ChunnelConnection<Data = (A, D)> + Send + Sync + 'static,
    InE: Send + Sync + 'static,
    A: Clone + Eq + Hash + std::fmt::Debug + Send + Sync + 'static,
    D: MsgId + Clone + Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Stream, Self::Error>>;
    type Connection = KvReliability<InC, A, D>;
    type Error = InE;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn serve(&mut self, inner: InS) -> Self::Future {
        let cfg = self.timeout;
        ready(Ok(
            Box::pin(inner.and_then(move |cn| async move { Ok(KvReliability::new(cn, cfg)) })) as _,
        ))
    }
}

impl<A, InC, D> Client<InC> for KvReliabilityChunnel
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
            inner.send(data.clone()).await?;
            loop {
                tokio::select!(
                    resp = inner.recv() => {
                        let resp = resp?;

                        // careful about blocking here, could deadlock
                        let recv_msg_id = resp.1.id();
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
                        return Ok(());
                    }
                    _ = tokio::time::sleep(to) => {
                        inner.send(data.clone()).await?;
                    }
                );
            }
        })
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>> {
        // return next signaled recv
        let r = Arc::clone(&self.recvs);
        Box::pin(async move { Ok(r.lock().await.recv().await.unwrap()) })
    }
}
