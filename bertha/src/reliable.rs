//! Chunnel implementing reliability.
//!
//! Takes as Data a `(u32, Vec<u8>)`, where the `u32` is a unique tag corresponding to a data
//! segment, the `Vec<u8>`.

use crate::{
    util::{ProjectLeft, ProjectLeftCn, Unproject},
    ChunnelConnection, Client, Negotiate, Serve,
};
use ahash::AHashMap;
use color_eyre::eyre;
use dashmap::DashMap;
use futures_util::future::{ready, Ready};
use futures_util::stream::{Stream, StreamExt, TryStreamExt};
use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, instrument, trace};
use tracing_futures::Instrument;

#[derive(Clone, Debug)]
pub struct ReliabilityChunnel<D> {
    inner: ReliabilityProjChunnel<(), D>,
}

impl<D> Default for ReliabilityChunnel<D> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
        }
    }
}

impl<D> Negotiate for ReliabilityChunnel<D> {
    type Capability = ();
    fn capabilities() -> Vec<Self::Capability> {
        vec![]
    }
}

impl<D> ReliabilityChunnel<D> {
    pub fn set_timeout_factor(&mut self, to: usize) -> &mut Self {
        self.inner.timeout = to;
        self
    }
}

impl<InS, InC, InE, D> Serve<InS> for ReliabilityChunnel<D>
where
    InS: Stream<Item = Result<InC, InE>> + Send + 'static,
    InC: ChunnelConnection<Data = Pkt<D>> + Send + Sync + 'static,
    InE: Send + Sync + 'static,
    D: Clone + Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Stream, Self::Error>>;
    type Connection = ProjectLeftCn<(), ReliabilityProj<(), D, Unproject<InC>>>;
    type Error = InE;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn serve(&mut self, inner: InS) -> Self::Future {
        let st = inner.map_ok(Unproject);
        match self.inner.serve(st).into_inner() {
            Ok(st) => ProjectLeft::from(()).serve(st),
            Err(e) => ready(Err(e)),
        }
    }
}

impl<InC, D> Client<InC> for ReliabilityChunnel<D>
where
    InC: ChunnelConnection<Data = Pkt<D>> + Send + Sync + 'static,
    D: Clone + Send + Sync + 'static,
{
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Connection = ProjectLeftCn<(), ReliabilityProj<(), D, Unproject<InC>>>;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        let fut = self.inner.connect_wrap(Unproject(cn));
        Box::pin(async move { ProjectLeft::from(()).connect_wrap(fut.await?).await })
    }
}

#[derive(Clone, Debug)]
pub struct ReliabilityProjChunnel<A, D> {
    timeout: usize,
    _data: std::marker::PhantomData<(A, D)>,
}

impl<A, D> Negotiate for ReliabilityProjChunnel<A, D> {
    type Capability = ();
    fn capabilities() -> Vec<Self::Capability> {
        vec![]
    }
}

impl<A, D> Default for ReliabilityProjChunnel<A, D> {
    fn default() -> Self {
        ReliabilityProjChunnel {
            timeout: 5,
            _data: Default::default(),
        }
    }
}

impl<A, D> ReliabilityProjChunnel<A, D> {
    pub fn set_timeout_factor(&mut self, to: usize) -> &mut Self {
        self.timeout = to;
        self
    }
}

impl<A, InS, InC, InE, D> Serve<InS> for ReliabilityProjChunnel<A, D>
where
    A: Clone + Eq + Hash + std::fmt::Debug + Send + Sync + 'static,
    InS: Stream<Item = Result<InC, InE>> + Send + 'static,
    InC: ChunnelConnection<Data = (A, Pkt<D>)> + Send + Sync + 'static,
    InE: Send + Sync + 'static,
    D: Clone + Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Stream, Self::Error>>;
    type Connection = ReliabilityProj<A, D, InC>;
    type Error = InE;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn serve(&mut self, inner: InS) -> Self::Future {
        let cfg = self.timeout;
        ready(Ok(Box::pin(
            inner.and_then(move |cn| async move { Ok(ReliabilityProj::new(cn, Some(cfg)).await) }),
        ) as _))
    }
}

impl<A, InC, D> Client<InC> for ReliabilityProjChunnel<A, D>
where
    A: Clone + Eq + Hash + std::fmt::Debug + Send + Sync + 'static,
    InC: ChunnelConnection<Data = (A, Pkt<D>)> + Send + Sync + 'static,
    D: Clone + Send + Sync + 'static,
{
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Connection = ReliabilityProj<A, D, InC>;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        let to = self.timeout;
        Box::pin(async move { Ok(ReliabilityProj::new(cn, Some(to)).await) })
    }
}

#[derive(Debug)]
pub struct ReliabilityProj<A: Eq + Hash, D, C> {
    timeout: usize,
    inner: Arc<C>,
    new_send: mpsc::UnboundedSender<(A, PktHeader, Option<oneshot::Sender<()>>, Instant)>,
    new_recv: mpsc::UnboundedSender<(A, PktHeader)>,
    delayed_ack: Arc<DashMap<A, PktHeader>>,
    pending_payload_send: mpsc::UnboundedSender<(A, Pkt<D>)>,
    pending_payload_recv: Arc<Mutex<mpsc::UnboundedReceiver<(A, Pkt<D>)>>>,
}

impl<A, D, C> ReliabilityProj<A, D, C>
where
    A: Eq + Hash + std::fmt::Debug + Clone + Send + Sync + 'static,
    D: Send + Sync + 'static,
    C: ChunnelConnection<Data = (A, Pkt<D>)> + Send + Sync + 'static,
{
    async fn new(inner: C, timeout: Option<usize>) -> Self {
        let (new_send, send_r) = mpsc::unbounded_channel();
        let (new_recv, recv_r) = mpsc::unbounded_channel();
        let delayed_ack = Default::default();
        let inner = Arc::new(inner);
        tokio::spawn(reliability_state(
            Arc::clone(&inner),
            send_r,
            recv_r,
            Arc::clone(&delayed_ack),
        ));

        let (s, r) = mpsc::unbounded_channel();
        ReliabilityProj {
            inner,
            timeout: timeout.unwrap_or_else(|| 5),
            new_send,
            new_recv,
            delayed_ack,
            pending_payload_send: s,
            pending_payload_recv: Arc::new(Mutex::new(r)),
        }
    }
}

impl<A: Eq + Hash, C, D> ReliabilityProj<A, D, C> {
    pub fn set_timeout_factor(&mut self, to: usize) -> &mut Self {
        self.timeout = to;
        self
    }
}

impl<A: Eq + Hash, C, D> Clone for ReliabilityProj<A, D, C> {
    fn clone(&self) -> Self {
        ReliabilityProj {
            timeout: self.timeout,
            inner: Arc::clone(&self.inner),
            new_send: self.new_send.clone(),
            new_recv: self.new_recv.clone(),
            delayed_ack: Arc::clone(&self.delayed_ack),
            pending_payload_send: self.pending_payload_send.clone(),
            pending_payload_recv: Arc::clone(&self.pending_payload_recv),
        }
    }
}

impl<A, C, D> ChunnelConnection for ReliabilityProj<A, D, C>
where
    A: Clone + Eq + Hash + std::fmt::Debug + Send + Sync + 'static,
    C: ChunnelConnection<Data = (A, Pkt<D>)> + Send + Sync + 'static,
    D: Clone + Send + Sync + 'static,
{
    type Data = (A, (u32, D)); // a tag and its data.

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>> {
        let inner = Arc::clone(&self.inner);
        let new_send = self.new_send.clone();
        let new_recv = self.new_recv.clone();
        let delayed_ack = Arc::clone(&self.delayed_ack);
        let pending_payload_send = self.pending_payload_send.clone();
        let timeout = self.timeout;
        let (addr, (seq, data)) = data;
        let a = addr.clone();
        Box::pin(
            async move {
                let (s, r) = oneshot::channel();
                let mut pkt = Pkt::payload(seq, data);
                // notify send
                new_send.send((addr.clone(), pkt.pkt_header(), Some(s), Instant::now()))?;
                // we just created pkt, so acks is clear.
                try_get_delayed_acks(&mut pkt, &addr, &delayed_ack);
                trace!(?pkt, "sending");

                // inner.send with the acks
                inner.send((addr.clone(), pkt.clone())).await?;
                pkt.clear_acks();
                transmit(
                    Arc::clone(&inner),
                    (addr, pkt),
                    r,
                    new_send,
                    new_recv,
                    pending_payload_send,
                    delayed_ack,
                    Duration::from_micros(1_000_000) * timeout as _,
                )
                .await?;
                Ok(())
            }
            .instrument(tracing::trace_span!("reliable_send", ?seq, to_addr = ?a)),
        )
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>> {
        let mut inner = Arc::clone(&self.inner);
        let pending_payload_recv = Arc::clone(&self.pending_payload_recv);
        let new_recv = self.new_recv.clone();

        Box::pin(
            async move {
                if let Ok((addr, p)) = pending_payload_recv.lock().expect("lock mutex").try_recv() {
                    trace!(pkt = ?p, addr = ?addr, was_pending = true, "returning packet");
                    return Ok((addr, p.payload.unwrap()));
                }

                let (addr, pkt) = do_recv(&mut inner, &new_recv).await?;
                trace!(pkt = ?&pkt, addr = ?addr, was_pending = false, "returning packet");
                let p = pkt.payload.unwrap();
                Ok((addr, p))
            }
            .instrument(tracing::trace_span!("reliable_recv")),
        )
    }
}

async fn transmit<A, C, D>(
    inner: Arc<C>,
    segment: (A, Pkt<D>),
    mut r: tokio::sync::oneshot::Receiver<()>,
    new_send: mpsc::UnboundedSender<(A, PktHeader, Option<oneshot::Sender<()>>, Instant)>,
    new_recv: mpsc::UnboundedSender<(A, PktHeader)>,
    pending_payload: mpsc::UnboundedSender<(A, Pkt<D>)>,
    delayed_ack: Arc<DashMap<A, PktHeader>>,
    timeout: Duration,
) -> Result<(), eyre::Report>
where
    A: Clone + Eq + Hash + std::fmt::Debug + Send + Sync + 'static,
    C: ChunnelConnection<Data = (A, Pkt<D>)> + Send + Sync + 'static,
    D: Clone + Send + Sync + 'static,
{
    let mut inner1 = Arc::clone(&inner);
    let mut to_ms = timeout.as_millis();
    loop {
        let to = Duration::from_millis(to_ms as u64);
        tokio::select!(
            Ok(_) = &mut r => {
                let (_, pkt) = segment;
                trace!(?pkt, "transmit done");
                return Ok::<_, eyre::Report>(());
            }
            _ = tokio::time::delay_for(to) => {
                to_ms *= to_ms;
                let (addr, mut pkt) = segment.clone();
                // pkt.acks was cleared when passed in to this fn.
                try_get_delayed_acks(&mut pkt, &addr, &delayed_ack);
                new_send.send((addr.clone(), pkt.pkt_header(), None, Instant::now()))?;
                inner.send((addr, pkt)).await?;
            }
            Ok((addr, pkt)) = do_recv(&mut inner1, &new_recv) => {
                pending_payload.send((addr, pkt))?;
            }
        );
    }
}

async fn do_recv<A, C, D>(
    inner: &mut Arc<C>,
    new_recv: &mpsc::UnboundedSender<(A, PktHeader)>,
) -> Result<(A, Pkt<D>), eyre::Report>
where
    A: Clone + Eq + Hash + std::fmt::Debug + Send + Sync + 'static,
    C: ChunnelConnection<Data = (A, Pkt<D>)> + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    loop {
        let (addr, mut pkt) = inner.recv().await?;
        trace!("called inner recv");

        let hdr = pkt.take_pkt_header();
        new_recv.send((addr.clone(), hdr))?;

        if pkt.payload.is_some() {
            return Ok((addr, pkt));
        }
    }
}

// pkt.acks must be clear
fn try_get_delayed_acks<A, D>(
    pkt: &mut Pkt<D>,
    addr: &A,
    delayed_ack: impl AsRef<DashMap<A, PktHeader>>,
) where
    A: Clone + Eq + Hash + std::fmt::Debug + Send + Sync + 'static,
{
    if let Some(mut acks_hdr) = delayed_ack.as_ref().get_mut(addr) {
        std::mem::swap(&mut acks_hdr.deref_mut().acks, &mut pkt.acks);
    }
}

/// Message format for reliability chunnel
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Pkt<D> {
    pub acks: Vec<u32>,
    pub payload: Option<(u32, D)>,
}

impl<D> Default for Pkt<D> {
    fn default() -> Self {
        Pkt {
            acks: Default::default(),
            payload: None,
        }
    }
}

impl<D> Pkt<D> {
    pub fn payload(seq: u32, payload: D) -> Self {
        Pkt {
            acks: Default::default(),
            payload: Some((seq, payload)),
        }
    }

    fn from_header(p: PktHeader) -> Self {
        Self {
            acks: p.acks,
            payload: None,
        }
    }

    pub fn add_ack(&mut self, ack: u32) {
        self.acks.push(ack);
    }

    pub fn take_acks(&mut self) -> Vec<u32> {
        let a = Vec::new();
        std::mem::replace(&mut self.acks, a)
    }

    pub fn clear_acks(&mut self) {
        self.acks.clear();
    }

    pub fn add_payload(&mut self, seq: u32, p: D) -> Option<(u32, D)> {
        let t = self.payload.take();
        self.payload = Some((seq, p));
        t
    }

    fn take_pkt_header(&mut self) -> PktHeader {
        let acks = self.take_acks();
        PktHeader {
            acks,
            seq: self.payload.as_ref().map(|x| x.0),
        }
    }

    fn pkt_header(&self) -> PktHeader {
        PktHeader {
            acks: self.acks.clone(),
            seq: self.payload.as_ref().map(|x| x.0),
        }
    }
}

impl<D> std::fmt::Debug for Pkt<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut p = f.debug_struct("Pkt");
        if let Some((s, _)) = self.payload {
            p.field("seq", &s);
        }
        if !self.acks.is_empty() {
            p.field("acks", &self.acks);
        }
        p.finish()
    }
}

#[derive(Debug, Default)]
struct PktHeader {
    seq: Option<u32>,
    acks: Vec<u32>,
}

#[derive(Debug)]
struct ReliabilityState {
    inflight: HashMap<u32, (oneshot::Sender<()>, Instant, usize)>, // list of inflight seqs
    rtt_est: Duration,
    last_print: Option<Instant>,
    raw_rtts: hdrhistogram::Histogram<u64>,
    extra_acks: usize,
    retx_ctrs: hdrhistogram::Histogram<u64>,
}

impl ReliabilityState {
    fn dump(&self) {
        tracing::info!(
            p5 = self.raw_rtts.value_at_quantile(0.05),
            p25 = self.raw_rtts.value_at_quantile(0.25),
            p50 = self.raw_rtts.value_at_quantile(0.5),
            p75 = self.raw_rtts.value_at_quantile(0.75),
            p95 = self.raw_rtts.value_at_quantile(0.95),
            cnt = self.raw_rtts.len(),
            extra_acks = self.extra_acks,
            "last tx -> ack rtts (us)",
        );
        tracing::info!(
            p5 = self.retx_ctrs.value_at_quantile(0.05),
            p25 = self.retx_ctrs.value_at_quantile(0.25),
            p50 = self.retx_ctrs.value_at_quantile(0.5),
            p75 = self.retx_ctrs.value_at_quantile(0.75),
            p95 = self.retx_ctrs.value_at_quantile(0.95),
            cnt = self.retx_ctrs.len(),
            "retx_ctrs",
        );
    }
}

impl Default for ReliabilityState {
    fn default() -> Self {
        ReliabilityState {
            inflight: HashMap::new(),
            rtt_est: Duration::from_micros(1_000_000),
            last_print: None,
            raw_rtts: hdrhistogram::Histogram::new_with_max(10_000_000, 2).unwrap(),
            extra_acks: 0,
            retx_ctrs: hdrhistogram::Histogram::new_with_max(100, 2).unwrap(),
        }
    }
}

impl Drop for ReliabilityState {
    fn drop(&mut self) {
        self.dump();
    }
}

#[instrument(skip(inner, new_send, new_recv, delayed_ack))]
async fn reliability_state<A: Eq + Hash + Clone + std::fmt::Debug, D, C>(
    inner: Arc<C>,
    mut new_send: mpsc::UnboundedReceiver<(A, PktHeader, Option<oneshot::Sender<()>>, Instant)>,
    mut new_recv: mpsc::UnboundedReceiver<(A, PktHeader)>,
    delayed_ack: Arc<DashMap<A, PktHeader>>,
) where
    C: ChunnelConnection<Data = (A, Pkt<D>)> + Send + Sync + 'static,
{
    let mut state = AHashMap::<A, ReliabilityState>::new();
    let mut delayed_ack_to = tokio::time::interval(Duration::from_millis(10));
    let mut pending_send_futs = futures_util::stream::FuturesUnordered::new();
    //let mut select_time: hdrhistogram::Histogram<u64> =
    //    hdrhistogram::Histogram::new_with_max(100, 2).unwrap();
    //let start = Instant::now();
    //let mut last_print = Instant::now();
    loop {
        //let select_start = Instant::now();
        tokio::select! (
            Some((addr, pkt, s, t)) = new_send.recv() => {
                let st = state.entry(addr.clone()).or_default();
                if let Some(seq) = pkt.seq {
                    if let Some(s) = s {
                        st.inflight.insert(seq, (s, t, 0));
                    } else if let Some((_, time, ctr)) = st.inflight.get_mut(&seq) {
                        trace!(?pkt, ?addr, "retransmitted");
                        *time = t;
                        *ctr += 1;
                    }
                }
            }
            Some((addr, hdr)) = new_recv.recv() => {
                let mut st = state.get_mut(&addr);
                if st.is_none() && !hdr.acks.is_empty() {
                    trace!(?hdr, from = ?&addr, event = "bad ack, unknown addr", "got pkt");
                    continue;
                } else if st.is_none() {
                    // there are no acks so we don't expect there to be state, but we need to init state to
                    // handle delayed acks.
                    state.insert(addr.clone(), Default::default());
                    st = state.get_mut(&addr);
                }

                let st = st.unwrap();
                for seq in hdr.acks {
                    if let Some((s, send_time, ctr)) = st.inflight.remove(&seq) {
                        let elapsed = send_time.elapsed();
                        trace!(seq = ?&seq, from = ?&addr, rtt = ?elapsed, retx_ctr = ?ctr, event = "good ack", "got pkt");
                        st.raw_rtts.saturating_record(elapsed.as_micros() as _);
                        st.retx_ctrs.saturating_record(ctr as _);
                        match st.last_print {
                            None => {
                                st.last_print = Some(Instant::now());
                            }
                            Some(t) if t.elapsed() > Duration::from_secs(10) => {
                                st.dump();
                                st.raw_rtts.clear();
                                st.last_print = Some(Instant::now());
                            }
                            _ => (),
                        }

                        s.send(()).unwrap();
                    } else {
                        trace!(seq = ?&seq, from = ?&addr, event = "bad ack, unknown ack", "got pkt");
                        st.extra_acks += 1;
                    }
                }


                if let Some(seq) = hdr.seq {
                    // queue an ack
                    let mut ent = delayed_ack.get_mut(&addr);
                    if ent.is_none() {
                        delayed_ack.insert(addr.clone(), Default::default());
                        ent = delayed_ack.get_mut(&addr);
                    }

                    let mut ent = ent.unwrap();
                    ent.acks.push(seq);
                    trace!(?seq, from = ?&addr, event = "payload, queued ack", "got pkt");
                    if ent.acks.len() > 1 {
                        let mut send_p: PktHeader = Default::default();
                        std::mem::swap(&mut send_p.acks, &mut ent.acks);
                        pending_send_futs.push(inner.send((addr.clone(), Pkt::from_header(send_p))));
                    }
                }
            }
            Some(res) = pending_send_futs.next() => {
                if let Err(err) = res {
                    debug!(?err, "inner send failed");
                }
            }
            _ = delayed_ack_to.tick() => {
                // Ticks every interval, and sends out any unsent delayed acks.
                // This is sucky, but even Linux doesn't have a better way...
                for mut ent in delayed_ack.iter_mut().filter(|e| !e.value().acks.is_empty()) {
                    let (addr, h) = ent.pair_mut();
                    let mut send_p: PktHeader = Default::default();
                    std::mem::swap(&mut send_p.acks, &mut h.acks);
                    trace!(?addr, ?send_p, "sent delayed acks");
                    pending_send_futs.push(inner.send((addr.clone(), Pkt::from_header(send_p))));
                }
            }
        );

        //select_time.saturating_record(select_start.elapsed().as_micros() as _);
        //if last_print.elapsed() > Duration::from_secs(3) {
        //    tracing::info!(
        //        p5 = select_time.value_at_quantile(0.05),
        //        p25 = select_time.value_at_quantile(0.25),
        //        p50 = select_time.value_at_quantile(0.5),
        //        p75 = select_time.value_at_quantile(0.75),
        //        p95 = select_time.value_at_quantile(0.95),
        //        cnt = select_time.len(),
        //        elapsed = ?start.elapsed(),
        //        "select_time",
        //    );
        //    select_time.clear();
        //    last_print = Instant::now();
        //}
    }
}

#[cfg(test)]
mod test {
    use super::ReliabilityChunnel;
    use crate::chan_transport::Chan;
    use crate::{
        bincode::SerializeChunnel, util::ProjectLeft, ChunnelConnection, ChunnelConnector,
        ChunnelListener, Client, CxList, Serve,
    };
    use futures_util::StreamExt;
    use tracing::{debug, info};
    use tracing_error::ErrorLayer;
    use tracing_futures::Instrument;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    async fn do_transmit(
        snd_ch: impl ChunnelConnection<Data = (u32, Vec<u8>)> + Send + Sync + 'static,
        rcv_ch: impl ChunnelConnection<Data = (u32, Vec<u8>)> + Send + Sync + 'static,
        msgs: Vec<(u32, Vec<u8>)>,
    ) {
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
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        color_eyre::install().unwrap_or_else(|_| ());
        let msgs = (0..7).map(|i| (i, vec![i as u8; 10])).collect();

        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_time()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                let (mut srv, mut cln) = Chan::default().split();
                let mut l = CxList::from(ReliabilityChunnel::default())
                    .wrap(SerializeChunnel::default())
                    .wrap(ProjectLeft::from(()));

                let rcv_st = srv.listen(()).await.unwrap();
                let mut rcv_st = l.serve(rcv_st).await.unwrap();
                let rcv = rcv_st.next().await.unwrap().unwrap();

                let cln = cln.connect(()).await.unwrap();
                let snd = l.connect_wrap(cln).await.unwrap();

                do_transmit(snd, rcv, msgs).await;
            }
            .instrument(tracing::info_span!("no_drops")),
        );
    }

    #[test]
    fn drop_2() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        color_eyre::install().unwrap_or_else(|_| ());
        let msgs = (0..7).map(|i| (i, vec![i as u8; 10])).collect();

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

                let (mut srv, mut cln) = t.split();
                let mut l = CxList::from(ReliabilityChunnel::default())
                    .wrap(SerializeChunnel::default())
                    .wrap(ProjectLeft::from(()));

                let rcv_st = srv.listen(()).await.unwrap();
                let mut rcv_st = l.serve(rcv_st).await.unwrap();
                let rcv = rcv_st.next().await.unwrap().unwrap();

                let cln = cln.connect(()).await.unwrap();
                let snd = l.connect_wrap(cln).await.unwrap();

                do_transmit(snd, rcv, msgs).await;
            }
            .instrument(tracing::info_span!("drop_2")),
        );
    }

    async fn do_transmit_tagged(
        snd_ch: impl ChunnelConnection<Data = Vec<u8>> + Send + Sync + 'static,
        rcv_ch: impl ChunnelConnection<Data = Vec<u8>> + Send + Sync + 'static,
        msgs: Vec<Vec<u8>>,
    ) {
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
    fn drop_2_tagged() {
        use crate::{tagger::TaggerChunnel, CxList};

        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        color_eyre::install().unwrap_or_else(|_| ());
        let msgs = vec![vec![0u8; 10], vec![1u8; 10], vec![2u8; 10]];

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

                let (mut srv, mut cln) = t.split();
                let mut stack = CxList::from(TaggerChunnel)
                    .wrap(ReliabilityChunnel::default())
                    .wrap(SerializeChunnel::default())
                    .wrap(ProjectLeft::from(()));

                let rcv_st = srv.listen(()).await.unwrap();
                let mut rcv_st = stack.serve(rcv_st).await.unwrap();
                let rcv = rcv_st.next().await.unwrap().unwrap();

                let cln = cln.connect(()).await.unwrap();
                let snd = stack.connect_wrap(cln).await.unwrap();

                do_transmit_tagged(snd, rcv, msgs).await;
            }
            .instrument(tracing::info_span!("drop_2_tagged")),
        );
    }
}
