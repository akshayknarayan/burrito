//! Chunnel implementing reliability.
//!
//! Takes as Data a `(u32, Vec<u8>)`, where the `u32` is a unique tag corresponding to a data
//! segment, the `Vec<u8>`.

use crate::{
    util::{ProjectLeft, Unproject},
    Chunnel, ChunnelConnection, Negotiate,
};
use color_eyre::eyre;
use dashmap::DashMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use tracing::{debug, instrument, trace};
use tracing_futures::Instrument;

#[derive(Clone, Debug)]
pub struct ReliabilityProjChunnel {
    timeout: usize,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub enum ReliabilityNeg {
    Reliability,
    Ordering,
}

impl crate::negotiate::CapabilitySet for ReliabilityNeg {
    fn guid() -> u64 {
        0x9209a313d34f3ce4
    }

    // return None to force both sides to match
    fn universe() -> Option<Vec<Self>> {
        None
    }
}

impl Negotiate for ReliabilityProjChunnel {
    type Capability = ReliabilityNeg;

    fn guid() -> u64 {
        0xcdf794776e9c6ca1
    }

    fn capabilities() -> Vec<Self::Capability> {
        vec![ReliabilityNeg::Reliability]
    }
}

impl Default for ReliabilityProjChunnel {
    fn default() -> Self {
        ReliabilityProjChunnel { timeout: 5 }
    }
}

impl ReliabilityProjChunnel {
    pub fn set_timeout_factor(&mut self, to: usize) -> &mut Self {
        self.timeout = to;
        self
    }
}

impl<A, InC, D> Chunnel<InC> for ReliabilityProjChunnel
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
        Box::pin(async move {
            let r = ReliabilityProj::from(cn);
            // spawn the delayed ack thingy
            tokio::spawn(nagler(Arc::clone(&r.inner), Arc::clone(&r.state)));
            Ok(r)
        })
    }
}

#[derive(Clone, Debug)]
pub struct ReliabilityChunnel {
    inner: ReliabilityProjChunnel,
}

impl Default for ReliabilityChunnel {
    fn default() -> Self {
        Self {
            inner: Default::default(),
        }
    }
}

impl Negotiate for ReliabilityChunnel {
    type Capability = ReliabilityNeg;

    fn guid() -> u64 {
        0xcdf794776e9c6ca1
    }

    fn capabilities() -> Vec<Self::Capability> {
        vec![ReliabilityNeg::Reliability]
    }
}

impl ReliabilityChunnel {
    pub fn set_timeout_factor(&mut self, to: usize) -> &mut Self {
        self.inner.timeout = to;
        self
    }
}

impl<InC, D> Chunnel<InC> for ReliabilityChunnel
where
    InC: ChunnelConnection<Data = Pkt<D>> + Send + Sync + 'static,
    D: Clone + Send + Sync + 'static,
{
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Connection = ProjectLeft<(), ReliabilityProj<(), D, Unproject<InC>>>;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        let fut = self.inner.connect_wrap(Unproject(cn));
        Box::pin(async move { Ok(ProjectLeft::new((), fut.await?)) })
    }
}

#[derive(Debug)]
pub struct ReliabilityProj<A: Eq + Hash, D, C> {
    timeout: usize,
    inner: Arc<C>,
    state: Arc<DashMap<u64, ReliabilityState<A, D>>>,
}

impl<A: Eq + Hash, C, D> ReliabilityProj<A, D, C> {
    pub fn set_timeout_factor(&mut self, to: usize) -> &mut Self {
        self.timeout = to;
        self
    }
}

impl<Cx, A: Eq + Hash, D> From<Cx> for ReliabilityProj<A, D, Cx> {
    fn from(cx: Cx) -> ReliabilityProj<A, D, Cx> {
        ReliabilityProj {
            inner: Arc::new(cx),
            timeout: 5,
            state: Default::default(),
        }
    }
}

impl<A: Eq + Hash, C, D> Clone for ReliabilityProj<A, D, C> {
    fn clone(&self) -> Self {
        ReliabilityProj {
            timeout: self.timeout,
            inner: Arc::clone(&self.inner),
            state: Arc::clone(&self.state),
        }
    }
}

/// Message format for reliability chunnel
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Pkt<D> {
    pub flow_id: u64,
    pub acks: Vec<u32>,
    pub payload: Option<(u32, D)>,
}

impl<D> Default for Pkt<D> {
    fn default() -> Self {
        Pkt {
            flow_id: 0,
            acks: Default::default(),
            payload: None,
        }
    }
}

impl<D> Pkt<D> {
    pub fn with_flow_id(flow_id: u64) -> Self {
        Self {
            flow_id,
            ..Default::default()
        }
    }

    pub fn payload(seq: u32, payload: D) -> Self {
        Pkt {
            payload: Some((seq, payload)),
            ..Default::default()
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
}

impl<D> std::fmt::Debug for Pkt<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut p = f.debug_struct("Pkt");
        p.field("flow", &self.flow_id);
        if let Some((s, _)) = self.payload {
            p.field("seq", &s);
        }
        if !self.acks.is_empty() {
            p.field("acks", &self.acks);
        }
        p.finish()
    }
}

#[derive(Debug)]
struct ReliabilityState<A, D> {
    addr: Option<A>,
    inflight: HashMap<
        u32,
        (
            D,
            Option<oneshot::Sender<Result<(), eyre::Report>>>,
            Instant,
        ),
    >, // inflight seq num -> (payload, completion notifier, sent time)
    pending_payload: VecDeque<(A, Pkt<D>)>, // payloads we have received that are waiting for a recv() call
    pending_acks: Pkt<D>,
    rtt_est: Duration,
    last_print: Option<Instant>,
    raw_rtts: hdrhistogram::Histogram<u64>,
    extra_acks: usize,
    retx_ctrs: hdrhistogram::Histogram<u64>,
}

impl<A, D> ReliabilityState<A, D> {
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

impl<A, D> Default for ReliabilityState<A, D> {
    fn default() -> Self {
        ReliabilityState {
            addr: None,
            inflight: HashMap::new(),
            pending_payload: VecDeque::new(),
            pending_acks: Default::default(),
            rtt_est: Duration::from_micros(1_000_000),
            last_print: None,
            raw_rtts: hdrhistogram::Histogram::new_with_max(10_000_000, 2).unwrap(),
            extra_acks: 0,
            retx_ctrs: hdrhistogram::Histogram::new_with_max(100, 2).unwrap(),
        }
    }
}

impl<A, D> Drop for ReliabilityState<A, D> {
    fn drop(&mut self) {
        self.dump();
    }
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

// make sure we set pending_acks.flow_id, because it'll get mem::replaced out and sent if transmit gets
// called.
fn init_entry_in_state<A, D>(state: &Arc<DashMap<u64, ReliabilityState<A, D>>>, flow_id: u64) {
    state.entry(flow_id).or_default().pending_acks.flow_id = flow_id;
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
        let (addr, data) = data;
        let flow_id = calculate_hash(&addr);
        init_entry_in_state(&self.state, flow_id);
        let seq = data.0;
        let a = addr.clone();

        let state = Arc::clone(&self.state);
        let inner = Arc::clone(&self.inner);
        let timeout = self.timeout;
        Box::pin(
            async move {
                let (s, r) = oneshot::channel();
                let (mut pkt, to) = {
                    // unwrap ok because of .or_default() above
                    let mut st = state.get_mut(&flow_id).unwrap();
                    st.addr = Some(addr.clone());
                    st.inflight
                        .insert(seq, (data.1.clone(), Some(s), Instant::now()));
                    let new_p: Pkt<D> = Pkt::with_flow_id(flow_id);
                    let p = std::mem::replace(&mut st.pending_acks, new_p);
                    trace!(inflight = st.inflight.len(), "sending");
                    (p, st.rtt_est * (timeout as _))
                };

                pkt.add_payload(data.0, data.1);
                inner.send((addr.clone(), pkt.clone())).await?;
                pkt.clear_acks();
                transmit(Arc::clone(&inner), state, (addr, pkt), r, to).await?;
                Ok(())
            }
            .instrument(tracing::trace_span!("reliable_send", seq = ?seq, to_addr = ?a)),
        )
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>> {
        let mut state = Arc::clone(&self.state);
        let mut inner = Arc::clone(&self.inner);

        Box::pin(
            async move {
                for mut e in state.iter_mut() {
                    let st = e.value_mut();
                    if let Some((addr, p)) = st.pending_payload.pop_front() {
                        trace!(pkt = ?p, addr = ?addr, was_pending = true, "returning packet");
                        return Ok((addr, p.payload.unwrap()));
                    }
                }

                let (addr, pkt) = do_recv::<A, _, _>(&mut inner, &mut state).await?;
                trace!(pkt = ?&pkt, addr = ?addr, was_pending = false, "returning packet");
                let p = pkt.payload.unwrap();
                Ok((addr, p))
            }
            .instrument(tracing::trace_span!("reliable_recv")),
        )
    }
}

/// Makes sure `segment` is acked, and retransmits it at the right times otherwise.
///
/// `inner`: Inner connection
/// `state`: flow_id -> state map
/// `segment`: the packet: (dst_addr, packet_with_payload).
/// `r`: Done-ness notification
/// `timeout`.
async fn transmit<A, C, D>(
    inner: Arc<C>,
    state: Arc<DashMap<u64, ReliabilityState<A, D>>>,
    segment: (A, Pkt<D>),
    mut r: tokio::sync::oneshot::Receiver<Result<(), eyre::Report>>,
    timeout: Duration,
) -> Result<(), eyre::Report>
where
    A: Clone + Eq + Hash + std::fmt::Debug + Send + Sync + 'static,
    C: ChunnelConnection<Data = (A, Pkt<D>)> + Send + Sync + 'static,
    D: Clone + Send + Sync + 'static,
{
    let inner1 = Arc::clone(&inner);
    let state1 = Arc::clone(&state);
    futures_util::future::select(
        Box::pin(
            async move {
                let mut retx_ctr = 0;
                let mut to_ms = timeout.as_millis();
                loop {
                    let to = Duration::from_millis(to_ms as u64);
                    tokio::select!(
                        done = &mut r => {
                            done??;
                            let (_, pkt) = segment;
                            trace!(?pkt, ?retx_ctr, "transmit done");
                            let mut st = state.get_mut(&pkt.flow_id).expect("conn not found");
                            st.retx_ctrs.saturating_record(retx_ctr);
                            return Ok::<_, eyre::Report>(());
                        }
                        _ = tokio::time::sleep(to) => {
                            let (ref addr, ref pkt) = segment;
                            retx_ctr += 1;
                            to_ms *= to_ms;
                            trace!(?addr, ?pkt, ?retx_ctr, ?to, "retransmitting");
                            inner.send(segment.clone()).await?;
                            trace!(?addr, ?pkt, ?retx_ctr, ?to, "retransmitted");

                            // send_time is the time since the last transmission
                            let mut st = state.get_mut(&pkt.flow_id).expect("conn not found");
                            let seq = pkt.payload.as_ref().unwrap().0;
                            let (_, _, ref mut send_time) = st.inflight.get_mut(&seq).expect("pkt state not found");
                            *send_time = Instant::now();
                        }
                    );
                }
            }
            .instrument(tracing::debug_span!("send_retx_loop")),
        ),
        Box::pin(
            async move {
                let mut inner = inner1;
                let mut state = state1;
                loop {
                    let (addr, pkt) = do_recv(&mut inner, &mut state).await?;
                    init_entry_in_state(&state, pkt.flow_id);
                    // init_entry_in_state guarantees the entry is present
                    let mut st = state.get_mut(&pkt.flow_id).unwrap();
                    if st.addr.is_none() {
                        st.addr = Some(addr.clone());
                    }

                    st.pending_payload.push_back((addr, pkt));
                }
            }
            .instrument(tracing::debug_span!("send_select_recv")),
        ),
    )
    .await
    .factor_first()
    .0
}

/// Receive a packet and ack it. If there was a payload, return it.
///
/// `inner`: Inner connection
/// `state`: flow_id -> state map
/// returns -> (src_addr, packet_with_payload)
async fn do_recv<A, C, D>(
    inner: &mut Arc<C>,
    state: &mut Arc<DashMap<u64, ReliabilityState<A, D>>>,
) -> Result<(A, Pkt<D>), eyre::Report>
where
    A: Clone + Eq + Hash + std::fmt::Debug + Send + Sync + 'static,
    C: ChunnelConnection<Data = (A, Pkt<D>)> + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    loop {
        let (addr, mut pkt) = inner.recv().await?;
        let flow_id = pkt.flow_id;
        trace!("called inner recv");
        if !pkt.acks.is_empty() && !state.contains_key(&flow_id) {
            trace!(pkt = ?&pkt, from = ?&addr, event = "bad ack, unknown addr", "got pkt");
            continue;
        }

        // there are no acks so we don't expect there to be state, but we need to init state to
        // handle delayed acks.
        init_entry_in_state(state, flow_id);
        // init_entry_in_state guarantees the entry is present
        let mut st = state.get_mut(&pkt.flow_id).unwrap();
        if st.addr.is_none() {
            st.addr = Some(addr.clone());
        }

        let acks = pkt.take_acks();
        for seq in acks {
            if let Some((_, Some(s), send_time)) = st.inflight.remove(&seq) {
                let elapsed = send_time.elapsed();
                trace!(seq = ?&seq, from = ?&addr, rtt = ?elapsed, event = "good ack", "got pkt");
                st.raw_rtts.saturating_record(elapsed.as_micros() as _);
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

                //let rtt = send_time.elapsed();
                //st.rtt_est = Duration::from_secs_f64(
                //    st.rtt_est.as_secs_f64() * 0.9 + rtt.as_secs_f64() * 0.1,
                //);
                s.send(Ok(())).unwrap();
            } else {
                trace!(seq = ?&seq, from = ?&addr, event = "bad ack, unknown ack", "got pkt");
                st.extra_acks += 1;
            }
        }

        if let Some((seq, _)) = pkt.payload {
            // queue an ack
            st.pending_acks.acks.push(seq);
            trace!(pkt = ?&pkt, from = ?&addr, event = "payload, queued ack", "got pkt");
            if st.pending_acks.acks.len() > 1 {
                let mut send_p = Default::default();
                std::mem::swap(&mut send_p, &mut st.pending_acks);
                inner.send((addr.clone(), send_p)).await?;
            }

            return Ok((addr, pkt));
        }
    }
}

/// Ticks every 10 ms, and sends out any unsent delayed acks.
/// This is sucky, but even Linux doesn't have a better way...
#[instrument(skip(inner, state))]
async fn nagler<A: Eq + Hash + Clone + std::fmt::Debug, D, C>(
    inner: Arc<C>,
    state: Arc<DashMap<u64, ReliabilityState<A, D>>>,
) where
    C: ChunnelConnection<Data = (A, Pkt<D>)> + Send + Sync + 'static,
{
    loop {
        tokio::time::sleep(Duration::from_millis(10)).await;
        for mut entry in state.iter_mut() {
            let (flow_id, st) = entry.pair_mut();
            if !st.pending_acks.acks.is_empty() {
                let addr = st.addr.clone().unwrap();
                let mut send_p = Pkt::with_flow_id(*flow_id);
                std::mem::swap(&mut send_p, &mut st.pending_acks);
                trace!(?addr, ?send_p, "sent delayed acks");
                if let Err(e) = inner.send((addr.clone(), send_p)).await {
                    debug!(err = ?e, ?addr, "failed sending delayed acks");
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::ReliabilityChunnel;
    use crate::chan_transport::Chan;
    use crate::test::Serve;
    use crate::{
        bincode::SerializeChunnel, Chunnel, ChunnelConnection, ChunnelConnector, ChunnelListener,
        CxList,
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
        color_eyre::install().unwrap_or(());
        let msgs = (0..7).map(|i| (i, vec![i as u8; 10])).collect();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                let (mut srv, mut cln) = Chan::default().split();
                let mut l =
                    CxList::from(ReliabilityChunnel::default()).wrap(SerializeChunnel::default());

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
        color_eyre::install().unwrap_or(());
        let msgs = vec![(0, vec![0u8; 10]), (1, vec![1u8; 10]), (2, vec![2u8; 10])];

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
                let mut l =
                    CxList::from(ReliabilityChunnel::default()).wrap(SerializeChunnel::default());

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

    #[test]
    fn at_least_once() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        color_eyre::install().unwrap_or(());
        let msgs = vec![(0, vec![0u8; 10]), (1, vec![1u8; 10]), (2, vec![2u8; 10])];

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        use std::sync::{Arc, Mutex};

        rt.block_on(
            async move {
                let mut t = Chan::default();
                // duplicate each packet once
                let saved: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
                t.link_conditions(move |x: Option<Vec<u8>>| match x {
                    Some(p) => {
                        *saved.lock().unwrap() = Some(p.clone());
                        Some(p)
                    }
                    None => saved.lock().unwrap().take(),
                });

                let (mut srv, mut cln) = t.split();
                let mut l =
                    CxList::from(ReliabilityChunnel::default()).wrap(SerializeChunnel::default());

                let rcv_st = srv.listen(()).await.unwrap();
                let mut rcv_st = l.serve(rcv_st).await.unwrap();
                let rcv = rcv_st.next().await.unwrap().unwrap();

                let cln = cln.connect(()).await.unwrap();
                let snd = l.connect_wrap(cln).await.unwrap();

                do_transmit(snd, rcv, msgs).await;
            }
            .instrument(tracing::info_span!("at-least-once")),
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
        color_eyre::install().unwrap_or(());
        let msgs = vec![vec![0u8; 10], vec![1u8; 10], vec![2u8; 10]];

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
                let mut stack = CxList::from(TaggerChunnel)
                    .wrap(ReliabilityChunnel::default())
                    .wrap(SerializeChunnel::default());

                let rcv_st = srv.listen(()).await.unwrap();
                let mut rcv_st = stack.serve(rcv_st).await.unwrap();
                let rcv = rcv_st.next().await.unwrap().unwrap();

                let cln = cln.connect(()).await.unwrap();
                let snd = stack.connect_wrap(cln).await.unwrap();

                do_transmit_tagged(snd, rcv, msgs).await;
            }
            .instrument(tracing::info_span!("drop_2")),
        );
    }
}
