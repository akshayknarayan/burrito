//! Chunnel implementing reliability.
//!
//! Takes as Data a `(u32, Vec<u8>)`, where the `u32` is a unique tag corresponding to a data
//! segment, the `Vec<u8>`.

use crate::{
    util::{ProjectLeft, ProjectLeftCn, Unproject},
    ChunnelConnection, Client, Negotiate, Serve,
};
use color_eyre::eyre;
use dashmap::DashMap;
use futures_util::future::{ready, Ready};
use futures_util::stream::Stream;
use futures_util::stream::TryStreamExt;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use tracing::{debug, trace};
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
    InC: ChunnelConnection<Data = (u32, Option<D>)> + Send + Sync + 'static,
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
    InC: ChunnelConnection<Data = (u32, Option<D>)> + Send + Sync + 'static,
    D: Clone + Send + Sync + 'static,
{
    type Future = <ProjectLeft<()> as Client<
        <ReliabilityProjChunnel<(), D> as Client<Unproject<InC>>>::Connection,
    >>::Future;
    type Connection = ProjectLeftCn<(), ReliabilityProj<(), D, Unproject<InC>>>;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        match self.inner.connect_wrap(Unproject(cn)).into_inner() {
            Ok(cn) => ProjectLeft::from(()).connect_wrap(cn),
            Err(e) => ready(Err(e)),
        }
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
    InC: ChunnelConnection<Data = (A, (u32, Option<D>))> + Send + Sync + 'static,
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
        ready(Ok(Box::pin(inner.map_ok(move |cn| {
            let mut r = ReliabilityProj::from(cn);
            r.set_timeout_factor(cfg);
            r
        })) as _))
    }
}

impl<A, InC, D> Client<InC> for ReliabilityProjChunnel<A, D>
where
    A: Clone + Eq + Hash + std::fmt::Debug + Send + Sync + 'static,
    InC: ChunnelConnection<Data = (A, (u32, Option<D>))> + Send + Sync + 'static,
    D: Clone + Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = ReliabilityProj<A, D, InC>;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        ready(Ok(ReliabilityProj::from(cn)))
    }
}

#[derive(Debug)]
pub struct ReliabilityProj<A: Eq + Hash, D, C> {
    timeout: usize,
    inner: Arc<C>,
    state: Arc<DashMap<A, ReliabilityState<(A, D)>>>,
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

#[derive(Debug)]
struct ReliabilityState<D> {
    inflight: HashMap<
        u32,
        (
            D,
            Option<oneshot::Sender<Result<(), eyre::Report>>>,
            Instant,
        ),
    >, // list of inflight seqs
    pending_payload: VecDeque<(u32, D)>, // payloads we have received that are waiting for a recv() call
    rtt_est: Duration,
    last_print: Option<Instant>,
    raw_rtts: hdrhistogram::Histogram<u64>,
    extra_acks: usize,
    retx_ctrs: hdrhistogram::Histogram<u64>,
}

impl<D> ReliabilityState<D> {
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

impl<D> Default for ReliabilityState<D> {
    fn default() -> Self {
        ReliabilityState {
            inflight: HashMap::new(),
            pending_payload: VecDeque::new(),
            rtt_est: Duration::from_micros(1_000_000),
            last_print: None,
            raw_rtts: hdrhistogram::Histogram::new_with_max(10_000_000, 2).unwrap(),
            extra_acks: 0,
            retx_ctrs: hdrhistogram::Histogram::new_with_max(100, 2).unwrap(),
        }
    }
}

impl<D> Drop for ReliabilityState<D> {
    fn drop(&mut self) {
        self.dump();
    }
}

impl<A, C, D> ChunnelConnection for ReliabilityProj<A, D, C>
where
    A: Clone + Eq + Hash + std::fmt::Debug + Send + Sync + 'static,
    C: ChunnelConnection<Data = (A, (u32, Option<D>))> + Send + Sync + 'static,
    D: Clone + Send + Sync + 'static,
{
    type Data = (A, (u32, D)); // a tag and its data.

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>> {
        let (addr, data) = data;
        self.state.entry(addr.clone()).or_default();
        let seq = data.0;
        let a = addr.clone();

        let state = Arc::clone(&self.state);
        let inner = Arc::clone(&self.inner);
        let timeout = self.timeout;
        Box::pin(
            async move {
                let (s, r) = oneshot::channel();
                let data = (data.0, Some(data.1));

                let to = {
                    let mut st = state.get_mut(&addr).unwrap();
                    st.inflight.insert(
                        seq,
                        (
                            (addr.clone(), data.1.clone().unwrap()),
                            Some(s),
                            Instant::now(),
                        ),
                    );
                    trace!(inflight = st.inflight.len(), "sending");
                    st.rtt_est * (timeout as _)
                };

                inner.send((addr.clone(), data.clone())).await?;
                transmit(Arc::clone(&inner), state, (addr, data), r, to).await?;
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
                    if let Some((seq, (addr, d))) = st.pending_payload.pop_front() {
                        trace!(seq = ?seq, addr = ?addr, was_pending = true, "returning packet");
                        return Ok((addr, (seq, d)));
                    }
                }

                let (addr, (seq, data)): (A, (u32, D)) =
                    do_recv::<A, _, _>(&mut inner, &mut state).await?;
                trace!(seq = ?seq, addr = ?addr, was_pending = false, "returning packet");
                Ok((addr, (seq, data)))
            }
            .instrument(tracing::trace_span!("reliable_recv")),
        )
    }
}

async fn transmit<A, C, D>(
    inner: Arc<C>,
    state: Arc<DashMap<A, ReliabilityState<(A, D)>>>,
    segment: (A, (u32, Option<D>)),
    mut r: tokio::sync::oneshot::Receiver<Result<(), eyre::Report>>,
    timeout: Duration,
) -> Result<(), eyre::Report>
where
    A: Clone + Eq + Hash + std::fmt::Debug + Send + Sync + 'static,
    C: ChunnelConnection<Data = (A, (u32, Option<D>))> + Send + Sync + 'static,
    D: Clone + Send + Sync + 'static,
{
    let inner1 = Arc::clone(&inner);
    let state1 = Arc::clone(&state);
    let jitter = {
        use rand::distributions::{Distribution, Uniform};
        let mut rng = rand::thread_rng();
        let dist = Uniform::from(25..50);
        let jitter: u64 = dist.sample(&mut rng);
        Duration::from_micros(jitter)
    };
    let timeout = timeout + jitter;
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
                            let (addr, (seq, _)) = segment;
                            trace!(?seq, ?retx_ctr, "transmit done");
                            let mut st = state.get_mut(&addr).expect("conn not found");
                            st.retx_ctrs.saturating_record(retx_ctr);
                            return Ok::<_, eyre::Report>(());
                        }
                        _ = tokio::time::delay_for(to) => {
                            let (ref addr, (seq, _)) = segment;
                            retx_ctr += 1;
                            to_ms *= to_ms;
                            trace!(addr = ?addr, seq = ?seq, cnt = ?retx_ctr, ?to, "retransmitting");
                            inner.send(segment.clone()).await?;
                            debug!(addr = ?addr, seq = ?seq, cnt = ?retx_ctr, ?to, "retransmitted");

                            // send_time is the time since the last transmission
                            let mut st = state.get_mut(addr).expect("conn not found");
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
                    let (addr, (seq, data)) = do_recv(&mut inner, &mut state).await?;
                    let mut st = state.entry(addr.clone()).or_default();
                    st.pending_payload.push_back((seq, (addr, data)));
                }
            }
            .instrument(tracing::debug_span!("send_select_recv")),
        ),
    )
    .await
    .factor_first()
    .0
}

async fn do_recv<A, C, D>(
    inner: &mut Arc<C>,
    state: &mut Arc<DashMap<A, ReliabilityState<(A, D)>>>,
) -> Result<(A, (u32, D)), eyre::Report>
where
    A: Clone + Eq + Hash + std::fmt::Debug + Send + Sync + 'static,
    C: ChunnelConnection<Data = (A, (u32, Option<D>))> + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    loop {
        let (addr, (seq, data)) = inner.recv().await?;
        trace!("called inner recv");

        if data.is_none() {
            // it was an ack
            let st = state.get_mut(&addr);
            if st.is_none() {
                tracing::warn!(seq = ?seq, from = ?&addr, event = "bad ack, unknown addr", "got pkt");
                continue;
            }

            let mut st = st.unwrap();
            if let Some((_, Some(s), send_time)) = st.inflight.remove(&seq) {
                let elapsed = send_time.elapsed();
                trace!(seq = ?seq, from = ?&addr, rtt = ?elapsed, event = "good ack", "got pkt");
                st.raw_rtts.saturating_record(elapsed.as_micros() as _);
                match st.last_print {
                    None => {
                        st.last_print = Some(Instant::now());
                    }
                    Some(t) if t.elapsed() > Duration::from_secs(1) => {
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
                trace!(seq = ?seq, from = ?&addr, event = "bad ack, unknown ack", "got pkt");
                st.extra_acks += 1;
            }

            continue;
        } else {
            // send an ack
            inner.send((addr.clone(), (seq, None))).await?;
            trace!(seq = ?seq, from = ?&addr, event = "payload, sent ack", "got pkt");
        }

        return Ok((addr, (seq, data.unwrap())));
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
        let msgs = vec![(0, vec![0u8; 10]), (1, vec![1u8; 10]), (2, vec![2u8; 10])];

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
            .instrument(tracing::info_span!("drop_2")),
        );
    }
}
