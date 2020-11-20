//! Chunnel which tags Data to provide at-most-once delivery.

use crate::{
    util::{ProjectLeft, ProjectLeftCn, Unproject},
    ChunnelConnection, Client, Negotiate, Serve,
};
use color_eyre::eyre;
use dashmap::DashMap;
use futures_util::future::{ready, Ready};
use futures_util::stream::{Stream, TryStreamExt};
use std::collections::BinaryHeap;
use std::convert::TryInto;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::{atomic::AtomicUsize, Arc};
use tracing::trace;
use tracing_futures::Instrument;

#[derive(Clone, Debug, Default)]
pub struct TaggerChunnel;

impl Negotiate for TaggerChunnel {
    type Capability = ();
    fn capabilities() -> Vec<Self::Capability> {
        vec![]
    }
}

impl<D, InS, InC, InE> Serve<InS> for TaggerChunnel
where
    InS: Stream<Item = Result<InC, InE>> + Send + 'static,
    InC: ChunnelConnection<Data = (u32, D)> + Send + Sync + 'static,
    InE: Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Stream, Self::Error>>;
    type Connection = ProjectLeftCn<(), TaggerProj<Unproject<InC>>>;
    type Error = InE;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn serve(&mut self, inner: InS) -> Self::Future {
        let st = inner.map_ok(Unproject);
        match TaggerProjChunnel.serve(st).into_inner() {
            Ok(st) => ProjectLeft::from(()).serve(st),
            Err(e) => ready(Err(e)),
        }
    }
}

impl<D, InC> Client<InC> for TaggerChunnel
where
    InC: ChunnelConnection<Data = (u32, D)> + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Future = <ProjectLeft<()> as Client<
        <TaggerProjChunnel as Client<Unproject<InC>>>::Connection,
    >>::Future;
    type Connection = ProjectLeftCn<(), TaggerProj<Unproject<InC>>>;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        match TaggerProjChunnel.connect_wrap(Unproject(cn)).into_inner() {
            Ok(cn) => ProjectLeft::from(()).connect_wrap(cn),
            Err(e) => ready(Err(e)),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct TaggerProjChunnel;

impl Negotiate for TaggerProjChunnel {
    type Capability = ();
    fn capabilities() -> Vec<Self::Capability> {
        vec![]
    }
}

impl<A, D, InS, InC, InE> Serve<InS> for TaggerProjChunnel
where
    InS: Stream<Item = Result<InC, InE>> + Send + 'static,
    InC: ChunnelConnection<Data = (A, (u32, D))> + Send + Sync + 'static,
    InE: Send + Sync + 'static,
    A: Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Stream, Self::Error>>;
    type Connection = TaggerProj<InC>;
    type Error = InE;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn serve(&mut self, inner: InS) -> Self::Future {
        ready(Ok(Box::pin(inner.map_ok(TaggerProj::from)) as _))
    }
}

impl<A, D, InC> Client<InC> for TaggerProjChunnel
where
    InC: ChunnelConnection<Data = (A, (u32, D))> + Send + Sync + 'static,
    A: Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = TaggerProj<InC>;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        ready(Ok(TaggerProj::from(cn)))
    }
}

/// Assigns an sequential tag to data segments and ignores the tag otherwise.
#[derive(Default, Debug)]
pub struct TaggerProj<C> {
    inner: Arc<C>,
    snd_nxt: Arc<AtomicUsize>,
}

impl<Cx> From<Cx> for TaggerProj<Cx> {
    fn from(cx: Cx) -> TaggerProj<Cx> {
        TaggerProj {
            inner: Arc::new(cx),
            snd_nxt: Default::default(),
        }
    }
}

impl<C> Clone for TaggerProj<C>
where
    C: Clone,
{
    fn clone(&self) -> Self {
        let inner: C = self.inner.as_ref().clone();
        Self {
            inner: Arc::new(inner),
            snd_nxt: self.snd_nxt.clone(),
        }
    }
}

impl<A, C, D> ChunnelConnection for TaggerProj<C>
where
    C: ChunnelConnection<Data = (A, (u32, D))> + Send + Sync + 'static,
    A: Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Data = (A, D);

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>> {
        let inner = Arc::clone(&self.inner);
        let snd_nxt = Arc::clone(&self.snd_nxt);
        Box::pin(async move {
            let seq = snd_nxt.fetch_add(1, std::sync::atomic::Ordering::SeqCst) as u32;
            let (addr, data) = data;
            inner.send((addr, (seq, data))).await?;
            Ok(())
        })
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            let (addr, (_, d)) = inner.recv().await?;
            return Ok((addr, d));
        })
    }
}

/// `OrderedChunnel` takes in Data segments and tags them for use with `(u32, D)` Chunnels.
///
/// It returns data segments in the order they were sent.
#[derive(Clone, Debug, Default)]
pub struct OrderedChunnel {
    inner: OrderedChunnelProj,
}

impl OrderedChunnel {
    pub fn ordering_threshold(&mut self, thresh: usize) -> &mut Self {
        self.inner.hole_thresh = Some(thresh);
        self
    }
}

impl Negotiate for OrderedChunnel {
    type Capability = ();
    fn capabilities() -> Vec<Self::Capability> {
        vec![]
    }
}

impl<D, InS, InC, InE> Serve<InS> for OrderedChunnel
where
    InS: Stream<Item = Result<InC, InE>> + Send + 'static,
    InC: ChunnelConnection<Data = (u32, D)> + Send + Sync + 'static,
    InE: Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Stream, Self::Error>>;
    type Connection = ProjectLeftCn<(), OrderedProj<(), Unproject<InC>, D>>;
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

impl<D, InC> Client<InC> for OrderedChunnel
where
    InC: ChunnelConnection<Data = (u32, D)> + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Future = <ProjectLeft<()> as Client<
        <OrderedChunnelProj as Client<Unproject<InC>>>::Connection,
    >>::Future;
    type Connection = ProjectLeftCn<(), OrderedProj<(), Unproject<InC>, D>>;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        match self.inner.connect_wrap(Unproject(cn)).into_inner() {
            Ok(cn) => ProjectLeft::from(()).connect_wrap(cn),
            Err(e) => ready(Err(e)),
        }
    }
}

/// `OrderedChunnelProj` takes in Data segments and tags them for use with `(A, (u32, D))` Chunnels.
///
/// It returns data segments in the order they were sent.
#[derive(Clone, Debug)]
pub struct OrderedChunnelProj {
    hole_thresh: Option<usize>,
}

impl OrderedChunnelProj {
    pub fn ordering_threshold(&mut self, thresh: usize) -> &mut Self {
        self.hole_thresh = Some(thresh);
        self
    }
}

impl Default for OrderedChunnelProj {
    fn default() -> Self {
        Self { hole_thresh: None }
    }
}

impl Negotiate for OrderedChunnelProj {
    type Capability = ();
    fn capabilities() -> Vec<Self::Capability> {
        vec![]
    }
}

impl<A, D, InS, InC, InE> Serve<InS> for OrderedChunnelProj
where
    InS: Stream<Item = Result<InC, InE>> + Send + 'static,
    InC: ChunnelConnection<Data = (A, (u32, D))> + Send + Sync + 'static,
    InE: Send + Sync + 'static,
    A: Eq + Hash + Clone + std::fmt::Debug + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Stream, Self::Error>>;
    type Connection = OrderedProj<A, InC, D>;
    type Error = InE;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn serve(&mut self, inner: InS) -> Self::Future {
        let cfg = self.hole_thresh;
        ready(Ok(
            Box::pin(inner.map_ok(move |cn| OrderedProj::new(cn, cfg))) as _,
        ))
    }
}

impl<A, D, InC> Client<InC> for OrderedChunnelProj
where
    InC: ChunnelConnection<Data = (A, (u32, D))> + Send + Sync + 'static,
    A: Eq + Hash + Clone + std::fmt::Debug + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = OrderedProj<A, InC, D>;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        ready(Ok(OrderedProj::new(cn, self.hole_thresh)))
    }
}

#[derive(Debug)]
struct OrderedState<D> {
    snd_nxt: u32,
    expected_recv: u32,
    recvd: BinaryHeap<DataPair<D>>, // list of out-of-order received seqs
}

impl<D> Default for OrderedState<D> {
    fn default() -> Self {
        OrderedState {
            snd_nxt: 0,
            expected_recv: 0,
            recvd: BinaryHeap::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct OrderedProj<A: Eq + Hash, C, D> {
    inner: Arc<C>,
    hole_thresh: Option<usize>,
    state: Arc<DashMap<A, OrderedState<(A, D)>>>,
}

impl<A: Eq + Hash, C, D> OrderedProj<A, C, D> {
    pub fn new(inner: C, hole_thresh: Option<usize>) -> Self {
        OrderedProj {
            inner: Arc::new(inner),
            hole_thresh,
            state: Default::default(),
        }
    }
}

impl<A, C, D> ChunnelConnection for OrderedProj<A, C, D>
where
    C: ChunnelConnection<Data = (A, (u32, D))> + Send + Sync + 'static,
    D: Send + Sync + 'static,
    A: Eq + Hash + Clone + std::fmt::Debug + Send + Sync + 'static,
{
    type Data = (A, D);

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>> {
        let state = Arc::clone(&self.state);
        let inner = Arc::clone(&self.inner);
        Box::pin(
            async move {
                let (addr, data) = data;
                state.entry(addr.clone()).or_default();
                let seq = {
                    let mut st = state.get_mut(&addr).unwrap();
                    let seq = st.snd_nxt;
                    st.snd_nxt += 1;
                    seq
                };

                trace!(seq = ?seq, "sending");
                inner.send((addr, (seq, data))).await?;
                trace!(seq = ?seq, "finished send");
                Ok(())
            }
            .instrument(tracing::trace_span!("orderedproj_send")),
        )
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>> {
        let state = Arc::clone(&self.state);
        let inner = Arc::clone(&self.inner);
        let hole_thresh = self.hole_thresh;
        Box::pin(
            async move {
                loop {
                    for mut e in state.iter_mut() {
                        let (a, st) = e.pair_mut();
                        if let Some(DataPair(seq, _)) = st.recvd.peek() {
                            let mut pop = *seq == st.expected_recv;
                            if let Some(thresh) = hole_thresh {
                                pop = pop || (st.recvd.len() > thresh);
                            }

                            if pop {
                                trace!(addr = ?a, seq = ?seq, next_expected = ?st.expected_recv, pileup = ?st.recvd.len(), "returning ordered packet");
                                st.expected_recv += 1;
                                return Ok(st.recvd.pop().unwrap().1);
                            } else {
                                trace!(addr = ?a, head_seq = ?seq, next_expected = ?st.expected_recv, pileup = ?st.recvd.len(), "calling inner recv");
                            }
                        }
                    }

                    let (a, (seq, d)) = inner.recv().await?;
                    trace!(seq = ?seq, from=?a, "received pkt, locking state");
                    let mut st = state.entry(a.clone()).or_default();
                    #[allow(clippy::comparison_chain)]
                    if seq == st.expected_recv {
                        trace!(seq = ?seq, from=?a, "received in-order");
                        st.expected_recv += 1;
                        return Ok((a, d));
                    } else if seq > st.expected_recv {
                        trace!(seq = ?seq, from=?a, expected = ?st.expected_recv, "received out-of-order");
                        st.recvd.push((seq, (a, d)).into());
                    } else {
                        trace!(seq = ?seq, from=?a, expected = ?st.expected_recv, "dropping segment");
                    }
                }
            }
            .instrument(tracing::trace_span!("orderedproj_recv")),
        )
    }
}

#[derive(Clone, Copy, Debug)]
struct DataPair<D>(u32, D);

impl<D> Into<(u32, D)> for DataPair<D> {
    fn into(self) -> (u32, D) {
        (self.0, self.1)
    }
}

impl<D> From<(u32, D)> for DataPair<D> {
    fn from(f: (u32, D)) -> Self {
        DataPair(f.0, f.1)
    }
}

impl<D> PartialEq for DataPair<D> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<D> Eq for DataPair<D> {}

impl<D> PartialOrd for DataPair<D> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // make it a min-heap
        other.0.partial_cmp(&self.0)
    }
}

impl<D> Ord for DataPair<D> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // make it a min-heap
        other.0.cmp(&self.0)
    }
}

/// Simpler serialization for (u32, Vec<u8>) cases.
#[derive(Debug, Clone)]
pub struct SeqUnreliableChunnel;

impl<InS, InC, InE> Serve<InS> for SeqUnreliableChunnel
where
    InS: Stream<Item = Result<InC, InE>> + Send + 'static,
    InC: ChunnelConnection<Data = Vec<u8>> + Send + Sync + 'static,
    InE: Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Stream, Self::Error>>;
    type Connection = SeqUnreliable<InC>;
    type Error = InE;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn serve(&mut self, inner: InS) -> Self::Future {
        ready(Ok(Box::pin(inner.map_ok(SeqUnreliable::from)) as _))
    }
}

impl<InC> Client<InC> for SeqUnreliableChunnel
where
    InC: ChunnelConnection<Data = Vec<u8>> + Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = SeqUnreliable<InC>;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        ready(Ok(SeqUnreliable::from(cn)))
    }
}

/// `SeqUnreliable` accepts (u32, Vec<u8>) pairs as Data for transmission.
#[derive(Default, Debug, Clone)]
pub struct SeqUnreliable<C> {
    inner: Arc<C>,
}

impl<Cx> From<Cx> for SeqUnreliable<Cx> {
    fn from(cx: Cx) -> SeqUnreliable<Cx> {
        SeqUnreliable {
            inner: Arc::new(cx),
        }
    }
}

impl<C> ChunnelConnection for SeqUnreliable<C>
where
    C: ChunnelConnection<Data = Vec<u8>> + Send + Sync + 'static,
{
    type Data = (u32, Vec<u8>);

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(
            async move {
                let (seq, data) = data;
                let mut buf = seq.to_be_bytes().to_vec();
                buf.extend(&data);
                inner.send(buf).await?;
                Ok(())
            }
            .instrument(tracing::trace_span!("sequnreliable_send")),
        )
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(
            async move {
                let mut seq = inner.recv().await?;
                // pop off the seqno
                let data = seq.split_off(4);
                let seq = u32::from_be_bytes(seq[0..4].try_into().unwrap());
                Ok((seq, data))
            }
            .instrument(tracing::trace_span!("sequnreliable_recv")),
        )
    }
}

#[cfg(test)]
mod test {
    use super::{OrderedChunnel, SeqUnreliableChunnel, TaggerChunnel};
    use crate::chan_transport::Chan;
    use crate::{
        util::ProjectLeft, ChunnelConnection, ChunnelConnector, ChunnelListener, Client, CxList,
        Serve,
    };
    use color_eyre::Report;
    use futures_util::StreamExt;
    use tracing::{debug, info};
    use tracing_error::ErrorLayer;
    use tracing_futures::Instrument;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    #[test]
    fn tag_only() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        color_eyre::install().unwrap_or_else(|_| ());

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                let (mut srv, mut cln) = Chan::default().split();
                let mut stack = CxList::from(TaggerChunnel)
                    .wrap(SeqUnreliableChunnel)
                    .wrap(ProjectLeft::from(()));

                let rcv_st = srv.listen(()).await?;
                let mut rcv_st = stack.serve(rcv_st).await?;
                let rcv = rcv_st.next().await.unwrap().unwrap();

                let cln = cln.connect(()).await?;
                let snd = stack.connect_wrap(cln).await?;

                do_transmit(snd, rcv).await;

                Ok::<_, Report>(())
            }
            .instrument(tracing::info_span!("tag_only")),
        )
        .unwrap();
    }

    async fn do_transmit<C>(snd_ch: C, rcv_ch: C)
    where
        C: ChunnelConnection<Data = Vec<u8>> + Send + Sync + 'static,
    {
        let msgs: Vec<_> = (0..10).map(|i| vec![i; 10]).collect();

        // recv side
        let ms = msgs.clone();
        tokio::spawn(
            async move {
                let futs: futures_util::stream::FuturesOrdered<_> = (msgs.into_iter().map(|m| {
                    debug!(m = ?m, "sending");
                    snd_ch.send(m)
                }))
                .collect();
                futs.collect::<Vec<_>>()
                    .await
                    .into_iter()
                    .collect::<Result<(), _>>()
                    .unwrap();
            }
            .instrument(tracing::debug_span!("sender")),
        );

        let msgs = ms;
        info!("starting receiver");
        let mut cnt = 0;
        loop {
            let m = rcv_ch.recv().await.unwrap();
            debug!(m = ?m, "rcvd");
            assert_eq!(m, msgs[cnt]);
            cnt += 1;
            if cnt == msgs.len() {
                info!("done");
                return;
            }
        }
    }

    #[test]
    fn no_drops() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        color_eyre::install().unwrap_or_else(|_| ());

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                let (mut srv, mut cln) = Chan::default().split();
                let mut stack = CxList::from(OrderedChunnel::default())
                    .wrap(SeqUnreliableChunnel)
                    .wrap(ProjectLeft::from(()));

                let rcv_st = srv.listen(()).await?;
                let mut rcv_st = stack.serve(rcv_st).await?;
                let rcv = rcv_st.next().await.unwrap().unwrap();

                let cln = cln.connect(()).await?;
                let snd = stack.connect_wrap(cln).await?;

                do_transmit(snd, rcv).await;

                Ok::<_, Report>(())
            }
            .instrument(tracing::info_span!("no_drops")),
        )
        .unwrap();
    }

    #[test]
    fn reorder() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.try_init().unwrap_or_else(|_| ());
        color_eyre::install().unwrap_or_else(|_| ());

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        use std::ops::DerefMut;
        use std::sync::{Arc, Mutex};

        rt.block_on(
            async move {
                let mut t = Chan::default();
                let staged: Arc<Mutex<(usize, Vec<((), Vec<u8>)>)>> = Default::default();
                t.link_conditions(move |x| {
                    let mut s = staged.lock().unwrap();
                    let (c, st) = &mut s.deref_mut();
                    if let Some(x) = x {
                        *c += 1;
                        if *c == 3 || *c == 5 {
                            debug!(cnt = ?c, msg = ?x, deferred = st.len(), "delaying packet");
                            st.push(x);
                            None
                        } else {
                            debug!(cnt = ?c, msg=?x, deferred = st.len(), "sending packet");
                            Some(x)
                        }
                    } else if *c > 8 && !st.is_empty() {
                        debug!(cnt = ?c, msg = ?&st[0], deferred = st.len(), "sending packet");
                        st.pop()
                    } else {
                        None
                    }
                });

                let (mut srv, mut cln) = t.split();

                let mut stack = CxList::from(OrderedChunnel::default())
                    .wrap(SeqUnreliableChunnel)
                    .wrap(ProjectLeft::from(()));

                let rcv_st = srv.listen(()).await?;
                let mut rcv_st = stack.serve(rcv_st).await?;
                let rcv = rcv_st.next().await.unwrap().unwrap();

                let cln = cln.connect(()).await?;
                let snd = stack.connect_wrap(cln).await?;

                do_transmit(snd, rcv).await;
                Ok::<_, Report>(())
            }
            .instrument(tracing::info_span!("reorder")),
        )
        .unwrap();
    }
}
