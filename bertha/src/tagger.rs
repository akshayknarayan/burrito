//! Chunnel which tags Data to provide at-most-once delivery.

use crate::{ChunnelConnection, Client, Serve};
use futures_util::future::{ready, Ready};
use futures_util::stream::Stream;
use futures_util::stream::TryStreamExt;
use std::collections::BinaryHeap;
use std::convert::TryInto;
use std::future::Future;
use std::pin::Pin;
use std::sync::{atomic::AtomicUsize, Arc};
use tokio::sync::RwLock;
use tracing::{debug, trace};
use tracing_futures::Instrument;

#[derive(Clone, Debug, Default)]
pub struct TaggerChunnel;

impl<D, InS, InC, InE> Serve<InS> for TaggerChunnel
where
    InS: Stream<Item = Result<InC, InE>> + Send + 'static,
    InC: ChunnelConnection<Data = (u32, D)> + Send + Sync + 'static,
    InE: Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Stream, Self::Error>>;
    type Connection = Tagger<InC>;
    type Error = InE;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn serve(&mut self, inner: InS) -> Self::Future {
        ready(Ok(Box::pin(inner.map_ok(|cn| Tagger::from(cn))) as _))
    }
}

impl<D, InC> Client<InC> for TaggerChunnel
where
    InC: ChunnelConnection<Data = (u32, D)> + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = Tagger<InC>;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        ready(Ok(Tagger::from(cn)))
    }
}

/// Assigns an sequential tag to data segments and ignores the tag otherwise.
#[derive(Default, Debug)]
pub struct Tagger<C> {
    inner: Arc<C>,
    snd_nxt: Arc<AtomicUsize>,
}

impl<Cx> From<Cx> for Tagger<Cx> {
    fn from(cx: Cx) -> Tagger<Cx> {
        Tagger {
            inner: Arc::new(cx),
            snd_nxt: Default::default(),
        }
    }
}

impl<C> Clone for Tagger<C>
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

impl<C, D> ChunnelConnection for Tagger<C>
where
    C: ChunnelConnection<Data = (u32, D)> + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Data = D;

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>> {
        let inner = Arc::clone(&self.inner);
        let snd_nxt = Arc::clone(&self.snd_nxt);
        Box::pin(
            async move {
                let seq = snd_nxt.fetch_add(1, std::sync::atomic::Ordering::SeqCst) as u32;
                trace!(seq = ?seq, "sending");
                inner.send((seq, data)).await?;
                trace!(seq = ?seq, "sent");
                Ok(())
            }
            .instrument(tracing::trace_span!("tagger_send")),
        )
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(
            async move {
                let (seq, d) = inner.recv().await?;
                trace!(seq = ?seq, "received");
                return Ok(d);
            }
            .instrument(tracing::trace_span!("tagger_recv")),
        )
    }
}

#[derive(Clone, Debug, Default)]
pub struct TaggerProjChunnel;

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
        ready(Ok(Box::pin(inner.map_ok(|cn| TaggerProj::from(cn))) as _))
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
        Box::pin(
            async move {
                let seq = snd_nxt.fetch_add(1, std::sync::atomic::Ordering::SeqCst) as u32;
                trace!(seq = ?seq, "sending");
                let (addr, data) = data;
                inner.send((addr, (seq, data))).await?;
                trace!(seq = ?seq, "sent");
                Ok(())
            }
            .instrument(tracing::trace_span!("tagger_send")),
        )
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(
            async move {
                let (addr, (seq, d)) = inner.recv().await?;
                trace!(seq = ?seq, "received");
                return Ok((addr, d));
            }
            .instrument(tracing::trace_span!("tagger_recv")),
        )
    }
}

#[derive(Clone, Debug)]
pub struct OrderedChunnel {
    hole_thresh: usize,
}

impl OrderedChunnel {
    pub fn ordering_threshold(&mut self, thresh: usize) -> &mut Self {
        self.hole_thresh = thresh;
        self
    }
}

impl Default for OrderedChunnel {
    fn default() -> Self {
        Self { hole_thresh: 5 }
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
    type Connection = Ordered<InC, D>;
    type Error = InE;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn serve(&mut self, inner: InS) -> Self::Future {
        let cfg = self.hole_thresh;
        ready(Ok(
            Box::pin(inner.map_ok(move |cn| Ordered::new(cn, cfg))) as _
        ))
    }
}

impl<D, InC> Client<InC> for OrderedChunnel
where
    InC: ChunnelConnection<Data = (u32, D)> + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = Ordered<InC, D>;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        ready(Ok(Ordered::new(cn, self.hole_thresh)))
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
        self.0.partial_cmp(&other.0)
    }
}

impl<D> Ord for DataPair<D> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
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

/// `Ordered` takes in `Vec<u8>` Data segments and tags them for use with `(u32, Vec<u8>)` Chunnels.
///
/// It returns data segments in the order they were sent.
#[derive(Clone, Debug)]
pub struct Ordered<C, D> {
    inner: Arc<C>,
    hole_thresh: usize,
    state: Arc<RwLock<OrderedState<D>>>,
}

impl<C, D> Ordered<C, D> {
    pub fn new(inner: C, hole_thresh: usize) -> Self {
        Ordered {
            inner: Arc::new(inner),
            hole_thresh,
            state: Default::default(),
        }
    }
}

impl<C, D> ChunnelConnection for Ordered<C, D>
where
    C: ChunnelConnection<Data = (u32, D)> + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Data = D;

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>> {
        let state = Arc::clone(&self.state);
        let inner = Arc::clone(&self.inner);
        Box::pin(
            async move {
                let seq = {
                    let mut st = state.write().await;
                    let seq = st.snd_nxt;
                    st.snd_nxt += 1;
                    seq
                };

                trace!(seq = ?seq, "sending");
                inner.send((seq, data)).await?;
                trace!(seq = ?seq, "sent");
                Ok(())
            }
            .instrument(tracing::trace_span!("tagger_send")),
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
                let expected = { state.read().await.expected_recv };
                loop {
                    {
                        let mut st = state.write().await;
                        let mut pop = false;
                        if let Some(DataPair(seq, _)) = st.recvd.peek() {
                            pop = *seq == expected || st.recvd.len() > hole_thresh;
                            if pop {
                                trace!(seq = ?seq, next_expected = ?expected, pileup = ?st.recvd.len(), "returning ordered packet");
                            }
                        }

                        if pop {
                            st.expected_recv += 1;
                            return Ok(st.recvd.pop().unwrap().1);
                        }
                    }

                    let (seq, d) = inner.recv().await?;
                    trace!(seq = ?seq, "received");
                    if seq == expected {
                        state.write().await.expected_recv += 1;
                        return Ok(d);
                    } else if seq > expected {
                        let mut st = state.write().await;
                        st.recvd.push((seq, d).into());
                    } else {
                        debug!(seq = ?seq, "dropping segment");
                    }
                }
            }
            .instrument(tracing::trace_span!("tagger_recv")),
        )
    }
}

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
        ready(Ok(Box::pin(inner.map_ok(|cn| SeqUnreliable::from(cn))) as _))
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
    use crate::chan_transport::{Chan, ChanAddr};
    use crate::{
        ChunnelConnection, ChunnelConnector, ChunnelListener, Client, ConnectAddress, CxList,
        ListenAddress, Serve,
    };
    use color_eyre::Report;
    use futures_util::StreamExt;
    use tracing::{debug, info, trace};
    use tracing_futures::Instrument;

    #[test]
    fn tag_only() {
        let _guard = tracing_subscriber::fmt::try_init();
        color_eyre::install().unwrap_or_else(|_| ());

        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_time()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                let a: ChanAddr<Vec<u8>> = Chan::default().into();
                let mut stack = CxList::from(TaggerChunnel).wrap(SeqUnreliableChunnel);

                let mut srv = a.listener();
                let rcv_st = srv.listen(a.clone()).await?;
                let mut rcv_st = stack.serve(rcv_st).await?;
                let rcv = rcv_st.next().await.unwrap().unwrap();

                let mut cln = a.connector();
                let cln = cln.connect(a).await?;
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
        let msgs = vec![vec![0u8; 10], vec![1u8; 10], vec![2u8; 10], vec![3u8; 10]];
        let (s, r) = tokio::sync::oneshot::channel::<()>();

        // recv side
        let ms = msgs.clone();
        tokio::spawn(
            async move {
                let msgs = ms;
                info!("starting receiver");
                let mut cnt = 0;
                loop {
                    let m = rcv_ch.recv().await.unwrap();
                    trace!(m = ?m, "rcvd");
                    assert_eq!(m, msgs[cnt]);
                    cnt += 1;
                    if cnt == msgs.len() {
                        break;
                    }
                }

                s.send(()).unwrap();
            }
            .instrument(tracing::debug_span!("receiver")),
        );

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

        r.await.unwrap();
        info!("done");
    }

    #[test]
    fn no_drops() {
        let _guard = tracing_subscriber::fmt::try_init();
        color_eyre::install().unwrap_or_else(|_| ());

        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_time()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                let a: ChanAddr<Vec<u8>> = Chan::default().into();
                let mut stack = CxList::from(OrderedChunnel::default()).wrap(SeqUnreliableChunnel);

                let mut srv = a.listener();
                let rcv_st = srv.listen(a.clone()).await?;
                let mut rcv_st = stack.serve(rcv_st).await?;
                let rcv = rcv_st.next().await.unwrap().unwrap();

                let mut cln = a.connector();
                let cln = cln.connect(a).await?;
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
        let _guard = tracing_subscriber::fmt::try_init();
        color_eyre::install().unwrap_or_else(|_| ());

        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_time()
            .build()
            .unwrap();

        use std::sync::{atomic::AtomicUsize, Arc, Mutex};

        rt.block_on(
            async move {
                let mut t = Chan::default();
                let ctr: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
                let staged: Arc<Mutex<Vec<Vec<u8>>>> = Default::default();
                t.link_conditions(move |x| {
                    let c = ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let mut s = staged.lock().unwrap();
                    if let Some(x) = x {
                        if c == 2 {
                            s.push(x);
                            debug!(cnt = ?c, "delaying packet");
                            None
                        } else {
                            Some(x)
                        }
                    } else {
                        if c > 2 {
                            s.pop()
                        } else {
                            None
                        }
                    }
                });
                let a: ChanAddr<Vec<u8>> = t.into();

                let mut stack = CxList::from(OrderedChunnel::default()).wrap(SeqUnreliableChunnel);

                let mut srv = a.listener();
                let rcv_st = srv.listen(a.clone()).await?;
                let mut rcv_st = stack.serve(rcv_st).await?;
                let rcv = rcv_st.next().await.unwrap().unwrap();

                let mut cln = a.connector();
                let cln = cln.connect(a).await?;
                let snd = stack.connect_wrap(cln).await?;

                do_transmit(snd, rcv).await;
                Ok::<_, Report>(())
            }
            .instrument(tracing::info_span!("reorder")),
        )
        .unwrap();
    }
}
