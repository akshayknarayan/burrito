//! Chunnel implementing reliability.
//!
//! Takes as Data a `(u32, Vec<u8>)`, where the `u32` is a unique tag corresponding to a data
//! segment, the `Vec<u8>`.

use crate::{ChunnelConnection, Client, Negotiate, Serve};
use futures_util::future::{ready, Ready};
use futures_util::stream::Stream;
use futures_util::stream::TryStreamExt;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use tokio::sync::RwLock;
use tracing::{debug, trace};
use tracing_futures::Instrument;

#[derive(Clone, Debug)]
pub struct ReliabilityChunnel<D> {
    timeout: Duration,
    _data: std::marker::PhantomData<D>,
}

impl<D> Negotiate for ReliabilityChunnel<D> {
    type Capability = ();
    fn capabilities() -> Vec<Self::Capability> {
        vec![]
    }
}

impl<D> Default for ReliabilityChunnel<D> {
    fn default() -> Self {
        ReliabilityChunnel {
            timeout: std::time::Duration::from_millis(100),
            _data: Default::default(),
        }
    }
}

impl<D> ReliabilityChunnel<D> {
    pub fn set_timeout(&mut self, to: Duration) -> &mut Self {
        self.timeout = to;
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
    type Connection = Reliability<InC, D>;
    type Error = InE;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn serve(&mut self, inner: InS) -> Self::Future {
        let cfg = self.timeout;
        ready(Ok(Box::pin(inner.map_ok(move |cn| {
            let mut r = Reliability::from(cn);
            r.set_timeout(cfg);
            r
        })) as _))
    }
}

impl<InC, D> Client<InC> for ReliabilityChunnel<D>
where
    InC: ChunnelConnection<Data = (u32, Option<D>)> + Send + Sync + 'static,
    D: Clone + Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = Reliability<InC, D>;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        ready(Ok(Reliability::from(cn)))
    }
}

#[derive(Debug)]
struct ReliabilityState<D> {
    inflight: HashMap<u32, (D, Option<oneshot::Sender<Result<(), eyre::Report>>>)>, // list of inflight seqs
    retx_tracker: BTreeMap<Instant, u32>,
    last_timeout: Option<Instant>,
    pending_payload: VecDeque<(u32, D)>, // payloads we have received that are waiting for a recv() call
}

impl<D> Default for ReliabilityState<D> {
    fn default() -> Self {
        ReliabilityState {
            inflight: HashMap::new(),
            retx_tracker: BTreeMap::new(),
            last_timeout: Default::default(),
            pending_payload: VecDeque::new(),
        }
    }
}

#[derive(Debug)]
pub struct Reliability<C, D> {
    timeout: Duration,
    inner: Arc<C>,
    state: Arc<RwLock<ReliabilityState<D>>>,
    _data: std::marker::PhantomData<D>,
}

impl<C, D> Reliability<C, D> {
    pub fn set_timeout(&mut self, to: Duration) -> &mut Self {
        self.timeout = to;
        self
    }
}

impl<Cx, D> From<Cx> for Reliability<Cx, D> {
    fn from(cx: Cx) -> Reliability<Cx, D> {
        Reliability {
            inner: Arc::new(cx),
            timeout: Duration::from_millis(100),
            state: Default::default(),
            _data: Default::default(),
        }
    }
}

impl<C, D> Clone for Reliability<C, D> {
    fn clone(&self) -> Self {
        Reliability {
            timeout: self.timeout,
            inner: Arc::clone(&self.inner),
            state: Arc::clone(&self.state),
            _data: Default::default(),
        }
    }
}

impl<C, D> ChunnelConnection for Reliability<C, D>
where
    C: ChunnelConnection<Data = (u32, Option<D>)> + Send + Sync + 'static,
    D: Clone + Send + Sync + 'static,
{
    type Data = (u32, D); // a tag and its data.

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>> {
        let seq = data.0;

        let state = Arc::clone(&self.state);
        let inner = Arc::clone(&self.inner);
        let timeout = self.timeout;
        Box::pin(
            async move {
                let (s, r) = oneshot::channel();
                let data = (data.0, Some(data.1));

                {
                    let mut state = state.write().await;
                    state
                        .inflight
                        .insert(seq, (data.1.clone().unwrap(), Some(s)));
                    state.retx_tracker.insert(Instant::now(), seq);
                    trace!(inflight = state.inflight.len(), seq = ?seq, "sent");
                }

                inner.send(data).await?;

                transmit(Arc::clone(&inner), Arc::clone(&state), r, timeout).await?;
                Ok(())
            }
            .instrument(tracing::debug_span!("send", seq = ?seq)),
        )
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>> {
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

async fn transmit<C, D>(
    inner: Arc<C>,
    state: Arc<RwLock<ReliabilityState<D>>>,
    mut r: tokio::sync::oneshot::Receiver<Result<(), eyre::Report>>,
    timeout: Duration,
) -> Result<(), eyre::Report>
where
    C: ChunnelConnection<Data = (u32, Option<D>)> + Send + Sync + 'static,
    D: Clone + Send + Sync + 'static,
{
    let inner1 = Arc::clone(&inner);
    let state1 = Arc::clone(&state);
    futures_util::future::select(
        Box::pin(
            async move {
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
                                        inner.send((seq, Some(d.clone()))).await?;
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
                let mut inner = inner1;
                let mut state = state1;
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

async fn do_recv<C, D>(
    inner: &mut Arc<C>,
    state: &mut Arc<RwLock<ReliabilityState<D>>>,
) -> Result<(u32, D), eyre::Report>
where
    C: ChunnelConnection<Data = (u32, Option<D>)> + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    loop {
        trace!("call inner recv");
        let (seq, data) = inner.recv().await?;

        if let None = data {
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
            inner.send((seq, None)).await?;
            trace!(seq = ?seq, "got payload, sent ack");
        }

        return Ok((seq, data.unwrap()));
    }
}

#[derive(Clone, Debug)]
pub struct ReliabilityProjChunnel<A, D> {
    timeout: Duration,
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
            timeout: std::time::Duration::from_millis(100),
            _data: Default::default(),
        }
    }
}

impl<A, D> ReliabilityProjChunnel<A, D> {
    pub fn set_timeout(&mut self, to: Duration) -> &mut Self {
        self.timeout = to;
        self
    }
}

impl<A, InS, InC, InE, D> Serve<InS> for ReliabilityProjChunnel<A, D>
where
    A: Clone + Send + Sync + 'static,
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
            r.set_timeout(cfg);
            r
        })) as _))
    }
}

impl<A, InC, D> Client<InC> for ReliabilityProjChunnel<A, D>
where
    A: Clone + Send + Sync + 'static,
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
pub struct ReliabilityProj<A, D, C> {
    timeout: Duration,
    inner: Arc<C>,
    state: Arc<RwLock<ReliabilityState<(A, D)>>>,
}

impl<A, C, D> ReliabilityProj<A, D, C> {
    pub fn set_timeout(&mut self, to: Duration) -> &mut Self {
        self.timeout = to;
        self
    }
}

impl<Cx, A, D> From<Cx> for ReliabilityProj<A, D, Cx> {
    fn from(cx: Cx) -> ReliabilityProj<A, D, Cx> {
        ReliabilityProj {
            inner: Arc::new(cx),
            timeout: Duration::from_millis(100),
            state: Default::default(),
        }
    }
}

impl<A, C, D> Clone for ReliabilityProj<A, D, C> {
    fn clone(&self) -> Self {
        ReliabilityProj {
            timeout: self.timeout,
            inner: Arc::clone(&self.inner),
            state: Arc::clone(&self.state),
        }
    }
}

impl<A, C, D> ChunnelConnection for ReliabilityProj<A, D, C>
where
    A: Clone + Send + Sync + 'static,
    C: ChunnelConnection<Data = (A, (u32, Option<D>))> + Send + Sync + 'static,
    D: Clone + Send + Sync + 'static,
{
    type Data = (A, (u32, D)); // a tag and its data.

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>> {
        let (addr, data) = data;
        let seq = data.0;

        let state = Arc::clone(&self.state);
        let inner = Arc::clone(&self.inner);
        let timeout = self.timeout;
        Box::pin(
            async move {
                let (s, r) = oneshot::channel();
                let data = (data.0, Some(data.1));

                {
                    let mut state = state.write().await;
                    state
                        .inflight
                        .insert(seq, ((addr.clone(), data.1.clone().unwrap()), Some(s)));
                    state.retx_tracker.insert(Instant::now(), seq);
                    trace!(inflight = state.inflight.len(), seq = ?seq, "sent");
                }

                inner.send((addr, data)).await?;

                transmit_proj(Arc::clone(&inner), Arc::clone(&state), r, timeout).await?;
                Ok(())
            }
            .instrument(tracing::debug_span!("send", seq = ?seq)),
        )
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>> {
        let mut state = Arc::clone(&self.state);
        let mut inner = Arc::clone(&self.inner);

        Box::pin(
            async move {
                {
                    let st = state.read().await;
                    if !st.pending_payload.is_empty() {
                        std::mem::drop(st);
                        let mut st = state.write().await;
                        let (seq, (addr, d)) = st.pending_payload.pop_front().unwrap();
                        return Ok((addr, (seq, d)));
                    }
                }

                let (addr, (seq, data)): (A, (u32, D)) =
                    do_recv_proj::<A, _, _>(&mut inner, &mut state).await?;
                Ok((addr, (seq, data)))
            }
            .instrument(tracing::debug_span!("recv")),
        )
    }
}

async fn transmit_proj<A, C, D>(
    inner: Arc<C>,
    state: Arc<RwLock<ReliabilityState<(A, D)>>>,
    mut r: tokio::sync::oneshot::Receiver<Result<(), eyre::Report>>,
    timeout: Duration,
) -> Result<(), eyre::Report>
where
    A: Clone + Send + Sync + 'static,
    C: ChunnelConnection<Data = (A, (u32, Option<D>))> + Send + Sync + 'static,
    D: Clone + Send + Sync + 'static,
{
    let inner1 = Arc::clone(&inner);
    let state1 = Arc::clone(&state);
    futures_util::future::select(
        Box::pin(
            async move {
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

                                    if let Some((addr, d)) = d {
                                        inner.send((addr, (seq, Some(d.clone())))).await?;
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
                let mut inner = inner1;
                let mut state = state1;
                loop {
                    let (addr, (seq, data)) = do_recv_proj(&mut inner, &mut state).await?;
                    let mut st = state.write().await;
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

async fn do_recv_proj<A, C, D>(
    inner: &mut Arc<C>,
    state: &mut Arc<RwLock<ReliabilityState<(A, D)>>>,
) -> Result<(A, (u32, D)), eyre::Report>
where
    A: Clone + Send + Sync + 'static,
    C: ChunnelConnection<Data = (A, (u32, Option<D>))> + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    loop {
        trace!("call inner recv");
        let (addr, (seq, data)) = inner.recv().await?;

        if let None = data {
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
            inner.send((addr.clone(), (seq, None))).await?;
            trace!(seq = ?seq, "got payload, sent ack");
        }

        return Ok((addr, (seq, data.unwrap())));
    }
}

#[cfg(test)]
mod test {
    use super::{Reliability, ReliabilityChunnel};
    use crate::chan_transport::{Chan, ChanAddr};
    use crate::{
        bincode::SerializeChunnel, util::ProjectLeft, ChunnelConnection, ChunnelConnector,
        ChunnelListener, Client, ConnectAddress, CxList, ListenAddress, Serve,
    };
    use futures_util::StreamExt;
    use tracing::{debug, info};
    use tracing_futures::Instrument;

    async fn do_transmit<C>(
        snd_ch: Reliability<C, Vec<u8>>,
        rcv_ch: Reliability<C, Vec<u8>>,
        msgs: Vec<(u32, Vec<u8>)>,
    ) where
        C: ChunnelConnection<Data = (u32, Option<Vec<u8>>)> + Send + Sync + 'static,
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
        color_eyre::install().unwrap_or_else(|_| ());
        let msgs = vec![(0, vec![0u8; 10]), (1, vec![1u8; 10]), (2, vec![2u8; 10])];

        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_time()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                let a: ChanAddr<((), Vec<u8>)> = Chan::default().into();
                let mut l = CxList::from(ReliabilityChunnel::default())
                    .wrap(SerializeChunnel::default())
                    .wrap(ProjectLeft::from(()));

                let mut srv = a.listener();
                let rcv_st = srv.listen(a.clone()).await.unwrap();
                let mut rcv_st = l.serve(rcv_st).await.unwrap();
                let rcv = rcv_st.next().await.unwrap().unwrap();

                let mut cln = a.connector();
                let cln = cln.connect(a).await.unwrap();
                let snd = l.connect_wrap(cln).await.unwrap();

                do_transmit(snd, rcv, msgs).await;
            }
            .instrument(tracing::info_span!("no_drops")),
        );
    }

    #[test]
    fn drop_2() {
        let _guard = tracing_subscriber::fmt::try_init();
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

                let a: ChanAddr<((), Vec<u8>)> = t.into();
                let mut l = CxList::from(ReliabilityChunnel::default())
                    .wrap(SerializeChunnel::default())
                    .wrap(ProjectLeft::from(()));

                let mut srv = a.listener();
                let rcv_st = srv.listen(a.clone()).await.unwrap();
                let mut rcv_st = l.serve(rcv_st).await.unwrap();
                let rcv = rcv_st.next().await.unwrap().unwrap();

                let mut cln = a.connector();
                let cln = cln.connect(a).await.unwrap();
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

        let _guard = tracing_subscriber::fmt::try_init();
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

                let a: ChanAddr<((), Vec<u8>)> = t.into();
                let mut stack = CxList::from(TaggerChunnel)
                    .wrap(ReliabilityChunnel::default())
                    .wrap(SerializeChunnel::default())
                    .wrap(ProjectLeft::from(()));

                let mut srv = a.listener();
                let rcv_st = srv.listen(a.clone()).await.unwrap();
                let mut rcv_st = stack.serve(rcv_st).await.unwrap();
                let rcv = rcv_st.next().await.unwrap().unwrap();

                let mut cln = a.connector();
                let cln = cln.connect(a).await.unwrap();
                let snd = stack.connect_wrap(cln).await.unwrap();

                do_transmit_tagged(snd, rcv, msgs).await;
            }
            .instrument(tracing::info_span!("drop_2")),
        );
    }
}
