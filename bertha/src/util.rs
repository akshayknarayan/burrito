//! Helper Chunnel types to transform Data types, etc.

use super::{Chunnel, ChunnelConnection};
use ahash::AHashMap;
use color_eyre::eyre::{eyre, Report};
use dashmap::DashMap;
use futures_util::future::{
    TryFutureExt, {ready, Ready},
};
use futures_util::stream::Stream;
use std::collections::VecDeque;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::{
    atomic::{self, AtomicUsize},
    Arc, Mutex as StdMutex,
};
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::trace;
use tracing_futures::Instrument;

pub trait MsgId {
    fn id(&self) -> usize;
}

/// Match incoming messages to previously sent ones on the given id() field.
pub struct MsgIdMatcher<C: ChunnelConnection, D> {
    inner: Arc<C>,
    inflight: Arc<DashMap<usize, oneshot::Sender<D>>>,
    sent: Arc<DashMap<usize, oneshot::Receiver<D>>>,
}

impl<C: ChunnelConnection, D> Clone for MsgIdMatcher<C, D> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            inflight: Arc::clone(&self.inflight),
            sent: Arc::clone(&self.sent),
        }
    }
}

impl<C, D> MsgIdMatcher<C, D>
where
    C: ChunnelConnection<Data = D> + Send + Sync + 'static,
    D: MsgId + Send + Sync + 'static,
{
    pub fn new(inner: C) -> Self {
        Self {
            inner: Arc::new(inner),
            inflight: Default::default(),
            sent: Default::default(),
        }
    }

    pub async fn send_msg(&self, data: D) -> Result<(), Report> {
        if !self.sent.contains_key(&data.id()) {
            let (s, r) = oneshot::channel();
            self.sent.insert(data.id(), r);
            self.inflight.insert(data.id(), s);
        }

        self.inner.send(data).await
    }

    pub async fn recv_msg(&self, msg_id: usize) -> Result<D, Report> {
        let (_, mut r) = self
            .sent
            .remove(&msg_id)
            .ok_or_else(|| eyre!("Requested msg id {:?} isn't known", msg_id))?;
        loop {
            tokio::select! {
                res = &mut r => {
                    trace!("MsgIdMatcher done");
                    return Ok(res.expect("sender won't be dropped"));
                }
                res = self.inner.recv() => {
                    let res = res?;
                    if let Some((_, s)) = self.inflight.remove(&res.id()) {
                        s.send(res).map_err(|_| ()).expect("receiver won't be dropped");
                    } else {
                        continue;
                    }
                }
            }
        }
    }
}

/// Remember the order of calls to recv(), and return packets from the underlying connection in
/// that order.
#[derive(Debug)]
pub struct RecvCallOrder<C: ChunnelConnection> {
    inner: Arc<C>,
    queue: Arc<StdMutex<VecDeque<oneshot::Sender<Result<C::Data, Report>>>>>,
}

impl<C: ChunnelConnection> Clone for RecvCallOrder<C> {
    fn clone(&self) -> Self {
        RecvCallOrder {
            inner: Arc::clone(&self.inner),
            queue: Arc::clone(&self.queue),
        }
    }
}

impl<C: ChunnelConnection> RecvCallOrder<C> {
    pub fn new(inner: C) -> Self {
        Self {
            inner: Arc::new(inner),
            queue: Default::default(),
        }
    }
}

impl<C> ChunnelConnection for RecvCallOrder<C>
where
    C: ChunnelConnection + Send + Sync + 'static,
    C::Data: Send + Sync + 'static,
{
    type Data = C::Data;

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        self.inner.send(data)
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        //self.inner.recv()
        let inner = Arc::clone(&self.inner);
        let recv_queue = Arc::clone(&self.queue);
        let (s, mut r) = oneshot::channel();
        {
            let mut rq = recv_queue.lock().unwrap();
            rq.push_back(s);
            trace!(pos = &rq.len(), "inserted");
        }

        Box::pin(async move {
            loop {
                tokio::select! {
                    res = &mut r => {
                        trace!("RecvCallOrder done");
                        return res.expect("sender won't be dropped");
                    }
                    res = inner.recv() => {
                        recv_queue
                            .lock()
                            .unwrap()
                            .pop_front()
                            .expect("Must have a sender waiting, since we pushed one")
                            .send(res)
                            .map_err(|_| ())
                            .expect("Must have a receiver waiting");
                    }
                }

                match r.try_recv() {
                    Ok(res) => {
                        trace!("RecvCallOrder done");
                        return res;
                    }
                    Err(oneshot::error::TryRecvError::Empty) => (),
                    Err(oneshot::error::TryRecvError::Closed) => unreachable!(),
                }
            }
        })
    }
}

/// Steer incoming packets to per-address connections
pub struct AddrSteer<C>(C);

impl<C> AddrSteer<C> {
    pub fn new(inner: C) -> Self {
        AddrSteer(inner)
    }
}

impl<A, C> AddrSteer<C>
where
    C: ChunnelConnection<Data = (A, Vec<u8>)> + Clone + Send + Sync + 'static,
    A: Clone + Eq + Hash + std::fmt::Debug + Send + 'static,
{
    pub fn steer<MkConn, Conn>(
        self,
        make_conn: MkConn,
    ) -> Pin<Box<dyn Stream<Item = Result<Conn, Report>> + Send + 'static>>
    where
        MkConn: Fn(A, C, Arc<Mutex<mpsc::UnboundedReceiver<(A, Vec<u8>)>>>, Arc<AtomicUsize>) -> Conn
            + Send
            + 'static,
        Conn: ChunnelConnection,
    {
        let sk = self.0;
        let pending_inc_ctr: Arc<AtomicUsize> = Default::default();
        let pending_dec_ctr: Arc<AtomicUsize> = Default::default();
        Box::pin(futures_util::stream::try_unfold(
            (
                sk,
                AHashMap::<_, mpsc::UnboundedSender<(A, Vec<u8>)>>::new(),
                pending_inc_ctr,
                pending_dec_ctr,
                make_conn,
            ),
            move |(sk, mut map, pending_inc_ctr, pending_dec_ctr, make_conn)| {
                async move {
                    let mut last_print = std::time::Instant::now();
                    loop {
                        // careful: potential deadlocks since .recv on returned connection blocks
                        // on .listen
                        let (from, data) = sk.recv().await?;
                        let ctr = pending_inc_ctr.fetch_add(1, atomic::Ordering::SeqCst);

                        let mut done = None;
                        let c = map.entry(from.clone()).or_insert_with(|| {
                            trace!(from = ?&from, "new connection");
                            let (sch, rch) = mpsc::unbounded_channel();
                            let cn = make_conn(
                                from.clone(),
                                sk.clone(),
                                Arc::new(Mutex::new(rch)),
                                Arc::clone(&pending_dec_ctr),
                            );

                            done = Some(cn);
                            sch
                        });

                        trace!(from = ?&from, pending = &ctr, "received pkt");

                        // the send fails only if the receiver stopped listening.
                        // so this becomes a new connection
                        if let Err(mpsc::error::SendError(data)) = c.send((from.clone(), data)) {
                            trace!(from = ?&from, "new connection");
                            let (sch, rch) = mpsc::unbounded_channel();
                            let cn = make_conn(
                                from.clone(),
                                sk.clone(),
                                Arc::new(Mutex::new(rch)),
                                Arc::clone(&pending_dec_ctr),
                            );

                            done = Some(cn);
                            sch.send(data).unwrap();
                            map.insert(from.clone(), sch);
                        }

                        if last_print.elapsed() > std::time::Duration::from_millis(1000) {
                            let p_inc = pending_inc_ctr.load(atomic::Ordering::SeqCst);
                            let p_dec = pending_dec_ctr.load(atomic::Ordering::SeqCst);
                            tracing::info!(
                                from = ?from,
                                ?p_inc, ?p_dec,
                                "pending"
                            );
                            last_print = std::time::Instant::now();
                        }

                        if let Some(d) = done {
                            return Ok(Some((
                                d,
                                (sk, map, pending_inc_ctr, pending_dec_ctr, make_conn),
                            )));
                        }
                    }
                }
                .instrument(tracing::info_span!("steer"))
            },
        )) as _
    }
}

pub struct Unproject<C>(pub C);

impl<C, D> ChunnelConnection for Unproject<C>
where
    C: ChunnelConnection<Data = D>,
    D: 'static,
{
    type Data = ((), D);

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        self.0.send(data.1)
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        let f = self.0.recv();
        Box::pin(async move { Ok(((), f.await?)) })
    }
}

/// Projects a given address over a connection's data.
///
/// Given a `impl ChunnelConnection`s which expect `(addr, data)`, expose an interface which
/// accepts just the `data`.
/// On `send`, wraps the data in the addr provided in `new()`. On `recv`, ignores the addr.
pub struct ProjectLeft<A, C>(A, C);

impl<A, C> ProjectLeft<A, C> {
    pub fn new(a: A, c: C) -> Self {
        ProjectLeft(a, c)
    }
}

impl<A, C, D> ChunnelConnection for ProjectLeft<A, C>
where
    A: Clone + Send + 'static,
    D: 'static,
    C: ChunnelConnection<Data = (A, D)> + Send + Sync + 'static,
{
    type Data = D;

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        Box::pin(self.1.send((self.0.clone(), data))) as _
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        Box::pin(self.1.recv().map_ok(|d| d.1)) as _
    }
}

/// Does nothing.
#[derive(Debug, Clone)]
pub struct Nothing<N = ()>(std::marker::PhantomData<N>);

impl<N> Default for Nothing<N> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<N> crate::negotiate::Negotiate for Nothing<N>
where
    N: crate::negotiate::CapabilitySet,
{
    type Capability = N;
    fn guid() -> u64 {
        0xa0c77d6bd6bef98c
    }
}

impl<D, InC, N> Chunnel<InC> for Nothing<N>
where
    InC: ChunnelConnection<Data = D> + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = InC;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        ready(Ok(cn))
    }
}

/// For testing-assertion purposes, a chunnel that errors if send() is called or inner.recv()
/// returns data.
#[derive(Debug, Clone, Default)]
pub struct Never;

impl<D, InC> Chunnel<InC> for Never
where
    InC: ChunnelConnection<Data = D> + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = NeverCn;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, _cn: InC) -> Self::Future {
        ready(Ok(Default::default()))
    }
}

#[derive(Clone, Debug, Default)]
pub struct NeverCn<D = ()>(std::marker::PhantomData<D>);

impl<D> ChunnelConnection for NeverCn<D> {
    type Data = D;

    fn send(
        &self,
        _: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        Box::pin(async move { Err(eyre!("No sends allowed on Never Chunnel")) })
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        Box::pin(async move { Err(eyre!("Unexpected recv in Never chunnel")) })
    }
}
