//! Helper Chunnel types to transform Data types, etc.

use super::{Chunnel, ChunnelConnection};
use ahash::AHashMap;
use color_eyre::eyre::{eyre, Report};
use dashmap::DashMap;
use futures_util::future::{ready, Ready};
use futures_util::stream::Stream;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::oneshot;

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
            self.inflight.insert(data.id(), s);
            self.sent.insert(data.id(), r);
        }

        self.inner.send(std::iter::once(data)).await
    }

    pub async fn recv_msg(&self, msg_id: usize) -> Result<D, Report> {
        let (_, mut r) = self
            .sent
            .remove(&msg_id)
            .ok_or_else(|| eyre!("Requested msg id {:?} isn't known", msg_id))?;
        let mut slots = [None, None, None, None];
        loop {
            tokio::select! {
                res = &mut r => {
                    let res = res.expect("sender won't be dropped");
                    return Ok(res);
                }
                res = self.inner.recv(&mut slots[..]) => {
                    let res = res?;
                    for i in 0..4 {
                        if let Some(res) = res[i].take() {
                            if let Some((_, s)) = self.inflight.remove(&res.id()) {
                                s.send(res).map_err(|_| ()).expect("receiver won't be dropped");
                            }
                        }
                    }
                }
            }
        }
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
        MkConn: Fn(A, C, flume::Receiver<(A, Vec<u8>)>) -> Conn + Send + 'static,
        Conn: ChunnelConnection,
    {
        let sk = self.0;
        Box::pin(futures_util::stream::try_unfold(
            (
                sk,
                AHashMap::<_, flume::Sender<(A, Vec<u8>)>>::new(),
                make_conn,
            ),
            move |(sk, mut map, make_conn)| {
                async move {
                    let mut slot = [None];
                    loop {
                        // careful: potential deadlocks since calling `.recv()` on returned
                        // connection blocks on `.listen()`. Make sure to use `and_then_concurrent`.
                        let msgs = sk.recv(&mut slot[..]).await?;
                        if msgs[0].is_none() {
                            continue;
                        }

                        let (from, data) = msgs[0].take().unwrap();

                        let mut done = None;
                        let c = map.entry(from.clone()).or_insert_with(|| {
                            let (sch, rch) = flume::unbounded();
                            done = Some(make_conn(from.clone(), sk.clone(), rch));
                            sch
                        });

                        // the send fails only if the receiver stopped listening.
                        // so this becomes a new connection
                        if let Err(flume::SendError(data)) = c.send((from.clone(), data)) {
                            let (sch, rch) = flume::unbounded();
                            done = Some(make_conn(from.clone(), sk.clone(), rch));
                            // Send again because the previous one failed, so the message would
                            // otherwise be dropped. This won't block because it's an unbounded
                            // channel.
                            sch.send(data).unwrap();
                            map.insert(from.clone(), sch);
                        }

                        if let Some(d) = done {
                            return Ok(Some((d, (sk, map, make_conn))));
                        }
                    }
                }
            },
        )) as _
    }
}

/// Projects a given address over a connection's data.
///
/// Given a `impl ChunnelConnection`s which expect `(addr, data)`, expose an interface which
/// accepts just the `data`.
/// On `send`, wraps the data in the addr provided in `new()`. On `recv`, ignores the addr.
#[derive(Clone, Debug)]
pub struct ProjectLeft<A, C>(A, C);

impl<A, C> ProjectLeft<A, C> {
    pub fn new(a: A, c: C) -> Self {
        ProjectLeft(a, c)
    }

    pub fn addr(&self) -> &A {
        &self.0
    }

    pub fn conn(&self) -> &C {
        &self.1
    }
}

impl<A, C, D> ChunnelConnection for ProjectLeft<A, C>
where
    A: Clone + Send + Sync + 'static,
    D: Send + 'static,
    C: ChunnelConnection<Data = (A, D)> + Send + Sync + 'static,
{
    type Data = D;

    fn send<'cn, B>(
        &'cn self,
        burst: B,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'cn>>
    where
        B: IntoIterator<Item = Self::Data> + Send + 'cn,
        <B as IntoIterator>::IntoIter: Send,
    {
        Box::pin(
            self.1
                .send(burst.into_iter().map(|data| (self.0.clone(), data))),
        ) as _
    }

    fn recv<'cn, 'buf>(
        &'cn self,
        msgs_buf: &'buf mut [Option<Self::Data>],
    ) -> Pin<Box<dyn Future<Output = Result<&'buf mut [Option<Self::Data>], Report>> + Send + 'cn>>
    where
        'buf: 'cn,
    {
        let mut slots: Vec<_> = (0..msgs_buf.len()).map(|_| None).collect();
        Box::pin(async move {
            let msgs = self.1.recv(&mut slots[..]).await?;
            for (slot, (_, d)) in msgs_buf
                .iter_mut()
                .zip(msgs.iter_mut().map_while(Option::take))
            {
                *slot = Some(d);
            }

            Ok(&mut msgs_buf[..])
        })
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

/// A chunnel that errors if it is invoked. Used as a base to build chunnel stacks from.
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

    fn send<'cn, B>(
        &'cn self,
        _: B,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'cn>>
    where
        B: IntoIterator<Item = Self::Data> + Send + 'cn,
        <B as IntoIterator>::IntoIter: Send,
    {
        Box::pin(async move { Err(eyre!("No sends allowed on Never Chunnel")) })
    }

    fn recv<'cn, 'buf>(
        &'cn self,
        _: &'buf mut [Option<Self::Data>],
    ) -> Pin<Box<dyn Future<Output = Result<&'buf mut [Option<Self::Data>], Report>> + Send + 'cn>>
    where
        'buf: 'cn,
    {
        Box::pin(async move { Err(eyre!("Unexpected recv in Never chunnel")) })
    }
}
