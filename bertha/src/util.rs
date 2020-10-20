//! Helper Chunnel types to transform Data types, etc.

use super::{ChunnelConnection, ChunnelConnector, Client, Serve};
use eyre::eyre;
use futures_util::future::{
    TryFutureExt, {ready, Ready},
};
use futures_util::stream::Stream;
use futures_util::stream::TryStreamExt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Dummy connection type for non-connection-oriented chunnels.
///
/// Exposes each message in the stream as a "connection". Sends via the C chunnel type.
pub struct Once<C, D, A>(Arc<Mutex<Option<(A, D)>>>, Arc<C>);

impl<C, D, A> Once<C, D, A> {
    pub fn new(send: Arc<C>, d: (A, D)) -> Self {
        Self(Arc::new(Mutex::new(Some(d))), send)
    }
}

impl<A, C, D> ChunnelConnection for Once<C, D, A>
where
    C: ChunnelConnection<Data = (A, D)>,
    (A, D): Send + Sync + 'static,
{
    type Data = (A, Option<D>);

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>> {
        if let (a, Some(d)) = data {
            self.1.send((a, d))
        } else {
            Box::pin(futures_util::future::ready(Err(eyre!("Can't send None")))) as _
        }
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>> {
        let d = Arc::clone(&self.0);
        Box::pin(async move {
            let data = d.lock().await.take();
            if let Some((a, d)) = data {
                Ok((a, Some(d)))
            } else {
                Err(eyre!("Tried to recv None"))
            }
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct ProjectLeft<A, N = ()>(A, std::marker::PhantomData<N>);

impl<A> From<A> for ProjectLeft<A> {
    fn from(a: A) -> Self {
        ProjectLeft(a, Default::default())
    }
}

impl<A, N> crate::negotiate::Negotiate for ProjectLeft<A, N>
where
    N: crate::negotiate::CapabilitySet,
{
    type Capability = N;
    fn capabilities() -> Vec<Self::Capability> {
        vec![]
    }
}

impl<N, A, D, InS, InC, InE> Serve<InS> for ProjectLeft<A, N>
where
    InS: Stream<Item = Result<InC, InE>> + Send + 'static,
    InC: ChunnelConnection<Data = (A, D)> + Send + Sync + 'static,
    InE: Send + Sync + 'static,
    A: Clone + Send + 'static,
    D: 'static,
{
    type Future = Ready<Result<Self::Stream, Self::Error>>;
    type Connection = ProjectLeftCn<A, InC>;
    type Error = InE;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn serve(&mut self, inner: InS) -> Self::Future {
        let a = self.0.clone();
        ready(Ok(
            Box::pin(inner.map_ok(move |cn| ProjectLeftCn::new(a.clone(), cn))) as _,
        ))
    }
}

impl<A, D, InC> Client<InC> for ProjectLeft<A>
where
    InC: ChunnelConnection<Data = (A, D)> + Send + Sync + 'static,
    A: Clone + Send + 'static,
    D: Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = ProjectLeftCn<A, InC>;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        let a = self.0.clone();
        ready(Ok(ProjectLeftCn::new(a, cn)))
    }
}

pub struct ProjectLeftCn<A, C>(A, C);

impl<A, C> ProjectLeftCn<A, C> {
    pub fn new(a: A, c: C) -> Self {
        ProjectLeftCn(a, c)
    }
}

impl<A, C, D> ChunnelConnection for ProjectLeftCn<A, C>
where
    A: Clone + Send + 'static,
    D: 'static,
    C: ChunnelConnection<Data = (A, D)> + Send + Sync + 'static,
{
    type Data = D;

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>> {
        Box::pin(self.1.send((self.0.clone(), data))) as _
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>> {
        Box::pin(self.1.recv().map_ok(|d| d.1)) as _
    }
}

/// Chunnel type transposing the Data type of the underlying connection
/// to be `Option`-wrapped.
#[derive(Debug, Clone, Default)]
pub struct OptionWrap;

impl<InS, InC, InE> Serve<InS> for OptionWrap
where
    InS: Stream<Item = Result<InC, InE>> + Send + 'static,
    InC: ChunnelConnection + Send + Sync + 'static,
    InE: Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Stream, Self::Error>>;
    type Connection = OptionWrapCn<InC>;
    type Error = InE;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn serve(&mut self, inner: InS) -> Self::Future {
        ready(Ok(Box::pin(inner.map_ok(|cn| OptionWrapCn::new(cn))) as _))
    }
}

impl<D, InC> Client<InC> for OptionWrap
where
    InC: ChunnelConnection<Data = D> + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = OptionWrapCn<InC>;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        ready(Ok(OptionWrapCn::new(cn)))
    }
}

#[derive(Debug, Clone)]
pub struct OptionWrapCn<C>(Arc<C>);

impl<C> OptionWrapCn<C> {
    pub fn new(inner: C) -> Self {
        Self(Arc::new(inner))
    }
}

impl<C, D> ChunnelConnection for OptionWrapCn<C>
where
    C: ChunnelConnection<Data = D> + Send + Sync + 'static,
{
    type Data = Option<D>;

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>> {
        if let Some(d) = data {
            self.0.send(d)
        } else {
            Box::pin(futures_util::future::ready(Ok(()))) as _
        }
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>> {
        let c = Arc::clone(&self.0);
        Box::pin(async move {
            let d = c.recv().await;
            Ok(Some(d?))
        })
    }
}

/// Chunnel combinator for working with Option types.
///
/// Deals with inner chunnel that has Data = Option<T> by transforming None into an error.
#[derive(Debug, Default, Clone)]
pub struct OptionUnwrap;

impl crate::negotiate::Negotiate for OptionUnwrap {
    type Capability = ();
    fn capabilities() -> Vec<Self::Capability> {
        vec![]
    }
}

impl<D, InS, InC, InE> Serve<InS> for OptionUnwrap
where
    InS: Stream<Item = Result<InC, InE>> + Send + 'static,
    InC: ChunnelConnection<Data = Option<D>> + Send + Sync + 'static,
    InE: Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Stream, Self::Error>>;
    type Connection = OptionUnwrapCn<InC>;
    type Error = InE;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn serve(&mut self, inner: InS) -> Self::Future {
        ready(Ok(Box::pin(inner.map_ok(|cn| OptionUnwrapCn::new(cn))) as _))
    }
}

impl<D, InC> Client<InC> for OptionUnwrap
where
    InC: ChunnelConnection<Data = Option<D>> + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = OptionUnwrapCn<InC>;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        ready(Ok(OptionUnwrapCn::new(cn)))
    }
}

#[derive(Clone, Debug)]
pub struct OptionUnwrapCn<C>(Arc<C>);

impl<C> OptionUnwrapCn<C> {
    pub fn new(inner: C) -> Self {
        Self(Arc::new(inner))
    }
}

impl<C, D> ChunnelConnection for OptionUnwrapCn<C>
where
    C: ChunnelConnection<Data = Option<D>> + Send + Sync + 'static,
{
    type Data = D;

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>> {
        self.0.send(Some(data))
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>> {
        let c = Arc::clone(&self.0);
        Box::pin(async move {
            Ok(c.recv()
                .await?
                .ok_or_else(|| eyre!("Received None value"))?)
        })
    }
}

/// Chunnel combinator for working with Option types.
///
/// Deals with inner chunnel that has Data = Option<T> by transforming None into an error.
#[derive(Debug, Default, Clone)]
pub struct OptionUnwrapProj;

impl crate::negotiate::Negotiate for OptionUnwrapProj {
    type Capability = ();
    fn capabilities() -> Vec<Self::Capability> {
        vec![]
    }
}

impl<A, D, InS, InC, InE> Serve<InS> for OptionUnwrapProj
where
    InS: Stream<Item = Result<InC, InE>> + Send + 'static,
    InC: ChunnelConnection<Data = (A, Option<D>)> + Send + Sync + 'static,
    InE: Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Stream, Self::Error>>;
    type Connection = OptionUnwrapProjCn<InC>;
    type Error = InE;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn serve(&mut self, inner: InS) -> Self::Future {
        ready(Ok(
            Box::pin(inner.map_ok(|cn| OptionUnwrapProjCn::new(cn))) as _
        ))
    }
}

impl<A, D, InC> Client<InC> for OptionUnwrapProj
where
    InC: ChunnelConnection<Data = (A, Option<D>)> + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = OptionUnwrapProjCn<InC>;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        ready(Ok(OptionUnwrapProjCn::new(cn)))
    }
}

#[derive(Clone, Debug)]
pub struct OptionUnwrapProjCn<C>(Arc<C>);

impl<C> OptionUnwrapProjCn<C> {
    pub fn new(inner: C) -> Self {
        Self(Arc::new(inner))
    }
}

impl<A, C, D> ChunnelConnection for OptionUnwrapProjCn<C>
where
    C: ChunnelConnection<Data = (A, Option<D>)> + Send + Sync + 'static,
{
    type Data = (A, D);

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>> {
        let (addr, data) = data;
        self.0.send((addr, Some(data)))
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>> {
        let c = Arc::clone(&self.0);
        Box::pin(async move {
            let (addr, data) = c.recv().await?;
            Ok((addr, data.ok_or_else(|| eyre!("Received None value"))?))
        })
    }
}

/// Chunnel translating between passing Address with Data and passing address in `connect()`.
///
/// Start with Address = (), Data = (Address, Data), produce Address = Address, Data = Data: by
/// remembering the Address via connect().
#[derive(Debug)]
pub struct AddrWrap<A, C>(C, std::marker::PhantomData<A>);

impl<A, C> AddrWrap<A, C> {
    pub fn new(inner: C) -> Self {
        Self(inner, Default::default())
    }
}

impl<A, C> From<C> for AddrWrap<A, C> {
    fn from(f: C) -> Self {
        AddrWrap::new(f)
    }
}

impl<A, C: Clone> Clone for AddrWrap<A, C> {
    fn clone(&self) -> Self {
        self.0.clone().into()
    }
}

impl<A, C, D> ChunnelConnector for AddrWrap<A, C>
where
    A: Clone + Send + 'static,
    C: ChunnelConnector<Addr = ()> + Clone + Send + Sync + 'static,
    <C as ChunnelConnector>::Connection: ChunnelConnection<Data = (A, D)> + Send + Sync + 'static,
    <C as ChunnelConnector>::Error: Send + Sync + 'static,
{
    type Addr = A;
    type Connection = AddrWrapCn<A, <C as ChunnelConnector>::Connection>;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = <C as ChunnelConnector>::Error;

    fn connect(&mut self, a: Self::Addr) -> Self::Future {
        let mut c = self.0.clone();
        Box::pin(async move {
            let cn = c.connect(()).await?;
            Ok(AddrWrapCn::new(a, cn))
        })
    }
}

#[derive(Clone, Debug)]
pub struct AddrWrapCn<A, C>(A, Arc<C>);

impl<A, C> AddrWrapCn<A, C> {
    pub fn new(addr: A, inner: C) -> Self {
        Self(addr, Arc::new(inner))
    }
}

impl<A, C, D> ChunnelConnection for AddrWrapCn<A, C>
where
    A: Clone,
    C: ChunnelConnection<Data = (A, D)> + Send + Sync + 'static,
{
    type Data = D;

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>> {
        self.1.send((self.0.clone(), data))
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>> {
        let c = Arc::clone(&self.1);
        Box::pin(async move { Ok(c.recv().await?.1) })
    }
}

/// For testing-assertion purposes, a chunnel that errors if send() is called or inner.recv()
/// returns data.
#[derive(Debug, Clone, Default)]
pub struct Never;

impl<InS, InC, InE> Serve<InS> for Never
where
    InS: Stream<Item = Result<InC, InE>> + Send + 'static,
    InC: ChunnelConnection + Send + Sync + 'static,
    InE: Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Stream, Self::Error>>;
    type Connection = NeverCn<InC>;
    type Error = InE;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn serve(&mut self, inner: InS) -> Self::Future {
        ready(Ok(Box::pin(inner.map_ok(|cn| NeverCn::new(cn))) as _))
    }
}

impl<D, InC> Client<InC> for Never
where
    InC: ChunnelConnection<Data = D> + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = NeverCn<InC>;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        ready(Ok(NeverCn::new(cn)))
    }
}

#[derive(Clone, Debug)]
pub struct NeverCn<C>(Arc<C>);

impl<C> NeverCn<C> {
    pub fn new(inner: C) -> Self {
        Self(Arc::new(inner))
    }
}

impl<C, D> ChunnelConnection for NeverCn<C>
where
    C: ChunnelConnection<Data = D> + Send + Sync + 'static,
{
    type Data = D;

    fn send(
        &self,
        _: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>> {
        Box::pin(async move { Err(eyre!("No sends allowed on Never Chunnel")) })
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>> {
        let c = Arc::clone(&self.0);
        Box::pin(async move {
            let _ = c.recv().await?;
            Err(eyre!("Unexpected recv in Never chunnel"))
        })
    }
}
