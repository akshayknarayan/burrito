//! Helper Chunnel types to transform Data types, etc.

use super::{ChunnelConnection, ChunnelConnector, Context, Endedness, InheritChunnel, Scope};
use eyre::eyre;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Dummy connection type for non-connection-oriented chunnels.
///
/// Exposes each message in the stream as a "connection". Sends via the C chunnel type.
pub struct Once<C, D>(Arc<Mutex<Option<D>>>, Arc<C>);

impl<C, D> Once<C, D> {
    pub fn new(send: Arc<C>, d: D) -> Self {
        Self(Arc::new(Mutex::new(Some(d))), send)
    }
}

impl<C, D> ChunnelConnection for Once<C, D>
where
    C: ChunnelConnection<Data = D>,
    D: Send + Sync + 'static,
{
    type Data = Option<D>;

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>> {
        if let Some(d) = data {
            self.1.send(d)
        } else {
            Box::pin(futures_util::future::ready(Err(eyre!("Can't send None")))) as _
        }
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>> {
        let d = Arc::clone(&self.0);
        Box::pin(async move { Ok(d.lock().await.take()) })
    }
}

/// Chunnel type transposing the Data type of the underlying connection
/// to be `Option`-wrapped.
pub struct OptionWrap<C>(Arc<C>);

impl<C> OptionWrap<C> {
    pub fn new(inner: C) -> Self {
        Self(Arc::new(inner))
    }
}

impl<C> From<C> for OptionWrap<C> {
    fn from(f: C) -> OptionWrap<C> {
        OptionWrap::new(f)
    }
}

impl<C> Context for OptionWrap<C> {
    type ChunnelType = C;

    fn context(&self) -> &Self::ChunnelType {
        &self.0
    }

    fn context_mut(&mut self) -> &mut Self::ChunnelType {
        Arc::get_mut(&mut self.0).unwrap()
    }
}

impl<B, C, D> InheritChunnel<C> for OptionWrap<B>
where
    C: ChunnelConnection<Data = D> + Send + Sync + 'static,
{
    type Connection = OptionWrapCn<C>;
    type Config = ();

    fn get_config(&mut self) -> Self::Config {}

    fn make_connection(cx: C, _cfg: Self::Config) -> Self::Connection {
        OptionWrapCn::new(cx)
    }
}

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
pub struct OptionUnwrap<C>(Arc<C>);

impl<C> OptionUnwrap<C> {
    pub fn new(inner: C) -> Self {
        Self(Arc::new(inner))
    }
}

impl<C> From<C> for OptionUnwrap<C> {
    fn from(f: C) -> OptionUnwrap<C> {
        OptionUnwrap::new(f)
    }
}

impl<C: Clone> Clone for OptionUnwrap<C> {
    fn clone(&self) -> Self {
        Self(Arc::new(self.0.as_ref().clone()))
    }
}

impl<C> Context for OptionUnwrap<C> {
    type ChunnelType = C;

    fn context(&self) -> &Self::ChunnelType {
        &self.0
    }

    fn context_mut(&mut self) -> &mut Self::ChunnelType {
        // this is ok because we never clone the Arc. The Clone impl does a deep-clone instead.
        Arc::get_mut(&mut self.0).unwrap()
    }
}

impl<B, C, D> InheritChunnel<C> for OptionUnwrap<B>
where
    C: ChunnelConnection<Data = Option<D>> + Send + Sync + 'static,
{
    type Connection = OptionUnwrapCn<C>;
    type Config = ();

    fn get_config(&mut self) -> Self::Config {}

    fn make_connection(cx: C, _cfg: Self::Config) -> Self::Connection {
        OptionUnwrapCn::new(cx)
    }
}

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

/// Chunnel translating between passing Address with Data and passing address in `connect()`.
///
/// Start with Address = (), Data = (Address, Data), produce Address = Address, Data = Data: by remembering the Address
/// via connect().
pub struct AddrWrap<C>(C);
impl<C> AddrWrap<C> {
    pub fn new(inner: C) -> Self {
        Self(inner)
    }
}

impl<C> From<C> for AddrWrap<C> {
    fn from(f: C) -> AddrWrap<C> {
        AddrWrap::new(f)
    }
}

impl<C: Clone> Clone for AddrWrap<C> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<A, C, E, N, D> ChunnelConnector for AddrWrap<C>
where
    A: Clone + Send + 'static,
    C: ChunnelConnector<Addr = (), Connection = N, Error = E> + Clone + Send + Sync + 'static,
    N: ChunnelConnection<Data = (A, D)> + Send + Sync + 'static,
    D: Send + 'static,
    E: Send + Sync + 'static,
{
    type Addr = A;
    type Connection = AddrWrapCn<A, N>;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = E;

    fn connect(&mut self, a: Self::Addr) -> Self::Future {
        let mut c = self.0.clone();
        Box::pin(async move {
            let cn = c.connect(()).await?;
            Ok(AddrWrapCn::new(a, cn))
        })
    }

    fn scope() -> Scope {
        C::scope()
    }
    fn endedness() -> Endedness {
        C::endedness()
    }
    fn implementation_priority() -> usize {
        C::implementation_priority()
    }
}

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
pub struct Never<C>(Arc<C>);

impl<C> Never<C> {
    pub fn new(inner: C) -> Self {
        Self(Arc::new(inner))
    }
}

impl<C> From<C> for Never<C> {
    fn from(f: C) -> Never<C> {
        Never::new(f)
    }
}

impl<C: Clone> Clone for Never<C> {
    fn clone(&self) -> Self {
        Self(Arc::new(self.0.as_ref().clone()))
    }
}

impl<C> Context for Never<C> {
    type ChunnelType = C;

    fn context(&self) -> &Self::ChunnelType {
        &self.0
    }

    fn context_mut(&mut self) -> &mut Self::ChunnelType {
        Arc::get_mut(&mut self.0).unwrap()
    }
}

impl<B, C, D> InheritChunnel<C> for Never<B>
where
    C: ChunnelConnection<Data = D> + Send + Sync + 'static,
{
    type Connection = NeverCn<C>;
    type Config = ();

    fn get_config(&mut self) -> Self::Config {}

    fn make_connection(cx: C, _cfg: Self::Config) -> Self::Connection {
        NeverCn::new(cx)
    }
}

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