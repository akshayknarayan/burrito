use eyre::eyre;
use futures_util::stream::Stream;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;

pub mod bincode;
pub mod chan_transport;
pub mod reliable;
pub mod tagger;
pub mod udp;

/// A specification of application network semantics.
pub trait ChunnelListener {
    type Addr;
    type Connection: ChunnelConnection;

    fn listen(
        &mut self,
        a: Self::Addr,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Pin<
                        Box<
                            dyn Stream<Item = Result<Self::Connection, eyre::Report>>
                                + Send
                                + 'static,
                        >,
                    >,
                > + Send
                + 'static,
        >,
    >;

    fn scope(&self) -> Scope;
    fn endedness(&self) -> Endedness;
    fn implementation_priority(&self) -> usize;
    // fn resource_requirements(&self) -> ?;
}

pub trait ChunnelConnector {
    type Addr;
    type Connection: ChunnelConnection;

    fn connect(
        &mut self,
        a: Self::Addr,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Connection, eyre::Report>> + Send + 'static>>;

    fn scope(&self) -> Scope;
    fn endedness(&self) -> Endedness;
    fn implementation_priority(&self) -> usize;
    // fn resource_requirements(&self) -> ?;
}

/// A connection with the semantics of the Chunnel type's functionality.
pub trait ChunnelConnection {
    type Data;

    /// Send a message
    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>>;

    /// Retrieve next incoming message.
    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>>;
}

pub enum Either<A, B> {
    Left(A),
    Right(B),
}

impl<A, B, D> ChunnelConnection for Either<A, B>
where
    A: ChunnelConnection<Data = D>,
    B: ChunnelConnection<Data = D>,
{
    type Data = D;

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>> {
        match self {
            Either::Left(a) => a.send(data),
            Either::Right(b) => b.send(data),
        }
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>> {
        match self {
            Either::Left(a) => a.recv(),
            Either::Right(b) => b.recv(),
        }
    }
}

/// A standard way to access the downstream chunnel.
pub trait Context {
    type ChunnelType;
    fn context(&self) -> &Self::ChunnelType;
    fn context_mut(&mut self) -> &mut Self::ChunnelType;
}

/// A simpler trait to implement for traits with simpler (or no) Chunnel setup.
pub trait InheritChunnel<CxConn: ChunnelConnection>: Context {
    type Connection: ChunnelConnection;
    type Config: Clone + Send + Sync + 'static;

    fn get_config(&mut self) -> Self::Config;
    fn make_connection(cx: CxConn, cfg: Self::Config) -> Self::Connection;
}

impl<C> ChunnelListener for C
where
    C: Context,
    <C as Context>::ChunnelType: ChunnelListener,
    <<C as Context>::ChunnelType as ChunnelListener>::Connection: 'static,
    C: InheritChunnel<<<C as Context>::ChunnelType as ChunnelListener>::Connection>,
{
    type Addr = <<C as Context>::ChunnelType as ChunnelListener>::Addr;
    type Connection = C::Connection;

    fn listen(
        &mut self,
        a: Self::Addr,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Pin<
                        Box<
                            dyn Stream<Item = Result<Self::Connection, eyre::Report>>
                                + Send
                                + 'static,
                        >,
                    >,
                > + Send
                + 'static,
        >,
    > {
        use futures_util::StreamExt;

        let f = self.context_mut().listen(a);
        let cfg = self.get_config();
        Box::pin(async move {
            let cfg = cfg;
            let conn_stream = f.await;
            Box::pin(conn_stream.map(move |conn| {
                let cfg = cfg.clone();
                Ok(C::make_connection(conn?, cfg))
            })) as _
        })
    }

    fn scope(&self) -> Scope {
        self.context().scope()
    }

    fn endedness(&self) -> Endedness {
        self.context().endedness()
    }

    fn implementation_priority(&self) -> usize {
        self.context().implementation_priority()
    }
    // fn resource_requirements(&self) -> ?;
}

impl<C> ChunnelConnector for C
where
    C: Context,
    <C as Context>::ChunnelType: ChunnelConnector,
    <<C as Context>::ChunnelType as ChunnelConnector>::Connection: 'static,
    C: InheritChunnel<<<C as Context>::ChunnelType as ChunnelConnector>::Connection>,
{
    type Addr = <<C as Context>::ChunnelType as ChunnelConnector>::Addr;
    type Connection = C::Connection;

    fn connect(
        &mut self,
        a: Self::Addr,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Connection, eyre::Report>> + Send + 'static>>
    {
        let f = self.context_mut().connect(a);
        let cfg = self.get_config();
        Box::pin(async move { Ok(C::make_connection(f.await?, cfg)) })
    }

    fn scope(&self) -> Scope {
        self.context().scope()
    }

    fn endedness(&self) -> Endedness {
        self.context().endedness()
    }

    fn implementation_priority(&self) -> usize {
        self.context().implementation_priority()
    }
    // fn resource_requirements(&self) -> ?;
}

/// For address types to expose ip and port information for inner addresses.
pub trait IpPort {
    fn ip(&self) -> std::net::IpAddr;
    fn port(&self) -> u16;
}

impl IpPort for std::net::SocketAddr {
    fn ip(&self) -> std::net::IpAddr {
        self.ip()
    }

    fn port(&self) -> u16 {
        self.port()
    }
}

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

impl<A, C, N, D> ChunnelConnector for AddrWrap<C>
where
    A: Clone + Send + 'static,
    C: ChunnelConnector<Addr = (), Connection = N> + Clone + Send + Sync + 'static,
    N: ChunnelConnection<Data = (A, D)> + Send + Sync + 'static,
    D: Send + 'static,
{
    type Addr = A;
    type Connection = AddrWrapCn<A, N>;

    fn connect(
        &mut self,
        a: Self::Addr,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Connection, eyre::Report>> + Send + 'static>>
    {
        let mut c = self.0.clone();
        Box::pin(async move {
            let cn = c.connect(()).await?;
            Ok(AddrWrapCn::new(a, cn))
        })
    }

    fn scope(&self) -> Scope {
        self.0.scope()
    }
    fn endedness(&self) -> Endedness {
        self.0.endedness()
    }
    fn implementation_priority(&self) -> usize {
        self.0.implementation_priority()
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

/// Where the Chunnel implementation allows functionality to be implemented.
pub enum Scope {
    /// Must be inside the application.
    Application,
    /// Anywhere on the same host.
    Host,
    /// Anywhere in the local network.
    Local,
    /// Anywhere.
    Global,
}

/// Semantics of the Chunnel.
pub enum Endedness {
    /// Chunnel uses wrap/unwrap semantics
    Both,
    /// Chunnel doesn't change semantics
    Either,
}

//pub fn register_chunnel<A, C>(name: &str, factory: impl Fn(A) -> C, endpt: Endedness, sc: Scope)
//where
//    C: Chunnel,
//{
//    unimplemented!();
//}
