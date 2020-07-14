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
                Output = Pin<Box<dyn Stream<Item = Result<Self::Connection, eyre::Report>>>>,
            >,
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
    ) -> Pin<Box<dyn Future<Output = Result<Self::Connection, eyre::Report>>>>;

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
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + Sync>>;

    /// Retrieve next incoming message.
    fn recv(&self)
        -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + Sync>>;
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
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + Sync>> {
        match self {
            Either::Left(a) => a.send(data),
            Either::Right(b) => b.send(data),
        }
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + Sync>> {
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
    fn context_mut(&mut self) -> Option<&mut Self::ChunnelType>;
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
                Output = Pin<Box<dyn Stream<Item = Result<Self::Connection, eyre::Report>>>>,
            >,
        >,
    > {
        use futures_util::StreamExt;
        let f = self
            .context_mut()
            .expect("There were multiple references to the Arc<Context>")
            .listen(a);
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
    ) -> Pin<Box<dyn Future<Output = Result<Self::Connection, eyre::Report>>>> {
        let f = self
            .context_mut()
            .expect("There were multiple references to the Arc<Context>")
            .connect(a);
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

/// Dummy connection type for non-connection-oriented chunnels.
///
/// Exposes each message in the stream as a "connection". Receive-only.
pub struct Once<D>(Arc<Mutex<Option<D>>>);

impl<D> Once<D> {
    pub fn new(d: D) -> Self {
        Self(Arc::new(Mutex::new(Some(d))))
    }
}

impl<D> ChunnelConnection for Once<D>
where
    D: Send + Sync + 'static,
{
    type Data = Option<D>;

    fn send(
        &self,
        _data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + Sync>> {
        unimplemented!()
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + Sync>> {
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

impl<C> Context for OptionWrap<C> {
    type ChunnelType = C;

    fn context(&self) -> &Self::ChunnelType {
        &self.0
    }

    fn context_mut(&mut self) -> Option<&mut Self::ChunnelType> {
        Arc::get_mut(&mut self.0)
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
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + Sync>> {
        if let Some(d) = data {
            self.0.send(d)
        } else {
            Box::pin(futures_util::future::ready(Ok(()))) as _
        }
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + Sync>> {
        let c = Arc::clone(&self.0);
        Box::pin(async move {
            let d = c.recv().await;
            Ok(Some(d?))
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
