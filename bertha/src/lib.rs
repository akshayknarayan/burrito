use futures_util::stream::Stream;
use std::future::Future;
use std::pin::Pin;

pub mod bincode;
pub mod chan_transport;
pub mod reliable;
pub mod tagger;

/// A specification of application network semantics.
pub trait Chunnel {
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

    fn connect(
        &mut self,
        a: Self::Addr,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Connection, eyre::Report>>>>;

    fn init(&mut self) {}
    fn teardown(&mut self) {}

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

/// A standard way to access the downstream chunnel.
pub trait Context {
    type ChunnelType;
    fn context(&self) -> &Self::ChunnelType;
    fn context_mut(&mut self) -> Option<&mut Self::ChunnelType>;
}

/// A simpler trait to implement for traits with simpler (or no) Chunnel setup.
pub trait InheritChunnel: Context
where
    <Self as Context>::ChunnelType: Chunnel,
{
    type Connection: ChunnelConnection;
    type Config: Clone + Send + Sync + 'static;

    fn get_config(&mut self) -> Self::Config;

    fn make_connection(
        cx: <<Self as Context>::ChunnelType as Chunnel>::Connection,
        cfg: Self::Config,
    ) -> Self::Connection;
}

impl<C> Chunnel for C
where
    C: InheritChunnel,
    <C as Context>::ChunnelType: Chunnel,
    <<C as Context>::ChunnelType as Chunnel>::Connection: 'static,
{
    type Addr = <<C as Context>::ChunnelType as Chunnel>::Addr;
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

    fn init(&mut self) {
        if let Some(c) = self.context_mut() {
            c.init();
        }
    }

    fn teardown(&mut self) {
        if let Some(c) = self.context_mut() {
            c.teardown();
        }
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
