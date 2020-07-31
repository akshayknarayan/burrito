//! Helper traits to avoid re-writing common Chunnel implementation code for simple Chunnels.

use super::{ChunnelConnection, ChunnelConnector, ChunnelListener, Endedness, Scope};
use futures_util::stream::Stream;
use std::future::Future;
use std::pin::Pin;

/// A standard way to access the downstream chunnel.
///
/// Be careful: Most implementors which contain Arcs will need to do a deep clone to implement
/// context_mut().
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
    <C as InheritChunnel<<<C as Context>::ChunnelType as ChunnelListener>::Connection>>::Connection:
        'static,
{
    type Addr = <<C as Context>::ChunnelType as ChunnelListener>::Addr;
    type Connection = C::Connection;
    type Error = <<C as Context>::ChunnelType as ChunnelListener>::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn listen(&mut self, a: Self::Addr) -> Self::Future {
        use futures_util::StreamExt;

        let f = self.context_mut().listen(a);
        let cfg = self.get_config();
        Box::pin(async move {
            let cfg = cfg;
            let conn_stream = f.await?;
            Ok(Box::pin(conn_stream.map(move |conn| {
                let cfg = cfg.clone();
                Ok(C::make_connection(conn?, cfg))
            })) as _)
        })
    }

    fn scope() -> Scope {
        <C as Context>::ChunnelType::scope()
    }

    fn endedness() -> Endedness {
        <C as Context>::ChunnelType::endedness()
    }

    fn implementation_priority() -> usize {
        <C as Context>::ChunnelType::implementation_priority()
    }
    // fn resource_requirements(&self) -> ?;
}

impl<C> ChunnelConnector for C
where
    C: Context,
    <C as Context>::ChunnelType: ChunnelConnector,
    <<C as Context>::ChunnelType as ChunnelConnector>::Connection: 'static,
    C: InheritChunnel<<<C as Context>::ChunnelType as ChunnelConnector>::Connection>,
    <C as InheritChunnel<<<C as Context>::ChunnelType as ChunnelConnector>::Connection>>::Connection:
        'static,
{
    type Addr = <<C as Context>::ChunnelType as ChunnelConnector>::Addr;
    type Connection = C::Connection;
    type Error = <<C as Context>::ChunnelType as ChunnelConnector>::Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn connect(&mut self, a: Self::Addr) -> Self::Future {
        let f = self.context_mut().connect(a);
        let cfg = self.get_config();
        Box::pin(async move { Ok(C::make_connection(f.await?, cfg)) })
    }

    fn scope() -> Scope {
        <C as Context>::ChunnelType::scope()
    }

    fn endedness() -> Endedness {
        <C as Context>::ChunnelType::endedness()
    }

    fn implementation_priority() -> usize {
        <C as Context>::ChunnelType::implementation_priority()
    }
    // fn resource_requirements(&self) -> ?;
}
