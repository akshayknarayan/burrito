//! Tools for working with Chunnels.

use futures_util::stream::Stream;
use std::future::Future;
use std::pin::Pin;

pub mod bincode;
pub mod chan_transport;
pub mod either;
pub mod inherit;
pub mod reliable;
pub mod tagger;
pub mod udp;
pub mod util;

pub use either::*;
use inherit::*;
use util::*;

/// `ChunnelListener`s are used to get a stream of incoming connections.
pub trait ChunnelListener {
    type Future: Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static;
    type Addr;
    type Connection: ChunnelConnection + 'static;
    type Error: Send + Sync + 'static;
    type Stream: Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static;

    fn listen(&mut self, a: Self::Addr) -> Self::Future;

    fn scope() -> Scope;
    fn endedness() -> Endedness;
    fn implementation_priority() -> usize;
    // fn resource_requirements(&self) -> ?;
}

/// `ChunnelConnector`s connect to a single remote Chunnel endpoint and return one connection.
pub trait ChunnelConnector {
    type Future: Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static;
    type Addr;
    type Connection: ChunnelConnection + 'static;
    type Error: Send + Sync + 'static;

    fn connect(&mut self, a: Self::Addr) -> Self::Future;

    fn scope() -> Scope;
    fn endedness() -> Endedness;
    fn implementation_priority() -> usize;
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

pub struct NegotiatorChunnel<T1, T2>(T1, T2);

impl<A, E, T1, C1, T2, C2, D> ChunnelListener for NegotiatorChunnel<T1, T2>
where
    T1: ChunnelListener<Addr = A, Error = E, Connection = C1>,
    T2: ChunnelListener<Addr = A, Error = E, Connection = C2>,
    C1: ChunnelConnection<Data = D> + 'static,
    C2: ChunnelConnection<Data = D> + 'static,
    E: Send + Sync + 'static,
{
    type Addr = A;
    type Connection = Either<T1::Connection, T2::Connection>;
    type Error = E;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn listen(&mut self, a: Self::Addr) -> Self::Future {
        let use_t1 = match (T1::scope(), T2::scope()) {
            (Scope::Application, _) => true,
            (_, Scope::Application) => false,
            (Scope::Host, _) => true,
            (_, Scope::Host) => false,
            (Scope::Local, _) => true,
            (_, Scope::Local) => false,
            (Scope::Global, _) => true,
        };

        use futures_util::TryStreamExt;

        if use_t1 {
            let fut = self.0.listen(a);
            Box::pin(async move {
                Ok::<_, E>(Box::pin(fut.await?.map_ok(|c| Either::Left(c)))
                    as Pin<
                        Box<
                            dyn Stream<Item = Result<Self::Connection, Self::Error>>
                                + Send
                                + 'static,
                        >,
                    >)
            }) as _
        } else {
            let fut = self.1.listen(a);
            Box::pin(async move {
                Ok::<_, E>(Box::pin(fut.await?.map_ok(|c| Either::Right(c)))
                    as Pin<
                        Box<
                            dyn Stream<Item = Result<Self::Connection, Self::Error>>
                                + Send
                                + 'static,
                        >,
                    >)
            }) as _
        }
    }

    fn scope() -> Scope {
        unimplemented!()
    }
    fn endedness() -> Endedness {
        unimplemented!()
    }
    fn implementation_priority() -> usize {
        unimplemented!()
    }
}

impl<A, E, T1, C1, T2, C2, D> ChunnelConnector for NegotiatorChunnel<T1, T2>
where
    T1: ChunnelConnector<Addr = A, Error = E, Connection = C1>,
    T2: ChunnelConnector<Addr = A, Error = E, Connection = C2>,
    C1: ChunnelConnection<Data = D> + 'static,
    C2: ChunnelConnection<Data = D> + 'static,
    E: Send + Sync + 'static,
{
    type Addr = A;
    type Connection = Either<T1::Connection, T2::Connection>;
    type Error = E;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn connect(&mut self, a: Self::Addr) -> Self::Future {
        let use_t1 = match (T1::scope(), T2::scope()) {
            (Scope::Application, _) => true,
            (_, Scope::Application) => false,
            (Scope::Host, _) => true,
            (_, Scope::Host) => false,
            (Scope::Local, _) => true,
            (_, Scope::Local) => false,
            (Scope::Global, _) => true,
        };

        if use_t1 {
            let fut = self.0.connect(a);
            Box::pin(async move { Ok(Either::Left(fut.await?)) }) as _
        } else {
            let fut = self.1.connect(a);
            Box::pin(async move { Ok(Either::Right(fut.await?)) }) as _
        }
    }

    fn scope() -> Scope {
        unimplemented!()
    }
    fn endedness() -> Endedness {
        unimplemented!()
    }
    fn implementation_priority() -> usize {
        unimplemented!()
    }
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

/// Where the Chunnel implementation allows functionality to be implemented.
#[derive(Debug, Clone, PartialEq, Eq)]
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
