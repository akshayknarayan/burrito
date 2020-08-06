//! Tools for working with Chunnels.

use futures_util::stream::Stream;
use std::future::Future;
use std::pin::Pin;

pub mod bincode;
pub mod chan_transport;
pub mod either;
pub mod inherit;
pub mod negotiate;
pub mod reliable;
pub mod tagger;
pub mod udp;
pub mod uds;
pub mod util;

pub use either::*;
use inherit::*;
pub use negotiate::*;
use util::*;

/// `ChunnelListener`s are used to get a stream of incoming connections.
pub trait ChunnelListener<D> {
    type Future: Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static;
    type Addr;
    type Connection: ChunnelConnection<Data = D> + 'static;
    type Error: Send + Sync + 'static;
    type Stream: Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static;

    fn listen(&mut self, a: Self::Addr) -> Self::Future;

    fn scope() -> Scope;
    fn endedness() -> Endedness;
    fn implementation_priority() -> usize;
    // fn resource_requirements(&self) -> ?;
}

/// `ChunnelConnector`s connect to a single remote Chunnel endpoint and return one connection.
pub trait ChunnelConnector<D> {
    type Future: Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static;
    type Addr;
    type Connection: ChunnelConnection<Data = D> + 'static;
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
