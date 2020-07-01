//! Implementation of a plain tokio TCP connection as a `Chunnel`.

use crate::{Chunnel, Endedness, Scope};
use futures_util::stream::Stream;
use std::future::Future;
use std::pin::Pin;
use tokio::net::{TcpListener, TcpStream};

pub struct Tcp {}

impl Chunnel for Tcp {
    type Addr = std::net::SocketAddr;
    type Connection = TcpStream;

    fn with_context<C: Chunnel>(&mut self, cx: C) -> &mut Self {
        self
    }

    fn init(&mut self) {}

    fn teardown(&mut self) {}

    fn scope(&self) -> Scope {
        Scope::Application
    }

    fn endedness(&self) -> Endedness {
        Endedness::Both
    }

    fn implementation_priority(&self) -> usize {
        1
    }

    fn listen(
        &mut self,
        a: Self::Addr,
    ) -> Pin<Box<dyn Future<Output = Box<dyn Stream<Item = Self::Connection>>>>> {
        use futures_util::stream::StreamExt;
        Box::pin(
            async move { Box::new(TcpListener::bind(a).await.unwrap().map(|i| i.unwrap())) as _ },
        )
    }

    fn connect(&mut self, a: Self::Addr) -> Pin<Box<dyn Future<Output = Self::Connection>>> {
        Box::pin(async move { TcpStream::connect(a).await.unwrap() })
    }
}
