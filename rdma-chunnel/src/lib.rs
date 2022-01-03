//! Demikernel-powered RDMA chunnel.

use bertha::{ChunnelConnection, ChunnelConnector, ChunnelListener};
use color_eyre::eyre::{eyre, Report, WrapErr};
use futures_util::future::{ready, Ready};
use futures_util::stream::Stream;
use io_queue_rdma::{IoQueue, QueueDescriptor};
use nix::sys::socket::{InetAddr, SockAddr};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex};
use std::task::{Context, Poll};
use tracing::{debug, warn};

#[derive(Clone, Debug)]
pub struct RdmaChunnel {}

impl RdmaChunnel {
    pub fn new(config: impl AsRef<Path>) -> Self {
        todo!()
    }
}

impl ChunnelListener for RdmaChunnel {
    type Addr = SocketAddr;
    type Connection = RdmaConnSrv;
    type Future = Ready<Result<Self::Stream, Self::Error>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Report;

    fn listen(&mut self, addr: Self::Addr) -> Self::Future {
        let address = SockAddr::new_inet(InetAddr::from_std("0.0.0.0".parse()?));
        Box::pin(async move {
            let mut libos = IoQueue::new().set_async();
            // Setup connection.
            let mut listening_qd: QueueDescriptor = libos.socket();
            libos.bind(&mut listening_qd, &address).unwrap();
            libos.listen(&mut listening_qd);
            Ok(RdmaListen {
                listening_qd,
                libos,
            })
        })
    }
}

/// a stream of incoming rdma connections
pub struct RdmaListen {
    listening_qd: QueueDescriptor,
    libos: IoQueue,
}

impl Stream for RdmaListen {
    type Item = Result<RdmaConnSrv, Report>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let fut = self.libos.accept(&mut self.listening_qd);
        let conn_qd = futures_util::poll!(fut);
        let conn_srv_inner = RdmaConnSrvInner {
            conn: conn_qd,
            libos: self.libos,
        };
    }
}

struct RdmaConnSrvInner {
    conn: QueueDescriptor,
    libos: IoQueue,
}

impl RdmaConnSrvInner {
    fn start(self) -> RdmaConnSrv {
        let RdmaConnSrvInner { conn, libos } = self;
        let (incoming_s, incoming_r) = flume::unbounded();
        let (outgoing_s, outgoing_r) = flume::unbounded();
        // recv thread
        std::thread::spawn(move || {
            let recv_qt = libos.pop(&mut conn);
        });
    }
}

pub struct RdmaConnSrv {}

impl ChunnelConnection for RdmaConnSrv {
    type Data = Vec<u8>;

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        let inner = self.inner.clone();
        let inner_g = inner.lock().unwrap();
        let qt = inner_g.libos.pop(&mut inner_g.conn);
        inner.libos.wait_any(&[qt]);
    }

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        todo!();
    }
}

impl ChunnelConnector for RdmaChunnel {
    type Addr = ();
    type Connection = RdmaConn;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Report;

    fn connect(&mut self, _a: Self::Addr) -> Self::Future {
        todo!()
    }
}
