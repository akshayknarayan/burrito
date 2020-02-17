//! Flatbuffer format for burrito schema.
//!
//! TODO move to tokio-tower via [`tokio-codec`](https://docs.rs/tokio-util/0.2.0/tokio_util/codec/index.html)
//! Currently this is done manually.

use burrito_flatbuf::{ListenReply, ListenRequest, OpenReply, OpenRequest};
use core::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::trace;

#[derive(Clone)]
pub struct Client {
    burrito_root: PathBuf,
    st: Arc<Mutex<tokio::net::UnixStream>>,
    buf: burrito_flatbuf::FlatBufferBuilder<'static>,
}

impl Client {
    #[tracing::instrument(level = "debug", skip(burrito_root))]
    pub async fn new(burrito_root: impl AsRef<Path>) -> Result<Self, failure::Error> {
        let burrito_ctl_addr = burrito_root.as_ref().join(burrito_ctl::CONTROLLER_ADDRESS);
        let burrito_root = burrito_root.as_ref().to_path_buf();

        trace!(
            ?burrito_root,
            ?burrito_ctl_addr,
            "burrito-addr connecting to burrito-ctl",
        );
        let controller_addr = burrito_root.join(burrito_ctl::burrito_net::CONTROLLER_ADDRESS);

        // connect to burrito-ctl
        //
        // The Arc<Mutex<_>> is only necessary because of the implementation of hyper::Service.
        // If we had existential types (https://github.com/rust-lang/rust/issues/63063), we could
        // instead write impl Future<Output=_> without having to Box::pin.
        let st = Arc::new(Mutex::new(
            tokio::net::UnixStream::connect(controller_addr).await?,
        ));
        trace!("burrito-addr connected to burrito-ctl");

        Ok(Self {
            burrito_root,
            st,
            buf: burrito_flatbuf::FlatBufferBuilder::new_with_capacity(128),
        })
    }

    pub async fn resolve(&mut self, dst: hyper::Uri) -> Result<crate::Addr, failure::Error> {
        let dst_addr = crate::Uri::socket_path(&dst)?;
        trace!(addr = ?&dst_addr, "Resolving_burrito_address");
        let msg = OpenRequest { dst_addr };
        msg.onto(&mut self.buf);
        let msg = self.buf.finished_data();

        let mut st = self.st.lock().await;

        burrito_util::write_msg(&mut *st, Some(burrito_flatbuf::OPEN_REQUEST as u32), msg).await?;

        let mut buf = [0u8; 128];
        let (len, msg_type) = burrito_util::read_msg_with_type(&mut *st, &mut buf).await?;
        let buf = &buf[..len];

        let msg = match msg_type as usize {
            burrito_flatbuf::OPEN_REPLY => OpenReply::from(buf),
            _ => unreachable!(),
        };

        trace!(resolved_addr = ?&msg, "Resolved_burrito_address");

        Ok(match msg {
            OpenReply::Unix(addr) => crate::Addr::Unix(
                self.burrito_root
                    .join(addr)
                    .into_os_string()
                    .into_string()
                    .expect("OS string as valid string"),
            ),
            OpenReply::Tcp(addr) => crate::Addr::Tcp(addr),
        })
    }
}

impl hyper::service::Service<hyper::Uri> for Client {
    type Response = crate::Conn;
    type Error = failure::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, failure::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, dst: hyper::Uri) -> Self::Future {
        let mut cl = self.clone();
        Box::pin(async move { cl.resolve(dst).await?.connect().await })
    }
}

pub struct Server {
    tl: tokio::net::TcpListener,
    ul: tokio::net::UnixListener,
}

impl Server {
    #[tracing::instrument(level = "debug", skip(burrito_root))]
    pub async fn start(
        service_addr: &str,
        port: u16,
        burrito_root: impl AsRef<Path>,
    ) -> Result<Self, failure::Error> {
        let burrito_root = burrito_root.as_ref();
        let burrito_ctl_addr = burrito_root.join(burrito_ctl::CONTROLLER_ADDRESS);
        trace!(addr = ?&burrito_ctl_addr, root = ?&burrito_root, "Connecting to burrito-ctl");

        // ask local burrito-ctl where to listen
        let mut st = tokio::net::UnixStream::connect(burrito_ctl_addr).await?;
        trace!("Connected to burrito-ctl");

        // do listen() rpc
        let mut buf = burrito_flatbuf::FlatBufferBuilder::new_with_capacity(128);
        let msg = ListenRequest {
            service_addr: service_addr.to_string(),
            port,
        };

        msg.onto(&mut buf);
        let msg = buf.finished_data();
        burrito_util::write_msg(&mut st, Some(burrito_flatbuf::LISTEN_REQUEST as u32), msg).await?;

        let mut buf = [0u8; 128];
        let (len, msg_type) = burrito_util::read_msg_with_type(&mut st, &mut buf).await?;
        let buf = &buf[..len];

        let msg = match msg_type as usize {
            burrito_flatbuf::LISTEN_REPLY => ListenReply::from(buf),
            _ => unreachable!(),
        };

        let listen_addr = msg.addr;

        trace!(addr = ?&listen_addr, root = ?&burrito_root, "Got listening address");

        let tl = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
        let ul = tokio::net::UnixListener::bind(burrito_root.join(listen_addr))?;
        Ok(Self { tl, ul })
    }
}

impl hyper::server::accept::Accept for Server {
    type Conn = crate::Conn;
    type Error = failure::Error;

    fn poll_accept(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Self::Conn, Self::Error>>> {
        use futures_util::future::{FutureExt, TryFutureExt};
        let this = &mut *self;

        let tf = this.tl.accept().map_ok(|(s, _)| crate::Conn::Tcp(s));
        let uf = this.ul.accept().map_ok(|(s, _)| crate::Conn::Unix(s));

        let mut f = futures_util::future::select(Box::pin(tf), Box::pin(uf))
            .map(|either_stream| either_stream.factor_first().0)
            .map_err(failure::Error::from)
            .map(|f| Some(f));

        Pin::new(&mut f).poll(cx)
    }
}

//type StdError = Box<dyn Error + Send + Sync + 'static>;
//inner: Arc<Mutex<pipeline::Client<UnixTransport, TokioError, OpenRequest>>>,
//let inner = Arc::new(Mutex::new(pipeline::client::Client::new(UnixTransport {
//    st,
//    buf: flatbuffers::FlatBufferBuilder::new_with_capacity(128),
//    curr_msg: None,
//})));
// tokio-tower way
//cl.call(msg)
//    .await
//    .map_err(|e| failure::format_err!("{:?}", e))
//use tokio_tower::pipeline;
//use tower_service::Service;
//
//type TokioError = tokio_tower::Error<UnixTransport, OpenRequest>;
//
//#[derive(Debug)]
//struct UnixTransport {
//    st: tokio::net::UnixStream,
//    buf: flatbuffers::FlatBufferBuilder<'static>,
//    curr_msg: Option<OpenRequest>,
//}
//
//impl futures_sink::Sink<OpenRequest> for UnixTransport {
//    type Error = StdError;
//
//    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
//        if self.curr_msg.is_some() {
//            Poll::Pending
//        } else {
//            Poll::Ready(Ok(()))
//        }
//    }
//
//    fn start_send(mut self: Pin<&mut Self>, item: OpenRequest) -> Result<(), Self::Error> {
//        self.curr_msg = Some(item);
//        Ok(())
//    }
//
//    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
//        use tokio::AsyncWrite;
//
//        let this = &*self;
//
//        // unwrap guaranteed to be ok because of start_send
//        this.curr_msg.unwrap().onto(&mut this.buf);
//        let msg = this.buf.finished_data();
//
//        this.st.poll_write(msg, cx)
//    }
//
//    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
//        Poll::Ready(Ok(()))
//    }
//}
//
//impl futures_util::stream::Stream for UnixTransport {
//    type Item = Result<crate::Addr, StdError>;
//
//    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
//        unimplemented!()
//    }
//}
//
