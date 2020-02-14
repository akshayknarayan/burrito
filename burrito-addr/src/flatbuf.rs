use core::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::trace;

#[allow(unused_imports, unused)]
mod serialization {
    include!(concat!(env!("OUT_DIR"), "/burrito_generated.rs"));
}

#[derive(Debug)]
struct OpenRequest {
    dst_addr: String,
}

impl OpenRequest {
    fn onto(&self, msg: &mut flatbuffers::FlatBufferBuilder) {
        let dst_addr_field = msg.create_string(&self.dst_addr);
        let req = serialization::OpenRequest::create(
            msg,
            &serialization::OpenRequestArgs {
                dst_addr: Some(dst_addr_field),
            },
        );

        msg.finish(req, None);
    }
}

#[derive(Clone)]
struct UnixSt {
    st: Arc<Mutex<tokio::net::UnixStream>>,
}

impl UnixSt {
    async fn new(controller_addr: impl AsRef<Path>) -> Result<Self, failure::Error> {
        let st = tokio::net::UnixStream::connect(controller_addr).await?;

        Ok(Self {
            st: Arc::new(Mutex::new(st)),
        })
    }

    async fn write_msg(&mut self, msg: &[u8]) -> Result<(), failure::Error> {
        use tokio::io::AsyncWriteExt;
        let mut cl = self.st.lock().await;

        cl.write_all(&msg.len().to_le_bytes()).await?;
        cl.write_all(msg).await?;
        Ok(())
    }

    async fn read_msg(&mut self, buf: &mut [u8]) -> Result<usize, failure::Error> {
        use tokio::io::AsyncReadExt;

        let mut cl = self.st.lock().await;

        let len_to_read = loop {
            cl.read_exact(&mut buf[0..4]).await?;

            // unsafe transmute ok because endianness is guaranteed to be the
            // same on either side of a UDS
            // will read only the first sizeof(u32) = 4 bytes
            let len_to_read: u32 = unsafe { std::mem::transmute_copy(&buf) };

            if len_to_read == 0 {
                tokio::task::yield_now().await;
                continue;
            } else if len_to_read as usize > buf.len() {
                failure::bail!("message size too large: {:?} > 128", len_to_read);
            } else {
                break len_to_read;
            }
        };

        cl.read_exact(&mut buf[0..len_to_read as usize]).await?;
        Ok(len_to_read as usize)
    }
}

#[derive(Clone)]
pub struct Client {
    burrito_root: PathBuf,
    st: UnixSt,
    buf: flatbuffers::FlatBufferBuilder<'static>,
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
        let st = UnixSt::new(controller_addr).await?;
        trace!("burrito-addr connected to burrito-ctl");

        Ok(Self {
            burrito_root,
            st,
            buf: flatbuffers::FlatBufferBuilder::new_with_capacity(128),
        })
    }

    pub async fn resolve(&mut self, dst: hyper::Uri) -> Result<crate::Addr, failure::Error> {
        let dst_addr = crate::Uri::socket_path(&dst)?;
        trace!(addr = ?&dst_addr, "Resolving_burrito_address");
        let msg = OpenRequest { dst_addr };

        msg.onto(&mut self.buf);
        let msg = self.buf.finished_data();

        self.st.write_msg(msg).await?;

        let mut buf = [0u8; 128];
        let len_to_read = self.st.read_msg(&mut buf).await?;

        let open_reply_reader =
            flatbuffers::get_root::<serialization::OpenReply>(&buf[0..len_to_read as usize]);

        if let None = open_reply_reader.send_addr() {
            failure::bail!("message didn't include send addr");
        }

        Ok(match open_reply_reader.addr_type() {
            serialization::AddrType::Unix => {
                crate::Addr::Unix(open_reply_reader.send_addr().unwrap().to_string())
            }
            serialization::AddrType::Tcp => {
                crate::Addr::Tcp(open_reply_reader.send_addr().unwrap().to_string())
            }
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

#[derive(Debug)]
struct ListenRequest {
    service_addr: String,
    port: u16,
}

impl ListenRequest {
    fn onto(&self, msg: &mut flatbuffers::FlatBufferBuilder) {
        let service_addr_field = msg.create_string(&self.service_addr);
        let req = serialization::ListenRequest::create(
            msg,
            &serialization::ListenRequestArgs {
                service_addr: Some(service_addr_field),
                listen_port: self.port,
            },
        );

        msg.finish(req, None);
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
        let burrito_ctl_addr = burrito_root.as_ref().join(burrito_ctl::CONTROLLER_ADDRESS);
        let burrito_root = burrito_root.as_ref().to_path_buf();
        trace!(addr = ?&burrito_ctl_addr, root = ?&burrito_root, "Connecting to burrito-ctl");

        // ask local burrito-ctl where to listen
        let mut st = UnixSt::new(burrito_ctl_addr).await?;
        trace!("Connected to burrito-ctl");

        // do listen() rpc
        let mut buf = flatbuffers::FlatBufferBuilder::new_with_capacity(128);
        let msg = ListenRequest {
            service_addr: service_addr.to_string(),
            port,
        };

        msg.onto(&mut buf);
        let msg = buf.finished_data();
        st.write_msg(msg).await?;

        let mut buf = [0u8; 128];
        let len_to_read = st.read_msg(&mut buf).await?;

        let listen_reply_reader =
            flatbuffers::get_root::<serialization::ListenReply>(&buf[0..len_to_read as usize]);

        let listen_addr = listen_reply_reader
            .listen_addr()
            .ok_or_else(|| failure::format_err!("message didn't include listen addr"))?;

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
