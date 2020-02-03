use core::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use failure::{ensure, format_err, Error};
use pin_project::{pin_project, project};
use std::path::{Path, PathBuf};
use tracing::{span, trace, Level};
use tracing_futures::Instrument;

mod rpc {
    tonic::include_proto!("burrito");
}

/// Burrito-aware URI type.
pub struct Uri<'a> {
    /// path + query string
    inner: std::borrow::Cow<'a, str>,
}

impl<'a> Into<hyper::Uri> for Uri<'a> {
    fn into(self) -> hyper::Uri {
        self.inner.as_ref().parse().unwrap()
    }
}

impl<'a> Uri<'a> {
    pub fn new(addr: &str) -> Self {
        Self::new_with_path(addr, "/")
    }

    pub fn new_with_path(addr: &str, path: &'a str) -> Self {
        let host = hex::encode(addr.as_bytes());
        let s = format!("burrito://{}:0{}", host, path);
        Uri {
            inner: std::borrow::Cow::Owned(s),
        }
    }

    pub fn socket_path(uri: &hyper::Uri) -> Option<String> {
        use hex::FromHex;
        uri.host()
            .iter()
            .filter_map(|host| {
                Vec::from_hex(host)
                    .ok()
                    .map(|raw| String::from_utf8_lossy(&raw).into_owned())
            })
            .next()
    }
}

/// A wrapper around a unix or tcp socket.
#[pin_project]
#[derive(Debug)]
pub enum Conn {
    Unix(#[pin] tokio::net::UnixStream),
    Tcp(#[pin] tokio::net::TcpStream),
}

// Implement a function on Pin<&mut Conn> that both tokio::net::UnixStream and
// tokio::net::TcpStream implement
macro_rules! conn_impl_fn {
    ($fn: ident |$first_var: ident: $first_typ: ty, $($var: ident: $typ: ty),*| -> $ret: ty ;;) => {
        #[project]
        fn $fn ($first_var: $first_typ, $( $var: $typ ),* ) -> $ret {
            #[project]
            match self.project() {
                Conn::Unix(u) => {
                    let ux: Pin<&mut tokio::net::UnixStream> = u;
                    ux.$fn($($var),*)
                }
                Conn::Tcp(t) => {
                    let tc: Pin<&mut tokio::net::TcpStream> = t;
                    tc.$fn($($var),*)
                }
            }
        }
    };
}

impl tokio::io::AsyncRead for Conn {
    conn_impl_fn!(poll_read |self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]| -> Poll<std::io::Result<usize>> ;;);
}

impl tokio::io::AsyncWrite for Conn {
    conn_impl_fn!(poll_write    |self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]| -> Poll<std::io::Result<usize>> ;;);
    conn_impl_fn!(poll_flush    |self: Pin<&mut Self>, cx: &mut Context<'_>| -> Poll<std::io::Result<()>> ;;);
    conn_impl_fn!(poll_shutdown |self: Pin<&mut Self>, cx: &mut Context<'_>| -> Poll<std::io::Result<()>> ;;);
}

/// Resolves a burrito address to a [`Conn`].
///
/// Connects to burrito-ctl to resolve.
#[derive(Clone)]
pub struct Client {
    burrito_root: PathBuf,
    burrito_client: rpc::connection_client::ConnectionClient<tonic::transport::channel::Channel>,
}

impl Client {
    #[tracing::instrument(level = "debug", skip(burrito_root, log))]
    pub async fn new(burrito_root: impl AsRef<Path>) -> Result<Self, Error> {
        let burrito_ctl_addr = burrito_root.as_ref().join(burrito_ctl::CONTROLLER_ADDRESS);
        let burrito_root = burrito_root.as_ref().to_path_buf();
        trace!(
            ?burrito_root,
            ?burrito_ctl_addr,
            "burrito-addr connecting to burrito-ctl",
        );

        let addr: hyper::Uri = hyper_unix_connector::Uri::new(burrito_ctl_addr, "/").into();
        let endpoint: tonic::transport::Endpoint = addr.into();
        let channel = endpoint
            .connect_with_connector(hyper_unix_connector::UnixClient)
            .await?;
        let burrito_client = rpc::connection_client::ConnectionClient::new(channel);

        trace!("burrito-addr connected to burrito-ctl");
        Ok(Self {
            burrito_root,
            burrito_client,
        })
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn resolve(
        &mut self,
        dst: hyper::Uri,
    ) -> Result<(String, rpc::open_reply::AddrType), Error> {
        ensure!(
            dst.scheme_str().map(|s| s == "burrito").is_some(),
            "URL scheme does not match: {:?}",
            dst.scheme_str()
        );

        let dst_addr = Uri::socket_path(&dst)
            .ok_or_else(|| format_err!("Could not get socket path for Destination"))?;
        let dst_addr_log = dst_addr.clone();

        trace!(addr = ?&dst_addr, "Resolving burrito address");

        let resp = self
            .burrito_client
            .open(rpc::OpenRequest { dst_addr })
            .await?
            .into_inner();

        let addr = resp.send_addr;
        let addr_type = resp.addr_type;
        let addr_type = rpc::open_reply::AddrType::from_i32(addr_type)
            .ok_or_else(|| failure::format_err!("Invalid AddrType {}", addr_type))?;

        trace!(addr = ?&dst_addr_log, addr_type = ?addr_type, resolved_addr = ?&addr, "Resolved burrito address");

        // It's somewhat unfortunate to have to match twice, once here and once in impl Service::call.
        // Could just return the string and handle there, but that would expose the message abstraction.
        match addr_type {
            rpc::open_reply::AddrType::Unix => Ok((
                self.burrito_root
                    .join(addr)
                    .into_os_string()
                    .into_string()
                    .expect("OS string as valid string"),
                rpc::open_reply::AddrType::Unix,
            )),
            rpc::open_reply::AddrType::Tcp => Ok((addr, rpc::open_reply::AddrType::Tcp)),
        }
    }
}

impl hyper::service::Service<hyper::Uri> for Client {
    type Response = Conn;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, dst: hyper::Uri) -> Self::Future {
        let mut cl = self.clone();
        Box::pin(async move {
            let (addr, addr_type) = cl
                .resolve(dst)
                .instrument(span!(Level::DEBUG, "resolve"))
                .await?;
            Ok(match addr_type {
                rpc::open_reply::AddrType::Unix => {
                    let st = tokio::net::UnixStream::connect(&addr)
                        .instrument(span!(Level::DEBUG, "UnixStream::connect"))
                        .await?;
                    trace!("Connected");
                    Conn::Unix(st)
                }
                rpc::open_reply::AddrType::Tcp => {
                    let st = tokio::net::TcpStream::connect(&addr)
                        .instrument(span!(Level::DEBUG, "TcpStream::connect"))
                        .await?;
                    trace!("Connected");
                    Conn::Tcp(st)
                }
            })
        })
    }
}

/// Listens at a burrito address.
///
/// Registers with burrito-ctl, then listens for incoming connections on the provided address.
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
    ) -> Result<Self, Error> {
        let burrito_ctl_addr = burrito_root.as_ref().join(burrito_ctl::CONTROLLER_ADDRESS);
        let burrito_root = burrito_root.as_ref().to_path_buf();
        trace!(addr = ?&burrito_ctl_addr, root = ?&burrito_root, "Connecting to burrito-ctl");

        // ask local burrito-ctl where to listen
        let addr: hyper::Uri = hyper_unix_connector::Uri::new(burrito_ctl_addr, "/").into();
        let endpoint: tonic::transport::Endpoint = addr.into();
        let channel = endpoint
            .connect_with_connector(hyper_unix_connector::UnixClient)
            .await?;
        let mut cl = rpc::connection_client::ConnectionClient::new(channel);
        trace!("Connected to burrito-ctl");

        let port_string = port.to_string();
        let listen_addr = cl
            .listen(rpc::ListenRequest {
                service_addr: service_addr.to_owned(),
                listen_port: port_string,
            })
            .await?
            .into_inner()
            .listen_addr;

        trace!(addr = ?&listen_addr, root = ?&burrito_root, "Got listening address");

        let tl = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
        let ul = tokio::net::UnixListener::bind(burrito_root.join(listen_addr))?;
        Ok(Self { tl, ul })
    }
}

impl hyper::server::accept::Accept for Server {
    type Conn = Conn;
    type Error = Error;

    fn poll_accept(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Self::Conn, Self::Error>>> {
        use futures_util::future::{FutureExt, TryFutureExt};
        let this = &mut *self;

        let tf = this.tl.accept().map_ok(|(s, _)| Conn::Tcp(s));
        let uf = this.ul.accept().map_ok(|(s, _)| Conn::Unix(s));

        let mut f = futures_util::future::select(Box::pin(tf), Box::pin(uf))
            .map(|either_stream| either_stream.factor_first().0)
            .map_err(Error::from)
            .map(|f| Some(f));

        Pin::new(&mut f).poll(cx)
    }
}
