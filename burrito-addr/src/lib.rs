use core::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use failure::{ensure, format_err, Error};
use pin_project::{pin_project, project};
use slog::{debug, trace};
use std::path::{Path, PathBuf};

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
    log: slog::Logger,
}

impl Client {
    pub async fn new(burrito_root: impl AsRef<Path>, log: &slog::Logger) -> Result<Self, Error> {
        let burrito_ctl_addr = burrito_root.as_ref().join(burrito_ctl::CONTROLLER_ADDRESS);
        let burrito_root = burrito_root.as_ref().to_path_buf();
        trace!(log, "burrito-addr connecting to burrito-ctl"; "burrito_root" => ?&burrito_root, "burrito_ctl_addr" => ?&burrito_ctl_addr);

        let addr: hyper::Uri = hyper_unix_connector::Uri::new(burrito_ctl_addr, "/").into();
        let endpoint: tonic::transport::Endpoint = addr.into();
        let channel = endpoint
            .connect_with_connector(hyper_unix_connector::UnixClient)
            .await?;
        let burrito_client = rpc::connection_client::ConnectionClient::new(channel);

        trace!(log, "burrito-addr connected to burrito-ctl");
        Ok(Self {
            burrito_root,
            burrito_client,
            log: log.clone(),
        })
    }

    pub async fn resolve(&mut self, dst: hyper::Uri) -> Result<String, Error> {
        ensure!(
            dst.scheme_str().map(|s| s == "burrito").is_some(),
            "URL scheme does not match: {:?}",
            dst.scheme_str()
        );

        let dst_addr = Uri::socket_path(&dst)
            .ok_or_else(|| format_err!("Could not get socket path for Destination"))?;
        let dst_addr_log = dst_addr.clone();

        trace!(self.log, "Resolving burrito address"; "addr" => &dst_addr);

        let addr = self
            .burrito_client
            .open(rpc::OpenRequest { dst_addr })
            .await?
            .into_inner()
            .send_addr;

        debug!(self.log, "Resolved burrito address"; "burrito addr" => &dst_addr_log, "resolved addr" => &addr);

        Ok(self
            .burrito_root
            .join(addr)
            .into_os_string()
            .into_string()
            .expect("OS string as valid string"))
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
            let path = cl.resolve(dst).await?;
            let st = tokio::net::UnixStream::connect(&path).await?;
            Ok(Conn::Unix(st))
        })
    }
}

/// Listens at a burrito address.
///
/// Registers with burrito-ctl, then listens for incoming connections on the provided address.
pub struct Server {
    conns: Option<
        std::pin::Pin<std::boxed::Box<dyn tokio::stream::Stream<Item = Result<Conn, Error>>>>,
    >,
    tl: tokio::net::TcpListener,
    ul: tokio::net::UnixListener,
}

impl Server {
    pub async fn start(
        service_addr: &str,
        port: u16,
        burrito_root: impl AsRef<Path>,
        log: Option<&slog::Logger>,
    ) -> Result<Self, Error> {
        let burrito_ctl_addr = burrito_root.as_ref().join(burrito_ctl::CONTROLLER_ADDRESS);
        let burrito_root = burrito_root.as_ref().to_path_buf();
        if let Some(l) = log {
            trace!(l, "Connecting to burrito-ctl"; "addr" => ?&burrito_ctl_addr, "root" => ?&burrito_root);
        }

        // ask local burrito-ctl where to listen
        let addr: hyper::Uri = hyper_unix_connector::Uri::new(burrito_ctl_addr, "/").into();
        let endpoint: tonic::transport::Endpoint = addr.into();
        let channel = endpoint
            .connect_with_connector(hyper_unix_connector::UnixClient)
            .await?;
        let mut cl = rpc::connection_client::ConnectionClient::new(channel);

        let port_string = port.to_string();
        let listen_addr = cl
            .listen(rpc::ListenRequest {
                service_addr: service_addr.to_owned(),
                listen_port: port_string,
            })
            .await?
            .into_inner()
            .listen_addr;

        if let Some(l) = log {
            trace!(l, "Got listening address"; "addr" => ?&listen_addr, "root" => ?&burrito_root);
        }

        let tl = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
        let ul = tokio::net::UnixListener::bind(burrito_root.join(listen_addr))?;
        let mut s = Self {
            tl,
            ul,
            conns: None,
        };

        s.make_stream();
        Ok(s)
    }

    fn make_stream(&mut self) {
        use futures_util::stream::TryStreamExt;

        // Why the `mem::transmute`??
        //
        // Without it, borrow checker is worried that self.(tl|us).incoming()
        // takes the lifetime of self, which has some lifetime. But, to use the stream it must have lifetime 'static:
        //
        //    Compiling burrito-addr v0.1.0 (/home/akshayn/burrito/burrito-addr)
        // error[E0495]: cannot infer an appropriate lifetime for autoref due to conflicting requirements
        //    --> burrito-addr/src/lib.rs:235:26
        //     |
        // 235 |         let ts = self.tl.incoming().map_ok(|s| Conn::Tcp(s));
        //     |                          ^^^^^^^^
        //     |
        // note: first, the lifetime cannot outlive the anonymous lifetime #1 defined on the method body at 232:5...
        //    --> burrito-addr/src/lib.rs:232:5
        //     |
        // 232 | /     fn make_stream(&mut self) {
        // 233 | |         use futures_util::stream::TryStreamExt;
        // 234 | |
        // 235 | |         let ts = self.tl.incoming().map_ok(|s| Conn::Tcp(s));
        // ...   |
        // 240 | |         ));
        // 241 | |     }
        //     | |_____^
        // note: ...so that reference does not outlive borrowed content
        //    --> burrito-addr/src/lib.rs:235:18
        //     |
        // 235 |         let ts = self.tl.incoming().map_ok(|s| Conn::Tcp(s));
        //     |                  ^^^^^^^
        //     = note: but, the lifetime must be valid for the static lifetime...
        //     = note: ...so that the expression is assignable:
        //             expected std::option::Option<std::pin::Pin<std::boxed::Box<(dyn futures_core::stream::Stream<Item = std::result::Result<Conn, failure::error::Error>> + 'static)>>>
        //                found std::option::Option<std::pin::Pin<std::boxed::Box<dyn futures_core::stream::Stream<Item = std::result::Result<Conn, failure::error::Error>>>>>
        //
        // However, this is actually fine: borrow checker is only worried that the listeners'
        // lifetimes might end before the future. But, this won't happen, since they all live in
        // self.
        //
        // So, we lie to rust about the lifetime of the stream.

        let ts: tokio::net::tcp::Incoming<'static> =
            unsafe { std::mem::transmute(self.tl.incoming()) };
        let ts = ts.map_ok(|s| Conn::Tcp(s));
        let us: tokio::net::unix::Incoming<'static> =
            unsafe { std::mem::transmute(self.ul.incoming()) };
        let us = us.map_ok(|s| Conn::Unix(s));

        self.conns = Some(Box::pin(
            futures_util::stream::select(ts, us).map_err(|e| Error::from(e)),
        ));
    }
}

// Prevent destructuring due to transmute above
impl Drop for Server {
    fn drop(&mut self) {}
}

impl hyper::server::accept::Accept for Server {
    type Conn = Conn;
    type Error = Error;

    fn poll_accept(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Self::Conn, Self::Error>>> {
        if let None = self.conns {
            return Poll::Pending;
        }

        self.conns.as_mut().unwrap().as_mut().poll_next(cx)



    }
}
