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
    ul: tokio::net::UnixListener,
}

impl Server {
    pub async fn start(
        service_addr: &str,
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

        let listen_addr = cl
            .listen(rpc::ListenRequest {
                service_addr: service_addr.to_owned(),
            })
            .await?
            .into_inner()
            .listen_addr;

        if let Some(l) = log {
            trace!(l, "Got listening address"; "addr" => ?&listen_addr, "root" => ?&burrito_root);
        }

        // TODO also listen on local TCP
        let ul = tokio::net::UnixListener::bind(burrito_root.join(listen_addr))?;
        Ok(Self { ul })
    }
}

impl hyper::server::accept::Accept for Server {
    type Conn = tokio::net::UnixStream;
    type Error = Error;

    fn poll_accept(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Self::Conn, Self::Error>>> {
        use futures_util::future::{FutureExt, TryFutureExt};

        // TODO once also listening on TCP,
        // use futures_util::select!() to listen on both
        let f = self
            .ul
            .accept()
            .map_ok(|(s, _a)| s)
            .map_err(|e| e.into())
            .map(|f| Some(f));
        Future::poll(Box::pin(f).as_mut(), cx)
    }
}

#[cfg(test)]
mod test {
    use failure::Error;
    use slog::{debug, error, trace};

    use super::Server;

    pub(crate) fn test_logger() -> slog::Logger {
        use slog::Drain;
        let plain = slog_term::PlainSyncDecorator::new(slog_term::TestStdoutWriter);
        let drain = slog_term::FullFormat::new(plain).build().fuse();
        slog::Logger::root(drain, slog::o!())
    }

    async fn block_for(d: std::time::Duration) {
        tokio::time::delay_for(d).await;
    }

    async fn start_burrito_ctl(log: &slog::Logger) -> Result<(), Error> {
        trace!(&log, "removing"; "dir" => "./tmp-test-bn/");
        std::fs::remove_dir_all("./tmp-test-bn/").unwrap_or_default();
        trace!(&log, "creating"; "dir" => "./tmp-test-bn/");
        std::fs::create_dir_all("./tmp-test-bn/")?;
        use burrito_ctl::BurritoNet;
        let bn = BurritoNet::new(
            Some(std::path::PathBuf::from("./tmp-test-bn/")),
            log.clone(),
        );

        let burrito_addr = bn.listen_path();
        debug!(&log, "burrito_addr"; "addr" => ?&burrito_addr);
        use hyper_unix_connector::UnixConnector;
        let uc: UnixConnector = tokio::net::UnixListener::bind(&burrito_addr)?.into();
        let bn = bn.start()?;
        let burrito_rpc_server =
            hyper::server::Server::builder(uc).serve(hyper::service::make_service_fn(move |_| {
                let bs = bn.clone();
                async move { Ok::<_, hyper::Error>(bs) }
            }));

        let l2 = log.clone();
        trace!(l2, "spawning burrito_rpc_server");
        let s = burrito_rpc_server.await;
        if let Err(e) = s {
            error!(l2, "burrito_rpc_server crashed"; "err" => ?e);
            panic!(e)
        }

        Ok(())
    }

    #[test]
    fn test_server_start() -> Result<(), Error> {
        let log = test_logger();
        let mut rt = tokio::runtime::Runtime::new()?;

        rt.block_on(async move {
            let l = log.clone();
            tokio::spawn(async move { start_burrito_ctl(&l).await.expect("Burrito Ctl") });
            block_for(std::time::Duration::from_millis(100)).await;
            Server::start("test-service", "./tmp-test-bn", Some(&log))
                .await
                .expect("start server")
        });

        Ok(())
    }
}
