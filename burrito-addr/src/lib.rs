use core::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use failure::Error;
use pin_project::{pin_project, project};

mod rpc {
    tonic::include_proto!("burrito");
}

#[pin_project]
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

pub struct Client {
    burrito_client: rpc::client::ConnectionClient<tonic::transport::channel::Channel>,
}

impl hyper::client::connect::Connect for Client {
    type Transport = Conn;
    type Error = Error;
    type Future = Pin<
        Box<
            dyn Future<Output = Result<(Self::Transport, hyper::client::connect::Connected), Error>>
                + Send,
        >,
    >;

    fn connect(&self, dst: hyper::client::connect::Destination) -> Self::Future {
        let mut cl = self.burrito_client.clone();
        Box::pin(async move {
            // ask burrito-ctl where to connect to
            let send_addr = cl
                .open(rpc::OpenRequest {
                    dst_addr: dst.host().to_owned(),
                })
                .await?
                .into_inner()
                .send_addr;

            // TODO deal with remote TCP addrs

            // connect to it
            let st = tokio::net::UnixStream::connect(&send_addr).await?;
            Ok((Conn::Unix(st), hyper::client::connect::Connected::new()))
        })
    }
}

pub struct Server {
    ul: tokio::net::UnixListener,
}

impl Server {
    pub async fn start(service_addr: &str, burrito_ctl_addr: &str) -> Result<Self, Error> {
        // ask local burrito-ctl where to listen
        let addr: hyper::Uri = hyper_unix_connector::Uri::new(burrito_ctl_addr, "/").into();
        let mut cl = rpc::client::ConnectionClient::connect(addr).await?;
        let listen_addr = cl
            .listen(rpc::ListenRequest {
                service_addr: service_addr.to_owned(),
            })
            .await?
            .into_inner()
            .listen_addr;

        // TODO also listen on local TCP

        let ul = tokio::net::UnixListener::bind(listen_addr)?;
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
        use futures_util::future::FutureExt;
        use futures_util::try_future::TryFutureExt;

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
        let when = tokio::clock::now() + d;
        tokio::timer::delay(when).await;
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
        trace!(l2, "spawning");
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
        let mut rt = tokio::runtime::current_thread::Runtime::new()?;

        rt.block_on(async move {
            tokio::spawn(async move { start_burrito_ctl(&log).await.expect("Burrito Ctl") });
            block_for(std::time::Duration::from_millis(100)).await;
            Server::start("test-service", "./tmp-test-bn/controller")
                .await
                .expect("start server")
        });

        Ok(())
    }
}
