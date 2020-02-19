use crate::conn::Conn;
use crate::uri::Uri;
use core::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use failure::Error;
use std::path::{Path, PathBuf};
use tracing::{span, trace, Level};
use tracing_futures::Instrument;

mod rpc {
    tonic::include_proto!("burrito");
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

    /// Resolves a [`hyper::Uri`] to an [`Addr`].
    ///
    /// The [`hyper::Uri`] must have the uri scheme `burrito`.
    pub async fn resolve(&mut self, dst: hyper::Uri) -> Result<crate::conn::Addr, Error> {
        let dst_addr = Uri::socket_path(&dst)?;

        trace!(addr = ?&dst_addr, "Resolving_burrito_address");

        let resp = self
            .burrito_client
            .open(rpc::OpenRequest { dst_addr })
            .instrument(span!(Level::DEBUG, "resolve_rpc"))
            .await?
            .into_inner();

        let addr = resp.send_addr;
        let addr_type = resp.addr_type;
        let addr_type = rpc::open_reply::AddrType::from_i32(addr_type)
            .ok_or_else(|| failure::format_err!("Invalid AddrType {}", addr_type))?;

        trace!(resolved_addr = ?&addr, "Resolved_burrito_address");

        // It's somewhat unfortunate to have to match twice, once here and once in impl Service::call.
        // Could just return the string and handle there, but that would expose the message abstraction.
        match addr_type {
            rpc::open_reply::AddrType::Unix => Ok(crate::conn::Addr::Unix(
                self.burrito_root
                    .join(addr)
                    .into_os_string()
                    .into_string()
                    .expect("OS string as valid string"),
            )),
            rpc::open_reply::AddrType::Tcp => Ok(crate::conn::Addr::Tcp(addr)),
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
        Box::pin(async move { cl.resolve(dst).await?.connect().await })
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
        let this = &mut *self;
        super::poll_select_accept(this.ul.incoming(), this.tl.incoming(), cx)
    }
}
