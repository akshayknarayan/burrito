use burrito_discovery_ctl::client::DiscoveryClient;
use burrito_localname_ctl::client::LocalNameClient;
use core::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use failure::ResultExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, trace, warn};

mod conn;
mod uri;

pub use conn::*;
pub use uri::*;

/// Client-side client frontend for burrito-ctls
#[derive(Clone)]
pub struct Client {
    burrito_root: PathBuf,
    local_cl: Option<Arc<Mutex<LocalNameClient>>>,
    disc_cl: Option<Arc<Mutex<DiscoveryClient>>>,
}

impl Client {
    pub async fn new(burrito_root: impl AsRef<Path>) -> Result<Self, failure::Error> {
        let burrito_ctl_addr = burrito_root
            .as_ref()
            .join(burrito_localname_ctl::CONTROLLER_ADDRESS);
        let burrito_root = burrito_root.as_ref().to_path_buf();

        trace!(
            ?burrito_root,
            ?burrito_ctl_addr,
            "burrito-addr connecting to burrito-ctl",
        );

        let lcl = burrito_localname_ctl::client::LocalNameClient::new(burrito_root.clone());
        let dcl = DiscoveryClient::new(burrito_root.clone());

        // connect to burrito-ctls
        //
        // The Arc<Mutex<_>> is only necessary because of the implementation of hyper::Service.
        // If we had existential types (https://github.com/rust-lang/rust/issues/63063), we could
        // instead write impl Future<Output=_> without having to Box::pin.
        match futures_util::join!(dcl, lcl) {
            (Ok(d), Ok(l)) => Ok(Self {
                burrito_root,
                local_cl: Some(Arc::new(Mutex::new(l))),
                disc_cl: Some(Arc::new(Mutex::new(d))),
            }),
            (Ok(d), Err(e)) => {
                warn!(err = ?e, "Could not connect to localname-ctl");
                Ok(Self {
                    burrito_root,
                    local_cl: None,
                    disc_cl: Some(Arc::new(Mutex::new(d))),
                })
            }
            (Err(e), Ok(l)) => {
                warn!(err = ?e, "Could not connect to discovery-ctl");
                Ok(Self {
                    burrito_root,
                    local_cl: Some(Arc::new(Mutex::new(l))),
                    disc_cl: None,
                })
            }
            (Err(e1), Err(e2)) => Err(failure::format_err!(
                "Could not connect to either local-ctl: {} or discovery-ctl: {}",
                e1,
                e2
            )),
        }
    }

    pub async fn resolve(&mut self, dst: hyper::Uri) -> Result<crate::Addr, failure::Error> {
        let name = crate::Uri::socket_path(&dst)?;

        use burrito_localname_ctl::proto::Addr;
        let mut addr: Addr = if let Some(ref disc_cl) = self.disc_cl {
            trace!(addr = ?&name, "Resolving discovery-ctl address");
            let dc = disc_cl.lock().await;
            discovery_ctl(dc, name).await?
        } else {
            let a = name
                .parse()
                .map_err(|e| failure::format_err!("Malformed address {}: {}", name, e))?;
            match a {
                Addr::Burrito(_) | Addr::Unix(_) => Err(failure::format_err!(
                    "Invalid address type without discovery-ctl: {}",
                    a
                ))?,
                _ => (),
            }

            a
        };

        trace!(resolved_addr = ?&addr, "Resolved discovery-ctl address");

        // 3. Resolve the local-name entry, if applicable.
        if let Some(ref cl) = self.local_cl {
            if let ref a @ Addr::Burrito(_) = addr {
                trace!(addr = ?&addr, "Resolving localname address");
                match cl.lock().await.query(a.clone()).await {
                    Ok(la) => addr = la.local_addr,
                    Err(e) => warn!(err = ?e, "Error querying localname-ctl"),
                }
            }
        }

        Ok(match addr {
            Addr::Unix(a) => {
                let a = self.burrito_root.join(a.file_name().ok_or_else(|| {
                    failure::format_err!("Local address must have valid file name.")
                })?);
                crate::Addr::Unix(a)
            }
            Addr::Tcp(a) => crate::Addr::Tcp(a),
            Addr::Udp(a) => crate::Addr::Other(a.to_string()),
            Addr::Burrito(a) => crate::Addr::Other(a),
        })
    }
}

async fn discovery_ctl(
    mut disc_cl: impl std::ops::DerefMut<Target = burrito_discovery_ctl::client::DiscoveryClient>,
    name: String,
) -> Result<burrito_localname_ctl::proto::Addr, failure::Error> {
    // 1. check discovery-ctl
    let rsp = disc_cl
        .query(name)
        .await
        .map_err(|e| failure::format_err!("Could not query discovery-ctl: {}", e))?;

    // 2. If there is a local-name entry, use it, otherwise default to name entry.
    rsp.services
        .into_iter()
        .fold(None, |acc, srv| {
            let burrito_discovery_ctl::proto::Service {
                service, address, ..
            } = srv;
            match service.as_str() {
                burrito_discovery_ctl::CONTROLLER_ADDRESS => acc.or_else(|| Some(address)),
                burrito_localname_ctl::CONTROLLER_ADDRESS => {
                    if let Some(s) = acc {
                        warn!(discarding = ?s, using = ?address, "Got duplicate local-name entry");
                    }

                    Some(address)
                }
                _ => acc,
            }
        })
        .ok_or_else(|| failure::format_err!("No address found"))
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
        // this will error on UDP remotes
        Box::pin(async move { cl.resolve(dst).await?.connect().await.map_err(|e| e.into()) })
    }
}

/// Server-side client frontend for burrito-ctls
pub struct Server {
    tl: tokio::net::TcpListener,
    ul: Option<tokio::net::UnixListener>,
}

impl Server {
    /// addr: ("tcp"|"udp"), port
    pub async fn start(
        service_addr: &str,
        addr: (&str, u16),
        burrito_root: impl AsRef<Path>,
    ) -> Result<Self, failure::Error> {
        use burrito_localname_ctl::proto::Addr;
        use std::net::ToSocketAddrs;
        let cl = Client::new(burrito_root.as_ref())
            .await
            .context("make client")?;
        let name = Addr::Burrito(service_addr.to_string());
        let port = addr.1;
        let sk: (std::net::IpAddr, u16) = (std::net::Ipv4Addr::UNSPECIFIED.into(), port);
        let sk = sk.to_socket_addrs()?.next().unwrap();
        let listen = match addr.0 {
            "tcp" => Addr::Tcp(sk),
            "udp" => Addr::Udp(sk),
            x => Err(failure::format_err!("Expected tcp or udp: {}", x))?,
        };

        let tl = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
        if let Some(ref lc) = cl.local_cl {
            let rep = lc.lock().await.register(name, Some(listen)).await?;
            let a = match rep.local_addr {
                Addr::Unix(la) => la,
                x => Err(failure::format_err!("Expected local address: {}", x))?,
            };

            let a =
                burrito_root.as_ref().join(a.file_name().ok_or_else(|| {
                    failure::format_err!("Local address must have valid file name.")
                })?);

            debug!( addr = ?&a, "Got listening address");
            let ul = tokio::net::UnixListener::bind(a).context("Could not bind UnixListener")?;
            Ok(Self { tl, ul: Some(ul) })
        } else if let Some(ref dc) = cl.disc_cl {
            dc.lock()
                .await
                .register(burrito_discovery_ctl::proto::Service {
                    name: service_addr.to_owned(),
                    scope: burrito_discovery_ctl::proto::Scope::Global,
                    service: burrito_discovery_ctl::CONTROLLER_ADDRESS.to_owned(),
                    address: listen,
                })
                .await
                .map_err(|e| failure::format_err!("{}", e))?;
            Ok(Self { tl, ul: None })
        } else {
            unreachable!()
        }
    }
}

impl futures_util::stream::Stream for Server {
    type Item = Result<crate::Conn, failure::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        if let Some(ref mut ul) = this.ul {
            poll_select_accept(ul.incoming(), this.tl.incoming(), cx)
        } else {
            let r = futures_util::ready!(Pin::new(&mut this.tl).poll_accept(cx));
            match r {
                Ok((s, _)) => {
                    // https://eklitzke.org/the-caveats-of-tcp-nodelay
                    s.set_nodelay(true)
                        .expect("set nodelay on accepted connection");
                    Poll::Ready(Some(Ok(Conn::Tcp(s))))
                }
                Err(e) => Poll::Ready(Some(Err(e.into()))),
            }
        }
    }
}

fn poll_select_accept(
    mut ui: tokio::net::unix::Incoming,
    mut ti: tokio::net::tcp::Incoming,
    cx: &mut Context,
) -> Poll<Option<Result<conn::Conn, failure::Error>>> {
    // If there is a high rate of incoming Unix connections, this approach could starve
    // the TcpListener, since if ui.poll_accept() is ready, we just return.
    // This is no better than what it replaced: a call to futures_util::future::select.
    // If starvation becomes an issue in the future (hah), consider a coinflip to pick.
    match Pin::new(&mut ui).poll_accept(cx) {
        Poll::Pending => (),
        Poll::Ready(Ok(x)) => return Poll::Ready(Some(Ok(Conn::Unix(x)))),
        Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e.into()))),
    };

    match Pin::new(&mut ti).poll_accept(cx) {
        Poll::Pending => (),
        Poll::Ready(Ok(s)) => {
            // https://eklitzke.org/the-caveats-of-tcp-nodelay
            s.set_nodelay(true)
                .expect("set nodelay on accepted connection");
            return Poll::Ready(Some(Ok(Conn::Tcp(s))));
        }
        Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e.into()))),
    };

    Poll::Pending
}
