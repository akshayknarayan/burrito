//! Add shared filesystem for pipes to new containers,
//! and translate between service-level addresses and
//! pipes.

#![warn(clippy::all)]
#![allow(clippy::type_complexity)]

pub const CONTROLLER_ADDRESS: &str = "localname-ctl";

pub mod client;
pub mod proto;

#[cfg(feature = "ctl")]
pub mod ctl;

#[cfg(feature = "docker")]
pub mod docker_proxy;

use bertha::{ChunnelConnection, ChunnelConnector, ChunnelListener, Either};
use eyre::Error;
use futures_util::stream::{Stream, StreamExt};
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;

/// LocalNameSrv is a ChunnelListener
///
/// On addr a of type SocketAddr from the inner chunnel,
/// registers with local-name-ctl to get a local address u, and
/// returns connection that listens on select(listen(a), listen(u))
pub struct LocalNameSrv<C, L> {
    cl: Arc<Mutex<client::LocalNameClient>>,
    pub_inner: Arc<Mutex<C>>,
    local_inner: Arc<Mutex<L>>,
}

impl<C, L> LocalNameSrv<C, L> {
    pub async fn new(root: impl AsRef<Path>, pub_inner: C, local_inner: L) -> Result<Self, Error> {
        let cl = client::LocalNameClient::new(root).await?;
        Ok(LocalNameSrv {
            cl: Arc::new(Mutex::new(cl)),
            pub_inner: Arc::new(Mutex::new(pub_inner)),
            local_inner: Arc::new(Mutex::new(local_inner)),
        })
    }
}

impl<C, Cn, L, Ln, D> ChunnelListener for LocalNameSrv<C, L>
where
    C: ChunnelListener<Addr = SocketAddr, Connection = Cn> + Send + 'static,
    L: ChunnelListener<Addr = PathBuf, Connection = Ln> + Send + 'static,
    Cn: ChunnelConnection<Data = D> + 'static,
    Ln: ChunnelConnection<Data = D> + 'static,
{
    type Addr = SocketAddr;
    type Connection = Either<Cn, Ln>;

    fn listen(
        &mut self,
        a: Self::Addr,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Pin<
                        Box<
                            dyn Stream<Item = Result<Self::Connection, eyre::Report>>
                                + Send
                                + 'static,
                        >,
                    >,
                > + Send
                + 'static,
        >,
    > {
        let cl = Arc::clone(&self.cl);
        let pub_inner = Arc::clone(&self.pub_inner);
        let local_inner = Arc::clone(&self.local_inner);
        Box::pin(async move {
            // call client.register
            let local_addr = cl.lock().await.register(a).await;
            if let Err(e) = local_addr {
                return Box::pin(futures_util::stream::once(async {
                    Err(e.wrap_err("Could not register local address"))
                })) as _;
            }
            let local_addr = local_addr.unwrap();

            // listen selects on returned addr and inner.listen(addr)
            let ext_str = pub_inner
                .lock()
                .await
                .listen(a)
                .await
                .map(|conn| Ok(Either::Left(conn?)));
            let int_str = local_inner
                .lock()
                .await
                .listen(local_addr)
                .await
                .map(|conn| Ok(Either::Right(conn?)));

            Box::pin(futures_util::stream::select(ext_str, int_str)) as _
        })
    }

    fn scope(&self) -> bertha::Scope {
        bertha::Scope::Local
    }
    fn endedness(&self) -> bertha::Endedness {
        bertha::Endedness::Either
    }
    fn implementation_priority(&self) -> usize {
        1
    }
}

/// `LocalNameCln` is a `ChuunelConnector`.
///
/// on addr a of type SocketAddr, it queries local-name-ctl.
/// if local address u is found, it returns a local connection to it.
/// otherwise it connects to a and returns that connection.
pub struct LocalNameCln<C, L> {
    cl: Arc<Mutex<client::LocalNameClient>>,
    pub_inner: Arc<Mutex<C>>,
    local_inner: Arc<Mutex<L>>,
}

impl<C, L, Cn, Ln, D> ChunnelConnector for LocalNameCln<C, L>
where
    C: ChunnelConnector<Addr = SocketAddr, Connection = Cn> + Send + 'static,
    L: ChunnelConnector<Addr = PathBuf, Connection = Ln> + Send + 'static,
    Cn: ChunnelConnection<Data = D>,
    Ln: ChunnelConnection<Data = D>,
{
    type Addr = SocketAddr;
    type Connection = Either<C::Connection, L::Connection>;

    fn connect(
        &mut self,
        a: Self::Addr,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Connection, eyre::Report>> + Send + 'static>>
    {
        let cl = Arc::clone(&self.cl);
        let ext = Arc::clone(&self.pub_inner);
        let inn = Arc::clone(&self.local_inner);
        Box::pin(async move {
            let addr = cl.lock().await.query(a).await?;
            Ok(if let Some(loc) = addr {
                Either::Right(inn.lock().await.connect(loc).await?)
            } else {
                Either::Left(ext.lock().await.connect(a).await?)
            })
        })
    }

    fn scope(&self) -> bertha::Scope {
        bertha::Scope::Local
    }
    fn endedness(&self) -> bertha::Endedness {
        bertha::Endedness::Either
    }
    fn implementation_priority(&self) -> usize {
        1
    }
}
