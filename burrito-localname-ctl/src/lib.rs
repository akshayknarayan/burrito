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
        let cl = client::LocalNameClient::new(root.as_ref()).await?;
        Ok(LocalNameSrv {
            cl: Arc::new(Mutex::new(cl)),
            pub_inner: Arc::new(Mutex::new(pub_inner)),
            local_inner: Arc::new(Mutex::new(local_inner)),
        })
    }
}

impl<C, Cn, L, Ln, D> ChunnelListener for LocalNameSrv<C, L>
where
    C: ChunnelListener<Addr = SocketAddr, Connection = Cn, Error = Error> + Send + 'static,
    L: ChunnelListener<Addr = PathBuf, Connection = Ln, Error = Error> + Send + 'static,
    Cn: ChunnelConnection<Data = D> + 'static,
    Ln: ChunnelConnection<Data = D> + 'static,
{
    type Addr = SocketAddr;
    type Connection = Either<Cn, Ln>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Error;

    fn listen(&mut self, a: Self::Addr) -> Self::Future {
        let cl = Arc::clone(&self.cl);
        let pub_inner = Arc::clone(&self.pub_inner);
        let local_inner = Arc::clone(&self.local_inner);
        Box::pin(async move {
            // call client.register
            let local_addr = cl.lock().await.register(a).await?;

            // listen selects on returned addr and inner.listen(addr)
            let ext_str = pub_inner
                .lock()
                .await
                .listen(a)
                .await?
                .map(|conn| Ok(Either::Left(conn?)));
            let int_str = local_inner
                .lock()
                .await
                .listen(local_addr)
                .await?
                .map(|conn| Ok(Either::Right(conn?)));

            Ok(Box::pin(futures_util::stream::select(ext_str, int_str)) as _)
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

impl<C, L> LocalNameCln<C, L> {
    pub async fn new(root: impl AsRef<Path>, pub_inner: C, local_inner: L) -> Result<Self, Error> {
        let cl = client::LocalNameClient::new(root.as_ref()).await?;
        Ok(LocalNameCln {
            cl: Arc::new(Mutex::new(cl)),
            pub_inner: Arc::new(Mutex::new(pub_inner)),
            local_inner: Arc::new(Mutex::new(local_inner)),
        })
    }
}

impl<C, L, Cn, Ln, D> ChunnelConnector for LocalNameCln<C, L>
where
    C: ChunnelConnector<Addr = SocketAddr, Connection = Cn, Error = Error> + Send + 'static,
    L: ChunnelConnector<Addr = PathBuf, Connection = Ln, Error = Error> + Send + 'static,
    Cn: ChunnelConnection<Data = D> + 'static,
    Ln: ChunnelConnection<Data = D> + 'static,
{
    type Addr = SocketAddr;
    type Connection = Either<C::Connection, L::Connection>;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Error;

    fn connect(&mut self, a: Self::Addr) -> Self::Future {
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

#[cfg(feature = "ctl")]
#[cfg(test)]
mod test {
    use super::{LocalNameCln, LocalNameSrv};
    use crate::ctl::serve_ctl;
    use bertha::{
        chan_transport::RendezvousChannel,
        udp::{UdpReqChunnel, UdpSkChunnel},
        AddrWrap, ChunnelConnection, ChunnelConnector, ChunnelListener, Never, OptionUnwrap,
    };
    use eyre::Error;
    use futures_util::stream::TryStreamExt;
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use tracing::{debug, info, trace};
    use tracing_futures::Instrument;

    #[test]
    fn local_name() {
        let _guard = tracing_subscriber::fmt::try_init();
        color_eyre::install().unwrap();

        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_time()
            .enable_io()
            .build()
            .unwrap();
        rt.block_on(
            async move {
                let root = PathBuf::from(r"./tmp-test-local-name");

                std::fs::remove_dir_all(&root).unwrap_or_else(|_| ());
                trace!(dir = ?&root, "create dir");
                std::fs::create_dir(&root)?;
                debug!(dir = ?&root, "start ctl");
                let r1 = root.clone();
                tokio::spawn(
                    async move { serve_ctl(Some(r1), false).await.unwrap() }
                        .instrument(tracing::info_span!("localname-ctl")),
                );

                tokio::time::delay_for(std::time::Duration::from_millis(100)).await;

                let (srv, cln) = RendezvousChannel::new(10).split();
                // udp |> localnamesrv server
                let udp_addr: SocketAddr = "127.0.0.1:21987".parse()?;
                let mut srv = LocalNameSrv::new(
                    root.clone(),
                    Never::from(UdpReqChunnel::default()),
                    OptionUnwrap::from(srv),
                )
                .await?;

                tokio::spawn(
                    async move {
                        let st = srv.listen(udp_addr).await.unwrap();
                        st.try_for_each_concurrent(None, |cn| async move {
                            let m = cn.recv().await?;
                            cn.send(m).await?;
                            Ok(())
                        })
                        .await
                    }
                    .instrument(tracing::debug_span!("server")),
                );

                // udp |> localnamecln client
                let mut cln = LocalNameCln::new(
                    root.clone(),
                    Never::from(AddrWrap::from(UdpSkChunnel::default())),
                    cln,
                )
                .await?;
                info!("connecting client");
                let cn = cln.connect(udp_addr).await?;

                cn.send(vec![1u8; 8]).await?;
                let d = cn.recv().await?;
                assert_eq!(d, vec![1u8; 8]);

                Ok::<_, Error>(())
            }
            .instrument(tracing::info_span!("local_name")),
        )
        .unwrap();
    }
}
