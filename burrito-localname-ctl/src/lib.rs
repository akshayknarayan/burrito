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

use bertha::{
    either::Either, Chunnel, ChunnelConnection, ChunnelConnector, ChunnelListener, Negotiate,
};
use color_eyre::eyre::Report;
use futures_util::stream::StreamExt;
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use tokio::sync::Mutex;
use tracing::{debug, trace};

/// LocalNameChunnel fast-paths data bound to local destinations.
///
/// `local_chunnel` is the fast-path chunnel.
#[derive(Debug, Clone)]
pub struct LocalNameChunnel<Lch, Lr, Ag> {
    cl: Option<Arc<Mutex<client::LocalNameClient>>>,
    listen_addr: Option<Ag>,
    local_raw: Lr,
    local_chunnel: Lch,
}

impl<Lch, Lr, Ag> Negotiate for LocalNameChunnel<Lch, Lr, Ag> {
    type Capability = ();

    fn guid() -> u64 {
        0xb3fa08967e518987
    }
}

impl<Lch, Lr, Ag> LocalNameChunnel<Lch, Lr, Ag> {
    pub async fn new(
        root: impl AsRef<Path>,
        listen_addr: Option<Ag>,
        local_raw: Lr,
        local_chunnel: Lch,
    ) -> Result<Self, Report> {
        let cl = client::LocalNameClient::new(root.as_ref()).await;
        if let Err(ref e) = &cl {
            debug!(err = %format!("{:#}", e), "LocalNameClient did not connect");
        }

        Ok(Self {
            cl: cl.map(Mutex::new).map(Arc::new).ok(),
            listen_addr,
            local_raw,
            local_chunnel,
        })
    }

    pub async fn server(
        root: impl AsRef<Path>,
        listen_addr: Ag,
        local_raw: Lr,
        local_chunnel: Lch,
    ) -> Result<Self, Report> {
        Self::new(root, Some(listen_addr), local_raw, local_chunnel).await
    }

    pub async fn client(
        root: impl AsRef<Path>,
        local_raw: Lr,
        local_chunnel: Lch,
    ) -> Result<Self, Report> {
        Self::new(root, None, local_raw, local_chunnel).await
    }
}

impl<Gc, Lr, LrCn, LrErr, Lrd, Lch, Lcn, LchErr, D> Chunnel<Gc>
    for LocalNameChunnel<Lch, Lr, SocketAddr>
where
    Gc: ChunnelConnection<Data = (SocketAddr, D)> + Send + Sync + 'static,
    D: Send + Sync + 'static,
    // Raw local connections. Lrd, local raw data, is probably Vec<u8> (e.g. for Lctr = UDS), but
    // don't assume this.
    Lr: ChunnelConnector<Connection = LrCn, Addr = (), Error = LrErr>
        + ChunnelListener<Connection = LrCn, Addr = PathBuf, Error = LrErr>
        + Clone
        + Send
        + 'static,
    <Lr as ChunnelListener>::Stream: Unpin,
    LrCn: ChunnelConnection<Data = (PathBuf, Lrd)> + Send,
    LrErr: Into<Report> + Send + Sync + 'static,
    // Local connections with semantics.
    Lch: Chunnel<LrCn, Connection = Lcn, Error = LchErr> + Clone + Send + 'static,
    Lcn: ChunnelConnection<Data = (PathBuf, D)> + Send + Sync + 'static,
    LchErr: Into<Report> + Send + Sync + 'static,
{
    type Connection = LocalNameCn<Gc, Lcn>;
    type Error = Report;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn connect_wrap(&mut self, inner: Gc) -> Self::Future {
        let mut local_raw = self.local_raw.clone();
        let mut local_chunnel = self.local_chunnel.clone();
        let cl = self.cl.as_ref().map(Arc::clone);
        let gaddr = self.listen_addr;

        Box::pin(async move {
            let local_raw_cn = match (gaddr, &cl) {
                (Some(gaddr), Some(cl)) => match cl.lock().await.register(gaddr).await {
                    Ok(laddr) => {
                        debug!(?laddr, "LocalNameClient registered");
                        local_raw
                            .listen(laddr)
                            .await
                            .map_err(Into::into)?
                            .next()
                            .await
                            .unwrap()
                            .map_err(Into::into)?
                    }
                    Err(e) => {
                        debug!(err = %format!("{:#}", e), "LocalNameClient register failed");
                        local_raw.connect(()).await.map_err(Into::into)?
                    }
                },
                _ => local_raw.connect(()).await.map_err(Into::into)?,
            };

            let local_cn = local_chunnel
                .connect_wrap(local_raw_cn)
                .await
                .map_err(Into::into)?;
            Ok(LocalNameCn::new(cl, inner, local_cn))
        })
    }
}

#[derive(Clone)]
enum LocalAddrCacheEntry<A> {
    Hit {
        laddr: A,
        expiry: std::time::Instant,
    },
    AntiHit {
        expiry: Option<std::time::Instant>,
    },
}

pub struct LocalNameCn<Gc, Lc> {
    cl: Option<Arc<Mutex<client::LocalNameClient>>>,
    global_cn: Arc<Gc>,
    local_cn: Arc<Lc>,
    addr_cache: Arc<StdMutex<HashMap<SocketAddr, LocalAddrCacheEntry<PathBuf>>>>,
    rev_addr_map: Arc<StdMutex<HashMap<PathBuf, SocketAddr>>>,
}

impl<Gc, Lc> LocalNameCn<Gc, Lc> {
    fn new(cl: Option<Arc<Mutex<client::LocalNameClient>>>, global_cn: Gc, local_cn: Lc) -> Self {
        Self {
            cl,
            global_cn: Arc::new(global_cn),
            local_cn: Arc::new(local_cn),
            addr_cache: Default::default(),
            rev_addr_map: Default::default(),
        }
    }
}

impl<Gc, Lc, D> ChunnelConnection for LocalNameCn<Gc, Lc>
where
    Gc: ChunnelConnection<Data = (SocketAddr, D)> + Send + Sync + 'static,
    Lc: ChunnelConnection<Data = (PathBuf, D)> + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Data = (Either<SocketAddr, PathBuf>, D);

    fn send(
        &self,
        (addr, data): Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        let addr_cache = Arc::clone(&self.addr_cache);
        let rev_addr_map = Arc::clone(&self.rev_addr_map);
        let cl = self.cl.as_ref().map(Arc::clone);
        let local_cn = Arc::clone(&self.local_cn);
        let global_cn = Arc::clone(&self.global_cn);
        Box::pin(async move {
            let addr = match addr {
                Either::Right(pb) => return local_cn.send((pb, data)).await,
                Either::Left(sk) => sk,
            };

            // 1. check local cache
            let entry = {
                let c = addr_cache.lock().unwrap();
                c.get(&addr).map(Clone::clone)
            };

            // 2. if match, send on correct connection.
            match entry {
                None => (),
                Some(LocalAddrCacheEntry::Hit { expiry, .. })
                | Some(LocalAddrCacheEntry::AntiHit {
                    expiry: Some(expiry),
                    ..
                }) if expiry < std::time::Instant::now() => (),
                Some(LocalAddrCacheEntry::Hit { laddr, .. }) => {
                    trace!(?laddr, "local send");
                    return local_cn.send((laddr.clone(), data)).await;
                }
                Some(LocalAddrCacheEntry::AntiHit { .. }) => {
                    trace!(?addr, "global send");
                    return global_cn.send((addr, data)).await;
                }
            };

            // 3. otherwise, do a lookup and then send accordingly.
            trace!(?addr, "do lookup");
            if let Some(cl) = cl {
                let mut cl_g = cl.lock().await;
                let res = cl_g.query(addr).await;
                std::mem::drop(cl_g);
                match res {
                    Ok(Some(laddr)) => {
                        {
                            let mut c = addr_cache.lock().unwrap();
                            c.insert(
                                addr,
                                LocalAddrCacheEntry::Hit {
                                    laddr: laddr.clone(),
                                    expiry: std::time::Instant::now()
                                        + std::time::Duration::from_millis(100),
                                },
                            );
                        }

                        {
                            rev_addr_map.lock().unwrap().insert(laddr.clone(), addr);
                        }

                        trace!(?laddr, "local send");
                        return local_cn.send((laddr.clone(), data)).await;
                    }
                    Ok(None) => {
                        {
                            let mut c = addr_cache.lock().unwrap();
                            c.insert(
                                addr,
                                LocalAddrCacheEntry::AntiHit {
                                    expiry: Some(
                                        std::time::Instant::now()
                                            + std::time::Duration::from_millis(100),
                                    ),
                                },
                            );
                        }

                        trace!(?addr, "global send");
                        return global_cn.send((addr, data)).await;
                    }
                    Err(e) => {
                        debug!(err = %format!("{:#}", e), ?addr, "LocalNameClient query failed");
                        return global_cn.send((addr, data)).await;
                    }
                }
            } else {
                {
                    let mut c = addr_cache.lock().unwrap();
                    c.insert(addr, LocalAddrCacheEntry::AntiHit { expiry: None });
                }

                return global_cn.send((addr, data)).await;
            }
        })
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        use futures_util::future::{self, Either as FEither};
        let rev_addr_map = Arc::clone(&self.rev_addr_map);
        let local_cn = Arc::clone(&self.local_cn);
        let global_cn = Arc::clone(&self.global_cn);
        Box::pin(async move {
            match future::select(global_cn.recv(), local_cn.recv()).await {
                FEither::Left((Ok((gaddr, data)), _)) => Ok((Either::Left(gaddr), data)),
                FEither::Left((Err(e), _)) => Err(e),
                FEither::Right((Ok((laddr, data)), _)) => {
                    trace!(?laddr, "recvd");
                    let c = rev_addr_map.lock().unwrap();
                    match c.get(&laddr) {
                        Some(addr) => Ok((Either::Left(*addr), data)),
                        None => Ok((Either::Right(laddr), data)),
                    }
                }
                FEither::Right((Err(e), _)) => Err(e.wrap_err("local_cn recv erred")),
            }
        })
    }
}

#[cfg(test)]
mod test {
    use super::LocalNameChunnel;
    use bertha::{
        either::Either, negotiate_client, negotiate_server, udp::UdpSkChunnel, uds::UnixSkChunnel,
        util::Nothing, ChunnelConnection, ChunnelConnector, ChunnelListener,
    };
    use futures_util::stream::TryStreamExt;
    use tracing::info;
    use tracing_error::ErrorLayer;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    #[test]
    fn no_ctl() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        color_eyre::install().unwrap_or(());

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let addr = "127.0.0.1:19052".parse().unwrap();
        let lch = LocalNameChunnel {
            cl: None,
            listen_addr: None,
            local_raw: UnixSkChunnel,
            local_chunnel: Nothing::<()>::default(),
        };

        rt.block_on(async move {
            let lch_s = lch.clone();
            tokio::spawn(async move {
                let st = negotiate_server(lch_s, UdpSkChunnel.listen(addr).await.unwrap())
                    .await
                    .unwrap();
                st.try_for_each_concurrent(None, |cn| async move {
                    loop {
                        let m = cn.recv().await.unwrap();
                        info!(?m, "got msg");
                        cn.send(m).await.unwrap();
                    }
                })
                .await
                .unwrap();
            });

            tokio::time::sleep(std::time::Duration::from_millis(100)).await;

            let cn = negotiate_client(lch, UdpSkChunnel.connect(()).await.unwrap(), addr)
                .await
                .unwrap();

            cn.send((Either::Left(addr), vec![0u8; 10])).await.unwrap();
            let m = cn.recv().await.unwrap();
            assert_eq!(m, (Either::Left(addr), vec![0u8; 10]));
        });
    }
}
