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

use bertha::{ChunnelConnection, ChunnelConnector, ChunnelListener, Client, Either, Serve};
use eyre::{ensure, eyre, Error};
use futures_util::stream::{Stream, StreamExt};
use std::collections::HashMap;
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
#[derive(Debug, Clone)]
pub struct LocalNameSrv<L, Ls> {
    cl: Arc<Mutex<client::LocalNameClient>>,
    local_inner: Arc<Mutex<L>>,
    local_serve: Ls,
    extern_addr: SocketAddr,
}

impl<L, Ls> LocalNameSrv<L, Ls> {
    pub async fn new(
        root: impl AsRef<Path>,
        extern_addr: SocketAddr,
        local_listen: L,
        local_serve: Ls,
    ) -> Result<Self, Error> {
        let cl = client::LocalNameClient::new(root.as_ref()).await?;
        Ok(LocalNameSrv {
            cl: Arc::new(Mutex::new(cl)),
            local_inner: Arc::new(Mutex::new(local_listen)),
            local_serve,
            extern_addr,
        })
    }
}

impl<L, Ls, Lrc, Lc, Le, I, Ic, Ie, D> Serve<I> for LocalNameSrv<L, Ls>
where
    I: Stream<Item = Result<Ic, Ie>> + Send + 'static,
    Ic: ChunnelConnection<Data = (SocketAddr, D)> + Send + Sync + 'static,
    L: ChunnelListener<Addr = PathBuf, Connection = Lrc, Error = Le> + Send + 'static,
    Ls: Serve<L::Stream, Connection = Lc> + Clone + Send + 'static,
    <Ls as Serve<L::Stream>>::Error: Into<Error> + Send + Sync + 'static,
    Lrc: ChunnelConnection + Send + 'static,
    Lc: ChunnelConnection<Data = (PathBuf, D)> + Send + Sync + 'static,
    Le: Into<Error> + Send + Sync + 'static,
    Ie: Into<Error> + Send + Sync + 'static,
    D: Send + 'static,
{
    type Connection = LocalNameSrvCn<Ic, Lc>;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn serve(&mut self, inner: I) -> Self::Future {
        let cl = Arc::clone(&self.cl);
        let local_inner = Arc::clone(&self.local_inner);
        let mut local_serve = self.local_serve.clone();
        let extern_addr = self.extern_addr;
        Box::pin(async move {
            // call client.register
            let local_addr = cl.lock().await.register(extern_addr).await?;

            let ext_str =
                inner.map(|conn| Ok::<_, Error>(LocalNameSrvCn::Ext(conn.map_err(Into::into)?)));

            // need to listen on the returned addr
            let local_raw_cn = local_inner
                .lock()
                .await
                .listen(local_addr)
                .await
                .map_err(Into::into)?;
            let int_str = local_serve
                .serve(local_raw_cn)
                .await
                .map_err(Into::into)?
                .map(|conn| Ok(LocalNameSrvCn::Loc(conn.map_err(Into::into)?)));

            Ok(Box::pin(futures_util::stream::select(ext_str, int_str)) as _)
        })
    }
}

pub enum LocalNameSrvCn<P, L> {
    Ext(P),
    Loc(L),
}

impl<P, L, D> ChunnelConnection for LocalNameSrvCn<P, L>
where
    P: ChunnelConnection<Data = (SocketAddr, D)> + Send + Sync + 'static,
    L: ChunnelConnection<Data = (PathBuf, D)> + Send + Sync + 'static,
    D: Send + 'static,
{
    type Data = (Either<SocketAddr, PathBuf>, D);

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>> {
        match self {
            Self::Ext(cn) => {
                let fut = cn.recv();
                Box::pin(async move {
                    let (a, d) = fut.await?;
                    Ok((Either::Left(a), d))
                }) as _
            }
            Self::Loc(cn) => {
                let fut = cn.recv();
                Box::pin(async move {
                    let (a, d) = fut.await?;
                    Ok((Either::Right(a), d))
                }) as _
            }
        }
    }

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>> {
        let (addr, data) = data;
        match self {
            Self::Ext(cn) => {
                let fut = match addr {
                    Either::Left(sk) => Ok(cn.send((sk, data))),
                    _ => Err(eyre!("Incorrect address type for connection")),
                };

                Box::pin(async move {
                    fut?.await?;
                    Ok(())
                }) as _
            }
            Self::Loc(cn) => {
                let fut = match addr {
                    Either::Right(sk) => Ok(cn.send((sk, data))),
                    _ => Err(eyre!("Incorrect address type for connection")),
                };

                Box::pin(async move {
                    fut?.await?;
                    Ok(())
                }) as _
            }
        }
    }
}

/// `LocalNameCln` decides which inner connection to send on based on the address.
pub struct LocalNameCln<L, W> {
    cl: Arc<Mutex<client::LocalNameClient>>,
    local_inner: L,
    local_stack: W,
}

impl<L, W> LocalNameCln<L, W> {
    pub async fn new(
        root: impl AsRef<Path>,
        local_inner: L,
        local_stack: W,
    ) -> Result<Self, Error> {
        let cl = client::LocalNameClient::new(root.as_ref()).await?;
        Ok(LocalNameCln {
            cl: Arc::new(Mutex::new(cl)),
            local_inner,
            local_stack,
        })
    }
}

impl<L, W, Wc, I, D> Client<I> for LocalNameCln<L, W>
where
    I: ChunnelConnection<Data = (SocketAddr, D)> + Send + Sync + 'static,
    L: ChunnelConnector<Addr = PathBuf> + Clone + Send + 'static,
    L::Error: Into<Error>,
    L::Connection: Send + 'static,
    W: Client<L::Connection, Connection = Wc> + Clone + Send + 'static,
    Wc: ChunnelConnection<Data = (PathBuf, D)> + Send + Sync + 'static,
    W::Error: Into<Error>,
    D: Send + Sync + 'static,
{
    type Connection = LocalNameCn<I, L, W>;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Error;

    // `inner` is the extern connection with (A, D) semantics
    // also make a local connection
    fn connect_wrap(&mut self, inner: I) -> Self::Future {
        let cl = Arc::clone(&self.cl);
        let local_inner = self.local_inner.clone();
        let local_stack = self.local_stack.clone();
        Box::pin(async move { Ok(LocalNameCn::new(inner, cl, local_inner, local_stack)) })
    }
}

#[derive(Debug, Clone)]
enum LocalConnsState<C> {
    Conn(PathBuf, C),
    Checked(std::time::Instant),
}

impl<C> LocalConnsState<C> {
    fn unwrap_conn(&self) -> &C {
        match self {
            LocalConnsState::Conn(_, c) => c,
            _ => unreachable!(),
        }
    }
}

pub struct LocalNameCn<C, L: ChunnelConnector, W: Client<L::Connection>> {
    cl: Arc<Mutex<client::LocalNameClient>>,
    extern_conn: Arc<Mutex<C>>,
    local_connector: L,
    local_stack: W,
    // local cache of SocketAddr to local connection.
    local_conns: Arc<Mutex<HashMap<SocketAddr, LocalConnsState<W::Connection>>>>,
    // local cache to remember which original SocketAddr the peer is.
    local_addrs: Arc<Mutex<HashMap<PathBuf, SocketAddr>>>,
}

impl<C, L, W> LocalNameCn<C, L, W>
where
    L: ChunnelConnector,
    W: Client<L::Connection>,
{
    pub fn new(
        cn: C,
        cl: Arc<Mutex<client::LocalNameClient>>,
        local_connector: L,
        local_stack: W,
    ) -> Self {
        LocalNameCn {
            cl,
            extern_conn: Arc::new(Mutex::new(cn)),
            local_connector,
            local_stack,
            local_conns: Default::default(),
            local_addrs: Default::default(),
        }
    }
}

impl<C, L, W, Wc, D> ChunnelConnection for LocalNameCn<C, L, W>
where
    C: ChunnelConnection<Data = (SocketAddr, D)> + Send + Sync + 'static,
    L: ChunnelConnector<Addr = PathBuf> + Clone + Send + 'static,
    L::Connection: Send + 'static,
    L::Error: Into<Error>,
    W: Client<L::Connection, Connection = Wc> + Clone + Send + 'static,
    Wc: ChunnelConnection<Data = (PathBuf, D)> + Send + Sync + 'static,
    W::Error: Into<Error>,
    D: Send + Sync + 'static,
{
    type Data = (SocketAddr, D);

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>> {
        use LocalConnsState::*;
        let (addr, data) = data;
        let extern_cn = Arc::clone(&self.extern_conn);
        let mut local_connector = self.local_connector.clone();
        let mut local_stack = self.local_stack.clone();
        let local_conns = Arc::clone(&self.local_conns);
        let local_addrs = Arc::clone(&self.local_addrs);
        let cl = Arc::clone(&self.cl);
        Box::pin(async move {
            let mut local_conns_g = local_conns.lock().await;
            let mut local_addrs_g = local_addrs.lock().await;
            match local_conns_g.get(&addr) {
                Some(Conn(local_addr, cn)) => cn.send((local_addr.clone(), data)).await,
                Some(Checked(then)) if then.elapsed() > std::time::Duration::from_millis(100) => {
                    // not in local connections cache, ask localname-ctl
                    if let Some(local_addr) = cl.lock().await.query(addr).await? {
                        local_addrs_g.insert(local_addr.clone(), addr);
                        let raw_cn = local_connector
                            .connect(local_addr.clone())
                            .await
                            .map_err(Into::into)?;
                        let cn = local_stack.connect_wrap(raw_cn).await.map_err(Into::into)?;
                        local_conns_g.insert(addr, Conn(local_addr.clone(), cn));
                        local_conns_g
                            .get(&addr)
                            .unwrap()
                            .unwrap_conn()
                            .send((local_addr, data))
                            .await
                    } else {
                        let entry = local_conns_g.get_mut(&addr).unwrap();
                        *entry = Checked(std::time::Instant::now());
                        extern_cn.lock().await.send((addr, data)).await
                    }
                }
                Some(Checked(_)) => extern_cn.lock().await.send((addr, data)).await,
                None => {
                    local_conns_g.insert(addr, Checked(std::time::Instant::now()));
                    extern_cn.lock().await.send((addr, data)).await
                }
            }
        })
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>> {
        let local_conns = Arc::clone(&self.local_conns);
        let local_addrs = Arc::clone(&self.local_addrs);
        let extern_conn = Arc::clone(&self.extern_conn);
        Box::pin(async move {
            let local_addrs = Arc::clone(&local_addrs);
            let local_conns_g = local_conns.lock().await;
            let mut recv_futs: Vec<_> = local_conns_g
                .values()
                .filter_map(move |v| {
                    let local_addrs = Arc::clone(&local_addrs);
                    match v {
                        LocalConnsState::Conn(path, cn) => Some(Box::pin(async move {
                            let (rpath, data) = cn.recv().await?;
                            ensure!(
                                &rpath == path,
                                "Client connection address (path) mismatch: {:?} != {:?}",
                                rpath,
                                path
                            );

                            let local_addrs_g = local_addrs.lock().await;
                            let sk = local_addrs_g.get(&rpath).expect(
                                "local_addrs has same keys as local_conns, which we already queried",
                            );
                            Ok((*sk, data))
                        })
                            as Pin<Box<dyn Future<Output = _> + Send>>),
                        _ => None,
                    }
                })
                .collect();

            recv_futs.push(Box::pin(async move {
                let extern_conn_g = extern_conn.lock().await;
                extern_conn_g.recv().await
            }) as _);

            let (rep, _) = futures_util::future::select_ok(recv_futs).await?;
            Ok(rep)
        })
    }
}
