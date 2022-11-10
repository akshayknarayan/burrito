use crate::switcher::{ActiveConnection, DatapathConnectionMigrator, LoadedConnections};
use ahash::{HashMap, HashSet};
use bertha::{either::Either, ChunnelConnection, ChunnelConnector, ChunnelListener};
use color_eyre::eyre::{ensure, eyre, Report, WrapErr};
use dpdk_wrapper::{BoundDpdkConn, DpdkConn, DpdkIoKernel, DpdkIoKernelHandle};
use flume::Receiver;
use futures_util::stream::{Stream, StreamExt};
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex};

lazy_static::lazy_static! {
static ref RUNTIME_HANDLE: Arc<StdMutex<Option<DpdkIoKernelHandle>>> =
    Arc::new(StdMutex::new(None));
}

fn iokernel_start(config: PathBuf) -> Result<DpdkIoKernelHandle, Report> {
    let mut rt_guard = RUNTIME_HANDLE
        .lock()
        .expect("Runtime lock acquisition failure is critical");
    if let Some(ref s) = *rt_guard {
        return Ok(s.clone());
    }

    let (handle_s, handle_r) = flume::bounded(1);
    std::thread::spawn(move || {
        let (iokernel, handle) = DpdkIoKernel::new(config.to_path_buf()).unwrap();
        handle_s.send(handle).unwrap();
        iokernel.run();

        // exit and shut down. need to clear out the out handle.
        let mut rt_guard = RUNTIME_HANDLE
            .lock()
            .expect("Runtime lock acquisition failure is critical");
        rt_guard.take();
    });

    let handle = handle_r.recv().wrap_err("Could not start DpdkIoKernel")?;
    rt_guard.replace(handle.clone());
    Ok(handle)
}

#[derive(Clone, Debug)]
pub struct DpdkUdpSkChunnel {
    handle: DpdkIoKernelHandle,
    active_conns: Arc<StdMutex<HashSet<ActiveConnection>>>,
}

struct LoadedConns {
    conns: HashMap<ActiveConnection, Either<DpdkUdpSk, BoundDpdkUdpSk>>,
    further_conns: HashMap<u16, Receiver<BoundDpdkConn>>,
}

impl DpdkUdpSkChunnel {
    pub fn new(config: impl AsRef<Path>) -> Result<Self, Report> {
        let config = config.as_ref().to_path_buf();
        let handle = iokernel_start(config)?;
        Ok(Self {
            handle,
            active_conns: Default::default(),
        })
    }

    fn new_conn(&self, cn: ActiveConnection) {
        self.active_conns.lock().unwrap().insert(cn);
    }

    fn active_connections(&self) -> Vec<ActiveConnection> {
        self.active_conns
            .lock()
            .unwrap()
            .iter()
            .map(|x| *x)
            .collect()
    }

    fn shut_down(&self) {
        self.handle.shutdown();
        loop {
            let mut rt_guard = RUNTIME_HANDLE
                .lock()
                .expect("Runtime lock acquisition failure is critical");
            if rt_guard.is_none() {
                break;
            }
        }
    }

    fn load_connections(&mut self, conns: Vec<ActiveConnection>) -> Result<LoadedConns, Report> {
        let mut out = HashMap::default();
        // first group by ActiveConnection type
        let acceptors: Result<HashMap<_, _>, _> = conns
            .into_iter()
            .map(|c| match c {
                ActiveConnection::UnConnected { local_port } => {
                    let inner = self.handle.socket(Some(local_port))?;
                    let cn = DpdkUdpSk {
                        inner,
                        active_conns: Arc::clone(&self.active_conns),
                    };

                    out.insert(c, Either::Left(cn));
                    Ok::<_, Report>(None)
                }
                ActiveConnection::Connected {
                    local_port,
                    remote_addr,
                } => Ok::<_, Report>(Some((local_port, remote_addr))),
            })
            // need to group into port, [remote_addr]
            .try_fold(HashMap::<_, Vec<_>>::default(), |mut acceptors, e| {
                if let Some((local_port, remote_addr)) = e? {
                    acceptors.entry(local_port).or_default().push(remote_addr);
                }

                Ok::<_, Report>(acceptors)
            });

        let mut further_conns = HashMap::default();
        for (local_port, remote_addrs) in acceptors? {
            let (conns, further_conns_on_port) =
                self.handle.init_accepted(local_port, remote_addrs)?;
            for (remote_addr, conn) in conns {
                out.insert(
                    ActiveConnection::Connected {
                        local_port,
                        remote_addr,
                    },
                    Either::Right(BoundDpdkUdpSk {
                        inner: conn,
                        active_conns: Arc::clone(&self.active_conns),
                    }),
                );
            }

            further_conns.insert(local_port, further_conns_on_port);
        }

        Ok(LoadedConns {
            conns: out,
            further_conns,
        })
    }
}

impl DatapathConnectionMigrator for DpdkUdpSkChunnel {
    type Conn = Either<DpdkUdpSk, BoundDpdkUdpSk>;
    type Stream = futures_util::stream::Empty<Result<Self::Conn, Self::Error>>;
    type Error = Report;

    fn active_connections(&self) -> Vec<ActiveConnection> {
        self.active_connections()
    }

    fn shut_down(&mut self) -> Result<(), Report> {
        self.shut_down();
        Ok(())
    }

    fn load_connections(
        &mut self,
        conns: Vec<ActiveConnection>,
    ) -> Result<LoadedConnections<Self::Conn, Self::Stream>, Self::Error> {
        let LoadedConns {
            conns,
            further_conns,
        } = self.load_connections(conns)?;

        ensure!(
            further_conns.is_empty(),
            "DpdkUdpSkChunnel should not have any accept-style connections"
        );

        Ok(LoadedConnections {
            conns,
            accept_streams: Default::default(),
        })
    }
}

impl ChunnelListener for DpdkUdpSkChunnel {
    type Addr = SocketAddr;
    type Connection = DpdkUdpSk;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Report;

    fn listen(&mut self, addr: Self::Addr) -> Self::Future {
        use SocketAddr::*;
        match addr {
            V4(a) => {
                let sk = self.handle.socket(Some(a.port()));
                self.new_conn(ActiveConnection::UnConnected {
                    local_port: a.port(),
                });
                Box::pin(futures_util::future::ready(Ok(
                    Box::pin(futures_util::stream::once(futures_util::future::ready(
                        sk.map(|inner| DpdkUdpSk {
                            inner,
                            active_conns: Arc::clone(&self.active_conns),
                        }),
                    ))) as _,
                )))
            }
            V6(a) => Box::pin(futures_util::future::ready(Err(eyre!(
                "Only IPv4 is supported: {:?}",
                a
            )))),
        }
    }
}

impl ChunnelConnector for DpdkUdpSkChunnel {
    type Addr = SocketAddr;
    type Connection = DpdkUdpSk;
    type Future = futures_util::future::Ready<Result<Self::Connection, Report>>;
    type Error = Report;

    fn connect(&mut self, _a: Self::Addr) -> Self::Future {
        let sk = self.handle.socket(None).map(|inner| DpdkUdpSk {
            inner,
            active_conns: Arc::clone(&self.active_conns),
        });
        futures_util::future::ready(sk)
    }
}

pub struct DpdkUdpSk {
    inner: DpdkConn,
    active_conns: Arc<StdMutex<HashSet<ActiveConnection>>>,
}

impl Drop for DpdkUdpSk {
    fn drop(&mut self) {
        let desc = ActiveConnection::UnConnected {
            local_port: self.inner.get_port(),
        };

        self.active_conns.lock().unwrap().remove(&desc);
    }
}

impl std::fmt::Debug for DpdkUdpSk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DpdkUdpSk").finish()
    }
}

impl ChunnelConnection for DpdkUdpSk {
    type Data = (SocketAddr, Vec<u8>);

    fn send<'cn, B>(
        &'cn self,
        burst: B,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'cn>>
    where
        B: IntoIterator<Item = Self::Data> + Send + 'cn,
        <B as IntoIterator>::IntoIter: Send,
    {
        Box::pin(async move {
            for (addr, data) in burst {
                self.inner.send_async(addr, data).await?;
            }

            Ok(())
        })
    }

    fn recv<'cn, 'buf>(
        &'cn self,
        msgs_buf: &'buf mut [Option<Self::Data>],
    ) -> Pin<Box<dyn Future<Output = Result<&'buf mut [Option<Self::Data>], Report>> + Send + 'cn>>
    where
        'buf: 'cn,
    {
        Box::pin(async move { self.inner.recv_async_batch(msgs_buf).await })
    }
}

pub struct BoundDpdkUdpSk {
    inner: BoundDpdkConn,
    active_conns: Arc<StdMutex<HashSet<ActiveConnection>>>,
}

impl std::fmt::Debug for BoundDpdkUdpSk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundDpdkUdpSk").finish()
    }
}

impl Drop for BoundDpdkUdpSk {
    fn drop(&mut self) {
        let desc = ActiveConnection::Connected {
            local_port: self.inner.port(),
            remote_addr: self.inner.remote_addr(),
        };

        self.active_conns.lock().unwrap().remove(&desc);
    }
}

impl ChunnelConnection for BoundDpdkUdpSk {
    type Data = (SocketAddr, Vec<u8>);

    fn send<'cn, B>(
        &'cn self,
        burst: B,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'cn>>
    where
        B: IntoIterator<Item = Self::Data> + Send + 'cn,
        <B as IntoIterator>::IntoIter: Send,
    {
        Box::pin(async move {
            for (_, data) in burst {
                self.inner.send_async(data).await?;
            }

            Ok(())
        })
    }

    fn recv<'cn, 'buf>(
        &'cn self,
        msgs_buf: &'buf mut [Option<Self::Data>],
    ) -> Pin<Box<dyn Future<Output = Result<&'buf mut [Option<Self::Data>], Report>> + Send + 'cn>>
    where
        'buf: 'cn,
    {
        Box::pin(async move { self.inner.recv_async_batch(msgs_buf).await })
    }
}

#[derive(Clone)]
pub struct DpdkUdpReqChunnel(pub DpdkUdpSkChunnel);

impl ChunnelListener for DpdkUdpReqChunnel {
    type Addr = SocketAddr;
    type Connection = BoundDpdkUdpSk;
    type Future = futures_util::future::Ready<Result<Self::Stream, Self::Error>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Report;

    fn listen(&mut self, addr: Self::Addr) -> Self::Future {
        use SocketAddr::*;
        match addr {
            V4(a) => {
                let sk = self.0.handle.accept(a.port());
                let active_conns = Arc::clone(&self.0.active_conns);
                futures_util::future::ready(sk.map(move |s| {
                    Box::pin(s.into_stream().map(move |inner| {
                        Ok(BoundDpdkUdpSk {
                            inner,
                            active_conns: Arc::clone(&active_conns),
                        })
                    })) as _
                }))
            }
            V6(a) => futures_util::future::ready(Err(eyre!("Only IPv4 is supported: {:?}", a))),
        }
    }
}

impl DatapathConnectionMigrator for DpdkUdpReqChunnel {
    type Conn = Either<DpdkUdpSk, BoundDpdkUdpSk>;
    type Stream = Pin<Box<dyn Stream<Item = Result<Self::Conn, Self::Error>> + Send + 'static>>;
    type Error = Report;

    fn active_connections(&self) -> Vec<ActiveConnection> {
        self.0.active_connections()
    }

    fn shut_down(&mut self) -> Result<(), Report> {
        self.0.shut_down();
        Ok(())
    }

    fn load_connections(
        &mut self,
        conns: Vec<ActiveConnection>,
    ) -> Result<LoadedConnections<Self::Conn, Self::Stream>, Self::Error> {
        let LoadedConns {
            conns,
            further_conns,
        } = self.0.load_connections(conns)?;

        let accept_streams = further_conns
            .into_iter()
            .map(|(port, further_conns_on_port)| {
                let active_conns = Arc::clone(&self.0.active_conns);
                (
                    port,
                    Box::pin(further_conns_on_port.into_stream().map(move |inner| {
                        Ok(Either::Right(BoundDpdkUdpSk {
                            inner,
                            active_conns: Arc::clone(&active_conns),
                        }))
                    })) as _,
                )
            })
            .collect();

        Ok(LoadedConnections {
            conns,
            accept_streams,
        })
    }
}
