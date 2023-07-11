use crate::switcher::{ActiveConnection, DatapathConnectionMigrator};
use ahash::HashMap;
use bertha::{either::Either, ChunnelConnection, ChunnelConnector, ChunnelListener};
use color_eyre::eyre::{bail, eyre, Report, WrapErr};
use dpdk_wrapper::{BoundDpdkConn, DpdkConn, DpdkIoKernel, DpdkIoKernelHandle};
use futures_util::future::{ready, Ready};
use futures_util::stream::{once, Once, Stream, StreamExt};
use macaddr::MacAddr6 as MacAddress;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
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

fn iokernel_restart(
    ip_addr: Ipv4Addr,
    arp_table: HashMap<Ipv4Addr, MacAddress>,
) -> Result<DpdkIoKernelHandle, Report> {
    let mut rt_guard = RUNTIME_HANDLE
        .lock()
        .expect("Runtime lock acquisition failure is critical");
    if let Some(ref s) = *rt_guard {
        return Ok(s.clone());
    }

    let (handle_s, handle_r) = flume::bounded(1);
    std::thread::spawn(move || {
        let (iokernel, handle) = DpdkIoKernel::new_configure_only(ip_addr, arp_table).unwrap();
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
}

impl DpdkUdpSkChunnel {
    pub fn new(config: impl AsRef<Path>) -> Result<Self, Report> {
        let config = config.as_ref().to_path_buf();
        let handle = iokernel_start(config)?;
        Ok(Self { handle })
    }

    pub fn new_preconfig(
        ip_addr: Ipv4Addr,
        arp_table: HashMap<Ipv4Addr, MacAddress>,
    ) -> Result<Self, Report> {
        let handle = iokernel_restart(ip_addr, arp_table)?;
        Ok(Self { handle })
    }

    pub fn get_cfg(&self) -> (Ipv4Addr, HashMap<Ipv4Addr, MacAddress>) {
        (self.handle.ip_addr(), self.handle.arp_table())
    }

    fn do_shutdown(&self) {
        self.handle.shutdown();
        loop {
            let rt_guard = RUNTIME_HANDLE
                .lock()
                .expect("Runtime lock acquisition failure is critical");
            if rt_guard.is_none() {
                break;
            }
        }
    }

    pub(crate) fn load_connections(
        &mut self,
        conns: Vec<ActiveConnection>,
    ) -> Result<HashMap<ActiveConnection, Either<DpdkUdpSk, BoundDpdkUdpSk>>, Report> {
        let mut out = HashMap::default();
        // first group by ActiveConnection type
        let acceptors: Result<HashMap<_, _>, _> = conns
            .into_iter()
            .map(|c| match c {
                ActiveConnection::UnConnected { local_port } => {
                    let inner = self.handle.socket(Some(local_port))?;
                    let cn = DpdkUdpSk {
                        inner,
                        drain_first_recv: Some(Default::default()),
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
                        drain_first_recv: Some(Default::default()),
                    }),
                );
            }

            further_conns.insert(local_port, further_conns_on_port);
        }

        Ok(out)
    }

    pub(crate) fn do_listen_non_accept(&mut self, addr: SocketAddr) -> Result<DpdkUdpSk, Report> {
        let a = match addr {
            SocketAddr::V4(a) => a,
            _ => bail!("Only IPv4 is supported: {:?}", addr),
        };

        let inner = self.handle.socket(Some(a.port()))?;

        Ok(DpdkUdpSk {
            inner,
            drain_first_recv: None,
        })
    }
}

impl DatapathConnectionMigrator for DpdkUdpSkChunnel {
    type Conn = Either<DpdkUdpSk, BoundDpdkUdpSk>;
    type Error = Report;

    fn shut_down(&mut self) -> Result<(), Report> {
        self.do_shutdown();
        Ok(())
    }

    fn load_connections(
        &mut self,
        conns: Vec<ActiveConnection>,
    ) -> Result<HashMap<ActiveConnection, Self::Conn>, Self::Error> {
        self.load_connections(conns)
    }
}

impl ChunnelListener for DpdkUdpSkChunnel {
    type Addr = SocketAddr;
    type Connection = DpdkUdpSk;
    type Future = Ready<Result<Self::Stream, Report>>;
    type Stream = Once<Ready<Result<Self::Connection, Self::Error>>>;
    type Error = Report;

    fn listen(&mut self, addr: Self::Addr) -> Self::Future {
        let res = self.do_listen_non_accept(addr);
        match res {
            Ok(r) => ready(Ok(once(ready(Ok(r))))),
            Err(e) => ready(Err(e)),
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
            drain_first_recv: None,
        });
        futures_util::future::ready(sk)
    }
}

pub struct DpdkUdpSk {
    inner: DpdkConn,
    drain_first_recv: Option<AtomicBool>,
}

impl DpdkUdpSk {
    pub fn local_port(&self) -> u16 {
        self.inner.get_port()
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
        if let Some(ref o) = self.drain_first_recv {
            if let Ok(_) = o.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed) {
                return Box::pin(async move { self.inner.recv_async_batch_drain(msgs_buf).await });
            }
        }

        Box::pin(async move { self.inner.recv_async_batch(msgs_buf).await })
    }
}

pub struct BoundDpdkUdpSk {
    inner: BoundDpdkConn,
    drain_first_recv: Option<AtomicBool>,
}

impl BoundDpdkUdpSk {
    pub fn local_port(&self) -> u16 {
        self.inner.port()
    }

    pub fn remote_addr(&self) -> SocketAddrV4 {
        self.inner.remote_addr()
    }
}

impl std::fmt::Debug for BoundDpdkUdpSk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundDpdkUdpSk").finish()
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
        if let Some(ref o) = self.drain_first_recv {
            if let Ok(_) = o.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed) {
                return Box::pin(async move { self.inner.recv_async_batch_drain(msgs_buf).await });
            }
        }

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
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + Sync + 'static>>;
    type Error = Report;

    fn listen(&mut self, addr: Self::Addr) -> Self::Future {
        use SocketAddr::*;
        match addr {
            V4(a) => {
                let sk = self.0.handle.accept(a.port());
                futures_util::future::ready(sk.map(move |s| {
                    Box::pin(s.into_stream().map(move |inner| {
                        Ok(BoundDpdkUdpSk {
                            inner,
                            drain_first_recv: None,
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
    type Error = Report;

    fn shut_down(&mut self) -> Result<(), Report> {
        self.0.do_shutdown();
        Ok(())
    }

    fn load_connections(
        &mut self,
        conns: Vec<ActiveConnection>,
    ) -> Result<HashMap<ActiveConnection, Self::Conn>, Self::Error> {
        self.0.load_connections(conns)
    }
}
