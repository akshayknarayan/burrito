use bertha::{ChunnelConnection, ChunnelConnector, ChunnelListener};
use color_eyre::eyre::{eyre, Report, WrapErr};
use dpdk_wrapper::{BoundDpdkConn, DpdkConn, DpdkIoKernel, DpdkIoKernelHandle};
use futures_util::stream::Stream;
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex};

lazy_static::lazy_static! {
static ref RUNTIME_HANDLE: Arc<StdMutex<Option<DpdkIoKernelHandle>>> =
    Arc::new(StdMutex::new(None));
}

// everything here blocks.
// On success, this won't return while the DpdkIoKernel still lives.
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
                Box::pin(futures_util::future::ready(Ok(
                    Box::pin(futures_util::stream::once(futures_util::future::ready(
                        sk.map(|inner| DpdkUdpSk { inner }),
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
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Report;

    fn connect(&mut self, _a: Self::Addr) -> Self::Future {
        let sk = self.handle.socket(None).map(|inner| DpdkUdpSk { inner });
        Box::pin(futures_util::future::ready(sk))
    }
}

pub struct DpdkUdpSk {
    inner: DpdkConn,
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

pub struct BoundDpdkUdpSk(BoundDpdkConn);

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
                self.0.send_async(data).await?;
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
        Box::pin(async move { self.0.recv_async_batch(msgs_buf).await })
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
        use futures_util::stream::StreamExt;
        use SocketAddr::*;
        match addr {
            V4(a) => {
                let sk = self.0.handle.accept(a.port());
                futures_util::future::ready(
                    sk.map(|s| Box::pin(s.into_stream().map(BoundDpdkUdpSk).map(Ok)) as _),
                )
            }
            V6(a) => futures_util::future::ready(Err(eyre!("Only IPv4 is supported: {:?}", a))),
        }
    }
}
