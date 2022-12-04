use crate::{dpdk_iokernel::BoundDpdkUdpSk, DpdkInlineCn, DpdkUdpSk};
use bertha::{ChunnelConnection, Either};
use color_eyre::Report;
use flume::Receiver;
use std::{
    fmt::Debug,
    future::Future,
    net::{SocketAddr, SocketAddrV4},
    pin::Pin,
    sync::Arc,
};
use tokio::sync::Mutex;
use tracing::debug;

#[derive(Debug)]
pub(crate) enum DatapathCnInner {
    Thread(Either<DpdkUdpSk, BoundDpdkUdpSk>),
    Inline(DpdkInlineCn),
}

impl DatapathCnInner {
    pub(crate) fn local_port(&self) -> u16 {
        match self {
            DatapathCnInner::Thread(Either::Left(sk)) => sk.local_port(),
            DatapathCnInner::Thread(Either::Right(sk)) => sk.local_port(),
            DatapathCnInner::Inline(sk) => sk.local_port(),
        }
    }

    pub(crate) fn remote_addr(&self) -> Option<SocketAddrV4> {
        match self {
            DatapathCnInner::Thread(Either::Left(_)) => None,
            DatapathCnInner::Thread(Either::Right(sk)) => Some(sk.remote_addr()),
            DatapathCnInner::Inline(sk) => sk.remote_addr(),
        }
    }
}

/// Either of the types of connections, with a way to reconstruct connection state either way.
/// SAFETY: `DatapathCn` has an `UnsafeCell` in it, but must have an `unsafe impl Sync`. It can
/// only be safely used in single-threaded contexts. Specifically, `maybe_swap_datapath` cannot
/// be called concurrently with `send`/`recv`.
pub struct DatapathCn {
    pub(crate) inner: Arc<Mutex<DatapathCnInner>>,
    pub(crate) new_datapath: Receiver<DatapathCnInner>,
    pub(crate) local_port: u16,
    pub(crate) remote_addr: Option<SocketAddrV4>,
}

impl Debug for DatapathCn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatapathCn").finish()
    }
}

impl DatapathCn {
    pub fn local_port(&self) -> u16 {
        self.local_port
    }

    pub fn remote_addr(&self) -> Option<SocketAddrV4> {
        self.remote_addr
    }
}

impl ChunnelConnection for DatapathCn {
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
            if let Ok(new_dp) = self.new_datapath.try_recv() {
                debug!("applying upgraded semantics (send)");
                let mut inner_g = self.inner.lock().await;
                *inner_g = new_dp;
            }

            let inner_g = self.inner.lock().await;
            match *inner_g {
                DatapathCnInner::Thread(ref s) => s.send(burst).await,
                DatapathCnInner::Inline(ref s) => s.send(burst).await,
            }
        })
    }

    fn recv<'cn, 'buf>(
        &'cn self,
        msgs_buf: &'buf mut [Option<Self::Data>],
    ) -> Pin<Box<dyn Future<Output = Result<&'buf mut [Option<Self::Data>], Report>> + Send + 'cn>>
    where
        'buf: 'cn,
    {
        use futures_util::future;
        Box::pin(async move {
            let mut slots: Vec<_> = (0..msgs_buf.len()).map(|_| None).collect();
            loop {
                let slots_borrow = &mut slots;
                let recv_fut = async move {
                    let inner_g = self.inner.lock().await;
                    match *inner_g {
                        DatapathCnInner::Thread(ref s) => s.recv(slots_borrow).await,
                        DatapathCnInner::Inline(ref s) => s.recv(slots_borrow).await,
                    }
                };

                let upgrade_fut = self.new_datapath.recv_async();

                // We need to `Box::pin` this so that the `drop` call below actually drops the future,
                // instead of dropping a `Pin` of the future. We need to actually drop the future so
                // that it drops the `MutexGuard` it holds, so the lock doesn't deadlock.
                let recv_fut = Box::pin(recv_fut);
                let upgrade_fut = Box::pin(upgrade_fut);
                // we need a temporary variable here to let the compiler figure out slots_borrow will
                // be dropped before slots
                match future::select(recv_fut, upgrade_fut).await {
                    future::Either::Left((recvd, _)) => {
                        let mut slot_idx = 0;
                        for r in recvd?.into_iter().map_while(Option::take) {
                            msgs_buf[slot_idx] = Some(r);
                            slot_idx += 1;
                        }

                        break Ok(&mut msgs_buf[..slot_idx]);
                    }
                    future::Either::Right((new_dp, recvr)) => {
                        debug!("performing datapath connection swap");
                        std::mem::drop(recvr); // cancel the future and drop, so its lock on inner is dropped.
                        let mut inner = self.inner.lock().await;
                        *inner = new_dp.expect("datapath transition sender disappeared");
                        debug!("swapped datapath connection");
                    }
                }
            }
        })
    }
}
