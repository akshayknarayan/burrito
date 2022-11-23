use crate::{dpdk_iokernel::BoundDpdkUdpSk, DpdkInlineCn, DpdkUdpSk};
use bertha::{ChunnelConnection, Either};
use color_eyre::Report;
use flume::Receiver;
use std::{
    cell::UnsafeCell,
    fmt::Debug,
    future::Future,
    net::{SocketAddr, SocketAddrV4},
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    sync::{Arc, Barrier, RwLock},
    task::{Context, Poll},
};
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
    pub(crate) inner: UnsafeCell<DatapathCnInner>,
    pub(crate) wait_for_datapath_swap_now: Arc<AtomicBool>,
    pub(crate) new_datapath: Receiver<DatapathCnInner>,
    pub(crate) swap_barrier: Arc<RwLock<Barrier>>,
}

impl Debug for DatapathCn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatapathCn").finish()
    }
}

unsafe impl Sync for DatapathCn {}

impl DatapathCn {
    pub fn local_port(&self) -> u16 {
        unsafe { (&*self.inner.get()).local_port() }
    }

    pub fn remote_addr(&self) -> Option<SocketAddrV4> {
        unsafe { (&*self.inner.get()).remote_addr() }
    }

    /// Check if we should swap datapaths, and maybe do so.
    ///
    /// This function is synchronous because it is perfectly ok to block the current thread on
    /// synchronization operations among *other threads* - in fact, blocking is desirable.
    ///
    /// # Safety
    /// When all threads are ready to swap (and thus won't try to perform DPDK operations), we
    /// *know* that no one is currently using references to connection types, because we just
    /// barrier-synchronized on being ready to swap. Thus, swapping the pointer is ok.
    fn maybe_swap_datapath(&self) {
        if self.wait_for_datapath_swap_now.load(Ordering::SeqCst) {
            // it is important to wait on the barrier here because we need to make sure that our
            // various threads are using the same datapath!
            // we take a read lock because we are using the barrier now.
            // write locks should be taken when updating the number of active threads.
            debug!("waiting on datapath swap barrier");
            self.swap_barrier.read().unwrap().wait();

            let new_dp = self
                .new_datapath
                .recv()
                .expect("new datapath sender disappeared");
            debug!("performing datapath swap");
            // We are swapping now!
            unsafe {
                self.do_swap(new_dp);
            }
        } else {
            // we're already using the new datapath, so we just need to do the transition.
            if let Ok(new_dp) = self.new_datapath.try_recv() {
                debug!("performing datapath swap");
                unsafe {
                    self.do_swap(new_dp);
                }
            }
        }
    }

    unsafe fn do_swap(&self, new_dp: DatapathCnInner) {
        let dp = &mut *self.inner.get();
        let _drop = std::mem::replace(dp, new_dp);
        // drop the old dp
    }
}

// we need manual implementations of the send and recv futures to prevent the borrow checker from
// falsely assuming the future is not `Send`
struct DatapathCnSendFuture<'cn> {
    inner: Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'cn>>,
}

impl<'cn> DatapathCnSendFuture<'cn> {
    fn new<B>(s: &'cn DatapathCnInner, burst: B) -> Self
    where
        B: IntoIterator<Item = (SocketAddr, Vec<u8>)> + Send + 'cn,
        <B as IntoIterator>::IntoIter: Send,
    {
        Self {
            inner: match s {
                DatapathCnInner::Thread(s) => s.send(burst),
                DatapathCnInner::Inline(s) => s.send(burst),
            },
        }
    }
}

impl<'cn> Future for DatapathCnSendFuture<'cn> {
    type Output = Result<(), Report>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner).poll(cx)
    }
}

struct DatapathCnRecvFuture<'cn, 'buf> {
    inner: Pin<
        Box<
            dyn Future<Output = Result<&'buf mut [Option<(SocketAddr, Vec<u8>)>], Report>>
                + Send
                + 'cn,
        >,
    >,
}

impl<'cn, 'buf> DatapathCnRecvFuture<'cn, 'buf>
where
    'buf: 'cn,
{
    fn new(s: &'cn DatapathCnInner, msgs_buf: &'buf mut [Option<(SocketAddr, Vec<u8>)>]) -> Self {
        Self {
            inner: match s {
                DatapathCnInner::Thread(s) => s.recv(msgs_buf),
                DatapathCnInner::Inline(s) => s.recv(msgs_buf),
            },
        }
    }
}

impl<'cn, 'buf> Future for DatapathCnRecvFuture<'cn, 'buf> {
    type Output = Result<&'buf mut [Option<(SocketAddr, Vec<u8>)>], Report>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner).poll(cx)
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
        self.maybe_swap_datapath();
        unsafe {
            let inner = &*self.inner.get();
            Box::pin(DatapathCnSendFuture::new(inner, burst))
        }
    }

    fn recv<'cn, 'buf>(
        &'cn self,
        msgs_buf: &'buf mut [Option<Self::Data>],
    ) -> Pin<Box<dyn Future<Output = Result<&'buf mut [Option<Self::Data>], Report>> + Send + 'cn>>
    where
        'buf: 'cn,
    {
        self.maybe_swap_datapath();
        unsafe {
            let inner = &*self.inner.get();
            Box::pin(DatapathCnRecvFuture::new(inner, msgs_buf))
        }
    }
}
