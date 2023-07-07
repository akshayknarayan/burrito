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
    sync::{Arc, Barrier, Mutex, RwLock},
    task::{Context, Poll, Waker},
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
    pub(crate) thread_wakers: Arc<Mutex<Vec<Option<Waker>>>>,
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
    fn maybe_swap_datapath(&self) -> bool {
        let swap_now = self.wait_for_datapath_swap_now.load(Ordering::SeqCst);
        let new_dp = if !swap_now {
            match self.new_datapath.try_recv() {
                Err(flume::TryRecvError::Empty) => return false, // common case - no work to do.
                Err(flume::TryRecvError::Disconnected) => {
                    panic!("datapath update sender disappeared");
                    // TODO need to distribute among threads?
                }
                Ok(new_dp) => new_dp,
            }
        } else {
            // it is important to wait on the barrier here because we need to make sure that our
            // various threads are using the same datapath!
            // we take a read lock because we are using the barrier now.
            // write locks should be taken when updating the number of active threads.
            debug!(loc = "DatapathCn", "waiting on datapath swap barrier");
            self.swap_barrier.read().unwrap().wait();

            self.new_datapath
                .recv()
                .expect("new datapath sender disappeared")
        };

        let port = self.local_port();
        let remote_addr = self.remote_addr();
        debug!(?port, ?remote_addr, "performing datapath swap");
        unsafe { self.do_swap(new_dp) };
        true
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

/// Recv future implementation that can change inner implementation if the datapath was changed.
///
/// This must check for a new datapath on every poll because the datapath could have changed while
/// the internal future had been active but had yielded. So, the next time this future is polled,
/// if we go back to the same inner future, we would end up using the now-defunct datapath.
///
/// This does not apply to `DatapathCnSendFuture` because `send()` futures complete quickly, so
/// they won't be concurrent with a datapath swap.
///
/// ###SAFETY
/// This future implementation stores a `*mut Option<(SocketAddr, Vec<u8>)>` which is a pointer to
/// the receive message buffer. We need to keep this around because if the datapath changes, we
/// need to create a new `recv()` future and the old one will have claimed our receive message
/// buffer. Keeping it around is safe because we only re-materialize the buffer when we need to
/// make a new `recv()` inner future, and we only do that if we are about to drop the old future
/// which was using the slice.
struct DatapathCnRecvFuture<'cn, 'buf> {
    inner: Pin<
        Box<
            dyn Future<Output = Result<&'buf mut [Option<(SocketAddr, Vec<u8>)>], Report>>
                + Send
                + 'cn,
        >,
    >,
    msgs_buf_ptr: *mut Option<(SocketAddr, Vec<u8>)>,
    msgs_buf_len: usize,
    cn: &'cn DatapathCn,
}

unsafe impl<'cn, 'buf> Send for DatapathCnRecvFuture<'cn, 'buf> {}

impl<'cn, 'buf> DatapathCnRecvFuture<'cn, 'buf>
where
    'buf: 'cn,
{
    fn new(s: &'cn DatapathCn, msgs_buf: &'buf mut [Option<(SocketAddr, Vec<u8>)>]) -> Self {
        let inner = unsafe { &*s.inner.get() };
        let msgs_buf_ptr = msgs_buf.as_mut_ptr();
        let msgs_buf_len = msgs_buf.len();
        Self {
            inner: match inner {
                DatapathCnInner::Thread(i) => i.recv(msgs_buf),
                DatapathCnInner::Inline(i) => i.recv(msgs_buf),
            },
            msgs_buf_ptr,
            msgs_buf_len,
            cn: s,
        }
    }
}

impl<'cn, 'buf> Future for DatapathCnRecvFuture<'cn, 'buf>
where
    'buf: 'cn,
{
    type Output = Result<&'buf mut [Option<(SocketAddr, Vec<u8>)>], Report>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let swapped = self.cn.maybe_swap_datapath();
        if swapped {
            let cn: &'cn _ = unsafe { std::mem::transmute(&*self.cn.inner.get()) };
            let new_msgs_buf: &'buf mut [_] =
                unsafe { std::slice::from_raw_parts_mut(self.msgs_buf_ptr, self.msgs_buf_len) };
            self.inner = match cn {
                DatapathCnInner::Thread(i) => i.recv(new_msgs_buf),
                DatapathCnInner::Inline(i) => i.recv(new_msgs_buf),
            };
        }

        match Pin::new(&mut self.inner).poll(cx) {
            Poll::Ready(r) => Poll::Ready(r),
            Poll::Pending => {
                super::store_thread_waker(&self.cn.thread_wakers, cx.waker().clone());
                Poll::Pending
            }
        }
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
        Box::pin(DatapathCnRecvFuture::new(self, msgs_buf))
    }
}
