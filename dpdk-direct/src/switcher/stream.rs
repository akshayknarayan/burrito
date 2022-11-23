use super::{DatapathInner, ReqDatapathStream};
use crate::{
    ActiveConnection, DatapathCnInner, DpdkInlineReqChunnel, DpdkReqDatapath, DpdkUdpReqChunnel,
};
use ahash::HashMap;
use bertha::{ChunnelListener, Either};
use color_eyre::{
    eyre::{eyre, WrapErr},
    Report,
};
use flume::{Receiver, Sender};
use futures_util::stream::Stream;
use futures_util::TryStreamExt;
use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    sync::{Arc, Barrier, Mutex, RwLock},
    task::{Context, Poll, Waker},
};
use tracing::{debug, warn};

pub struct UpgradeStream {
    port: u16,
    inner: ReqDatapathStream,
    updates: Receiver<DatapathInner>,
    conns: Arc<Mutex<HashMap<ActiveConnection, Sender<DatapathCnInner>>>>,
    wait_for_datapath_swap_now: Arc<AtomicBool>,
    swap_barrier: Arc<RwLock<Barrier>>,
    thread_wakers: Arc<Mutex<Vec<Option<Waker>>>>,
    poisoned: bool,
}

impl UpgradeStream {
    pub(crate) fn new(
        port: u16,
        inner: ReqDatapathStream,
        updates: Receiver<DatapathInner>,
        conns: Arc<Mutex<HashMap<ActiveConnection, Sender<DatapathCnInner>>>>,
        wait_for_datapath_swap_now: Arc<AtomicBool>,
        swap_barrier: Arc<RwLock<Barrier>>,
        thread_wakers: Arc<Mutex<Vec<Option<Waker>>>>,
    ) -> Self {
        Self {
            port,
            inner,
            updates,
            conns,
            wait_for_datapath_swap_now,
            swap_barrier,
            thread_wakers,
            poisoned: false,
        }
    }

    fn maybe_swap_datapath(&mut self) -> Result<(), Report> {
        let swap_now = self.wait_for_datapath_swap_now.load(Ordering::SeqCst);
        let new_dp = if !swap_now {
            match self.updates.try_recv() {
                Err(flume::TryRecvError::Empty) => return Ok(()), // common case - no work to do.
                Err(flume::TryRecvError::Disconnected) => {
                    panic!("datapath update sender disappeared")
                }
                Ok(new_dp) => new_dp,
            }
        } else {
            debug!(loc = "UpgradeStream", "waiting on datapath swap barrier");
            self.swap_barrier.read().unwrap().wait();
            self.updates
                .recv()
                .wrap_err(eyre!("stream update wait on port {}", self.port))
                .expect("datapath update sender disappeared")
        };

        debug!(?new_dp, "performing datapath stream swap");
        // now we construct a new stream.
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, self.port));
        let inner: Pin<
            Box<dyn Stream<Item = Result<DatapathCnInner, Report>> + Send + Sync + 'static>,
        > = match new_dp {
            DatapathInner::Thread(s) => Box::pin(
                DpdkUdpReqChunnel(s)
                    .listen(addr)
                    .into_inner()?
                    .map_ok(Either::Right)
                    .map_ok(DatapathCnInner::Thread),
            ) as _,
            DatapathInner::Inline(s) => Box::pin(
                DpdkInlineReqChunnel::from(s)
                    .listen(addr)
                    .into_inner()?
                    .map_ok(DatapathCnInner::Inline),
            ) as _,
        };

        debug!("datapath stream swap adapt_inner");
        let st = DpdkReqDatapath::adapt_inner_stream(
            inner,
            self.conns.clone(),
            self.wait_for_datapath_swap_now.clone(),
            self.swap_barrier.clone(),
            self.thread_wakers.clone(),
        );

        self.inner = st;
        debug!("datapath stream swap done");
        Ok(())
    }
}

impl Stream for UpgradeStream {
    type Item = <ReqDatapathStream as Stream>::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Err(err) = self.maybe_swap_datapath() {
            warn!(?err, "maybe_swap_datapath errored");
            self.poisoned = true;
            return Poll::Ready(Some(Err(err)));
        } else if self.poisoned {
            return Poll::Ready(None);
        }

        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(r) => Poll::Ready(r),
            Poll::Pending => {
                super::store_thread_waker(&self.thread_wakers, cx.waker().clone());
                Poll::Pending
            }
        }
    }
}
