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
    task::{Context, Poll},
};
use tracing::debug;

pub struct UpgradeStream {
    pub(crate) port: u16,
    pub(crate) inner: ReqDatapathStream,
    pub(crate) updates: Receiver<DatapathInner>,
    pub(crate) conns: Arc<Mutex<HashMap<ActiveConnection, Sender<DatapathCnInner>>>>,
    pub(crate) wait_for_datapath_swap_now: Arc<AtomicBool>,
    pub(crate) swap_barrier: Arc<RwLock<Barrier>>,
}

impl UpgradeStream {
    fn maybe_swap_datapath(&mut self) -> Result<(), Report> {
        let swap_now = self.wait_for_datapath_swap_now.load(Ordering::SeqCst);
        let new_dp = if !swap_now {
            match self.updates.try_recv() {
                Err(_) => return Ok(()), // common case - no work to do.
                Ok(new_dp) => new_dp,
            }
        } else {
            debug!("waiting on datapath swap barrier");
            self.swap_barrier.read().unwrap().wait();

            self.updates
                .recv()
                .wrap_err(eyre!("stream update wait on port {}", self.port))
                .expect("datapath update sender disappeared")
        };

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

        let st = DpdkReqDatapath::adapt_inner_stream(
            inner,
            self.conns.clone(),
            self.wait_for_datapath_swap_now.clone(),
            self.swap_barrier.clone(),
        );

        debug!("performing datapath swap");
        self.inner = st;
        Ok(())
    }
}

impl Stream for UpgradeStream {
    type Item = <ReqDatapathStream as Stream>::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Err(err) = self.maybe_swap_datapath() {
            return Poll::Ready(Some(Err(err)));
        }

        Pin::new(&mut self.inner).poll_next(cx)
    }
}
