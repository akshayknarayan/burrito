use super::{DatapathInner, ReqDatapathStream};
use crate::{
    ActiveConnection, DatapathCnInner, DpdkInlineReqChunnel, DpdkReqDatapath, DpdkUdpReqChunnel,
};
use ahash::HashMap;
use bertha::{ChunnelListener, Either};
use color_eyre::Report;
use flume::{Receiver, Sender};
use futures_util::stream::Stream;
use futures_util::TryStreamExt;
use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    pin::Pin,
    sync::{Arc, Mutex as StdMutex},
};
use tracing::debug;

pub struct UpgradeStream {
    port: u16,
    inner: ReqDatapathStream,
    updates: Receiver<DatapathInner>,
    conns: Arc<StdMutex<HashMap<ActiveConnection, Sender<DatapathCnInner>>>>,
}

impl UpgradeStream {
    pub(crate) fn new(
        port: u16,
        inner: ReqDatapathStream,
        updates: Receiver<DatapathInner>,
        conns: Arc<StdMutex<HashMap<ActiveConnection, Sender<DatapathCnInner>>>>,
    ) -> Self {
        Self {
            port,
            inner,
            updates,
            conns,
        }
    }

    pub async fn next(mut self) -> Result<Option<(super::DatapathCn, Self)>, Report> {
        let cn = loop {
            match futures_util::future::select(self.updates.recv_async(), self.inner.try_next())
                .await
            {
                futures_util::future::Either::Left((new_dp, _)) => {
                    let new_dp = new_dp.expect("datapath transition sender disappeared");
                    debug!(?new_dp, "performing datapath stream swap");
                    // now we construct a new stream.
                    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, self.port));
                    let inner: Pin<
                        Box<
                            dyn Stream<Item = Result<DatapathCnInner, Report>>
                                + Send
                                + Sync
                                + 'static,
                        >,
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

                    let st = DpdkReqDatapath::adapt_inner_stream(inner, self.conns.clone());
                    self.inner = st;
                    debug!("datapath stream swap done");
                }
                futures_util::future::Either::Right((cn, _)) => {
                    break cn?;
                }
            }
        };

        return match cn {
            Some(c) => Ok::<_, Report>(Some((c, self))),
            None => Ok(None),
        };
    }
}
