//! Chunnel which tags Data to provide at-most-once delivery.

use crate::{reliable::ReliabilityNeg, Chunnel, ChunnelConnection, Negotiate};
use color_eyre::eyre;
use dashmap::DashMap;
use futures_util::future::{ready, Ready};
use std::collections::BinaryHeap;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use tracing::trace;
use tracing_futures::Instrument;

/// `OrderedChunnel` takes in Data segments and tags them for use with `(A, (u32, D))` Chunnels.
///
/// It returns data segments in the order they were sent.
#[derive(Clone, Debug, Default)]
pub struct OrderedChunnel {
    pub hole_thresh: Option<usize>,
}

impl OrderedChunnel {
    pub fn ordering_threshold(&mut self, thresh: usize) -> &mut Self {
        self.hole_thresh = Some(thresh);
        self
    }
}

impl Negotiate for OrderedChunnel {
    type Capability = ReliabilityNeg;

    fn guid() -> u64 {
        0xc93fad8b01f04036
    }

    fn capabilities() -> Vec<Self::Capability> {
        vec![ReliabilityNeg::Ordering]
    }
}

impl<A, D, InC> Chunnel<InC> for OrderedChunnel
where
    InC: ChunnelConnection<Data = (A, (u32, D))> + Send + Sync + 'static,
    A: serde::Serialize
        + serde::de::DeserializeOwned
        + Clone
        + std::fmt::Debug
        + Eq
        + Hash
        + Send
        + Sync
        + 'static,
    D: Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = OrderedCn<A, InC, D>;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        ready(Ok(OrderedCn::new(cn, self.hole_thresh)))
    }
}

#[derive(Debug)]
struct OrderedState<D> {
    snd_nxt: u32,
    expected_recv: u32,
    recvd: BinaryHeap<DataPair<D>>, // list of out-of-order received seqs
}

impl<D> Default for OrderedState<D> {
    fn default() -> Self {
        OrderedState {
            snd_nxt: 0,
            expected_recv: 0,
            recvd: BinaryHeap::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct OrderedCn<A: Eq + Hash, C, D> {
    inner: C,
    hole_thresh: Option<usize>,
    state: Arc<DashMap<A, OrderedState<(A, D)>>>,
}

impl<A: Eq + Hash, C, D> OrderedCn<A, C, D> {
    pub fn new(inner: C, hole_thresh: Option<usize>) -> Self {
        OrderedCn {
            inner,
            hole_thresh,
            state: Default::default(),
        }
    }
}

impl<A, C, D> ChunnelConnection for OrderedCn<A, C, D>
where
    C: ChunnelConnection<Data = (A, (u32, D))> + Send + Sync + 'static,
    D: Send + Sync + 'static,
    A: serde::Serialize
        + serde::de::DeserializeOwned
        + Clone
        + std::fmt::Debug
        + Eq
        + Hash
        + Send
        + Sync
        + 'static,
{
    type Data = (A, D);

    fn send<'cn, B>(
        &'cn self,
        burst: B,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'cn>>
    where
        B: IntoIterator<Item = Self::Data> + Send + 'cn,
        <B as IntoIterator>::IntoIter: Send,
    {
        Box::pin(
            async move {
                trace!("sending");
                self.inner
                    .send(burst.into_iter().map(|(addr, data)| {
                        self.state.entry(addr.clone()).or_default();
                        let seq = {
                            let mut st = self.state.get_mut(&addr).unwrap();
                            let seq = st.snd_nxt;
                            st.snd_nxt += 1;
                            seq
                        };

                        (addr, (seq, data))
                    }))
                    .await?;
                trace!("finished send");
                Ok(())
            }
            .instrument(tracing::trace_span!("ordered_send")),
        )
    }

    fn recv<'cn, 'buf>(
        &'cn self,
        msgs_buf: &'buf mut [Option<Self::Data>],
    ) -> Pin<
        Box<dyn Future<Output = Result<&'buf mut [Option<Self::Data>], eyre::Report>> + Send + 'cn>,
    >
    where
        'buf: 'cn,
    {
        Box::pin(
            async move {
                let mut slot_idx = 0;
                'peel: loop {
                    if slot_idx >= msgs_buf.len() {
                        return Ok(msgs_buf);
                    }

                    for mut e in self.state.iter_mut() {
                        let (a, st) = e.pair_mut();
                        if let Some(DataPair(seq, _)) = st.recvd.peek() {
                            let mut pop = *seq == st.expected_recv;
                            if let Some(thresh) = self.hole_thresh {
                                pop = pop || (st.recvd.len() > thresh);
                            }

                            if pop {
                                trace!(addr = ?a, seq = ?seq, next_expected = ?st.expected_recv, pileup = ?st.recvd.len(), "returning ordered packet");
                                st.expected_recv += 1;
                                msgs_buf[slot_idx] = Some(st.recvd.pop().unwrap().1);
                                slot_idx += 1;
                                continue 'peel;
                            } else {
                                trace!(addr = ?a, head_seq = ?seq, next_expected = ?st.expected_recv, pileup = ?st.recvd.len(), "calling inner recv");
                            }
                        }
                    }

                    // need to fill.
                    let mut slots: Vec<_> = (0..msgs_buf.len() - slot_idx).map(|_| None).collect();
                    let msgs = self.inner.recv(&mut slots).await?;

                    for (from, (seq, data)) in msgs.into_iter().map_while(Option::take) {
                        trace!(seq = ?seq, ?from, "received pkt, locking state");
                        let mut st = self.state.entry(from.clone()).or_default();
                        #[allow(clippy::comparison_chain)]
                        if seq == st.expected_recv {
                            trace!(seq = ?seq, ?from, "received in-order");
                            st.expected_recv += 1;
                            msgs_buf[slot_idx] = Some((from, data));
                            slot_idx += 1;
                        } else if seq > st.expected_recv {
                            trace!(seq = ?seq, ?from, expected = ?st.expected_recv, "received out-of-order");
                            st.recvd.push((seq, (from, data)).into());
                        } else {
                            trace!(seq = ?seq, ?from, expected = ?st.expected_recv, "dropping segment");
                        }
                    }

                    return Ok(msgs_buf);
                }
            }
            .instrument(tracing::trace_span!("ordered_recv")),
        )
    }
}

#[derive(Clone, Copy, Debug)]
struct DataPair<D>(u32, D);

impl<D> From<DataPair<D>> for (u32, D) {
    fn from(dp: DataPair<D>) -> (u32, D) {
        (dp.0, dp.1)
    }
}

impl<D> From<(u32, D)> for DataPair<D> {
    fn from(f: (u32, D)) -> Self {
        DataPair(f.0, f.1)
    }
}

impl<D> PartialEq for DataPair<D> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<D> Eq for DataPair<D> {}

impl<D> PartialOrd for DataPair<D> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // make it a min-heap
        other.0.partial_cmp(&self.0)
    }
}

impl<D> Ord for DataPair<D> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // make it a min-heap
        other.0.cmp(&self.0)
    }
}

#[cfg(test)]
mod test {
    use super::OrderedChunnel;
    use crate::chan_transport::Chan;
    use crate::test::Serve;
    use crate::{Chunnel, ChunnelConnection, ChunnelConnector, ChunnelListener};
    use color_eyre::Report;
    use futures_util::StreamExt;
    use tracing::{debug, info};
    use tracing_error::ErrorLayer;
    use tracing_futures::Instrument;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    async fn do_transmit<C>(snd_ch: C, rcv_ch: C)
    where
        C: ChunnelConnection<Data = ((), Vec<u8>)> + Send + Sync + 'static,
    {
        let msgs: Vec<_> = (0..10).map(|i| ((), vec![i; 10])).collect();

        let ms = msgs.clone();
        tokio::spawn(
            async move {
                snd_ch.send(msgs).await?;
                let _: () = futures_util::future::pending().await;
                Ok::<_, Report>(())
            }
            .instrument(tracing::debug_span!("sender")),
        );

        let msgs = ms;
        info!("starting receiver");
        let mut cnt = 0;
        let mut slots: Vec<_> = (0..8).map(|_| None).collect();
        loop {
            debug!("calling recv");
            let ms = rcv_ch.recv(&mut slots).await.unwrap();
            for m in ms.into_iter().map_while(Option::take) {
                debug!(m = ?m, "rcvd");
                assert_eq!(m, msgs[cnt]);
                cnt += 1;
                if cnt == msgs.len() {
                    info!("done");
                    return;
                }
            }
        }
    }

    #[test]
    fn no_drops() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.try_init().unwrap_or(());
        color_eyre::install().unwrap_or(());

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                let t = Chan::<((), (u32, Vec<u8>)), _>::default();
                let (mut srv, mut cln) = t.split();
                let mut stack = OrderedChunnel::default();

                let rcv_st = srv.listen(()).await?;
                let mut rcv_st = stack.serve(rcv_st).await?;
                let rcv = rcv_st.next().await.unwrap().unwrap();

                let cln = cln.connect(()).await?;
                let snd = stack.connect_wrap(cln).await?;

                do_transmit(snd, rcv).await;

                Ok::<_, Report>(())
            }
            .instrument(tracing::info_span!("no_drops")),
        )
        .unwrap();
    }

    #[test]
    fn reorder() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.try_init().unwrap_or(());
        color_eyre::install().unwrap_or(());

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        use std::ops::DerefMut;
        use std::sync::{Arc, Mutex};

        rt.block_on(
            async move {
                let mut t = Chan::<((), (u32, Vec<u8>)), _>::default();
                let staged: Arc<Mutex<(usize, Vec<_>)>> = Default::default();
                t.link_conditions(move |x| {
                    let mut s = staged.lock().unwrap();
                    let (c, st) = &mut s.deref_mut();
                    if let Some(x) = x {
                        *c += 1;
                        if *c == 3 || *c == 5 {
                            debug!(cnt = ?c, msg = ?x, deferred = st.len(), "delaying packet");
                            st.push(x);
                            None
                        } else {
                            debug!(cnt = ?c, msg=?x, deferred = st.len(), "sending packet");
                            Some(x)
                        }
                    } else if *c >= 8 && !st.is_empty() {
                        debug!(cnt = ?c, msg = ?&st.last(), deferred = st.len(), "sending deferred packet");
                        st.pop()
                    } else {
                        None
                    }
                });

                let (mut srv, mut cln) = t.split();

                let mut stack = OrderedChunnel::default();

                let rcv_st = srv.listen(()).await?;
                let mut rcv_st = stack.serve(rcv_st).await?;
                let rcv = rcv_st.next().await.unwrap().unwrap();

                let cln = cln.connect(()).await?;
                let snd = stack.connect_wrap(cln).await?;

                do_transmit(snd, rcv).await;
                Ok::<_, Report>(())
            }
            .instrument(tracing::info_span!("reorder")),
        )
        .unwrap();
    }

    #[test]
    fn delivery() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.try_init().unwrap_or(());
        color_eyre::install().unwrap_or(());

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        use std::sync::{Arc, Mutex};

        rt.block_on(
            async move {
                let mut t = Chan::<((), (u32, Vec<u8>)), _>::default();
                // duplicate each packet once
                let saved: Arc<Mutex<Option<_>>> = Arc::new(Mutex::new(None));
                t.link_conditions(move |x: Option<_>| match x {
                    Some(p) => {
                        *saved.lock().unwrap() = Some(p.clone());
                        Some(p)
                    }
                    None => saved.lock().unwrap().take(),
                });

                let (mut srv, mut cln) = t.split();

                let mut stack = OrderedChunnel::default();

                let rcv_st = srv.listen(()).await?;
                let mut rcv_st = stack.serve(rcv_st).await?;
                let rcv = rcv_st.next().await.unwrap().unwrap();

                let cln = cln.connect(()).await?;
                let snd = stack.connect_wrap(cln).await?;

                do_transmit(snd, rcv).await;
                Ok::<_, Report>(())
            }
            .instrument(tracing::info_span!("delivery")),
        )
        .unwrap();
    }
}
