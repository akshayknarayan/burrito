//! Chunnel which tags Data to provide at-most-once delivery.

use crate::{Chunnel, Endedness, Scope};
use std::collections::BinaryHeap;
use std::convert::TryInto;
use std::future::Future;
use std::pin::Pin;
use std::sync::{atomic::AtomicUsize, Arc};
use tokio::sync::RwLock;
use tracing::{debug, trace};
use tracing_futures::Instrument;

/// Assigns an sequential tag to data segments and ignores the tag otherwise.
#[derive(Default)]
pub struct Tagger<C> {
    snd_nxt: Arc<AtomicUsize>,
    inner: Arc<C>,
}

impl<C> Chunnel for Tagger<C>
where
    C: Chunnel<Data = (u32, Vec<u8>)> + Send + Sync + 'static,
{
    type Data = Vec<u8>;

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + Sync>> {
        let inner = Arc::clone(&self.inner);
        let snd_nxt = Arc::clone(&self.snd_nxt);
        Box::pin(
            async move {
                let seq = snd_nxt.fetch_add(1, std::sync::atomic::Ordering::SeqCst) as u32;
                trace!(seq = ?seq, "sending");
                inner.send((seq, data)).await?;
                trace!(seq = ?seq, "sent");
                Ok(())
            }
            .instrument(tracing::trace_span!("tagger_send")),
        )
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + Sync>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(
            async move {
                let (seq, d) = inner.recv().await?;
                trace!(seq = ?seq, "received");
                return Ok(d);
            }
            .instrument(tracing::trace_span!("tagger_recv")),
        )
    }

    fn scope(&self) -> Scope {
        Scope::Application
    }

    fn endedness(&self) -> Endedness {
        Endedness::Either
    }

    fn implementation_priority(&self) -> usize {
        1
    }
}

impl<Cx> Tagger<Cx> {
    pub fn with_context<C>(self, cx: C) -> Tagger<C> {
        Tagger {
            snd_nxt: Default::default(),
            inner: Arc::new(cx),
        }
    }
}

#[derive(Default)]
struct OrderedState {
    snd_nxt: u32,
    expected_recv: u32,
    recvd: BinaryHeap<(u32, Vec<u8>)>, // list of out-of-order received seqs
}

/// `Ordered` takes in `Vec<u8>` Data segments and tags them for use with `(u32, Vec<u8>)` Chunnels.
///
/// It returns data segments in the order they were sent.
pub struct Ordered<C> {
    hole_thresh: usize,
    inner: Arc<C>,
    state: Arc<RwLock<OrderedState>>,
}

impl<Cx> Ordered<Cx> {
    pub fn with_context<C>(self, cx: C) -> Ordered<C> {
        Ordered {
            hole_thresh: self.hole_thresh,
            inner: Arc::new(cx),
            state: self.state,
        }
    }

    pub fn ordering_threshold(&mut self, thresh: usize) -> &mut Self {
        self.hole_thresh = thresh;
        self
    }
}

impl<C: Default> Default for Ordered<C> {
    fn default() -> Self {
        Self {
            hole_thresh: 5,
            state: Default::default(),
            inner: Default::default(),
        }
    }
}

impl<C> Chunnel for Ordered<C>
where
    C: Chunnel<Data = (u32, Vec<u8>)> + Send + Sync + 'static,
{
    type Data = Vec<u8>;

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + Sync>> {
        let state = Arc::clone(&self.state);
        let inner = Arc::clone(&self.inner);
        Box::pin(
            async move {
                let seq = {
                    let mut st = state.write().await;
                    let seq = st.snd_nxt;
                    st.snd_nxt += 1;
                    seq
                };

                trace!(seq = ?seq, "sending");
                inner.send((seq, data)).await?;
                trace!(seq = ?seq, "sent");
                Ok(())
            }
            .instrument(tracing::trace_span!("tagger_send")),
        )
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + Sync>> {
        let state = Arc::clone(&self.state);
        let inner = Arc::clone(&self.inner);
        let hole_thresh = self.hole_thresh;
        Box::pin(
            async move {
                let expected = { state.read().await.expected_recv };
                loop {
                    {
                        let mut st = state.write().await;
                        let mut pop = false;
                        if let Some((seq, _)) = st.recvd.peek() {
                            pop = *seq == expected || st.recvd.len() > hole_thresh;
                            if pop {
                                trace!(seq = ?seq, next_expected = ?expected, pileup = ?st.recvd.len(), "returning ordered packet");
                            }
                        }

                        if pop {
                            st.expected_recv += 1;
                            return Ok(st.recvd.pop().unwrap().1);
                        }
                    }

                    let (seq, d) = inner.recv().await?;
                    trace!(seq = ?seq, "received");
                    if seq == expected {
                        state.write().await.expected_recv += 1;
                        return Ok(d);
                    } else if seq > expected {
                        let mut st = state.write().await;
                        st.recvd.push((seq, d));
                    } else {
                        debug!(seq = ?seq, "dropping segment");
                    }
                }
            }
            .instrument(tracing::trace_span!("tagger_recv")),
        )
    }

    fn scope(&self) -> Scope {
        Scope::Application
    }

    fn endedness(&self) -> Endedness {
        Endedness::Both // we add a header
    }

    fn implementation_priority(&self) -> usize {
        1
    }
}

/// `SeqUnreliable` accepts (u32, Vec<u8>) pairs as Data for transmission.
#[derive(Default)]
pub struct SeqUnreliable<C> {
    inner: Arc<C>,
}

impl<Cx> SeqUnreliable<Cx> {
    pub fn with_context<C>(self, cx: C) -> SeqUnreliable<C> {
        SeqUnreliable {
            inner: Arc::new(cx),
        }
    }
}

impl<C> Chunnel for SeqUnreliable<C>
where
    C: Chunnel<Data = Vec<u8>> + Send + Sync + 'static,
{
    type Data = (u32, Vec<u8>);

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + Sync>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(
            async move {
                let (seq, data) = data;
                let mut buf = seq.to_be_bytes().to_vec();
                buf.extend(&data);
                inner.send(buf).await?;
                Ok(())
            }
            .instrument(tracing::trace_span!("sequnreliable_send")),
        )
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + Sync>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(
            async move {
                let mut seq = inner.recv().await?;
                // pop off the seqno
                let data = seq.split_off(4);
                let seq = u32::from_be_bytes(seq[0..4].try_into().unwrap());
                Ok((seq, data))
            }
            .instrument(tracing::trace_span!("sequnreliable_recv")),
        )
    }

    fn scope(&self) -> Scope {
        Scope::Application
    }

    fn endedness(&self) -> Endedness {
        Endedness::Both // we add a header
    }

    fn implementation_priority(&self) -> usize {
        1
    }
}

#[cfg(test)]
mod test {
    use super::{Ordered, SeqUnreliable, Tagger};
    use crate::chan_transport::Chan;
    use crate::{Chunnel, Connector};
    use futures_util::StreamExt;
    use tracing::{debug, info, trace};
    use tracing_futures::Instrument;

    #[test]
    fn tag_only() {
        let _guard = tracing_subscriber::fmt::try_init();

        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_time()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                let mut t = Chan::default();
                let mut rcv = t.listen(()).await;
                let rcv_cn = rcv.next().await.unwrap();
                let rcv_ch = Tagger::<()>::default()
                    .with_context(SeqUnreliable::<()>::default().with_context(rcv_cn));

                let snd = t.connect(()).await;
                let snd_ch = Tagger::<()>::default()
                    .with_context(SeqUnreliable::<()>::default().with_context(snd));

                do_transmit(snd_ch, rcv_ch).await;
            }
            .instrument(tracing::info_span!("no_drops")),
        );
    }

    async fn do_transmit<C>(snd_ch: C, rcv_ch: C)
    where
        C: Chunnel<Data = Vec<u8>> + Send + Sync + 'static,
    {
        let msgs = vec![vec![0u8; 10], vec![1u8; 10], vec![2u8; 10], vec![3u8; 10]];
        let (s, r) = tokio::sync::oneshot::channel::<()>();

        // recv side
        let ms = msgs.clone();
        tokio::spawn(
            async move {
                let msgs = ms;
                info!("starting receiver");
                let mut cnt = 0;
                loop {
                    let m = rcv_ch.recv().await.unwrap();
                    trace!(m = ?m, "rcvd");
                    assert_eq!(m, msgs[cnt]);
                    cnt += 1;
                    if cnt == msgs.len() {
                        break;
                    }
                }

                s.send(()).unwrap();
            }
            .instrument(tracing::debug_span!("receiver")),
        );

        let futs: futures_util::stream::FuturesOrdered<_> = (msgs.into_iter().map(|m| {
            debug!(m = ?m, "sending");
            snd_ch.send(m)
        }))
        .collect();
        futs.collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<(), _>>()
            .unwrap();

        r.await.unwrap();
        info!("done");
    }

    #[test]
    fn no_drops() {
        let _guard = tracing_subscriber::fmt::try_init();

        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_time()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                let mut t = Chan::default();
                let mut rcv = t.listen(()).await;
                let rcv_cn = rcv.next().await.unwrap();
                let rcv_ch = Ordered::<()>::default()
                    .with_context(SeqUnreliable::<()>::default().with_context(rcv_cn));

                let snd = t.connect(()).await;
                let snd_ch = Ordered::<()>::default()
                    .with_context(SeqUnreliable::<()>::default().with_context(snd));

                do_transmit(snd_ch, rcv_ch).await;
            }
            .instrument(tracing::info_span!("no_drops")),
        );
    }

    #[test]
    fn reorder() {
        let _guard = tracing_subscriber::fmt::try_init();

        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_time()
            .build()
            .unwrap();

        use std::sync::{atomic::AtomicUsize, Arc, Mutex};

        rt.block_on(
            async move {
                let mut t = Chan::default();
                let ctr: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
                let staged: Arc<Mutex<Vec<Vec<u8>>>> = Default::default();
                t.link_conditions(move |x| {
                    let c = ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let mut s = staged.lock().unwrap();
                    if let Some(x) = x {
                        if c == 2 {
                            s.push(x);
                            debug!(cnt = ?c, "delaying packet");
                            None
                        } else {
                            Some(x)
                        }
                    } else {
                        if c > 2 {
                            s.pop()
                        } else {
                            None
                        }
                    }
                });
                let mut rcv = t.listen(()).await;
                let rcv_cn = rcv.next().await.unwrap();
                let rcv_ch = Ordered::<()>::default()
                    .with_context(SeqUnreliable::<()>::default().with_context(rcv_cn));

                let snd = t.connect(()).await;
                let snd_ch = Ordered::<()>::default()
                    .with_context(SeqUnreliable::<()>::default().with_context(snd));

                do_transmit(snd_ch, rcv_ch).await;
            }
            .instrument(tracing::info_span!("reorder")),
        );
    }
}
