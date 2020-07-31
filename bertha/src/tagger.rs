//! Chunnel which tags Data to provide at-most-once delivery.

use crate::{ChunnelConnection, Context, InheritChunnel};
use std::collections::BinaryHeap;
use std::convert::TryInto;
use std::future::Future;
use std::pin::Pin;
use std::sync::{atomic::AtomicUsize, Arc};
use tokio::sync::RwLock;
use tracing::{debug, trace};
use tracing_futures::Instrument;

#[derive(Debug)]
pub struct TaggerChunnel<C> {
    inner: Arc<C>,
}

impl<Cx> From<Cx> for TaggerChunnel<Cx> {
    fn from(cx: Cx) -> TaggerChunnel<Cx> {
        TaggerChunnel {
            inner: Arc::new(cx),
        }
    }
}

impl<C> Clone for TaggerChunnel<C>
where
    C: Clone,
{
    fn clone(&self) -> Self {
        let inner: C = self.inner.as_ref().clone();
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl<C> Context for TaggerChunnel<C> {
    type ChunnelType = C;

    fn context(&self) -> &Self::ChunnelType {
        &self.inner
    }

    fn context_mut(&mut self) -> &mut Self::ChunnelType {
        Arc::get_mut(&mut self.inner).unwrap()
    }
}

impl<B, C> InheritChunnel<C> for TaggerChunnel<B>
where
    C: ChunnelConnection<Data = (u32, Vec<u8>)> + Send + Sync + 'static,
{
    type Connection = Tagger<C>;
    type Config = ();

    fn get_config(&mut self) -> Self::Config {}

    fn make_connection(cx: C, _cfg: Self::Config) -> Self::Connection {
        Tagger::from(cx)
    }
}

/// Assigns an sequential tag to data segments and ignores the tag otherwise.
#[derive(Default, Debug)]
pub struct Tagger<C> {
    inner: Arc<C>,
    snd_nxt: Arc<AtomicUsize>,
}

impl<Cx> From<Cx> for Tagger<Cx> {
    fn from(cx: Cx) -> Tagger<Cx> {
        Tagger {
            inner: Arc::new(cx),
            snd_nxt: Default::default(),
        }
    }
}

impl<C> Clone for Tagger<C>
where
    C: Clone,
{
    fn clone(&self) -> Self {
        let inner: C = self.inner.as_ref().clone();
        Self {
            inner: Arc::new(inner),
            snd_nxt: self.snd_nxt.clone(),
        }
    }
}

impl<C> ChunnelConnection for Tagger<C>
where
    C: ChunnelConnection<Data = (u32, Vec<u8>)> + Send + Sync + 'static,
{
    type Data = Vec<u8>;

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>> {
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
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>> {
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
}

#[derive(Debug)]
pub struct OrderedChunnel<C> {
    inner: Arc<C>,
    hole_thresh: usize,
}

impl<Cx> OrderedChunnel<Cx> {
    pub fn ordering_threshold(&mut self, thresh: usize) -> &mut Self {
        self.hole_thresh = thresh;
        self
    }
}

impl<C> Clone for OrderedChunnel<C>
where
    C: Clone,
{
    fn clone(&self) -> Self {
        let inner: C = self.inner.as_ref().clone();
        Self {
            inner: Arc::new(inner),
            hole_thresh: self.hole_thresh,
        }
    }
}

impl<C: Default> Default for OrderedChunnel<C> {
    fn default() -> Self {
        Self {
            hole_thresh: 5,
            inner: Default::default(),
        }
    }
}

impl<Cx> From<Cx> for OrderedChunnel<Cx> {
    fn from(cx: Cx) -> OrderedChunnel<Cx> {
        OrderedChunnel {
            inner: Arc::new(cx),
            hole_thresh: 5,
        }
    }
}

impl<C> Context for OrderedChunnel<C> {
    type ChunnelType = C;

    fn context(&self) -> &Self::ChunnelType {
        &self.inner
    }

    fn context_mut(&mut self) -> &mut Self::ChunnelType {
        Arc::get_mut(&mut self.inner).unwrap()
    }
}

impl<B, C> InheritChunnel<C> for OrderedChunnel<B>
where
    C: ChunnelConnection<Data = (u32, Vec<u8>)> + Send + Sync + 'static,
{
    type Connection = Ordered<C>;
    type Config = usize;

    fn get_config(&mut self) -> Self::Config {
        self.hole_thresh
    }

    fn make_connection(cx: C, cfg: Self::Config) -> Self::Connection {
        Ordered {
            inner: Arc::new(cx),
            hole_thresh: cfg,
            state: Default::default(),
        }
    }
}

#[derive(Debug, Default)]
struct OrderedState {
    snd_nxt: u32,
    expected_recv: u32,
    recvd: BinaryHeap<(u32, Vec<u8>)>, // list of out-of-order received seqs
}

/// `Ordered` takes in `Vec<u8>` Data segments and tags them for use with `(u32, Vec<u8>)` Chunnels.
///
/// It returns data segments in the order they were sent.
#[derive(Clone, Debug)]
pub struct Ordered<C> {
    inner: Arc<C>,
    hole_thresh: usize,
    state: Arc<RwLock<OrderedState>>,
}

impl<C> ChunnelConnection for Ordered<C>
where
    C: ChunnelConnection<Data = (u32, Vec<u8>)> + Send + Sync + 'static,
{
    type Data = Vec<u8>;

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>> {
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
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>> {
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
}

#[derive(Debug)]
pub struct SeqUnreliableChunnel<C> {
    inner: Arc<C>,
}

impl<Cx> From<Cx> for SeqUnreliableChunnel<Cx> {
    fn from(cx: Cx) -> SeqUnreliableChunnel<Cx> {
        SeqUnreliableChunnel {
            inner: Arc::new(cx),
        }
    }
}

impl<C> Clone for SeqUnreliableChunnel<C>
where
    C: Clone,
{
    fn clone(&self) -> Self {
        let inner: C = self.inner.as_ref().clone();
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl<C> Context for SeqUnreliableChunnel<C> {
    type ChunnelType = C;

    fn context(&self) -> &Self::ChunnelType {
        &self.inner
    }

    fn context_mut(&mut self) -> &mut Self::ChunnelType {
        Arc::get_mut(&mut self.inner).unwrap()
    }
}

impl<B, C> InheritChunnel<C> for SeqUnreliableChunnel<B>
where
    C: ChunnelConnection<Data = Vec<u8>> + Send + Sync + 'static,
{
    type Connection = SeqUnreliable<C>;
    type Config = ();

    fn get_config(&mut self) -> Self::Config {}

    fn make_connection(cx: C, _cfg: Self::Config) -> Self::Connection {
        SeqUnreliable::from(cx)
    }
}

/// `SeqUnreliable` accepts (u32, Vec<u8>) pairs as Data for transmission.
#[derive(Default, Debug, Clone)]
pub struct SeqUnreliable<C> {
    inner: Arc<C>,
}

impl<Cx> From<Cx> for SeqUnreliable<Cx> {
    fn from(cx: Cx) -> SeqUnreliable<Cx> {
        SeqUnreliable {
            inner: Arc::new(cx),
        }
    }
}

impl<C> Context for SeqUnreliable<C> {
    type ChunnelType = C;

    fn context(&self) -> &Self::ChunnelType {
        &self.inner
    }

    fn context_mut(&mut self) -> &mut Self::ChunnelType {
        Arc::get_mut(&mut self.inner).unwrap()
    }
}

impl<C> ChunnelConnection for SeqUnreliable<C>
where
    C: ChunnelConnection<Data = Vec<u8>> + Send + Sync + 'static,
{
    type Data = (u32, Vec<u8>);

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>> {
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
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>> {
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
}

#[cfg(test)]
mod test {
    use super::{OrderedChunnel, SeqUnreliableChunnel, TaggerChunnel};
    use crate::chan_transport::Chan;
    use crate::{ChunnelConnection, ChunnelConnector, ChunnelListener};
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
                let (srv, cln) = Chan::default().split();

                let mut rcv = TaggerChunnel::from(SeqUnreliableChunnel::from(srv));
                let mut rcv = rcv.listen(()).await.unwrap();
                let rcv = rcv.next().await.unwrap().unwrap();

                let mut snd = TaggerChunnel::from(SeqUnreliableChunnel::from(cln));
                let snd = snd.connect(()).await.unwrap();

                do_transmit(snd, rcv).await;
            }
            .instrument(tracing::info_span!("tag_only")),
        );
    }

    async fn do_transmit<C>(snd_ch: C, rcv_ch: C)
    where
        C: ChunnelConnection<Data = Vec<u8>> + Send + Sync + 'static,
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
                let (srv, cln) = Chan::default().split();

                let mut rcv = OrderedChunnel::from(SeqUnreliableChunnel::from(srv));
                let mut rcv = rcv.listen(()).await.unwrap();
                let rcv = rcv.next().await.unwrap().unwrap();

                let mut snd = OrderedChunnel::from(SeqUnreliableChunnel::from(cln));
                let snd = snd.connect(()).await.unwrap();

                do_transmit(snd, rcv).await;
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

                let (srv, cln) = t.split();
                let mut rcv = OrderedChunnel::from(SeqUnreliableChunnel::from(srv));
                let mut rcv = rcv.listen(()).await.unwrap();
                let rcv = rcv.next().await.unwrap().unwrap();

                let mut snd = OrderedChunnel::from(SeqUnreliableChunnel::from(cln));
                let snd = snd.connect(()).await.unwrap();

                do_transmit(snd, rcv).await;
            }
            .instrument(tracing::info_span!("reorder")),
        );
    }
}
