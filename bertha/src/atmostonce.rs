use crate::{Chunnel, ChunnelConnection};
use color_eyre::eyre::Report;
use dashmap::DashMap;
use futures_util::future::{ready, Ready};
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use tracing::{debug, trace, trace_span};
use tracing_futures::Instrument;

/// At Most Once Delivery semantics with no other assumptions.
///
/// [`crate::tagger::OrderedChunnelProj`] will already provide at-most-once semantics, but this
/// implementation relaxes the ordering requirement.
#[derive(Debug, Clone)]
pub struct AtMostOnceChunnel {
    /// Time horizon to remember old messages for
    pub sunset: std::time::Duration,
}

impl Default for AtMostOnceChunnel {
    fn default() -> Self {
        Self {
            sunset: std::time::Duration::from_secs(600),
        }
    }
}

impl<InC, A, D> Chunnel<InC> for AtMostOnceChunnel
where
    InC: ChunnelConnection<Data = (A, (u32, D))> + Send + Sync + 'static,
    A: serde::Serialize
        + serde::de::DeserializeOwned
        + Clone
        + Eq
        + Hash
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
    D: Send,
{
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = AtMostOnceCn<InC, A>;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        ready(Ok(AtMostOnceCn::new(cn, self.sunset)))
    }
}

pub struct AtMostOnceCn<C, A> {
    inner: C,
    sunset: std::time::Duration,
    delivered_msgs: DashMap<A, RecvState>,
    next_seq: DashMap<A, u32>,
}

impl<C, A> AtMostOnceCn<C, A>
where
    A: Eq + Hash + Send + Sync + 'static,
{
    pub fn new(inner: C, sunset: std::time::Duration) -> Self {
        Self {
            inner,
            delivered_msgs: Default::default(),
            next_seq: Default::default(),
            sunset,
        }
    }
}

impl<C, A, D> ChunnelConnection for AtMostOnceCn<C, A>
where
    A: serde::Serialize
        + serde::de::DeserializeOwned
        + Clone
        + Eq
        + Hash
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
    C: ChunnelConnection<Data = (A, (u32, D))> + Send + Sync + 'static,
    D: Send,
{
    type Data = (A, D);

    fn send<'cn, B>(
        &'cn self,
        burst: B,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'cn>>
    where
        B: IntoIterator<Item = Self::Data> + Send + 'cn,
        <B as IntoIterator>::IntoIter: Send,
    {
        Box::pin(async move {
            self.inner
                .send(burst.into_iter().map(|(addr, data)| {
                    let mut next_seq = self.next_seq.entry(addr.clone()).or_insert(0);
                    *next_seq += 1;
                    trace!(?addr, seq=?*next_seq, "sending");
                    (addr, (*next_seq, data))
                }))
                .await
        })
    }

    fn recv<'cn, 'buf>(
        &'cn self,
        msgs_buf: &'buf mut [Option<Self::Data>],
    ) -> Pin<Box<dyn Future<Output = Result<&'buf mut [Option<Self::Data>], Report>> + Send + 'cn>>
    where
        'buf: 'cn,
    {
        Box::pin(
            async move {
                let mut slot_idx = 0;
                let mut slots: Vec<_> = (0..msgs_buf.len()).map(|_| None).collect();
                loop {
                    let msgs = self.inner.recv(&mut slots).await?;
                    for (addr, (seq, data)) in msgs.into_iter().map_while(Option::take) {
                        let mut ent = self.delivered_msgs.entry(addr.clone()).or_default();
                        if ent.is_new(seq, self.sunset) {
                            msgs_buf[slot_idx] = Some((addr, data));
                            slot_idx += 1;

                            if slot_idx >= msgs_buf.len() {
                                return Ok(msgs_buf);
                            }
                        }
                    }

                    if slot_idx > 0 {
                        return Ok(&mut msgs_buf[..slot_idx]);
                    }
                }
            }
            .instrument(trace_span!("at-most-once:recv")),
        )
    }
}

#[derive(Debug, Default, Clone)]
struct RecvState {
    last_update: Option<std::time::Instant>,
    /// Times that we got the packets, so we can get all those past the `sunset`.
    deliver_times: BTreeMap<std::time::Instant, u32>,
    /// Corresponds to the values of `deliver_times`, but we want to look them up.
    delivered: HashSet<u32>,
}

impl RecvState {
    fn maybe_update(&mut self, sunset: std::time::Duration) {
        match self.last_update {
            None => {
                self.last_update = Some(std::time::Instant::now());
            }
            Some(ref mut t) if t.elapsed() > sunset => {
                // do the update.
                let cutoff = std::time::Instant::now() - sunset;
                let new_deliver_times = self.deliver_times.split_off(&cutoff);
                debug!(pruned_msgs = ?&self.deliver_times.len(), "forget old msgs");
                self.delivered = new_deliver_times.values().copied().collect();
                self.deliver_times = new_deliver_times;
                self.last_update = Some(std::time::Instant::now());
            }
            _ => (),
        }
    }

    fn is_new(&mut self, seq: u32, sunset: std::time::Duration) -> bool {
        let r = if !self.delivered.contains(&seq) {
            self.deliver_times.insert(std::time::Instant::now(), seq);
            self.delivered.insert(seq);
            true
        } else {
            trace!(?seq, "discarding repeat msg");
            false
        };

        self.maybe_update(sunset);
        r
    }
}

#[cfg(test)]
mod test {
    use super::AtMostOnceChunnel;
    use crate::test::Serve;
    use crate::test::COLOR_EYRE;
    use crate::{
        chan_transport::Chan, Chunnel, ChunnelConnection, ChunnelConnector, ChunnelListener,
    };
    use color_eyre::Report;
    use futures_util::StreamExt;
    use std::sync::Arc;
    use std::sync::Mutex;
    use tracing::{debug, info, info_span, instrument};
    use tracing_error::ErrorLayer;
    use tracing_futures::Instrument;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    #[test]
    fn at_most_once() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        COLOR_EYRE.call_once(|| color_eyre::install().unwrap_or(()));

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        let msgs = vec![(0, vec![0u8; 10]), (1, vec![1u8; 10]), (2, vec![2u8; 10])];
        rt.block_on(
            async move {
                let mut t = Chan::default();
                // duplicate each packet once
                let saved: Arc<Mutex<Option<((), (u32, (u32, Vec<u8>)))>>> =
                    Arc::new(Mutex::new(None));
                t.link_conditions(move |x: Option<((), (u32, (u32, Vec<u8>)))>| match x {
                    Some(p) => {
                        *saved.lock().unwrap() = Some(p.clone());
                        Some(p)
                    }
                    None => saved.lock().unwrap().take(),
                });

                let (mut srv, mut cln) = t.split();
                let mut l = AtMostOnceChunnel::default();

                let rcv_st = srv.listen(()).await.unwrap();
                let mut rcv_st = l.serve(rcv_st).await.unwrap();
                let rcv = rcv_st.next().await.unwrap().unwrap();

                let cln = cln.connect(()).await.unwrap();
                let snd = l.connect_wrap(cln).await.unwrap();

                do_transmit(snd, rcv, msgs).await;
                Ok::<_, Report>(())
            }
            .instrument(info_span!("at_most_once")),
        )
        .unwrap();
    }

    #[instrument(skip(snd_ch, rcv_ch, msgs))]
    async fn do_transmit(
        snd_ch: impl ChunnelConnection<Data = ((), (u32, Vec<u8>))> + Send + Sync + 'static,
        rcv_ch: impl ChunnelConnection<Data = ((), (u32, Vec<u8>))> + Send + Sync + 'static,
        msgs: Vec<(u32, Vec<u8>)>,
    ) {
        let num_msgs = msgs.len();
        tokio::spawn(
            async move { snd_ch.send(msgs.into_iter().map(|x| ((), x))).await }
                .instrument(info_span!("sender")),
        );

        info!("starting receiver");
        let mut slots = [None, None];
        let mut cnt = 0;
        loop {
            let ms = rcv_ch.recv(&mut slots).await.unwrap();
            for m in ms.into_iter().map_while(Option::take) {
                debug!(m = ?m, "rcvd");
                cnt += 1;
            }

            if cnt == num_msgs {
                break;
            }
        }

        // check for no duplicates
        match tokio::time::timeout(
            std::time::Duration::from_millis(100),
            rcv_ch.recv(&mut slots),
        )
        .await
        {
            Ok(Err(_)) | Err(_) => (),
            _ => panic!("unexpected recv"),
        }

        info!("done");
    }
}
