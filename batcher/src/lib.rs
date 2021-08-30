use bertha::ChunnelConnection;
use color_eyre::eyre::Report;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::{future::Future, pin::Pin};
use tokio::sync::mpsc;
use tracing::trace;

pub struct Batcher<C, D> {
    inner: Arc<C>,

    // send side
    bus_left_notify_arrival: Arc<Mutex<Option<mpsc::Sender<()>>>>,
    bus_boarding_join: Arc<Mutex<Option<mpsc::Sender<()>>>>,
    curr_pending: Arc<Mutex<(PendingSend<D>, hdrhistogram::Histogram<u64>)>>,
    max_batch_size: usize,

    // recv side
    pending_recvd: Arc<Mutex<VecDeque<D>>>,
    recv_batch_size: usize,
}

impl<C, D> Batcher<C, D> {
    pub fn new(inner: C) -> Self {
        Self {
            inner: Arc::new(inner),
            bus_left_notify_arrival: Default::default(),
            bus_boarding_join: Default::default(),
            curr_pending: Arc::new(Mutex::new((
                Default::default(),
                hdrhistogram::Histogram::new_with_max(512, 2).unwrap(),
            ))),
            max_batch_size: 16,
            pending_recvd: Default::default(),
            recv_batch_size: 1,
        }
    }

    pub fn set_max_batch_size(&mut self, max_batch_size: usize) -> &mut Self {
        self.max_batch_size = max_batch_size;
        self
    }

    pub fn set_recv_batch_size(&mut self, size: usize) -> &mut Self {
        self.recv_batch_size = size;
        self
    }
}

impl<C, D> Drop for Batcher<C, D> {
    fn drop(&mut self) {
        let (_, ref h) = *self.curr_pending.lock().unwrap();
        tracing::info!(
            p5 = h.value_at_quantile(0.05),
            p25 = h.value_at_quantile(0.25),
            p50 = h.value_at_quantile(0.5),
            p75 = h.value_at_quantile(0.75),
            p95 = h.value_at_quantile(0.95),
            cnt = h.len(),
            "send_batch_size",
        );
    }
}

impl<C, D> ChunnelConnection for Batcher<C, D>
where
    C: ChunnelConnection<Data = D> + Send + Sync + 'static,
    D: Send + 'static,
{
    type Data = C::Data;

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        let inner = Arc::clone(&self.inner);
        let pending = Arc::clone(&self.curr_pending);
        let max_batch_size = self.max_batch_size;

        // there is an ongoing send. this will fire when it finishes.
        // only one waiter should wait on this: whoever takes it first and starts a new batch.
        // the batch starter should make bus_boarding_join available, and clear it (bus_boarding_join) to None (locking the
        // batch membership to whoever was already waiting) once it starts.
        let bus_left_notify_arrival = Arc::clone(&self.bus_left_notify_arrival);

        // there is a send waiting. this will fire when it finishes.
        // many sends could clone and wait on this.
        let bus_boarding_join = Arc::clone(&self.bus_boarding_join);

        Box::pin(async move {
            // 1. did a bus just leave?
            let enroute_bus = bus_left_notify_arrival.lock().unwrap().take();
            // 2. a. if we're starting a new bus (Some case), fill in bus_boarding_join
            //    b. if we're not starting a new bus (None case), then join the existing bus.
            //    c. but, if there's no existing bus, then we are starting a new bus after all.
            //    d. if the existing bus has reached capacity, send it and replace board_bus
            let bus_boarding = bus_boarding_join.lock().unwrap().clone();
            match (enroute_bus, bus_boarding) {
                (None, Some(board_bus)) => {
                    // b. and d. cases
                    let batch_closed = {
                        let (ref mut h, ref mut record) = *pending.lock().unwrap();
                        h.push(data);
                        if h.size() >= max_batch_size {
                            record.saturating_record(h.size() as _);
                            Some(h.take())
                        } else {
                            None
                        }
                    };

                    if let Some(send_now) = batch_closed {
                        let (board_bus, mut this_bus_arrival) = mpsc::channel(1);
                        // overwrite the existing board_bus waiter.
                        {
                            let mut arrival = bus_left_notify_arrival.lock().unwrap();
                            *arrival = Some(board_bus);
                        }

                        trace!(?send_now, "sending");
                        match send_now {
                            PendingSend::Nil => unreachable!(),
                            PendingSend::Single(d) => {
                                inner.send(d).await?;
                            }
                            PendingSend::Batch(d) => {
                                inner.send_batch(d).await?;
                            }
                        }

                        trace!("sent");
                        // signal completion.
                        this_bus_arrival.close();
                    } else {
                        trace!("wait for batch completion");
                        board_bus.closed().await;
                    }
                    Ok(())
                }
                (enroute, _) => {
                    // a. and c. cases
                    // enroute being Some or None determines if we're passing a starting waiter to do_send
                    // set_this will be None.

                    let (board_bus, mut this_bus_arrival) = mpsc::channel(1);
                    {
                        let mut set_this = bus_boarding_join.lock().unwrap();
                        *set_this = Some(board_bus);
                    }

                    trace!("start new batch");

                    // we'll be signalled when the previous send is done.
                    if let Some(prev) = enroute {
                        trace!("wait for previous batch");
                        prev.closed().await;
                    }

                    trace!("prepare to send");
                    // the batch is now closed.
                    let send_now = {
                        let mut arrival = bus_left_notify_arrival.lock().unwrap();
                        let mut bus = bus_boarding_join.lock().unwrap();
                        let (ref mut s, ref mut record) = *pending.lock().unwrap();
                        *arrival = bus.take();
                        s.push(data);
                        let b = s.take();
                        record.saturating_record(b.size() as _);
                        b
                    };

                    trace!(?send_now, "sending");

                    // take whatever's ready to be sent now and send it.
                    match send_now {
                        PendingSend::Nil => unreachable!(),
                        PendingSend::Single(d) => {
                            inner.send(d).await?;
                        }
                        PendingSend::Batch(d) => {
                            inner.send_batch(d).await?;
                        }
                    }

                    trace!("sent");

                    // signal completion.
                    this_bus_arrival.close();
                    Ok(())
                }
            }
        })
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        let inner = Arc::clone(&self.inner);
        let pending = Arc::clone(&self.pending_recvd);
        let recv_batch_size = self.recv_batch_size;
        Box::pin(async move {
            {
                let mut pg = pending.lock().unwrap();
                if let Some(d) = pg.pop_front() {
                    return Ok(d);
                }
            }

            let data = inner.recv_batch(recv_batch_size).await?;

            let mut pg = pending.lock().unwrap();
            pg.extend(data);
            Ok(pg.pop_front().unwrap())
        })
    }

    fn send_batch<'cn, I>(
        &'cn self,
        data: I,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'cn>>
    where
        I: IntoIterator<Item = Self::Data> + Send + 'cn,
        <I as IntoIterator>::IntoIter: Send,
        Self::Data: Send,
        Self: Sync,
    {
        self.inner.send_batch(data)
    }

    fn recv_batch<'cn>(
        &'cn self,
        batch_size: usize,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Self::Data>, Report>> + Send + 'cn>>
    where
        Self::Data: Send,
        Self: Sync,
    {
        self.inner.recv_batch(batch_size)
    }
}

enum PendingSend<D> {
    Nil,
    Single(D),
    Batch(Vec<D>),
}

impl<D> Default for PendingSend<D> {
    fn default() -> Self {
        PendingSend::Nil
    }
}

impl<D> std::fmt::Debug for PendingSend<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            PendingSend::Nil => std::write!(f, "PendingSend::Nil")?,
            PendingSend::Single(_) => std::write!(f, "PendingSend::Single")?,
            PendingSend::Batch(b) => std::write!(f, "PendingSend::Batch({:?})", b.len())?,
        }
        Ok(())
    }
}

impl<D> PendingSend<D> {
    fn take(&mut self) -> Self {
        let s = std::mem::take(self);
        s
    }

    fn push(&mut self, data: D) -> bool {
        match self {
            PendingSend::Nil => {
                *self = PendingSend::Single(data);
                return false;
            }
            PendingSend::Single(_) => {
                let s = std::mem::replace(self, PendingSend::Nil);
                let d = match s {
                    PendingSend::Single(d) => d,
                    _ => unreachable!(),
                };
                *self = PendingSend::Batch(vec![d, data]);
                return true;
            }
            PendingSend::Batch(ref mut b) => {
                b.push(data);
                return true;
            }
        }
    }

    fn size(&self) -> usize {
        match self {
            PendingSend::Nil => 0,
            PendingSend::Single(_) => 1,
            PendingSend::Batch(ref b) => b.len(),
        }
    }
}

#[cfg(test)]
mod t {
    use super::Batcher;
    use super::Report;
    use bertha::{chan_transport::Chan, ChunnelConnection, ChunnelConnector, ChunnelListener};
    use futures_util::stream::StreamExt;
    use rand::Rng;
    use std::{future::Future, pin::Pin};
    use tracing::{debug_span, info, info_span, trace};
    use tracing_error::ErrorLayer;
    use tracing_futures::Instrument;
    use tracing_subscriber::{layer::SubscriberExt, prelude::*};

    /// Because the send stack has basically no processing, there's no opportunity to batch.
    /// Batching will only happen at the receiver.
    #[test]
    fn recv_batch() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        color_eyre::install().unwrap_or(());

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                let (mut srv, mut cln) = Chan::default().split();

                let (s, r) = tokio::sync::oneshot::channel();

                tokio::spawn(
                    async move {
                        let mut st = srv.listen(()).await.unwrap();
                        s.send(()).unwrap();
                        let cn = st.next().await.unwrap().unwrap();

                        info!("listening");

                        loop {
                            let d = cn.recv().await.unwrap();
                            info!("got msg");
                            cn.send(d).await.unwrap();
                        }
                    }
                    .instrument(info_span!("receiver")),
                );

                r.await?;

                info!("starting");

                let cn = cln.connect(()).await?;
                let cn = Batcher::new(cn);

                let sends: Result<_, _> = futures_util::future::join_all((0..20).map(|i| {
                    cn.send(((), vec![1u8; 8]))
                        .instrument(debug_span!("send", ?i))
                }))
                .await
                .into_iter()
                .collect();
                sends?;

                info!("sent all");

                for i in 0..20 {
                    let (_, d) = cn.recv().await?;
                    assert_eq!(d, vec![1u8; 8]);
                    info!(?i, "recvd");
                }

                info!("done");
                Ok::<_, Report>(())
            }
            .instrument(info_span!("batch_test")),
        )
        .unwrap();
    }

    struct RandomWaitSender<C>(C);

    impl<C> ChunnelConnection for RandomWaitSender<C>
    where
        C: ChunnelConnection,
    {
        type Data = C::Data;

        fn send(
            &self,
            data: Self::Data,
        ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
            let mut rng = rand::thread_rng();
            let r: f32 = rng.gen();
            let delay = std::time::Duration::from_micros((r * 100.) as u64);
            let f = self.0.send(data);
            Box::pin(async move {
                trace!(?delay, "injecting send delay");
                tokio::time::sleep(delay).await;
                f.await
            })
        }

        fn recv(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
            self.0.recv()
        }
    }

    #[test]
    fn send_batch() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        color_eyre::install().unwrap_or(());

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                let (mut srv, mut cln) = Chan::default().split();

                let (s, r) = tokio::sync::oneshot::channel();

                tokio::spawn(
                    async move {
                        let mut st = srv.listen(()).await.unwrap();
                        s.send(()).unwrap();
                        let cn = st.next().await.unwrap().unwrap();

                        info!("listening");

                        loop {
                            let d = cn.recv().await.unwrap();
                            info!("got msg");
                            cn.send(d).await.unwrap();
                        }
                    }
                    .instrument(info_span!("receiver")),
                );

                r.await?;

                info!("starting");

                let cn = cln.connect(()).await?;
                let cn = Batcher::new(RandomWaitSender(cn));

                use futures_util::stream::{FuturesUnordered, TryStreamExt};
                let jhs = FuturesUnordered::new();
                let mut rng = rand::thread_rng();
                for i in 0..20 {
                    let f = cn.send(((), vec![1u8; 8]));
                    let r: f32 = rng.gen();
                    let delay = std::time::Duration::from_micros((r * 30.) as u64);
                    tokio::time::sleep(delay).await;
                    let jh = tokio::spawn(f.instrument(debug_span!("send", ?i)));
                    jhs.push(jh);
                }

                let r: Result<Vec<_>, _> = jhs.try_collect().await;
                r?;

                info!("sent all");

                for i in 0..20 {
                    let (_, d) = cn.recv().await?;
                    assert_eq!(d, vec![1u8; 8]);
                    info!(?i, "recvd");
                }

                info!("done");
                Ok::<_, Report>(())
            }
            .instrument(info_span!("batch_test")),
        )
        .unwrap();
    }
}
