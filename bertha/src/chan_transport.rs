//! Channel-based Chunnel which acts as a transport.

use crate::{ChunnelConnection, ChunnelConnector, ChunnelListener};
use color_eyre::eyre::{eyre, Report};
use dashmap::DashMap;
use futures_util::stream::{Stream, StreamExt};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tracing::debug;

#[derive(Clone, Debug, Copy, Default)]
pub struct Srv;
#[derive(Clone, Debug, Copy, Default)]
pub struct Cln;

/// Channel connector where server registers on listen(), and client grabs client on connect().
pub struct RendezvousChannel<Addr, Data, Side> {
    channel_size: usize,
    map: Arc<DashMap<Addr, mpsc::Sender<ChanChunnel<(Addr, Data)>>>>,
    side: std::marker::PhantomData<Side>,
}

impl<A, D> RendezvousChannel<A, D, ()>
where
    A: Clone + Eq + std::hash::Hash + std::fmt::Debug + Send + Sync + 'static,
{
    pub fn new(channel_size: usize) -> Self {
        Self {
            channel_size,
            map: Default::default(),
            side: Default::default(),
        }
    }

    pub fn split(self) -> (RendezvousChannel<A, D, Srv>, RendezvousChannel<A, D, Cln>) {
        (
            RendezvousChannel {
                channel_size: self.channel_size,
                map: Arc::clone(&self.map),
                side: Default::default(),
            },
            RendezvousChannel {
                channel_size: self.channel_size,
                map: Arc::clone(&self.map),
                side: Default::default(),
            },
        )
    }
}

impl<A, D, S> Clone for RendezvousChannel<A, D, S> {
    fn clone(&self) -> Self {
        Self {
            channel_size: self.channel_size,
            map: Arc::clone(&self.map),
            side: Default::default(),
        }
    }
}

impl<A, D> ChunnelListener for RendezvousChannel<A, D, Srv>
where
    A: Clone + Eq + std::hash::Hash + std::fmt::Debug + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Addr = A;
    type Connection = ChanChunnel<(A, D)>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Report;

    fn listen(&mut self, a: Self::Addr) -> Self::Future {
        let m = Arc::clone(&self.map);
        Box::pin(async move {
            let (s, r) = mpsc::channel(1);
            m.insert(a.clone(), s);
            debug!(addr = ?&a, "RendezvousChannel listening");
            Ok(Box::pin(r.map(move |x| {
                debug!(addr = ?a, "RendezvousChannel returning new connection");
                Ok(x)
            })) as _)
        })
    }
}

impl<A, D> ChunnelConnector for RendezvousChannel<A, D, Cln>
where
    A: Clone + Eq + std::hash::Hash + std::fmt::Debug + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Addr = A;
    type Connection = ChanChunnel<(A, D)>;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Report;

    fn connect(&mut self, a: Self::Addr) -> Self::Future {
        if let Some(notify_listener) = self.map.get(&a) {
            let notify_listener = notify_listener.clone();
            let channel_size = self.channel_size;
            Box::pin(async move {
                let (s1, r1) = mpsc::channel(channel_size);
                let (s2, r2) = mpsc::channel(channel_size);
                let connect_ch = ChanChunnel::new(s1, r2, Arc::new(|x| x));
                let listen_ch = ChanChunnel::new(s2, r1, Arc::new(|x| x));
                notify_listener
                    .send(listen_ch)
                    .await
                    .map_err(|e| eyre!("Could not send connection to listener: {}", e))?;
                Ok(connect_ch)
            })
        } else {
            let keys: Vec<_> = self.map.iter().map(|x| x.key().clone()).collect();
            Box::pin(async move { Err(eyre!("Address not found: {:?}: not in {:?}", a, keys)) })
        }
    }
}

pub struct Chan<Data, Side> {
    snd1: Option<mpsc::Sender<Data>>,
    rcv1: Option<mpsc::Receiver<Data>>,
    snd2: Option<mpsc::Sender<Data>>,
    rcv2: Option<mpsc::Receiver<Data>>,
    link: Arc<dyn Fn(Option<Data>) -> Option<Data> + Send + Sync + 'static>,
    _side: std::marker::PhantomData<Side>,
}

impl<T> Default for Chan<T, ()> {
    fn default() -> Self {
        let (s1, r1) = mpsc::channel(100);
        let (s2, r2) = mpsc::channel(100);
        Self {
            snd1: Some(s1),
            rcv1: Some(r1),
            snd2: Some(s2),
            rcv2: Some(r2),
            link: Arc::new(|x| x),
            _side: Default::default(),
        }
    }
}

impl<T, U> Chan<T, U> {
    /// For testing, provide a function that `Chan` will use to drop or reorder packets.
    ///
    /// For each segment `d` the `ChanChunnel` gets, it will call `link` with `Some(d)`. Then, it
    /// will repeatedly call `link` with `None`, transmitting all returned segments until `link`
    /// returns `None`.
    pub fn link_conditions(
        &mut self,
        link: impl Fn(Option<T>) -> Option<T> + Send + Sync + 'static,
    ) -> &mut Self {
        self.link = Arc::new(link);
        self
    }

    /// Split into a (server, client) pair.
    pub fn split(self) -> (Chan<T, Srv>, Chan<T, Cln>) {
        (
            Chan {
                snd1: self.snd1,
                rcv2: self.rcv2,
                rcv1: None,
                snd2: None,
                link: self.link.clone(),
                _side: Default::default(),
            },
            Chan {
                snd2: self.snd2,
                rcv1: self.rcv1,
                rcv2: None,
                snd1: None,
                link: self.link.clone(),
                _side: Default::default(),
            },
        )
    }
}

impl<D> ChunnelListener for Chan<D, Srv>
where
    D: Send + Sync + 'static,
{
    type Addr = ();
    type Connection = ChanChunnel<D>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Report;

    fn listen(&mut self, _a: Self::Addr) -> Self::Future {
        let r = ChanChunnel::new(
            self.snd1.take().unwrap(),
            self.rcv2.take().unwrap(),
            Arc::clone(&self.link),
        );
        Box::pin(async move { Ok(Box::pin(futures_util::stream::once(async { Ok(r) })) as _) })
    }
}

impl<D> ChunnelConnector for Chan<D, Cln>
where
    D: Send + Sync + 'static,
{
    type Addr = ();
    type Connection = ChanChunnel<D>;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Report;

    fn connect(&mut self, _a: Self::Addr) -> Self::Future {
        let r = ChanChunnel::new(
            self.snd2.take().unwrap(),
            self.rcv1.take().unwrap(),
            Arc::clone(&self.link),
        );
        Box::pin(async { Ok(r) })
    }
}

pub struct ChanChunnel<Data> {
    snd: Arc<Mutex<mpsc::Sender<Data>>>,
    rcv: Arc<Mutex<mpsc::Receiver<Data>>>,
    link: Arc<dyn Fn(Option<Data>) -> Option<Data> + Send + Sync + 'static>,
}

impl<D> ChanChunnel<D> {
    fn new(
        s: mpsc::Sender<D>,
        r: mpsc::Receiver<D>,
        link: Arc<dyn Fn(Option<D>) -> Option<D> + Send + Sync + 'static>,
    ) -> Self {
        Self {
            snd: Arc::new(Mutex::new(s)),
            rcv: Arc::new(Mutex::new(r)),
            link,
        }
    }
}

impl<D> ChunnelConnection for ChanChunnel<D>
where
    D: Send + Sync + 'static,
{
    type Data = D;

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        let s = Arc::clone(&self.snd);
        let link = Arc::clone(&self.link);

        Box::pin(async move {
            if let Some(data) = link(Some(data)) {
                s.lock()
                    .await
                    .send(data)
                    .await
                    .map_err(|_| eyre!("receiver channel dropped"))?;
            } else {
                debug!("dropping send");
            }

            while let Some(d) = link(None) {
                debug!("sending deferred segment");
                s.lock()
                    .await
                    .send(d)
                    .await
                    .map_err(|_| eyre!("receiver channel dropped"))?;
            }

            Ok(())
        })
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        let r = Arc::clone(&self.rcv);
        Box::pin(async move {
            let mut l = r.lock().await;
            Ok(l.recv().await.ok_or_else(|| eyre!("All senders dropped"))?)
        })
    }
}

#[cfg(test)]
mod test {
    use super::{Chan, RendezvousChannel};
    use crate::{ChunnelConnection, ChunnelConnector, ChunnelListener};
    use color_eyre::eyre::{Report, WrapErr};
    use futures_util::stream::{StreamExt, TryStreamExt};
    use tracing::{debug, info, trace};
    use tracing_error::ErrorLayer;
    use tracing_futures::Instrument;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    #[test]
    fn chan() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        color_eyre::install().unwrap_or_else(|_| ());

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                let (mut srv, mut cln) = Chan::default().split();

                let (s, r) = tokio::sync::oneshot::channel();

                tokio::spawn(async move {
                    let mut st = srv.listen(()).await.unwrap();
                    s.send(()).unwrap();
                    let cn = st.next().await.unwrap().unwrap();
                    let d = cn.recv().await.unwrap();
                    cn.send(d).await.unwrap();
                });

                r.await?;
                let cn = cln.connect(()).await?;
                cn.send(((), vec![1u8; 8])).await?;
                let (_, d) = cn.recv().await?;

                assert_eq!(d, vec![1u8; 8]);
                Ok::<_, Report>(())
            }
            .instrument(tracing::info_span!("chan_test")),
        )
        .unwrap();
    }

    #[test]
    fn rendezvous() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        color_eyre::install().unwrap_or_else(|_| ());

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                let (mut srv, cln) = RendezvousChannel::new(100).split();
                let (s, r) = tokio::sync::oneshot::channel();
                let address = String::from("P Sherman 42 Wallaby Way Sydney");

                let laddr = address.clone();
                tokio::spawn(async move {
                    let st = srv.listen(laddr).await.expect("listen");
                    s.send(()).unwrap();
                    let mut i = 0;
                    st.try_for_each_concurrent(None, |cn| {
                        let idx = i;
                        i += 1;
                        async move {
                            debug!("new connection");
                            loop {
                                match cn.recv().await {
                                    Ok(d) => {
                                        cn.send(d).await.expect("server send");
                                        debug!("echoed");
                                    }
                                    Err(e) => {
                                        trace!(err = ?e, "error in recv");
                                        break;
                                    }
                                }
                            }

                            Ok(())
                        }
                        .instrument(tracing::info_span!("conn", idx))
                    })
                    .instrument(tracing::info_span!("server"))
                    .await
                    .unwrap();
                });

                info!("client 1");

                r.await?;
                let mut cln1 = cln.clone();
                let caddr1 = address.clone();
                async move {
                    let cn = cln1.connect(caddr1.clone()).await?;

                    cn.send((caddr1.clone(), vec![1u8; 8])).await?;
                    let (_, d) = cn.recv().await?;
                    assert_eq!(d, vec![1u8; 8]);
                    Ok::<_, Report>(())
                }
                .instrument(tracing::info_span!("client 1"))
                .await?;

                info!("client 2");

                let mut cln2 = cln.clone();
                let caddr2 = address.clone();
                async move {
                    let cn = cln2.connect(caddr2.clone()).await?;
                    info!("connected");

                    cn.send((caddr2.clone(), vec![2u8; 8]))
                        .await
                        .wrap_err("client send")?;
                    let (_, d) = cn.recv().await.wrap_err("client recv")?;
                    assert_eq!(d, vec![2u8; 8]);
                    Ok::<_, Report>(())
                }
                .instrument(tracing::info_span!("client 2"))
                .await?;
                Ok::<_, Report>(())
            }
            .instrument(tracing::info_span!("chan::rendezvous")),
        )
        .unwrap();
    }
}
