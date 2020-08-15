//! Channel-based Chunnel which acts as a transport.

use crate::{
    ChunnelConnection, ChunnelConnector, ChunnelListener, ConnectAddress, Endedness, ListenAddress,
    Once, Scope,
};
use futures_util::stream::{Stream, StreamExt};
use std::collections::HashMap;
use std::future::Future;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use tokio::sync::{mpsc, Mutex};
use tracing::debug;

pub struct Srv;
pub struct Cln;

/// Channel connector where server registers on listen(), and client grabs client on connect().
pub struct RendezvousChannel<Addr, Data, Side> {
    channel_size: usize,
    map: Arc<Mutex<HashMap<Addr, ChanChunnel<Data>>>>,
    side: std::marker::PhantomData<Side>,
}

impl<A, D> RendezvousChannel<A, D, ()> {
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

/// Type implementing the `Address` trait for `RendezvousChannel`.
#[derive(Clone)]
pub struct RendezvousChannelAddr<A, D>(A, RendezvousChannel<A, D, Cln>);

impl<A, D> From<(A, RendezvousChannel<A, D, Cln>)> for RendezvousChannelAddr<A, D> {
    fn from(from: (A, RendezvousChannel<A, D, Cln>)) -> Self {
        Self(from.0, from.1)
    }
}

impl<A, D> ConnectAddress for RendezvousChannelAddr<A, D>
where
    A: Clone + Eq + std::hash::Hash + std::fmt::Debug + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Connector = RendezvousChannel<A, D, Cln>;
    fn connector(&self) -> Self::Connector {
        self.1.clone()
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
    type Connection = Once<ChanChunnel<D>, D>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = eyre::Report;

    fn listen(&mut self, a: Self::Addr) -> Self::Future {
        let channel_size = self.channel_size;
        let (s, r) = mpsc::channel(channel_size);
        let (s1, r1) = mpsc::channel(channel_size);
        let m = Arc::clone(&self.map);
        Box::pin(async move {
            debug!(addr = ?&a, "RendezvousChannel listening");
            m.lock()
                .await
                .insert(a, ChanChunnel::new(s, r1, Arc::new(|x| x)));
            // `r.map` maps over the messages in the receive channel stream.
            Ok(Box::pin(r.map(move |d| {
                // `Once` will not call recv() on the inner C, so pass a dummy receiver.
                let (_, r2) = mpsc::channel(1);
                let resp_chunnel = ChanChunnel::new(s1.clone(), r2, Arc::new(|x| x));
                Ok(Once::new(Arc::new(resp_chunnel), d))
            })) as _)
        })
    }
}

impl<A, D> ChunnelConnector for RendezvousChannel<A, D, Cln>
where
    A: Clone + Eq + std::hash::Hash + std::fmt::Debug + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Addr = RendezvousChannelAddr<A, D>;
    type Connection = ChanChunnel<D>;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = eyre::Report;

    fn connect(&mut self, a: Self::Addr) -> Self::Future {
        let addr = a.0.clone();
        let m = Arc::clone(&self.map);
        Box::pin(async move {
            let s = m.lock().await.remove(&addr);
            if let Some(s) = s {
                Ok(s)
            } else {
                let mp = m.lock().await;
                let keys: Vec<_> = mp.keys().collect();
                Err(eyre::eyre!(
                    "Address not found: {:?}: not in {:?}",
                    addr,
                    keys
                ))
            }
        })
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

impl Default for Chan<Vec<u8>, ()> {
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

enum ChanAddrInner<D> {
    Both(Chan<D, ()>),
    SrvTaken(Chan<D, Cln>),
    ClnTaken(Chan<D, Srv>),
    Done,
}

#[derive(Clone)]
pub struct ChanAddr<D>(Arc<StdMutex<ChanAddrInner<D>>>);

impl<D> From<Chan<D, ()>> for ChanAddr<D> {
    fn from(f: Chan<D, ()>) -> Self {
        ChanAddr(Arc::new(StdMutex::new(ChanAddrInner::Both(f))))
    }
}

impl<D> ListenAddress for ChanAddr<D>
where
    D: Send + Sync + 'static,
{
    type Listener = Chan<D, Srv>;
    fn listener(&self) -> Self::Listener {
        let mut this = self.0.lock().unwrap();
        let c = std::mem::replace(this.deref_mut(), ChanAddrInner::Done);
        let (new, ret) = match c {
            ChanAddrInner::Both(c) => {
                let (srv, cln) = c.split();
                (srv, ChanAddrInner::SrvTaken(cln))
            }
            ChanAddrInner::ClnTaken(c) => (c, ChanAddrInner::Done),
            _ => unreachable!(),
        };

        let _ = std::mem::replace(this.deref_mut(), ret);
        new
    }
}

impl<D> ConnectAddress for ChanAddr<D>
where
    D: Send + Sync + 'static,
{
    type Connector = Chan<D, Cln>;
    fn connector(&self) -> Self::Connector {
        let mut this = self.0.lock().unwrap();
        let c = std::mem::replace(this.deref_mut(), ChanAddrInner::Done);
        let (new, ret) = match c {
            ChanAddrInner::Both(c) => {
                let (srv, cln) = c.split();
                (cln, ChanAddrInner::ClnTaken(srv))
            }
            ChanAddrInner::SrvTaken(c) => (c, ChanAddrInner::Done),
            _ => unreachable!(),
        };

        let _ = std::mem::replace(this.deref_mut(), ret);
        new
    }
}

impl<D> ChunnelListener for Chan<D, Srv>
where
    D: Send + Sync + 'static,
{
    type Addr = ChanAddr<D>;
    type Connection = ChanChunnel<D>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = eyre::Report;

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
    type Addr = ChanAddr<D>;
    type Connection = ChanChunnel<D>;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = eyre::Report;

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
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>> {
        let s = Arc::clone(&self.snd);
        let link = Arc::clone(&self.link);

        Box::pin(async move {
            if let Some(data) = link(Some(data)) {
                s.lock().await.send(data).await.map_err(|_| ()).unwrap();
            } else {
                debug!("dropping send");
            }

            while let Some(d) = link(None) {
                debug!("sending deferred segment");
                s.lock().await.send(d).await.map_err(|_| ()).unwrap();
            }

            Ok(())
        })
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>> {
        let r = Arc::clone(&self.rcv);
        Box::pin(async move { Ok(r.lock().await.recv().await.unwrap()) })
    }
}

#[cfg(test)]
mod test {
    use super::{Chan, ChanAddr};
    use crate::{
        ChunnelConnection, ChunnelConnector, ChunnelListener, ConnectAddress, ListenAddress,
    };
    use futures_util::stream::StreamExt;
    use tracing_futures::Instrument;

    #[test]
    fn chan() {
        let _guard = tracing_subscriber::fmt::try_init();

        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_time()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                let a: ChanAddr<Vec<u8>> = Chan::default().into();

                let (s, r) = tokio::sync::oneshot::channel();

                let mut srv = a.listener();
                let addr = a.clone();
                tokio::spawn(async move {
                    let mut st = srv.listen(addr).await.unwrap();
                    s.send(()).unwrap();
                    let cn = st.next().await.unwrap().unwrap();
                    let d = cn.recv().await.unwrap();
                    cn.send(d).await.unwrap();
                });

                r.await?;
                let mut cln = a.connector();
                let cn = cln.connect(a).await?;

                cn.send(vec![1u8; 8]).await?;
                let d = cn.recv().await?;

                assert_eq!(d, vec![1u8; 8]);
                Ok::<_, eyre::Report>(())
            }
            .instrument(tracing::info_span!("chan_test")),
        )
        .unwrap();
    }
}
