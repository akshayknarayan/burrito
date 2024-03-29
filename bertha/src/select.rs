//! A `ChunnelListener` which selects across two other `ChunnelListener`s

use crate::{either::Either, ChunnelConnection, ChunnelListener};
use color_eyre::eyre::Error;
use futures_util::stream::{Stream, StreamExt};
use std::future::Future;
use std::pin::Pin;
use tracing::{debug, debug_span};
use tracing_futures::Instrument;

pub struct SameAddr;
pub struct DiffAddrs;

/// Select across two listeners.
///
/// Listens on an external chunnel, for direct connections from clients, as well as an internal
/// chunnel from the canonical_addr proxy.  The first caller to call `serve` will get the internal
/// chunnel, and subsequent callers will error.
pub struct SelectListener<A, B, C = SameAddr> {
    a: A,
    b: B,
    c: std::marker::PhantomData<C>,
}

impl<A, B> SelectListener<A, B, SameAddr> {
    /// internal: A way to listen for messages forwarded from the fallback canonical address listener.
    /// external: Listen for messages over the network.
    pub fn new(a: A, b: B) -> Self {
        Self {
            a,
            b,
            c: Default::default(),
        }
    }

    pub fn separate_addresses(self) -> SelectListener<A, B, DiffAddrs> {
        SelectListener {
            a: self.a,
            b: self.b,
            c: Default::default(),
        }
    }
}

macro_rules! do_listen {
    ($a: expr, $b: expr) => {
        Box::pin(
            async move {
                let (a_stream, b_stream) = futures_util::join!($a, $b);
                let a_stream = a_stream
                    .map_err(Into::into)?
                    .map(|conn| Ok(Either::Left(conn.map_err(Into::into)?)));
                let b_stream = b_stream
                    .map_err(Into::into)?
                    .map(|conn| Ok(Either::Right(conn.map_err(Into::into)?)));

                debug!("serving");
                Ok(Box::pin(futures_util::stream::select(a_stream, b_stream))
                    as Pin<
                        Box<
                            dyn Stream<Item = Result<Self::Connection, Self::Error>>
                                + Send
                                + 'static,
                        >,
                    >)
            }
            .instrument(debug_span!("SelectListener::listen")),
        )
    };
}

impl<A, Ac, Ae, B, Bc, Be, Addr, D> ChunnelListener for SelectListener<A, B, SameAddr>
where
    A: ChunnelListener<Addr = Addr, Connection = Ac, Error = Ae>,
    Ac: ChunnelConnection<Data = D> + Send + Sync + 'static,
    Ae: Into<Error> + Send + Sync + 'static,
    B: ChunnelListener<Addr = Addr, Connection = Bc, Error = Be> + Send + 'static,
    Bc: ChunnelConnection<Data = D> + Send + Sync + 'static,
    Be: Into<Error> + Send + Sync + 'static,
    Addr: Clone,
    D: Send + Sync + 'static,
{
    type Addr = Addr;
    type Connection = Either<Ac, Bc>;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn listen(&mut self, addr: Addr) -> Self::Future {
        let a_listen_fut = self.a.listen(addr.clone());
        let b_listen_fut = self.b.listen(addr);
        do_listen!(a_listen_fut, b_listen_fut)
    }
}

impl<A, Ac, Ae, Aa, B, Bc, Be, Ba, D> ChunnelListener for SelectListener<A, B, DiffAddrs>
where
    A: ChunnelListener<Addr = Aa, Connection = Ac, Error = Ae>,
    Ac: ChunnelConnection<Data = D> + Send + Sync + 'static,
    Ae: Into<Error> + Send + Sync + 'static,
    B: ChunnelListener<Addr = Ba, Connection = Bc, Error = Be> + Send + 'static,
    Bc: ChunnelConnection<Data = D> + Send + Sync + 'static,
    Be: Into<Error> + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Addr = (Aa, Ba);
    type Connection = Either<Ac, Bc>;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn listen(&mut self, (a_addr, b_addr): Self::Addr) -> Self::Future {
        let a_listen_fut = self.a.listen(a_addr);
        let b_listen_fut = self.b.listen(b_addr);
        do_listen!(a_listen_fut, b_listen_fut)
    }
}

#[cfg(test)]
mod test {
    use super::SelectListener;
    use crate::chan_transport::RendezvousChannel;
    use crate::test::COLOR_EYRE;
    use crate::{ChunnelConnection, ChunnelConnector, ChunnelListener};
    use color_eyre::Report;
    use futures_util::TryStreamExt;
    use tracing::{debug, info_span, trace};
    use tracing_error::ErrorLayer;
    use tracing_futures::Instrument;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    #[test]
    fn select_listener() {
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

        rt.block_on(
            async move {
                let (l1, mut c1) = RendezvousChannel::new(10).split();
                let (l2, mut c2) = RendezvousChannel::new(10).split();

                let (s, r) = tokio::sync::oneshot::channel();
                let address = String::from("12 Grimmauld Place");
                let laddr = address.clone();

                let mut srv = SelectListener::new(l1, l2);
                tokio::spawn(async move {
                    let st = srv.listen(laddr).await.expect("listen");
                    s.send(()).unwrap();
                    let mut i = 0;
                    st.try_for_each_concurrent(None, |cn| {
                        let idx = i;
                        i += 1;
                        async move {
                            debug!("new connection");
                            let mut slots = [None, None];
                            loop {
                                match cn.recv(&mut slots).await {
                                    Ok(ms) => {
                                        cn.send(ms.into_iter().map_while(Option::take))
                                            .await
                                            .expect("server send");
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
                        .instrument(info_span!("conn", idx))
                    })
                    .instrument(info_span!("server"))
                    .await
                    .unwrap();
                });

                r.await?;
                let cn = c1.connect(address.clone()).await?;
                cn.send(std::iter::once((address.clone(), vec![1u8; 1])))
                    .await?;
                let mut slots = [None];
                let ms = cn.recv(&mut slots).await?;
                assert_eq!(ms[0].take().unwrap().1, vec![1u8; 1]);
                let cn = c2.connect(address.clone()).await?;
                cn.send(std::iter::once((address.clone(), vec![1u8; 1])))
                    .await?;
                let ms = cn.recv(&mut slots).await?;
                assert_eq!(ms[0].take().unwrap().1, vec![1u8; 1]);
                Ok::<_, Report>(())
            }
            .instrument(info_span!("select_listener")),
        )
        .unwrap();
    }
}
