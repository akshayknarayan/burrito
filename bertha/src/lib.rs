//! Tools for working with Chunnels.

// Pin<Box<...>> is necessary and not worth breaking up
#![allow(clippy::type_complexity)]

use color_eyre::eyre;
use futures_util::stream::Stream;
use std::future::Future;
use std::pin::Pin;

mod and_then_concurrent;
pub mod atmostonce;
pub mod bincode;
pub mod chan_transport;
pub mod either;
pub mod negotiate;
pub mod reliable;
pub mod select;
pub mod split;
pub mod tagger;
pub mod udp;
pub mod uds;
pub mod util;

pub use either::*;
pub use negotiate::*;

/// `Chunnel`s transform semantics of the data flowing through them in some way.
pub trait Chunnel<I> {
    type Future: Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static;
    type Connection: ChunnelConnection + 'static;
    type Error: Send + Sync + 'static;

    fn connect_wrap(&mut self, inner: I) -> Self::Future;
}

#[derive(Clone, Debug, Default, Copy, PartialEq, Eq)]
pub struct CxNil;

impl<C> Chunnel<C> for CxNil
where
    C: ChunnelConnection + Send + 'static,
{
    type Future = futures_util::future::Ready<Result<Self::Connection, Self::Error>>;
    type Connection = C;
    type Error = eyre::Report;

    fn connect_wrap(&mut self, inner: C) -> Self::Future {
        futures_util::future::ready(Ok(inner))
    }
}

/// Chain multiple chunnels together with the `Chunnel` trait.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CxList<Head, Tail> {
    pub head: Head,
    pub tail: Tail,
}

impl<T> From<T> for CxList<T, CxNil> {
    fn from(t: T) -> Self {
        CxList {
            head: t,
            tail: CxNil,
        }
    }
}

impl<T, L> From<(T, L)> for CxList<T, CxList<L, CxNil>> {
    fn from(t: (T, L)) -> Self {
        CxList::from(t.1).wrap(t.0)
    }
}

impl<H, L> CxList<H, L> {
    pub fn wrap<T>(self, head: T) -> CxList<T, CxList<H, L>> {
        CxList { head, tail: self }
    }
}

impl<H, T, I> Chunnel<I> for CxList<H, T>
where
    H: Chunnel<I>,
    <H as Chunnel<I>>::Connection: Send + 'static,
    T: Chunnel<<H as Chunnel<I>>::Connection> + Clone + Send + 'static,
    <T as Chunnel<<H as Chunnel<I>>::Connection>>::Error: From<<H as Chunnel<I>>::Error>,
{
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Connection = T::Connection;
    type Error = T::Error;

    fn connect_wrap(&mut self, inner: I) -> Self::Future {
        let cn_fut = self.head.connect_wrap(inner);
        let mut tail = self.tail.clone();
        Box::pin(async move {
            let cn = cn_fut.await?;
            let cn = tail.connect_wrap(cn).await?;
            Ok(cn)
        })
    }
}

pub trait ChunnelListener {
    type Future: Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static;
    type Addr;
    type Connection: ChunnelConnection + 'static;
    type Error: Send + Sync + 'static;
    type Stream: Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static;

    fn listen(&mut self, a: Self::Addr) -> Self::Future;
}

/// `ChunnelConnector`s connect to a single remote Chunnel endpoint and return one connection.
pub trait ChunnelConnector {
    type Future: Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static;
    type Addr;
    type Connection: ChunnelConnection + 'static;
    type Error: Send + Sync + 'static;

    fn connect(&mut self, a: Self::Addr) -> Self::Future;
}

/// A connection with the semantics of the Chunnel type's functionality.
pub trait ChunnelConnection {
    type Data;

    fn send<'cn, B>(
        &'cn self,
        burst: B,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'cn>>
    where
        B: IntoIterator<Item = Self::Data> + Send + 'cn,
        <B as IntoIterator>::IntoIter: Send;

    fn recv<'cn, 'buf>(
        &'cn self,
        msgs_buf: &'buf mut [Option<Self::Data>],
    ) -> Pin<
        Box<dyn Future<Output = Result<&'buf mut [Option<Self::Data>], eyre::Report>> + Send + 'cn>,
    >
    where
        'buf: 'cn;
}

/// For address types to expose ip and port information for inner addresses.
pub trait IpPort {
    fn ip(&self) -> std::net::IpAddr;
    fn port(&self) -> u16;
}

impl IpPort for std::net::SocketAddr {
    fn ip(&self) -> std::net::IpAddr {
        self.ip()
    }

    fn port(&self) -> u16 {
        self.port()
    }
}

#[cfg(test)]
mod test {
    use crate::chan_transport::Chan;
    use crate::{Chunnel, ChunnelConnection, ChunnelConnector, ChunnelListener, CxList, CxNil};
    use color_eyre::Report;
    use futures_util::{
        future::{ready, Ready, TryFutureExt},
        stream::Stream,
        StreamExt, TryStreamExt,
    };
    use std::{future::Future, pin::Pin};
    use tracing_error::ErrorLayer;
    use tracing_futures::Instrument;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    pub(crate) trait Serve<I> {
        type Future: Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static;
        type Connection: ChunnelConnection + 'static;
        type Error: Send + Sync + 'static;
        type Stream: Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static;

        fn serve(&mut self, inner: I) -> Self::Future;
    }

    impl<InS, InC, InE, C> Serve<InS> for C
    where
        C: Chunnel<InC> + Clone + Send + 'static,
        <C as Chunnel<InC>>::Error: Into<Report> + Send + Sync + 'static,
        InS: Stream<Item = Result<InC, InE>> + Send + 'static,
        InC: Send + 'static,
        InE: Into<Report> + Send + Sync + 'static,
    {
        type Future = Ready<Result<Self::Stream, Self::Error>>;
        type Connection = C::Connection;
        type Error = Report;
        type Stream =
            Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;

        fn serve(&mut self, inner: InS) -> Self::Future {
            use crate::and_then_concurrent::TryStreamExtExt;
            let mut this = self.clone();
            ready(Ok(Box::pin(
                inner
                    .map_err(Into::into)
                    .and_then_concurrent(move |cn| this.connect_wrap(cn).map_err(Into::into)),
            ) as _))
        }
    }

    #[test]
    fn cxnil() {
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

                let stack = CxList::from(CxNil);
                let mut stack = stack.wrap(CxNil);

                let rcv_st = srv.listen(()).await?;
                let mut rcv_st = stack.serve(rcv_st).await?;
                let rcv = rcv_st.next().await.unwrap().unwrap();

                let cln = cln.connect(()).await?;
                let snd = stack.connect_wrap(cln).await?;

                snd.send(std::iter::once(((), vec![1u8; 1]))).await?;
                let mut recv_buf = [None];
                let bufs = rcv.recv(&mut recv_buf).await?;
                assert_eq!(bufs.len(), 1);
                let (_, buf) = bufs[0].take().unwrap();
                assert_eq!(buf, vec![1u8; 1]);
                Ok::<_, Report>(())
            }
            .instrument(tracing::info_span!("cxnil")),
        )
        .unwrap();
    }
}
