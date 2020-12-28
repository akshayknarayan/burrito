//! Serialization chunnel with bincode.

use crate::{
    util::{ProjectLeft, Unproject},
    ChunnelConnection, Client, Negotiate, Serve,
};
use color_eyre::eyre::{eyre, Report, WrapErr};
use futures_util::future::{ready, Ready};
use futures_util::stream::Stream;
use futures_util::stream::TryStreamExt;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tracing::debug;

#[derive(Clone, Debug)]
pub struct SerializeSelect<T1, T2>(SerializeChunnelProject<T1>, SerializeChunnelProject<T2>);

impl<T1, T2> Default for SerializeSelect<T1, T2>
where
    SerializeChunnelProject<T1>: Default,
    SerializeChunnelProject<T2>: Default,
{
    fn default() -> Self {
        Self(
            SerializeChunnelProject::<T1>::default(),
            SerializeChunnelProject::<T2>::default(),
        )
    }
}

impl<T1: 'static, T2: 'static, Inp: 'static> crate::negotiate::Apply<Inp>
    for SerializeSelect<T1, T2>
{
    // the only place this D could come from is the trait itself, Apply<D>.
    type Applied = SerializeChunnelProject<Inp>;

    fn apply(
        self,
        picked_offers: HashMap<u64, Vec<crate::negotiate::Offer>>,
    ) -> Result<(Self::Applied, HashMap<u64, Vec<crate::negotiate::Offer>>), Report> {
        let datatype = std::any::TypeId::of::<Inp>();
        match datatype {
            // safety: we know the types are equivalent because TypeId tells us.
            x if x == std::any::TypeId::of::<T1>() => {
                Ok((unsafe { std::mem::transmute(self.0) }, picked_offers))
            }
            x if x == std::any::TypeId::of::<T2>() => {
                Ok((unsafe { std::mem::transmute(self.1) }, picked_offers))
            }
            _ => Err(eyre!("No match on serialization")),
        }
    }
}

use crate::negotiate::GetOffers;
impl<T1, T2> GetOffers for SerializeSelect<T1, T2> {
    type Iter = std::iter::Empty<crate::negotiate::Offer>;
    fn offers(&self) -> Self::Iter {
        std::iter::empty()
    }
}

#[derive(Debug, Clone)]
pub struct SerializeChunnelProject<D> {
    _data: std::marker::PhantomData<D>,
}

impl<D> Negotiate for SerializeChunnelProject<D> {
    type Capability = ();
}

impl<D> Default for SerializeChunnelProject<D> {
    fn default() -> Self {
        SerializeChunnelProject {
            _data: Default::default(),
        }
    }
}

impl<A, D, InS, InC, InE> Serve<InS> for SerializeChunnelProject<D>
where
    InS: Stream<Item = Result<InC, InE>> + Send + 'static,
    InC: ChunnelConnection<Data = (A, Vec<u8>)> + Send + Sync + 'static,
    InE: Send + Sync + 'static,
    A: Send + Sync + 'static,
    D: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Stream, Self::Error>>;
    type Connection = SerializeProject<A, D, InC>;
    type Error = InE;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn serve(&mut self, inner: InS) -> Self::Future {
        ready(Ok(Box::pin(inner.map_ok(SerializeProject::from)) as _))
    }
}

impl<A, D, InC> Client<InC> for SerializeChunnelProject<D>
where
    InC: ChunnelConnection<Data = (A, Vec<u8>)> + Send + Sync + 'static,
    A: Send + Sync + 'static,
    D: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = SerializeProject<A, D, InC>;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        ready(Ok(SerializeProject::from(cn)))
    }
}

#[derive(Debug, Clone, Default)]
pub struct SerializeChunnel<D> {
    _data: std::marker::PhantomData<D>,
}

impl<D: 'static> Negotiate for SerializeChunnel<D> {
    type Capability = ();
}

impl<D, InS, InC, InE> Serve<InS> for SerializeChunnel<D>
where
    InS: Stream<Item = Result<InC, InE>> + Send + 'static,
    InC: ChunnelConnection<Data = Vec<u8>> + Send + Sync + 'static,
    InE: Send + Sync + 'static,
    D: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Stream, Self::Error>>;
    type Connection = ProjectLeft<(), SerializeProject<(), D, Unproject<InC>>>;
    type Error = InE;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn serve(&mut self, inner: InS) -> Self::Future {
        let st = inner.map_ok(Unproject);
        match SerializeChunnelProject::default().serve(st).into_inner() {
            Ok(st) => ready(Ok(Box::pin(st.map_ok(|cn| ProjectLeft::new((), cn))) as _)),
            Err(e) => ready(Err(e)),
        }
    }
}

impl<D, InC> Client<InC> for SerializeChunnel<D>
where
    InC: ChunnelConnection<Data = Vec<u8>> + Send + Sync + 'static,
    D: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Connection = ProjectLeft<(), SerializeProject<(), D, Unproject<InC>>>;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        Box::pin(async move {
            let cn = SerializeChunnelProject::default()
                .connect_wrap(Unproject(cn))
                .await?;
            Ok(ProjectLeft::new((), cn))
        })
    }
}

#[derive(Default, Debug, Clone)]
pub struct SerializeProject<A, D, C> {
    inner: Arc<C>,
    _data: std::marker::PhantomData<(A, D)>,
}

impl<Cx, A, D> From<Cx> for SerializeProject<A, D, Cx> {
    fn from(cx: Cx) -> SerializeProject<A, D, Cx> {
        debug!(data = ?std::any::type_name::<D>(), "make serialize chunnel");
        SerializeProject {
            inner: Arc::new(cx),
            _data: Default::default(),
        }
    }
}

impl<A, C, D> ChunnelConnection for SerializeProject<A, D, C>
where
    C: ChunnelConnection<Data = (A, Vec<u8>)> + Send + Sync + 'static,
    A: Send + Sync + 'static,
    D: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    type Data = (A, D);

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            let buf = bincode::serialize(&data.1).wrap_err("serialize failed")?;
            inner.send((data.0, buf)).await?;
            Ok(())
        })
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            let (a, buf) = inner.recv().await?;
            let data = bincode::deserialize(&buf[..]).wrap_err(eyre!(
                "deserialize failed: {:?} -> {:?}",
                buf,
                std::any::type_name::<D>()
            ))?;
            Ok((a, data))
        })
    }
}

#[cfg(test)]
mod test {
    use super::{SerializeChunnel, SerializeChunnelProject};
    use crate::chan_transport::Chan;
    use crate::{
        util::ProjectLeft, ChunnelConnection, ChunnelConnector, ChunnelListener, Client, Serve,
    };
    use color_eyre::eyre::Report;
    use futures_util::{
        future::{ready, Ready},
        stream::{Stream, StreamExt, TryStreamExt},
    };
    use std::future::Future;
    use std::pin::Pin;
    use tracing::{debug, trace};
    use tracing_error::ErrorLayer;
    use tracing_futures::Instrument;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    async fn send_thingy<
        C: ChunnelConnection<Data = T>,
        T: serde::Serialize
            + serde::de::DeserializeOwned
            + std::fmt::Debug
            + Send
            + Sync
            + Clone
            + PartialEq,
    >(
        snd_ch: C,
        rcv_ch: C,
        t: T,
    ) {
        let s = t.clone();
        trace!(msg = ?t, "sending");
        snd_ch.send(t).await.expect("send");
        let r = rcv_ch.recv().await.expect("recv");
        trace!(msg = ?r, "received");
        assert_eq!(r, s);
    }

    #[test]
    fn send_u32() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        color_eyre::install().unwrap_or_else(|_| ());
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_time()
            .build()
            .unwrap();
        rt.block_on(
            async move {
                let (mut srv, mut cln) = Chan::default().split();

                let rcv_st = srv.listen(()).await.unwrap();
                let mut stack = SerializeChunnel::default();

                let mut rcv_st = stack.serve(rcv_st).await.unwrap();
                let rcv = rcv_st.next().await.unwrap().unwrap();

                let cln = cln.connect(()).await.unwrap();
                let snd = stack.connect_wrap(cln).await.unwrap();

                send_thingy(snd, rcv, 42u32).await;
            }
            .instrument(tracing::info_span!("send_u32")),
        );
    }

    #[test]
    fn send_struct() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        color_eyre::install().unwrap_or_else(|_| ());
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_time()
            .build()
            .unwrap();

        #[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, Debug, Default)]
        struct Foo {
            a: u32,
            b: u64,
            c: String,
        }

        rt.block_on(
            async move {
                let (mut srv, mut cln) = Chan::default().split();
                let mut stack = SerializeChunnel::default();

                let rcv_st = srv.listen(()).await.unwrap();
                let mut rcv_st = stack.serve(rcv_st).await.unwrap();
                let rcv = rcv_st.next().await.unwrap().unwrap();

                let cln = cln.connect(()).await.unwrap();
                let snd = stack.connect_wrap(cln).await.unwrap();

                send_thingy(
                    snd,
                    rcv,
                    Foo {
                        a: 4,
                        b: 5,
                        c: "hello".to_owned(),
                    },
                )
                .await;
            }
            .instrument(tracing::info_span!("send_struct")),
        );
    }

    #[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, Debug, Default)]
    struct Foo {
        a: u32,
        b: u64,
        c: String,
    }

    #[test]
    fn serialize_negotiate_one() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        color_eyre::install().unwrap_or_else(|_| ());
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_time()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                let (mut srv, mut cln) = Chan::default().split();
                let stack = SerializeChunnelProject::default();

                let srv_stack = stack.clone();
                tokio::spawn(async move {
                    let rcv_st = srv.listen(()).await.unwrap();
                    let st = crate::negotiate::negotiate_server(srv_stack, rcv_st)
                        .await
                        .unwrap();
                    st.try_for_each_concurrent(None, |cn| async move {
                        let cn = ProjectLeft::new((), cn);
                        loop {
                            let buf = cn.recv().await?;
                            cn.send(buf).await?;
                        }
                    })
                    .await
                    .unwrap();
                });

                let raw_cn = cln.connect(()).await.unwrap();
                let cn = crate::negotiate::negotiate_client(stack, raw_cn, ())
                    .await
                    .unwrap();
                let cn = ProjectLeft::new((), cn);

                let obj = Foo {
                    a: 4,
                    b: 5,
                    c: "hello".to_owned(),
                };

                cn.send(obj.clone()).await.unwrap();
                let r = cn.recv().await.unwrap();
                assert_eq!(r, obj);
            }
            .instrument(tracing::info_span!("serialize_negotiate")),
        );
    }

    #[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, Debug, Default)]
    struct Bar {
        d: u64,
        c: String,
    }

    #[derive(Clone, Copy, Debug, Default)]
    struct BarChunnel;

    impl crate::negotiate::Negotiate for BarChunnel {
        type Capability = ();
    }

    impl<A, InS, InC, InE> Serve<InS> for BarChunnel
    where
        InS: Stream<Item = Result<InC, InE>> + Send + 'static,
        InC: ChunnelConnection<Data = (A, Foo)> + Send + Sync + 'static,
        InE: Send + Sync + 'static,
        A: 'static,
    {
        type Future = Ready<Result<Self::Stream, Self::Error>>;
        type Connection = BarCn<InC>;
        type Error = InE;
        type Stream =
            Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;

        fn serve(&mut self, inner: InS) -> Self::Future {
            ready(Ok(Box::pin(inner.map_ok(|i| BarCn(i))) as _))
        }
    }

    impl<A, InC> Client<InC> for BarChunnel
    where
        A: 'static,
        InC: ChunnelConnection<Data = (A, Foo)> + Send + Sync + 'static,
    {
        type Future = Ready<Result<Self::Connection, Self::Error>>;
        type Connection = BarCn<InC>;
        type Error = Report;

        fn connect_wrap(&mut self, inner: InC) -> Self::Future {
            ready(Ok(BarCn(inner)))
        }
    }

    struct BarCn<C>(C);

    impl<A, C> ChunnelConnection for BarCn<C>
    where
        C: ChunnelConnection<Data = (A, Foo)>,
        A: 'static,
    {
        type Data = (A, Bar);

        fn send(
            &self,
            data: Self::Data,
        ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
            let foo = Foo {
                a: 0,
                b: data.1.d,
                c: data.1.c,
            };

            self.0.send((data.0, foo))
        }

        fn recv(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
            let fut = self.0.recv();
            Box::pin(async move {
                let foo = fut.await?;
                Ok((
                    foo.0,
                    Bar {
                        d: foo.1.a as u64 + foo.1.b,
                        c: foo.1.c,
                    },
                ))
            })
        }
    }

    #[test]
    fn serialize_negotiate_multi() {
        use crate::CxList;

        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        color_eyre::install().unwrap_or_else(|_| ());
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_time()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                let (mut srv, mut cln) = Chan::default().split();
                let stack = CxList::from(BarChunnel).wrap(SerializeChunnelProject::default());

                let srv_stack = stack.clone();
                tokio::spawn(async move {
                    let rcv_st = srv.listen(()).await.unwrap();
                    let st = crate::negotiate::negotiate_server(srv_stack, rcv_st)
                        .await
                        .unwrap();
                    st.try_for_each_concurrent(None, |cn| async move {
                        let cn = ProjectLeft::new((), cn);
                        loop {
                            let buf = cn.recv().await?;
                            cn.send(buf).await?;
                        }
                    })
                    .await
                    .unwrap();
                });

                let stack = CxList::from(SerializeChunnelProject::default()).wrap(BarChunnel);
                let raw_cn = cln.connect(()).await.unwrap();
                let cn = crate::negotiate::negotiate_client(stack, raw_cn, ())
                    .await
                    .unwrap();
                let cn = ProjectLeft::new((), cn);

                fn dump_type<T>(_: &T) {
                    debug!(conn = ?std::any::type_name::<T>(), "got connection");
                }

                dump_type(&cn);

                let obj = Bar {
                    d: 9,
                    c: "hello".to_owned(),
                };

                cn.send(obj.clone()).await.unwrap();
                let r = cn.recv().await.unwrap();
                assert_eq!(r, obj);
            }
            .instrument(tracing::info_span!("serialize_negotiate")),
        );
    }
}
