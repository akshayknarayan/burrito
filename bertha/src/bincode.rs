//! Serialization chunnel with bincode.

use crate::{
    util::{ProjectLeft, Unproject},
    Chunnel, ChunnelConnection, Negotiate,
};
use color_eyre::eyre::{eyre, Report, WrapErr};
use futures_util::future::{ready, Ready};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tracing::debug;

#[derive(Debug, Clone)]
pub struct SerializeChunnelProject<D> {
    _data: std::marker::PhantomData<D>,
}

impl<D> Negotiate for SerializeChunnelProject<D> {
    type Capability = ();

    fn guid() -> u64 {
        0xd5263bf239e761c3
    }
}

impl<D> Default for SerializeChunnelProject<D> {
    fn default() -> Self {
        SerializeChunnelProject {
            _data: Default::default(),
        }
    }
}

impl<A, D, InC> Chunnel<InC> for SerializeChunnelProject<D>
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
    fn guid() -> u64 {
        0xd5263bf239e761c3
    }
}

impl<D, InC> Chunnel<InC> for SerializeChunnel<D>
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
    use crate::test::Serve;
    use crate::{util::ProjectLeft, Chunnel, ChunnelConnection, ChunnelConnector, ChunnelListener};
    use futures_util::stream::{StreamExt, TryStreamExt};
    use tracing::trace;
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
        color_eyre::install().unwrap_or(());
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
        color_eyre::install().unwrap_or(());
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
        color_eyre::install().unwrap_or(());
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
}
