//! Serialization chunnel with bincode.

use crate::{ChunnelConnection, Context, InheritChunnel};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Debug)]
pub struct SerializeChunnel<C, D> {
    inner: Arc<C>,
    _data: std::marker::PhantomData<D>,
}

impl<Cx, D> From<Cx> for SerializeChunnel<Cx, D> {
    fn from(cx: Cx) -> SerializeChunnel<Cx, D> {
        SerializeChunnel {
            inner: Arc::new(cx),
            _data: Default::default(),
        }
    }
}

impl<C, D> Clone for SerializeChunnel<C, D>
where
    C: Clone,
{
    fn clone(&self) -> Self {
        let inner: C = self.inner.as_ref().clone();
        Self {
            inner: Arc::new(inner),
            _data: Default::default(),
        }
    }
}

impl<C, D> Context for SerializeChunnel<C, D> {
    type ChunnelType = C;

    fn context(&self) -> &Self::ChunnelType {
        &self.inner
    }

    fn context_mut(&mut self) -> &mut Self::ChunnelType {
        Arc::get_mut(&mut self.inner).unwrap()
    }
}

impl<B, C, D> InheritChunnel<C> for SerializeChunnel<B, D>
where
    C: ChunnelConnection<Data = Vec<u8>> + Send + Sync + 'static,
    D: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    type Connection = Serialize<C, D>;
    type Config = ();

    fn get_config(&mut self) -> Self::Config {}

    fn make_connection(cx: C, _cfg: Self::Config) -> Self::Connection {
        Serialize::from(cx)
    }
}

impl<Inner, D> crate::ChunnelListener<D> for SerializeChunnel<Inner, D>
where
    Inner: crate::ChunnelListener<Vec<u8>>,
    Inner::Connection: ChunnelConnection<Data = Vec<u8>> + Send + Sync + 'static,
    Self: InheritChunnel<Inner::Connection, Connection = Serialize<Inner::Connection, D>>,
    Self: Context<ChunnelType = Inner>,
    D: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    type Addr = Inner::Addr;
    type Connection = Serialize<Inner::Connection, D>;
    type Error = Inner::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>,
    >;
    type Stream = Pin<
        Box<
            dyn futures_util::stream::Stream<Item = Result<Self::Connection, Self::Error>>
                + Send
                + 'static,
        >,
    >;

    fn listen(&mut self, a: Self::Addr) -> Self::Future {
        use futures_util::StreamExt;
        let f = self.context_mut().listen(a);
        let cfg = self.get_config();
        Box::pin(async move {
            let cfg = cfg;
            let conn_stream = f.await?;
            Ok(
                Box::pin(conn_stream.map(move |conn: Result<Inner::Connection, _>| {
                    Ok(Self::make_connection(conn?, cfg.clone()))
                })) as _,
            )
        })
    }

    fn scope() -> crate::Scope {
        Inner::scope()
    }

    fn endedness() -> crate::Endedness {
        Inner::endedness()
    }

    fn implementation_priority() -> usize {
        Inner::implementation_priority()
    }
}

impl<Inner, D> crate::ChunnelConnector<D> for SerializeChunnel<Inner, D>
where
    Inner: crate::ChunnelConnector<Vec<u8>>,
    Inner::Connection: ChunnelConnection<Data = Vec<u8>> + Send + Sync + 'static,
    Self: InheritChunnel<Inner::Connection, Connection = Serialize<Inner::Connection, D>>,
    Self: Context<ChunnelType = Inner>,
    D: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    type Addr = Inner::Addr;
    type Connection = Serialize<Inner::Connection, D>;
    type Error = Inner::Error;
    type Future = std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Self::Connection, Self::Error>>
                + Send
                + 'static,
        >,
    >;

    fn connect(&mut self, a: Self::Addr) -> Self::Future {
        let f = self.context_mut().connect(a);
        let cfg = self.get_config();
        Box::pin(async move { Ok(Self::make_connection(f.await?, cfg)) })
    }

    fn scope() -> crate::Scope {
        Inner::scope()
    }

    fn endedness() -> crate::Endedness {
        Inner::endedness()
    }

    fn implementation_priority() -> usize {
        Inner::implementation_priority()
    }
}

#[derive(Default, Debug, Clone)]
pub struct Serialize<C, D> {
    inner: Arc<C>,
    _data: std::marker::PhantomData<D>,
}

impl<C, D> Context for Serialize<C, D> {
    type ChunnelType = C;

    fn context(&self) -> &Self::ChunnelType {
        &self.inner
    }

    fn context_mut(&mut self) -> &mut Self::ChunnelType {
        Arc::get_mut(&mut self.inner).unwrap()
    }
}

impl<Cx, D> From<Cx> for Serialize<Cx, D> {
    fn from(cx: Cx) -> Serialize<Cx, D> {
        Serialize {
            inner: Arc::new(cx),
            _data: Default::default(),
        }
    }
}

impl<C, D> ChunnelConnection for Serialize<C, D>
where
    C: ChunnelConnection<Data = Vec<u8>> + Send + Sync + 'static,
    D: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    type Data = D;

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            let buf = bincode::serialize(&data)?;
            inner.send(buf).await?;
            Ok(())
        })
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            let buf = inner.recv().await?;
            let data = bincode::deserialize(&buf[..])?;
            Ok(data)
        })
    }
}

#[cfg(test)]
mod test {
    use super::SerializeChunnel;
    use crate::chan_transport::Chan;
    use crate::{ChunnelConnection, ChunnelConnector, ChunnelListener};
    use futures_util::StreamExt;
    use tracing::trace;
    use tracing_futures::Instrument;

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
        let _guard = tracing_subscriber::fmt::try_init();
        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_time()
            .build()
            .unwrap();
        rt.block_on(
            async move {
                let (srv, cln) = Chan::default().split();

                let mut rcv = SerializeChunnel::from(srv);
                let mut rcv = rcv.listen(()).await.unwrap();
                let rcv = rcv.next().await.unwrap().unwrap();

                let mut snd = SerializeChunnel::from(cln);
                let snd = snd.connect(()).await.unwrap();

                send_thingy(snd, rcv, 42u32).await;
            }
            .instrument(tracing::info_span!("send_u32")),
        );
    }

    #[test]
    fn send_struct() {
        let _guard = tracing_subscriber::fmt::try_init();
        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
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
                let (srv, cln) = Chan::default().split();

                let mut rcv = SerializeChunnel::from(srv);
                let mut rcv = rcv.listen(()).await.unwrap();
                let rcv = rcv.next().await.unwrap().unwrap();

                let mut snd = SerializeChunnel::from(cln);
                let snd = snd.connect(()).await.unwrap();

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
}
