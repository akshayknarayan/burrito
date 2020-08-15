//! Serialization chunnel with bincode.

use crate::{ChunnelConnection, Client, Serve};
use futures_util::future::{ready, Ready};
use futures_util::stream::Stream;
use futures_util::stream::TryStreamExt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
pub struct SerializeChunnel<D> {
    _data: std::marker::PhantomData<D>,
}

impl<D, InS, InC, InE> Serve<InS> for SerializeChunnel<D>
where
    InS: Stream<Item = Result<InC, InE>> + Send + 'static,
    InC: ChunnelConnection<Data = Vec<u8>> + Send + Sync + 'static,
    InE: Send + Sync + 'static,
    D: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Stream, Self::Error>>;
    type Connection = Serialize<InC, D>;
    type Error = InE;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn serve(&mut self, inner: InS) -> Self::Future {
        ready(Ok(Box::pin(inner.map_ok(|cn| Serialize::from(cn))) as _))
    }
}

impl<D, InC> Client<InC> for SerializeChunnel<D>
where
    InC: ChunnelConnection<Data = Vec<u8>> + Send + Sync + 'static,
    D: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = Serialize<InC, D>;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        ready(Ok(Serialize::from(cn)))
    }
}

#[derive(Default, Debug, Clone)]
pub struct Serialize<C, D> {
    inner: Arc<C>,
    _data: std::marker::PhantomData<D>,
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
    use crate::chan_transport::{Chan, ChanAddr};
    use crate::{
        ChunnelConnection, ChunnelConnector, ChunnelListener, Client, ConnectAddress, CxList,
        ListenAddress, Serve,
    };
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
        color_eyre::install().unwrap_or_else(|_| ());
        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_time()
            .build()
            .unwrap();
        rt.block_on(
            async move {
                let a: ChanAddr<Vec<u8>> = Chan::default().into();

                let mut srv = a.listener();
                let rcv_st = srv.listen(a.clone()).await.unwrap();
                let mut rcv_st = CxList::from(SerializeChunnel::default())
                    .serve(rcv_st)
                    .await
                    .unwrap();
                let rcv = rcv_st.next().await.unwrap().unwrap();

                let mut cln = a.connector();
                let cln = cln.connect(a).await.unwrap();
                let snd = SerializeChunnel::default().connect_wrap(cln).await.unwrap();

                send_thingy(snd, rcv, 42u32).await;
            }
            .instrument(tracing::info_span!("send_u32")),
        );
    }

    #[test]
    fn send_struct() {
        let _guard = tracing_subscriber::fmt::try_init();
        color_eyre::install().unwrap_or_else(|_| ());
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
                let a: ChanAddr<Vec<u8>> = Chan::default().into();

                let mut srv = a.listener();
                let rcv_st = srv.listen(a.clone()).await.unwrap();
                let mut rcv_st = SerializeChunnel::default().serve(rcv_st).await.unwrap();
                let rcv = rcv_st.next().await.unwrap().unwrap();

                let mut cln = a.connector();
                let cln = cln.connect(a).await.unwrap();
                let snd = SerializeChunnel::default().connect_wrap(cln).await.unwrap();

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
