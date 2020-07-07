//! Serialization chunnel with bincode.

use crate::{Chunnel, Endedness, Scope};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Default)]
pub struct Serialize<C, D> {
    inner: Arc<C>,
    _data: std::marker::PhantomData<D>,
}

impl<Cx, D> Serialize<Cx, D> {
    pub fn with_context<C>(self, cx: C) -> Serialize<C, D> {
        Serialize {
            inner: Arc::new(cx),
            _data: Default::default(),
        }
    }
}

impl<C, D> Chunnel for Serialize<C, D>
where
    C: Chunnel<Data = Vec<u8>> + Send + Sync + 'static,
    D: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    type Data = D;

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + Sync>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            let buf = bincode::serialize(&data)?;
            inner.send(buf).await?;
            Ok(())
        })
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + Sync>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            let buf = inner.recv().await?;
            let data = bincode::deserialize(&buf[..])?;
            Ok(data)
        })
    }

    fn scope(&self) -> Scope {
        Scope::Application
    }

    fn endedness(&self) -> Endedness {
        Endedness::Both
    }

    fn implementation_priority(&self) -> usize {
        1
    }
}

#[cfg(test)]
mod test {
    use super::Serialize;
    use crate::chan_transport::Chan;
    use crate::{Chunnel, Connector};
    use futures_util::StreamExt;
    use tracing::trace;
    use tracing_futures::Instrument;

    async fn send_thingy<
        C: Chunnel<Data = T>,
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
                let mut t = Chan::default();
                let mut rcv = t.listen(()).await;
                let rcv_cn = rcv.next().await.unwrap();
                let rcv_ch = Serialize::<(), _>::default().with_context(rcv_cn);

                let snd = t.connect(()).await;
                let snd_ch = Serialize::<(), _>::default().with_context(snd);

                send_thingy(snd_ch, rcv_ch, 42u32).await;
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
                let mut t = Chan::default();
                let mut rcv = t.listen(()).await;
                let rcv_cn = rcv.next().await.unwrap();
                let rcv_ch = Serialize::<(), _>::default().with_context(rcv_cn);

                let snd = t.connect(()).await;
                let snd_ch = Serialize::<(), _>::default().with_context(snd);

                send_thingy(
                    snd_ch,
                    rcv_ch,
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
