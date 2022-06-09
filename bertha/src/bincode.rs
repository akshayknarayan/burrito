//! Serialization chunnel with bincode.

use crate::{Chunnel, ChunnelConnection, Negotiate};
use color_eyre::eyre::{eyre, Report, WrapErr};
use futures_util::future::{ready, Ready};
use std::future::Future;
use std::pin::Pin;
use tracing::{debug, trace, trace_span};
use tracing_futures::Instrument;

#[derive(Debug, Clone)]
pub struct SerializeChunnel<D> {
    _data: std::marker::PhantomData<D>,
}

impl<D> Negotiate for SerializeChunnel<D> {
    type Capability = ();

    fn guid() -> u64 {
        0xd5263bf239e761c3
    }
}

impl<D> Default for SerializeChunnel<D> {
    fn default() -> Self {
        SerializeChunnel {
            _data: Default::default(),
        }
    }
}

impl<A, D, InC> Chunnel<InC> for SerializeChunnel<D>
where
    InC: ChunnelConnection<Data = (A, Vec<u8>)> + Send + Sync + 'static,
    A: Send + Sync + 'static,
    D: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = SerializeCn<A, D, InC>;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        ready(Ok(SerializeCn::from(cn)))
    }
}

#[derive(Default, Debug, Clone)]
pub struct SerializeCn<A, D, C> {
    inner: C,
    _data: std::marker::PhantomData<(A, D)>,
}

impl<Cn, A, D> From<Cn> for SerializeCn<A, D, Cn> {
    fn from(cx: Cn) -> SerializeCn<A, D, Cn> {
        debug!(data = ?std::any::type_name::<D>(), "make serialize chunnel");
        SerializeCn {
            inner: cx,
            _data: Default::default(),
        }
    }
}

impl<A, C, D> ChunnelConnection for SerializeCn<A, D, C>
where
    C: ChunnelConnection<Data = (A, Vec<u8>)> + Send + Sync + 'static,
    A: Send + Sync + 'static,
    D: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    type Data = (A, D);

    fn send<'cn, B>(
        &'cn self,
        burst: B,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'cn>>
    where
        B: IntoIterator<Item = Self::Data> + Send + 'cn,
        <B as IntoIterator>::IntoIter: Send,
    {
        Box::pin(
            async move {
                let b: Result<Vec<_>, Report> = burst
                    .into_iter()
                    .map(|(a, d)| {
                        Ok((
                            a,
                            bincode::serialize(&d).wrap_err(eyre!("serialize error"))?,
                        ))
                    })
                    .collect();
                self.inner.send(b?).await
            }
            .instrument(trace_span!("bincode_send")),
        )
    }

    fn recv<'cn, 'buf>(
        &'cn self,
        msgs_buf: &'buf mut [Option<Self::Data>],
    ) -> Pin<Box<dyn Future<Output = Result<&'buf mut [Option<Self::Data>], Report>> + Send + 'cn>>
    where
        'buf: 'cn,
    {
        Box::pin(
            async move {
                let mut recv_slots: Vec<_> = (0..msgs_buf.len()).map(|_| None).collect();
                let bufs = self.inner.recv(&mut recv_slots).await?;
                trace!(num = ?bufs.len(), "recvd");

                let mut ret_len = 0;
                for (buf, slot) in bufs
                    .into_iter()
                    .map_while(|x| x.take())
                    .zip(msgs_buf.into_iter())
                {
                    // we have a &mut Option<(A, D)> slot and a (A, Vec<u8>) message.
                    // XXX might it be possible to re-use this already-existing Option<(A, D)>
                    // memory and
                    // `deserialize_into`(https://github.com/bincode-org/bincode/issues/252)?
                    //
                    // The main obstacle is what we do in the `None` case. The bytes are there, but
                    // using them would make assumptions about Option's memory layout and thus be
                    // UB.
                    let data = bincode::deserialize(&buf.1[..]).wrap_err(eyre!(
                        "deserialize failed: {:?} -> {:?}",
                        buf.1,
                        std::any::type_name::<D>()
                    ))?;
                    *slot = Some((buf.0, data));
                    ret_len += 1;
                }

                Ok(&mut msgs_buf[..ret_len])
            }
            .instrument(trace_span!("bincode_recv")),
        )
    }
}

#[derive(Debug, Clone, Default)]
pub struct Base64Chunnel;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct Base64Capability;

impl crate::negotiate::CapabilitySet for Base64Capability {
    fn guid() -> u64 {
        0xd7824464b109ede9
    }

    fn universe() -> Option<Vec<Self>> {
        None
    }
}

impl Negotiate for Base64Chunnel {
    type Capability = Base64Capability;

    fn guid() -> u64 {
        0xd9e3d97519e606e7
    }
}

impl<A, InC> Chunnel<InC> for Base64Chunnel
where
    InC: ChunnelConnection<Data = (A, String)> + Send + Sync + 'static,
    A: Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = Base64Cn<InC>;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        ready(Ok(Base64Cn::from(cn)))
    }
}

#[derive(Debug, Clone)]
pub struct Base64Cn<C> {
    inner: C,
}

impl<C> From<C> for Base64Cn<C> {
    fn from(inner: C) -> Self {
        Base64Cn { inner }
    }
}

impl<A, C> ChunnelConnection for Base64Cn<C>
where
    C: ChunnelConnection<Data = (A, String)> + Send + Sync,
    A: Send + Sync + 'static,
{
    type Data = (A, Vec<u8>);

    fn send<'cn, B>(
        &'cn self,
        burst: B,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'cn>>
    where
        B: IntoIterator<Item = Self::Data> + Send + 'cn,
        <B as IntoIterator>::IntoIter: Send,
    {
        self.inner
            .send(burst.into_iter().map(|(a, d)| (a, base64::encode(d))))
    }

    fn recv<'cn, 'buf>(
        &'cn self,
        msgs_buf: &'buf mut [Option<Self::Data>],
    ) -> Pin<Box<dyn Future<Output = Result<&'buf mut [Option<Self::Data>], Report>> + Send + 'cn>>
    where
        'buf: 'cn,
    {
        Box::pin(
            async move {
                let mut recv_slots: Vec<_> = (0..msgs_buf.len()).map(|_| None).collect();
                let bufs = self.inner.recv(&mut recv_slots).await?;
                trace!(num = ?bufs.len(), "recvd");

                let mut len = 0;
                for (buf, slot) in bufs
                    .into_iter()
                    .map_while(|x| x.take())
                    .zip(msgs_buf.into_iter())
                {
                    let data = base64::decode(&buf.1[..])
                        .wrap_err(eyre!("base64 decode failed: {:?}", buf.1))?;
                    *slot = Some((buf.0, data));
                    len += 1;
                }

                Ok(&mut msgs_buf[..len])
            }
            .instrument(trace_span!("base64_recv")),
        )
    }
}

#[cfg(test)]
mod test {
    use super::SerializeChunnel;
    use crate::chan_transport::Chan;
    use crate::test::Serve;
    use crate::{util::ProjectLeft, Chunnel, ChunnelConnection, ChunnelConnector, ChunnelListener};
    use futures_util::stream::{StreamExt, TryStreamExt};
    use tracing::trace;
    use tracing_error::ErrorLayer;
    use tracing_futures::Instrument;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    async fn send_thingy<
        C: ChunnelConnection<Data = ((), T)>,
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
        snd_ch.send(std::iter::once(((), t))).await.expect("send");
        let mut slots = [None; 1];
        let r = rcv_ch.recv(&mut slots).await.expect("recv");
        let r = r[0].take().unwrap().1;
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
                let stack = SerializeChunnel::default();

                let srv_stack = stack.clone();
                tokio::spawn(async move {
                    let rcv_st = srv.listen(()).await.unwrap();
                    let st = crate::negotiate::negotiate_server(srv_stack, rcv_st)
                        .await
                        .unwrap();
                    st.try_for_each_concurrent(None, |cn| async move {
                        let cn = ProjectLeft::new((), cn);
                        let mut slots = [None, None, None, None];
                        loop {
                            let msgs = cn.recv(&mut slots).await?;
                            cn.send(msgs.into_iter().map_while(|x| x.take())).await?;
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

                cn.send(std::iter::once(obj.clone())).await.unwrap();
                let mut slots = [None; 1];
                let r = cn.recv(&mut slots).await.unwrap();
                assert_eq!(r[0].take().unwrap(), obj);
            }
            .instrument(tracing::info_span!("serialize_negotiate")),
        );
    }
}
