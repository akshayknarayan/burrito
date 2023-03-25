//! Unix datagram/socket chunnel.

use crate::{ChunnelConnection, ChunnelConnector, ChunnelListener};
use color_eyre::eyre::{eyre, Report, WrapErr};
use futures_util::stream::Stream;
use std::fmt::Debug;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use tracing::{trace, trace_span};
use tracing_futures::Instrument;

/// UDP Chunnel connector.
///
/// Carries no state.
#[derive(Clone, Debug)]
pub struct UnixSkChunnel {
    root: PathBuf,
}

impl UnixSkChunnel {
    pub fn with_root(root: PathBuf) -> Self {
        Self { root }
    }
}

impl Default for UnixSkChunnel {
    fn default() -> Self {
        Self {
            root: std::env::temp_dir(),
        }
    }
}

impl ChunnelListener for UnixSkChunnel {
    type Addr = PathBuf;
    type Connection = UnixSk;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Report;

    fn listen(&mut self, a: Self::Addr) -> Self::Future {
        Box::pin(async move {
            std::fs::remove_file(a.as_path()).unwrap_or(());
            let sk = tokio::net::UnixDatagram::bind(a)?;
            Ok(
                Box::pin(futures_util::stream::once(futures_util::future::ready(Ok(
                    UnixSk::new(sk),
                )))) as _,
            )
        })
    }
}

impl ChunnelConnector for UnixSkChunnel {
    type Addr = ();
    type Connection = UnixSk;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Report;

    fn connect(&mut self, _a: Self::Addr) -> Self::Future {
        let d = self.root.clone();
        Box::pin(async move {
            use rand::distributions::{Alphanumeric, DistString};
            let mut rng = rand::thread_rng();
            let stem: String = Alphanumeric.sample_string(&mut rng, 10);
            let f = d.join(stem);
            let sk = tokio::net::UnixDatagram::bind(f)?;
            Ok(UnixSk::new(sk))
        })
    }
}

#[derive(Debug, Clone)]
pub struct UnixSk {
    sk: Arc<tokio::net::UnixDatagram>,
}

impl UnixSk {
    fn new(sk: tokio::net::UnixDatagram) -> Self {
        Self { sk: Arc::new(sk) }
    }
}

impl super::udp::TokioDatagramSk for Arc<tokio::net::UnixDatagram> {
    type Addr = PathBuf;
    fn try_recv_from(&self, buf: &mut [u8]) -> std::io::Result<(usize, Self::Addr)> {
        let (sz, from) = tokio::net::UnixDatagram::try_recv_from(self, buf)?;
        Ok((
            sz,
            from.as_pathname()
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::AddrNotAvailable,
                        "received from unnamed socket",
                    )
                })?
                .to_path_buf(),
        ))
    }
}

impl ChunnelConnection for UnixSk {
    type Data = (PathBuf, Vec<u8>);

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
                let mut sent = 0;
                for (addr, data) in burst {
                    self.sk
                        .send_to(&data, &addr)
                        .await
                        .wrap_err_with(|| eyre!("unixsk send to address: {:?}", &addr))?;
                    sent += 1;
                }

                trace!(?sent, "send");
                Ok(())
            }
            .instrument(trace_span!("uds_send")),
        )
    }

    fn recv<'cn, 'buf>(
        &'cn self,
        msgs_buf: &'buf mut [Option<Self::Data>],
    ) -> Pin<Box<dyn Future<Output = Result<&'buf mut [Option<Self::Data>], Report>> + Send + 'cn>>
    where
        'buf: 'cn,
    {
        use super::udp::{peel_one, peel_rest, PeelErr};

        // we could block repeatedly until the array is filled, but we will not do that. instead we
        // will only return what is immediately available.
        Box::pin(async move {
            let mut num_recveived = 0;
            loop {
                self.sk.readable().await?;

                // at least one has to be readable. after that, don't try to wait for further
                // readability and instead return what we have now.
                match peel_one(&self.sk, &mut msgs_buf[0]) {
                    Ok(_) => (),
                    Err(PeelErr::WouldBlock) => continue,
                    Err(PeelErr::Fatal(e)) => return Err(e),
                }

                num_recveived += 1;
                break;
            }

            num_recveived += peel_rest(&self.sk, &mut msgs_buf[1..])?;
            trace!(?num_recveived, "recv");
            Ok(msgs_buf)
        })
    }
}

#[derive(Default, Clone, Copy, Debug)]
pub struct UnixReqChunnel;

impl ChunnelListener for UnixReqChunnel {
    type Addr = PathBuf;
    type Connection = UnixConn;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Report;

    fn listen(&mut self, a: Self::Addr) -> Self::Future {
        Box::pin(async move {
            let sk = tokio::net::UnixDatagram::bind(a).wrap_err("socket bind failed")?;
            let sk = crate::util::AddrSteer::new(UnixSk::new(sk));
            Ok(sk.steer(UnixConn::new))
        })
    }
}

#[derive(Debug, Clone)]
pub struct UnixConn {
    resp_addr: PathBuf,
    recv: flume::Receiver<(PathBuf, Vec<u8>)>,
    send: UnixSk,
}

impl UnixConn {
    fn new(resp_addr: PathBuf, send: UnixSk, recv: flume::Receiver<(PathBuf, Vec<u8>)>) -> Self {
        UnixConn {
            resp_addr,
            recv,
            send,
        }
    }
}

impl ChunnelConnection for UnixConn {
    type Data = (PathBuf, Vec<u8>);

    fn send<'cn, B>(
        &'cn self,
        burst: B,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'cn>>
    where
        B: IntoIterator<Item = Self::Data> + Send + 'cn,
        <B as IntoIterator>::IntoIter: Send,
    {
        Box::pin(async move {
            self.send
                .send(
                    burst
                        .into_iter()
                        .map(|(_, data)| (self.resp_addr.clone(), data)),
                )
                .await
        })
    }

    fn recv<'cn, 'buf>(
        &'cn self,
        msgs_buf: &'buf mut [Option<Self::Data>],
    ) -> Pin<Box<dyn Future<Output = Result<&'buf mut [Option<Self::Data>], Report>> + Send + 'cn>>
    where
        'buf: 'cn,
    {
        Box::pin(super::udp::recv_channel_burst(&self.recv, msgs_buf)) as _
    }
}

#[cfg(test)]
mod test {
    use super::{UnixReqChunnel, UnixSkChunnel};
    use crate::test::COLOR_EYRE;
    use crate::{ChunnelConnection, ChunnelConnector, ChunnelListener};
    use futures_util::{StreamExt, TryStreamExt};
    use std::path::PathBuf;
    use tracing_error::ErrorLayer;
    use tracing_futures::Instrument;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    #[test]
    fn echo() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        COLOR_EYRE.call_once(|| color_eyre::install().unwrap_or(()));

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .unwrap();

        std::fs::remove_file("./tmp-unix-echo-addr").unwrap_or(());

        rt.block_on(
            async move {
                let addr = PathBuf::from(r"./tmp-unix-echo-addr");
                let srv = UnixSkChunnel::default()
                    .listen(addr.clone())
                    .await
                    .unwrap()
                    .next()
                    .await
                    .unwrap()
                    .unwrap();

                let cli = UnixSkChunnel::default().connect(()).await.unwrap();

                tokio::spawn(async move {
                    let mut recv_slots = [None, None];
                    loop {
                        let msgs = srv.recv(&mut recv_slots).await.unwrap();
                        srv.send(msgs.iter_mut().map_while(Option::take))
                            .await
                            .unwrap();
                    }
                });

                cli.send(std::iter::once((addr.clone(), vec![1u8; 12])))
                    .await
                    .unwrap();
                let mut recv_slots = [None, None];
                let msgs = cli.recv(&mut recv_slots).await.unwrap();
                let (from, data) = msgs[0].take().unwrap();
                assert_eq!(from, addr);
                assert_eq!(data, vec![1u8; 12]);
            }
            .instrument(tracing::info_span!("unix::echo")),
        );
    }

    #[test]
    fn rendezvous() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        COLOR_EYRE.call_once(|| color_eyre::install().unwrap_or(()));

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .unwrap();

        let path = r"./tmp-unix-req-echo-addr";
        std::fs::remove_file(path).unwrap_or(());

        rt.block_on(
            async move {
                let addr = PathBuf::from(path);
                let saddr = addr.clone();
                tokio::spawn(async move {
                    let srv = UnixReqChunnel::default().listen(saddr).await.unwrap();
                    srv.try_for_each_concurrent(None, |cn| async move {
                        let mut recv_slots = [None, None];
                        let data = cn.recv(&mut recv_slots).await?;
                        cn.send(data.into_iter().map_while(Option::take)).await?;
                        Ok(())
                    })
                    .await
                    .unwrap();
                });

                let cli = UnixSkChunnel::default().connect(()).await.unwrap();
                cli.send(std::iter::once((addr.clone(), vec![1u8; 12])))
                    .await
                    .unwrap();
                let mut recv_slots = [None];
                let recvs = cli.recv(&mut recv_slots).await.unwrap();
                let (from, data) = recvs[0].take().unwrap();
                assert_eq!(from, addr);
                assert_eq!(data, vec![1u8; 12]);
            }
            .instrument(tracing::info_span!("udp::rendezvous")),
        );
    }
}
