//! UDP chunnel.
//!
//! UDP chunnels are interesting because they involve a piece of metadata, the recv_from addr to
//! send a response to (in the case of e.g. sharding), which should be remembered.
//! There are two possible solutions, both implemented here.
//!
//! `UdpSkChunnel` exposes `Data = (SocketAddr, Vec<u8>)`. The address is considered part of the
//! data, and the connection type `UdpSk` has full generality.
//!
//! `UdpReqChunnel` exposes `Data = Vec<u8>`. `listen()` returns a bound connection such that
//! further `recv()`s will only be from the same address, and further sends will send to the same
//! address as the original recv_from.

use crate::{ChunnelConnection, ChunnelConnector, ChunnelListener};
use color_eyre::eyre::{eyre, Report, WrapErr};
use futures_util::{future::FutureExt, stream::Stream};
use std::fmt::Debug;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use tracing::{debug, trace};

/// UDP Chunnel connector.
///
/// Carries no state.
#[derive(Default, Clone, Debug)]
pub struct UdpSkChunnel;

impl ChunnelListener for UdpSkChunnel {
    type Addr = SocketAddr;
    type Connection = UdpSk<SocketAddr>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Report;

    fn listen(&mut self, a: Self::Addr) -> Self::Future {
        Box::pin(async move {
            let sk = tokio::net::UdpSocket::bind(a)
                .map(move |sk| Ok(UdpSk::new(sk.wrap_err(eyre!("Bind to {}", a))?)));
            Ok(Box::pin(futures_util::stream::once(sk)) as _)
        })
    }
}

impl ChunnelConnector for UdpSkChunnel {
    type Addr = ();
    type Connection = UdpSk<SocketAddr>;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Report;

    fn connect(&mut self, _a: Self::Addr) -> Self::Future {
        Box::pin(async move {
            use std::net::ToSocketAddrs;
            let sk = tokio::net::UdpSocket::bind(
                ("0.0.0.0:0").to_socket_addrs().unwrap().next().unwrap(),
            )
            .await
            .unwrap();
            let local_addr = sk.local_addr()?;
            debug!(?local_addr, "Bound to udp address");
            Ok(UdpSk::new(sk))
        })
    }
}

#[derive(Debug, Clone)]
pub struct UdpSk<A> {
    sk: Arc<tokio::net::UdpSocket>,
    _phantom: std::marker::PhantomData<A>,
}

impl<A> UdpSk<A> {
    fn new(sk: tokio::net::UdpSocket) -> Self {
        Self {
            sk: Arc::new(sk),
            _phantom: Default::default(),
        }
    }
}

impl<A> ChunnelConnection for UdpSk<A>
where
    A: Into<SocketAddr> + From<SocketAddr> + Debug + Send + 'static,
{
    type Data = (A, Vec<u8>);

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        let sk = Arc::clone(&self.sk);
        Box::pin(async move {
            let (addr, data) = data;
            trace!(to = ?&addr, "send");
            let addr = addr.into();
            sk.send_to(&data, &addr).await?;
            Ok(())
        })
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        let mut buf = [0u8; 2048];
        let sk = Arc::clone(&self.sk);

        Box::pin(async move {
            let (len, from) = sk.recv_from(&mut buf).await?;
            trace!(from = ?&from, "recv");
            let data = buf[0..len].to_vec();
            Ok((from.into(), data))
        })
    }
}

#[derive(Default, Clone, Copy, Debug)]
pub struct UdpReqChunnel;

impl ChunnelListener for UdpReqChunnel {
    type Addr = SocketAddr;
    type Connection = UdpConn;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Report;

    fn listen(&mut self, a: Self::Addr) -> Self::Future {
        Box::pin(async move {
            let sk = tokio::net::UdpSocket::bind(a)
                .await
                .wrap_err(eyre!("socket bind failed on {:?}", a))?;
            let sk = crate::util::AddrSteer::new(UdpSk::new(sk));
            Ok(sk.steer(UdpConn::new))
        })
    }
}

#[derive(Debug, Clone)]
pub struct UdpConn {
    resp_addr: SocketAddr,
    recv: Arc<flume::Receiver<(SocketAddr, Vec<u8>)>>,
    send: UdpSk<SocketAddr>,
}

impl UdpConn {
    fn new(
        resp_addr: SocketAddr,
        send: UdpSk<SocketAddr>,
        recv: flume::Receiver<(SocketAddr, Vec<u8>)>,
    ) -> Self {
        UdpConn {
            resp_addr,
            recv: Arc::new(recv),
            send,
        }
    }
}

impl ChunnelConnection for UdpConn {
    type Data = (SocketAddr, Vec<u8>);

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        let addr = self.resp_addr;
        let (_, data) = data;
        self.send.send((addr, data))
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        let r = Arc::clone(&self.recv);
        Box::pin(async move {
            let d = r.recv_async().await;
            trace!(from = ?&d.as_ref().map(|x| x.0), "recv pkt");
            d.wrap_err(eyre!("Nothing more to receive"))
        }) as _
    }
}

#[cfg(test)]
mod test {
    use super::{UdpReqChunnel, UdpSkChunnel};
    use crate::{ChunnelConnection, ChunnelConnector, ChunnelListener};
    use futures_util::{StreamExt, TryStreamExt};
    use std::net::ToSocketAddrs;
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
        color_eyre::install().unwrap_or(());

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                let addr = "127.0.0.1:35133".to_socket_addrs().unwrap().next().unwrap();
                let srv = UdpSkChunnel::default()
                    .listen(addr)
                    .await
                    .unwrap()
                    .next()
                    .await
                    .unwrap()
                    .unwrap();

                let cli = UdpSkChunnel::default().connect(()).await.unwrap();

                tokio::spawn(async move {
                    loop {
                        let (from, data) = srv.recv().await.unwrap();
                        srv.send((from, data)).await.unwrap();
                    }
                });

                cli.send((addr, vec![1u8; 12])).await.unwrap();
                let (from, data) = cli.recv().await.unwrap();
                assert_eq!(from, addr);
                assert_eq!(data, vec![1u8; 12]);
            }
            .instrument(tracing::info_span!("udp::echo")),
        );
    }

    #[test]
    fn rendezvous() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        color_eyre::install().unwrap_or(());

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                let addr = "127.0.0.1:35134".to_socket_addrs().unwrap().next().unwrap();

                tokio::spawn(async move {
                    let srv = UdpReqChunnel::default().listen(addr).await.unwrap();
                    srv.try_for_each_concurrent(None, |cn| async move {
                        let data = cn.recv().await?;
                        cn.send(data).await?;
                        Ok(())
                    })
                    .await
                    .unwrap();
                });

                let cli = UdpSkChunnel::default().connect(()).await.unwrap();
                cli.send((addr, vec![1u8; 12])).await.unwrap();
                let (from, data) = cli.recv().await.unwrap();
                assert_eq!(from, addr);
                assert_eq!(data, vec![1u8; 12]);
            }
            .instrument(tracing::info_span!("udp::rendezvous")),
        );
    }

    #[test]
    fn rendezvous_multiclient() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        color_eyre::install().unwrap_or(());

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                let addr = "127.0.0.1:35184".to_socket_addrs().unwrap().next().unwrap();

                tokio::spawn(async move {
                    let srv = UdpReqChunnel::default().listen(addr).await.unwrap();
                    srv.try_for_each_concurrent(None, |cn| async move {
                        loop {
                            let data = cn.recv().await?;
                            cn.send(data).await?;
                        }
                    })
                    .instrument(tracing::info_span!("echo-srv"))
                    .await
                    .unwrap();
                });

                let cli1 = UdpSkChunnel::default().connect(()).await.unwrap();
                let cli2 = UdpSkChunnel::default().connect(()).await.unwrap();

                for i in 0..10 {
                    cli1.send((addr, vec![i as u8; 12])).await.unwrap();
                    cli2.send((addr, vec![i + 1; 12])).await.unwrap();

                    let (from1, data1) = cli1.recv().await.unwrap();
                    let (from2, data2) = cli2.recv().await.unwrap();

                    assert_eq!(from1, addr);
                    assert_eq!(data1, vec![i as u8; 12]);
                    assert_eq!(from2, addr);
                    assert_eq!(data2, vec![i + 1; 12]);
                }
            }
            .instrument(tracing::info_span!("udp::rendezvous_multiclient")),
        );
    }
}
