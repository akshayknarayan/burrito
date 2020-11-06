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
use futures_util::{
    future::FutureExt,
    stream::{Stream, StreamExt},
};
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
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
            let sk = tokio::net::UdpSocket::bind(a).map(|sk| {
                let (recv, send) = sk?.split();
                Ok(UdpSk::new(send, recv))
            });
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
            let (recv, send) = sk.split();
            Ok(UdpSk::new(send, recv))
        })
    }
}

#[derive(Debug, Clone)]
pub struct UdpSk<A> {
    send: Arc<Mutex<tokio::net::udp::SendHalf>>,
    recv: Arc<Mutex<tokio::net::udp::RecvHalf>>,
    _phantom: std::marker::PhantomData<A>,
}

impl<A> UdpSk<A> {
    fn new(send: tokio::net::udp::SendHalf, recv: tokio::net::udp::RecvHalf) -> Self {
        Self {
            send: Arc::new(Mutex::new(send)),
            recv: Arc::new(Mutex::new(recv)),
            _phantom: Default::default(),
        }
    }
}

impl<A> ChunnelConnection for UdpSk<A>
where
    A: Into<SocketAddr> + From<SocketAddr> + Send + 'static,
{
    type Data = (A, Vec<u8>);

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        let sk = Arc::clone(&self.send);
        Box::pin(async move {
            let (addr, data) = data;
            let addr = addr.into();
            sk.lock().await.send_to(&data, &addr).await?;
            Ok(())
        })
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        let mut buf = [0u8; 1024];
        let sk = Arc::clone(&self.recv);

        Box::pin(async move {
            let (len, from) = sk.lock().await.recv_from(&mut buf).await?;
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
                .wrap_err("socket bind failed")?;

            let (recv, send) = sk.split();
            let sends = futures_util::stream::FuturesUnordered::new();
            Ok(Box::pin(futures_util::stream::try_unfold(
                (
                    recv,
                    Arc::new(Mutex::new(send)),
                    sends,
                    HashMap::<_, mpsc::Sender<(SocketAddr, Vec<u8>)>>::new(),
                ),
                |(mut r, s, mut sends, mut map)| async move {
                    let mut buf = [0u8; 1024];
                    loop {
                        // careful: potential deadlocks since .recv on returned connection blocks
                        // on .listen
                        tokio::select!(
                            Some((from, res)) = sends.next() => {
                                trace!(from = ?&from, "channel send completed");
                                if let Err(_) = res  {
                                    map.remove(&from);
                                }
                            }
                            Ok((len, from)) = r.recv_from(&mut buf) => {
                                let data = buf[0..len].to_vec();

                                let mut done = None;
                                let c = map.entry(from).and_modify(|_| {
                                    trace!(from = ?&from, pending_sends = sends.len(), "received pkt");
                                })
                                .or_insert_with(|| {
                                    trace!(from = ?&from, "new connection");
                                    let (sch, rch) = mpsc::channel(1);
                                    done = Some(UdpConn {
                                        resp_addr: from,
                                        recv: Arc::new(Mutex::new(rch)),
                                        send: Arc::clone(&s),
                                    });

                                    sch
                                });

                                let mut c = c.clone();
                                sends.push(async move {
                                    let res = c.send((from, data)).await;
                                    (from, res)
                                });

                                if let Some(d) = done {
                                    return Ok(Some((d, (r, s, sends,  map))));
                                }
                            }
                        )
                    }
                },
            )) as _)
        })
    }
}

#[derive(Debug, Clone)]
pub struct UdpConn {
    resp_addr: SocketAddr,
    recv: Arc<Mutex<mpsc::Receiver<(SocketAddr, Vec<u8>)>>>,
    send: Arc<Mutex<tokio::net::udp::SendHalf>>,
}

impl ChunnelConnection for UdpConn {
    type Data = (SocketAddr, Vec<u8>);

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        let sk = Arc::clone(&self.send);
        let addr = self.resp_addr;
        let (_, data) = data;
        Box::pin(async move {
            sk.lock().await.send_to(&data, &addr).await?;
            Ok(())
        })
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        let r = Arc::clone(&self.recv);
        Box::pin(async move {
            let d = r.lock().await.recv().await;
            trace!(from = ?&d.as_ref().map(|x| x.0), "got pkt");
            d.ok_or_else(|| eyre!("Nothing more to receive"))
        }) as _
    }
}

#[cfg(test)]
mod test {
    use super::{UdpReqChunnel, UdpSkChunnel};
    use crate::{ChunnelConnection, ChunnelConnector, ChunnelListener};
    use futures_util::{StreamExt, TryStreamExt};
    use std::net::{SocketAddr, ToSocketAddrs};
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
        color_eyre::install().unwrap_or_else(|_| ());

        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_time()
            .enable_io()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                let addr = "127.0.0.1:35133".to_socket_addrs().unwrap().next().unwrap();
                let srv = UdpSkChunnel::default()
                    .listen(addr.into())
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

                cli.send((addr.into(), vec![1u8; 12])).await.unwrap();
                let (from, data) = cli.recv().await.unwrap();

                let from: SocketAddr = from.into();
                let addr: SocketAddr = addr.into();
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
        color_eyre::install().unwrap_or_else(|_| ());

        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
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
                cli.send((addr.into(), vec![1u8; 12])).await.unwrap();
                let (from, data) = cli.recv().await.unwrap();

                let from: SocketAddr = from.into();
                let addr: SocketAddr = addr.into();
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
        color_eyre::install().unwrap_or_else(|_| ());

        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
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
                    cli1.send((addr.into(), vec![i as u8; 12])).await.unwrap();

                    cli2.send((addr.into(), vec![i + 1 as u8; 12]))
                        .await
                        .unwrap();

                    let (from1, data1) = cli1.recv().await.unwrap();
                    let (from2, data2) = cli2.recv().await.unwrap();

                    let addr: SocketAddr = addr.into();
                    let from1: SocketAddr = from1.into();
                    let from2: SocketAddr = from2.into();
                    assert_eq!(from1, addr);
                    assert_eq!(data1, vec![i as u8; 12]);
                    assert_eq!(from2, addr);
                    assert_eq!(data2, vec![i + 1 as u8; 12]);
                }
            }
            .instrument(tracing::info_span!("udp::rendezvous_multiclient")),
        );
    }
}
