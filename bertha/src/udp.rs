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
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::{fmt::Debug, net::Ipv4Addr};
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
                .map(move |sk| Ok(UdpSk::new(sk.wrap_err_with(|| eyre!("Bind to {}", a))?)));
            Ok(Box::pin(futures_util::stream::once(sk)) as _)
        })
    }
}

impl ChunnelConnector for UdpSkChunnel {
    type Addr = SocketAddr;
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

    pub fn local_addr(&self) -> Result<SocketAddr, Report> {
        Ok(self.sk.local_addr()?)
    }
}

pub(crate) trait TokioDatagramSk {
    type Addr;
    fn try_recv_from(&self, buf: &mut [u8]) -> std::io::Result<(usize, Self::Addr)>;
}

impl TokioDatagramSk for Arc<tokio::net::UdpSocket> {
    type Addr = SocketAddr;
    fn try_recv_from(&self, buf: &mut [u8]) -> std::io::Result<(usize, Self::Addr)> {
        tokio::net::UdpSocket::try_recv_from(self, buf)
    }
}

pub(crate) enum PeelErr {
    WouldBlock,
    Fatal(Report),
}

pub(crate) fn peel_one<S: TokioDatagramSk, A: From<S::Addr>>(
    sk: &S,
    dst_buf: &mut Option<(A, Vec<u8>)>,
) -> Result<(), PeelErr> {
    let mut buf = [0u8; 2048];
    let read_buf = dst_buf
        .as_mut()
        .map(|(_, x)| &mut x[..])
        .unwrap_or(&mut buf);
    match sk.try_recv_from(&mut read_buf[..]) {
        Ok((n, addr)) => {
            // we re-used the passed-in buffer, so we don't need to allocate. all we need to do is
            // set the address and the buffer length.
            if let Some(ref mut b) = dst_buf.as_mut() {
                b.0 = addr.into();
                b.1.truncate(n);
                Ok(())
            } else {
                *dst_buf = Some((addr.into(), buf[..n].to_vec()));
                Ok(())
            }
        }
        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => Err(PeelErr::WouldBlock),
        Err(e) => Err(PeelErr::Fatal(e.into())),
    }
}

pub(crate) fn peel_rest<S: TokioDatagramSk, A: From<S::Addr>>(
    sk: &S,
    msgs_buf: &mut [Option<(A, Vec<u8>)>],
) -> Result<usize, Report> {
    let mut num_recveived = 0;
    for slot in &mut msgs_buf[..] {
        match peel_one(sk, slot) {
            Ok(_) => num_recveived += 1,
            Err(PeelErr::WouldBlock) => {
                break;
            }
            Err(PeelErr::Fatal(e)) => return Err(e),
        }
    }

    Ok(num_recveived)
}

impl<A> ChunnelConnection for UdpSk<A>
where
    A: Into<SocketAddr> + From<SocketAddr> + Debug + Send + Sync + 'static,
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
        Box::pin(async move {
            let mut sent = 0;
            for (addr, data) in burst {
                let addr = addr.into();
                self.sk.send_to(&data, &addr).await?;
                sent += 1;
            }

            trace!(?sent, "send");
            Ok(())
        })
    }

    fn recv<'cn, 'buf>(
        &'cn self,
        msgs_buf: &'buf mut [Option<Self::Data>],
    ) -> Pin<Box<dyn Future<Output = Result<&'buf mut [Option<Self::Data>], Report>> + Send + 'cn>>
    where
        'buf: 'cn,
    {
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
            trace!(from = ?msgs_buf[0].as_ref().map(|x| &x.0), ?num_recveived, local_addr = ?self.local_addr(), "recv");
            Ok(msgs_buf)
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
            let sk = socket2::Socket::new(
                socket2::Domain::IPV4,
                socket2::Type::DGRAM,
                Some(socket2::Protocol::UDP),
            )?;
            sk.set_reuse_port(true)?;
            sk.set_nonblocking(true)?;
            sk.bind(&a.into())?;
            let sk = tokio::net::UdpSocket::from_std(sk.into())
                .wrap_err_with(|| eyre!("socket bind failed on {:?}", a))?;
            let sk = crate::util::AddrSteer::new(UdpSk::new(sk));
            Ok(sk.steer(UdpConn::new))
        })
    }
}

#[derive(Debug, Clone)]
pub struct UdpConn {
    resp_addr: SocketAddr,
    recv: flume::Receiver<(SocketAddr, Vec<u8>)>,
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
            recv,
            send,
        }
    }

    pub fn peer(&self) -> SocketAddr {
        self.resp_addr
    }

    pub fn inner(&self) -> &UdpSk<SocketAddr> {
        &self.send
    }
}

pub(crate) async fn recv_channel_burst<'cn, 'buf, A>(
    recv: &flume::Receiver<(A, Vec<u8>)>,
    msgs_buf: &'buf mut [Option<(A, Vec<u8>)>],
) -> Result<&'buf mut [Option<(A, Vec<u8>)>], Report>
where
    'buf: 'cn,
{
    let d = recv
        .recv_async()
        .await
        .wrap_err("Nothing more to receive")?;
    msgs_buf[0] = Some(d);

    let mut num_received = 1;
    for slot in &mut msgs_buf[1..] {
        if let Ok(d) = recv.try_recv() {
            *slot = Some(d);
            num_received += 1;
        } else {
            break;
        }
    }

    trace!(?num_received, "recv udp pkts");
    Ok(msgs_buf)
}

impl ChunnelConnection for UdpConn {
    type Data = (SocketAddr, Vec<u8>);

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
                .send(burst.into_iter().map(|(_, data)| (self.resp_addr, data)))
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
        Box::pin(recv_channel_burst(&self.recv, msgs_buf)) as _
    }
}

#[cfg(test)]
mod test {
    use super::{UdpReqChunnel, UdpSkChunnel};
    use crate::test::COLOR_EYRE;
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
        COLOR_EYRE.call_once(|| color_eyre::install().unwrap_or(()));

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                let addr = "127.0.0.1:35133".to_socket_addrs().unwrap().next().unwrap();
                let srv = UdpSkChunnel
                    .listen(addr)
                    .await
                    .unwrap()
                    .next()
                    .await
                    .unwrap()
                    .unwrap();

                let cli = UdpSkChunnel.connect(addr).await.unwrap();

                tokio::spawn(async move {
                    let mut recv_slots = [None, None];
                    loop {
                        let msgs = srv.recv(&mut recv_slots).await.unwrap();
                        srv.send(msgs.iter_mut().map_while(Option::take))
                            .await
                            .unwrap();
                    }
                });

                cli.send(std::iter::once((addr, vec![1u8; 12])))
                    .await
                    .unwrap();
                let mut recv_slots = [None, None];
                let msgs = cli.recv(&mut recv_slots).await.unwrap();
                let (from, data) = msgs[0].take().unwrap();
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
        COLOR_EYRE.call_once(|| color_eyre::install().unwrap_or(()));

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                let addr = "127.0.0.1:35134".to_socket_addrs().unwrap().next().unwrap();

                tokio::spawn(async move {
                    let srv = UdpReqChunnel.listen(addr).await.unwrap();
                    srv.try_for_each_concurrent(None, |cn| async move {
                        let mut recv_slots = [None, None];
                        let msgs = cn.recv(&mut recv_slots).await?;
                        cn.send(msgs.iter_mut().map_while(Option::take)).await?;
                        Ok(())
                    })
                    .await
                    .unwrap();
                });

                let cli = UdpSkChunnel.connect(addr).await.unwrap();
                cli.send(std::iter::once((addr, vec![1u8; 12])))
                    .await
                    .unwrap();
                let mut recv_slots = [None, None];
                let recvd = cli.recv(&mut recv_slots).await.unwrap();
                let (from, data) = recvd[0].take().unwrap();
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
        COLOR_EYRE.call_once(|| color_eyre::install().unwrap_or(()));

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                let addr = "127.0.0.1:35184".to_socket_addrs().unwrap().next().unwrap();

                tokio::spawn(async move {
                    let srv = UdpReqChunnel.listen(addr).await.unwrap();
                    srv.try_for_each_concurrent(None, |cn| async move {
                        let mut recv_slots = [None, None];
                        loop {
                            let data = cn.recv(&mut recv_slots).await?;
                            cn.send(data.iter_mut().map_while(Option::take)).await?;
                        }
                    })
                    .instrument(tracing::info_span!("echo-srv"))
                    .await
                    .unwrap();
                });

                let cli1 = UdpSkChunnel.connect(addr).await.unwrap();
                let cli2 = UdpSkChunnel.connect(addr).await.unwrap();

                for i in 0..10 {
                    cli1.send(std::iter::once((addr, vec![i; 12])))
                        .await
                        .unwrap();
                    cli2.send(std::iter::once((addr, vec![i + 1; 12])))
                        .await
                        .unwrap();

                    let mut recv_slots = [None, None];
                    let one = cli1.recv(&mut recv_slots).await.unwrap();
                    let (from1, data1) = one[0].take().unwrap();
                    assert_eq!(from1, addr);
                    assert_eq!(data1, vec![i; 12]);
                    let two = cli2.recv(&mut recv_slots).await.unwrap();
                    let (from2, data2) = two[0].take().unwrap();
                    assert_eq!(from2, addr);
                    assert_eq!(data2, vec![i + 1; 12]);
                }
            }
            .instrument(tracing::info_span!("udp::rendezvous_multiclient")),
        );
    }
}
