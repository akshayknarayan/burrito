//! UDP chunnel.

use crate::{ChunnelConnection, ChunnelConnector, ChunnelListener, Endedness, Scope};
use futures_util::{future::FutureExt, stream::Stream};
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;

/// UDP Chunnel connector.
///
/// Carries no state.
#[derive(Default, Clone, Debug)]
pub struct UdpChunnel {}

impl ChunnelListener for UdpChunnel {
    type Addr = SocketAddr;
    type Connection = UdpConn;

    fn listen(
        &mut self,
        a: Self::Addr,
    ) -> Pin<
        Box<
            dyn Future<
                Output = Pin<Box<dyn Stream<Item = Result<Self::Connection, eyre::Report>>>>,
            >,
        >,
    > {
        Box::pin(async move {
            let sk = tokio::net::UdpSocket::bind(a).map(|sk| {
                let (recv, send) = sk?.split();
                Ok(UdpConn {
                    send: Arc::new(Mutex::new(send)),
                    recv: Arc::new(Mutex::new(recv)),
                })
            });
            Box::pin(futures_util::stream::once(sk)) as _
        })
    }

    fn scope(&self) -> Scope {
        Scope::Host
    }
    fn endedness(&self) -> Endedness {
        Endedness::Both
    }

    fn implementation_priority(&self) -> usize {
        1
    }
}

impl ChunnelConnector for UdpChunnel {
    type Addr = SocketAddr;
    type Connection = UdpConn;

    fn connect(
        &mut self,
        _a: Self::Addr,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Connection, eyre::Report>>>> {
        Box::pin(async move {
            use std::net::ToSocketAddrs;
            let (recv, send) = tokio::net::UdpSocket::bind(
                ("0.0.0.0:0").to_socket_addrs().unwrap().next().unwrap(),
            )
            .await
            .unwrap()
            .split();
            Ok(UdpConn {
                send: Arc::new(Mutex::new(send)),
                recv: Arc::new(Mutex::new(recv)),
            })
        })
    }

    fn scope(&self) -> Scope {
        Scope::Host
    }
    fn endedness(&self) -> Endedness {
        Endedness::Both
    }

    fn implementation_priority(&self) -> usize {
        1
    }
}

pub struct UdpConn {
    send: Arc<Mutex<tokio::net::udp::SendHalf>>,
    recv: Arc<Mutex<tokio::net::udp::RecvHalf>>,
}

impl ChunnelConnection for UdpConn {
    type Data = (SocketAddr, Vec<u8>);

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + Sync>> {
        let sk = Arc::clone(&self.send);
        Box::pin(async move {
            let (addr, data) = data;
            sk.lock().await.send_to(&data, &addr).await?;
            Ok(())
        })
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + Sync>> {
        let mut buf = [0u8; 1024];
        let sk = Arc::clone(&self.recv);

        Box::pin(async move {
            let (len, from) = sk.lock().await.recv_from(&mut buf).await?;
            let data = buf[0..len].to_vec();
            Ok((from, data))
        })
    }
}

#[cfg(test)]
mod test {
    use super::UdpChunnel;
    use crate::{ChunnelConnection, ChunnelConnector, ChunnelListener};
    use futures_util::StreamExt;
    use std::net::ToSocketAddrs;
    use tracing_futures::Instrument;

    #[test]
    fn echo() {
        let _guard = tracing_subscriber::fmt::try_init();

        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_time()
            .enable_io()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                let addr = "127.0.0.1:35133".to_socket_addrs().unwrap().next().unwrap();
                let srv = UdpChunnel::default()
                    .listen(addr)
                    .await
                    .next()
                    .await
                    .unwrap()
                    .unwrap();

                let cli = UdpChunnel::default().connect(addr).await.unwrap();

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
}
