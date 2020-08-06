//! Unix datagram/socket chunnel.

use crate::{ChunnelConnection, ChunnelConnector, ChunnelListener, Endedness, Scope};
use eyre::{eyre, WrapErr};
use futures_util::stream::{Stream, StreamExt};
use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use tokio::net::UnixDatagram;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, trace};
use tracing_futures::Instrument;

/// Unix Chunnel connector.
///
/// Carries no state.
#[derive(Default, Clone, Debug)]
pub struct UnixSkChunnel {}

impl<S> crate::Negotiate<S> for UnixSkChunnel where S: crate::CapabilitySet + crate::NegotiateDummy {}

impl ChunnelListener<(PathBuf, Vec<u8>)> for UnixSkChunnel {
    type Addr = PathBuf;
    type Connection = UnixSk;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = eyre::Report;

    fn listen(&mut self, a: Self::Addr) -> Self::Future {
        Box::pin(async move {
            let sk = UnixDatagram::bind(a.clone()).map(|sk| UnixSk {
                sk: Arc::new(Mutex::new(sk)),
                path: a,
            })?;
            Ok(Box::pin(futures_util::stream::once(async move { Ok(sk) })) as _)
        })
    }

    fn scope() -> Scope {
        Scope::Host
    }

    fn endedness() -> Endedness {
        Endedness::Both
    }

    fn implementation_priority() -> usize {
        1
    }
}

impl ChunnelConnector<(PathBuf, Vec<u8>)> for UnixSkChunnel {
    type Addr = ();
    type Connection = UnixSk;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = eyre::Report;

    fn connect(&mut self, _a: Self::Addr) -> Self::Future {
        Box::pin(async move {
            use rand::Rng;
            let rng = rand::thread_rng();
            let stem: String = rng
                .sample_iter(&rand::distributions::Alphanumeric)
                .take(10)
                .collect();
            let d = std::env::temp_dir();
            let f = d.join(stem);
            let sk = tokio::net::UnixDatagram::bind(&f)?;
            Ok(UnixSk {
                sk: Arc::new(Mutex::new(sk)),
                path: f,
            })
        })
    }

    fn scope() -> Scope {
        Scope::Host
    }

    fn endedness() -> Endedness {
        Endedness::Both
    }

    fn implementation_priority() -> usize {
        1
    }
}

#[derive(Debug, Clone)]
pub struct UnixSk {
    // TODO when new version of tokio is released, use split()
    sk: Arc<Mutex<tokio::net::UnixDatagram>>,
    path: PathBuf,
}

impl ChunnelConnection for UnixSk {
    type Data = (PathBuf, Vec<u8>);

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>> {
        let sk = Arc::clone(&self.sk);
        Box::pin(async move {
            let (addr, data) = data;
            sk.lock().await.send_to(&data, &addr).await?;
            Ok(())
        })
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>> {
        let mut buf = [0u8; 1024];
        let sk = Arc::clone(&self.sk);

        Box::pin(async move {
            let (len, from) = sk.lock().await.recv_from(&mut buf).await?;
            trace!(from_addr = ?&from, "recvd");
            let from = from
                .as_pathname()
                .ok_or_else(|| eyre!("Unix address expected"))?
                .to_path_buf();
            let data = buf[0..len].to_vec();
            Ok((from, data))
        })
    }
}

impl Drop for UnixSk {
    fn drop(&mut self) {
        std::fs::remove_file(&self.path).unwrap_or_else(|_| ());
    }
}

#[derive(Default, Clone, Copy, Debug)]
pub struct UnixReqChunnel {}

impl<S> crate::Negotiate<S> for UnixReqChunnel where S: crate::CapabilitySet + crate::NegotiateDummy {}

impl ChunnelListener<Vec<u8>> for UnixReqChunnel {
    type Addr = PathBuf;
    type Connection = UnixConn;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = eyre::Report;

    fn listen(&mut self, a: Self::Addr) -> Self::Future {
        Box::pin(async move {
            // TODO when new version of tokio is released, use split()
            let sk = std::os::unix::net::UnixDatagram::bind(a).wrap_err("socket bind failed")?;
            let send_sk = Arc::new(Mutex::new(UnixDatagram::from_std(
                sk.try_clone().wrap_err("unix socket clone failed")?,
            )?));
            let sk = UnixDatagram::from_std(sk)?;
            let sends = futures_util::stream::FuturesUnordered::new();
            Ok(Box::pin(futures_util::stream::try_unfold(
                (
                    sk,
                    send_sk,
                    sends,
                    HashMap::<_, mpsc::Sender<Vec<u8>>>::new(),
                ),
                |(mut sk, send_sk, mut sends, mut map)| {
                    async move {
                    let mut buf = [0u8; 1024];
                    loop {
                        tokio::select!(
                            Ok((len, from)) = sk.recv_from(&mut buf) => {
                                let data = buf[0..len].to_vec();
                                let from = from.as_pathname().expect("Unix SocketAddr as_pathname").to_path_buf();

                                let mut done = None;
                                let f = from.clone();
                                let f1 = from.clone();
                                let c = map.entry(f).or_insert_with(|| {
                                    let (sch, rch) = mpsc::channel(100);
                                    debug!(addr = ?&f1, "new unixconn");
                                    done = Some(UnixConn {
                                        resp_addr: f1,
                                        recv: Arc::new(Mutex::new(rch)),
                                        send: Arc::clone(&send_sk),
                                    });

                                    sch
                                });

                                let mut c = c.clone();
                                sends.push(async move {
                                    let res = c.send(data).await;
                                    (from, res)
                                });

                                if let Some(d) = done {
                                    trace!("returning");
                                    return Ok(Some((d, (sk, send_sk, sends,  map))));
                                }
                            }
                            Some((from, res)) = sends.next() => {
                                if let Err(_) = res  {
                                    map.remove(&from);
                                }
                            }
                        )
                    }
                }.instrument(tracing::debug_span!("listen_loop"))
                },
            )) as _)
        })
    }

    fn scope() -> Scope {
        Scope::Host
    }
    fn endedness() -> Endedness {
        Endedness::Both
    }

    fn implementation_priority() -> usize {
        1
    }
}

#[derive(Debug, Clone)]
pub struct UnixConn {
    resp_addr: PathBuf,
    recv: Arc<Mutex<mpsc::Receiver<Vec<u8>>>>,
    send: Arc<Mutex<tokio::net::UnixDatagram>>,
}

impl ChunnelConnection for UnixConn {
    type Data = Vec<u8>;

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>> {
        let sk = Arc::clone(&self.send);
        let addr = self.resp_addr.clone();
        Box::pin(
            async move {
                trace!("sending");
                sk.lock().await.send_to(&data, &addr).await?;
                trace!("sent");
                Ok(())
            }
            .instrument(tracing::debug_span!("send")),
        )
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>> {
        let r = Arc::clone(&self.recv);
        Box::pin(async move {
            let d = r.lock().await.recv().await;
            d.ok_or_else(|| eyre::eyre!("Nothing more to receive"))
        }) as _
    }
}

#[cfg(test)]
mod test {
    use super::{UnixReqChunnel, UnixSkChunnel};
    use crate::{ChunnelConnection, ChunnelConnector, ChunnelListener};
    use futures_util::{StreamExt, TryStreamExt};
    use std::path::PathBuf;
    use tracing::{debug, info};
    use tracing_futures::Instrument;

    #[test]
    fn echo() {
        let _guard = tracing_subscriber::fmt::try_init();
        color_eyre::install().unwrap_or_else(|_| ());

        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_time()
            .enable_io()
            .build()
            .unwrap();

        std::fs::remove_file("./tmp-unix-echo-addr").unwrap_or_else(|_| ());

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

                tokio::spawn(
                    async move {
                        info!("srv starting");
                        loop {
                            let (from, data) = srv
                                .recv()
                                .instrument(tracing::debug_span!("recv"))
                                .await
                                .unwrap();
                            srv.send((from, data))
                                .instrument(tracing::debug_span!("send"))
                                .await
                                .unwrap();
                        }
                    }
                    .instrument(tracing::debug_span!("srv")),
                );

                info!("connecting");
                let cli = UnixSkChunnel::default().connect(()).await.unwrap();

                info!("sending");
                cli.send((addr.clone(), vec![1u8; 12])).await.unwrap();

                info!("receiving");
                let (from, data) = cli.recv().await.unwrap();

                assert_eq!(from, addr);
                assert_eq!(data, vec![1u8; 12]);
            }
            .instrument(tracing::info_span!("unix_echo")),
        );
    }

    #[test]
    fn req_echo() {
        let _guard = tracing_subscriber::fmt::try_init();
        color_eyre::install().unwrap_or_else(|_| ());

        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_time()
            .enable_io()
            .build()
            .unwrap();

        let path = r"./tmp-unix-req-echo-addr";
        std::fs::remove_file(path).unwrap_or_else(|_| ());

        rt.block_on(
            async move {
                let addr = PathBuf::from(path);
                let srv = UnixReqChunnel::default()
                    .listen(addr.clone())
                    .await
                    .unwrap();

                tokio::spawn(
                    async move {
                        srv.try_for_each_concurrent(None, |cn| async move {
                            let m = cn.recv().await?;
                            debug!(msg = ?&m, "got message");
                            cn.send(m).await?;
                            debug!("sent resp");
                            Ok(())
                        })
                        .await
                    }
                    .instrument(tracing::debug_span!("server")),
                );

                info!("connecting");
                let cli = UnixSkChunnel::default().connect(()).await.unwrap();

                info!("sending");
                cli.send((addr.clone(), vec![1u8; 6])).await.unwrap();

                info!("receiving");
                let (from, data) = cli.recv().await.unwrap();

                assert_eq!(from, addr);
                assert_eq!(data, vec![1u8; 6]);
            }
            .instrument(tracing::info_span!("unix_req_echo")),
        );
    }
}
