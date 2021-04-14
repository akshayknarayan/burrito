//! TLS encryption tunnel.
//!
//! Internally spawns and maintains ghostunnel client and server processes. Have to use stream
//! sockets, so handles that too.

// Pin<Box<...>> is necessary and not worth breaking up
#![allow(clippy::type_complexity)]

use bertha::{Chunnel, ChunnelConnection, Negotiate};
use color_eyre::eyre::{ensure, eyre, Report, WrapErr};
use futures_util::future::{ready, Ready};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::{future::Future, pin::Pin};
use tokio::{
    net::{UnixListener, UnixStream},
    sync::Mutex,
};
use tracing::{debug, trace, trace_span};
use tracing_futures::Instrument;

mod ghostunnel;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct Encryption;

impl bertha::negotiate::CapabilitySet for Encryption {
    fn guid() -> u64 {
        0xb4bf3b3e0fa957df
    }

    fn universe() -> Option<Vec<Self>> {
        // return None to force both sides to match
        None
    }
}

#[derive(Debug)]
pub struct TLSChunnel<A = TlsConnAddr> {
    unix_root: PathBuf,
    listen: Option<SocketAddr>,
    remote: Option<SocketAddr>,
    binary_path: PathBuf,
    certs_location: PathBuf,
    listen_cn: Arc<StdMutex<Option<Result<(UnixListener, ghostunnel::GhostTunnel), Report>>>>,
    send_cn: Arc<StdMutex<Option<Result<(UnixStream, ghostunnel::GhostTunnel), Report>>>>,
    _phantom: std::marker::PhantomData<A>,
}

impl<A> Clone for TLSChunnel<A> {
    fn clone(&self) -> Self {
        Self {
            unix_root: self.unix_root.clone(),
            binary_path: self.binary_path.clone(),
            certs_location: self.certs_location.clone(),
            listen_cn: Arc::new(StdMutex::new(None)),
            send_cn: Arc::new(StdMutex::new(None)),
            ..*self
        }
    }
}

impl<A> Negotiate for TLSChunnel<A>
where
    A: Send,
{
    type Capability = Encryption;

    fn guid() -> u64 {
        0x967d89ae5d240be3
    }

    fn capabilities() -> Vec<Self::Capability> {
        vec![Encryption]
    }

    fn picked<'s>(&mut self, _nonce: &'s [u8]) -> Pin<Box<dyn Future<Output = ()> + Send + 's>> {
        let root = self.unix_root.clone();
        // generate unix local addresses corresponding to the listen-addr and remote-addr for
        // ghostunnel
        use rand::Rng;
        let rng = rand::thread_rng();
        let stem: String = rng
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(10)
            .collect();

        let listen = self.listen.map(|srv| {
            debug!("enabling listen");
            let mut srv_unix_name = stem.clone();
            srv_unix_name.push_str("-srv");
            let mut srv_unix = root.clone();
            srv_unix.push(srv_unix_name);
            let gt = ghostunnel::GhostTunnel::start_server(
                srv,
                &srv_unix,
                &self.binary_path,
                &self.certs_location,
            )
            .wrap_err("listen-side tunnel process");
            let ul = UnixListener::bind(srv_unix).wrap_err("bind unix listener");
            ( Arc::clone(&self.listen_cn),
            async move {
                let mut gt = gt?;
                let ul = ul?;
                let try_conn_addr = if srv.ip().is_unspecified() {
                    let mut addr = srv;
                    addr.set_ip("127.0.0.1".parse().unwrap());
                    addr
                } else {
                    srv
                };

                let start = std::time::Instant::now();
                let mut tries = 0usize;
                trace!("waiting for external addr to come up");
                //ensure!(gt.wait_up(), "ghostunnel did not start listening");
                loop {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    if let Err(e) = tokio::net::TcpStream::connect(try_conn_addr).await {
                        if start.elapsed() > std::time::Duration::from_millis(1000) {
                            let ctx = eyre!(
                                "can't connect to {:?} after {:?} tries ({:?})",
                                try_conn_addr,
                                tries,
                                start.elapsed(),
                            );
                            return Err(Report::from(e).wrap_err(ctx));
                        } else {
                            tries += 1;
                            trace!(addr = ?&try_conn_addr, ?tries, err = %format!("{:#}", e), "failed connection");
                        }
                    } else { break; }
                }

                debug!(addr = ?try_conn_addr, elapsed = ?start.elapsed(), "remote is up");
                Ok((ul, gt))
            })
        });

        let send = self.remote.map(|rem| {
            let send_cn = Arc::clone(&self.send_cn);
            let mut cli_unix_name = stem.clone();
            cli_unix_name.push_str("-cli");
            let mut cli_unix = root.clone();
            cli_unix.push(cli_unix_name);
            let gt = ghostunnel::GhostTunnel::start_client(
                rem,
                &cli_unix,
                &self.binary_path,
                &self.certs_location,
            ).wrap_err("client-side tunnel process");
            (send_cn, async move {
                let mut gt = gt?;
                debug!(tunnel_entry = ?&cli_unix, "connecting to remote");
                //ensure!(gt.wait_up(), "ghostunnel did not start listening");
                // retry loop until the ghostunnel process starts listening
                let start = std::time::Instant::now();
                let mut tries = 0usize;
                let uc = loop {
                    match UnixStream::connect(&cli_unix).await {
                        Ok(uc) => break uc,
                        Err(e) => {
                            if start.elapsed() > std::time::Duration::from_millis(1000) {
                                let ctx = eyre!(
                                    "can't connect unix socket to {:?} after {:?} tries ({:?})",
                                    cli_unix,
                                    tries,
                                    start.elapsed(),
                                );
                                return Err(Report::from(e).wrap_err(ctx));
                            } else {
                                tries += 1;
                                trace!(addr = ?&cli_unix, ?tries, err = %format!("{:#}", e), "failed connection");
                                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                            }
                        }
                    }
                };

                let start = std::time::Instant::now();
                let mut tries = 0usize;
                loop {
                    if let Err(e) = tokio::net::TcpStream::connect(rem).await {
                        if start.elapsed() > std::time::Duration::from_millis(1000) {
                            let ctx = eyre!(
                                "can't connect to {:?} after {:?} tries ({:?})",
                                rem,
                                tries,
                                start.elapsed(),
                            );
                            return Err(Report::from(e).wrap_err(ctx));
                        } else {
                            tries += 1;
                            trace!(addr = ?&rem, ?tries, err = %format!("{:#}", e), "failed connection");
                        }
                    } else { break}
                };

                debug!(tunnel_entry = ?&cli_unix, elapsed = ?start.elapsed(), "connected to remote");
                Ok((uc, gt))
            })
        });

        Box::pin(async move {
            if let Some((listen_cn, l_fut)) = listen {
                let v = Some(l_fut.await);
                let mut l_cn_g = listen_cn.lock().unwrap();
                *l_cn_g = v;
            }

            if let Some((send_cn, send_fut)) = send {
                let v = Some(send_fut.await);
                let mut send_cn_g = send_cn.lock().unwrap();
                *send_cn_g = v;
            }
        })
    }
}

impl<A> TLSChunnel<A> {
    pub fn new(unix_root: PathBuf, binary_path: PathBuf, certs_location: PathBuf) -> Self {
        TLSChunnel {
            unix_root,
            binary_path,
            certs_location,
            listen: None,
            remote: None,
            listen_cn: Arc::new(StdMutex::new(None)),
            send_cn: Arc::new(StdMutex::new(None)),
            _phantom: Default::default(),
        }
    }

    pub fn listen(self, listen: SocketAddr) -> Self {
        TLSChunnel {
            listen: Some(listen),
            ..self
        }
    }

    pub fn connect(self, remote: SocketAddr) -> Self {
        TLSChunnel {
            remote: Some(remote),
            ..self
        }
    }
}

impl<A, T> Chunnel<T> for TLSChunnel<A>
where
    A: GetTlsConnAddr + Send + 'static,
{
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = TlsConn<A>;
    type Error = Report;

    // ingore the inner connection
    fn connect_wrap(&mut self, _: T) -> Self::Future {
        let remote_skaddr = self.remote.clone();
        let f = || {
            let mut scn_g = self.send_cn.lock().unwrap();
            let mut lcn_g = self.listen_cn.lock().unwrap();
            if scn_g.is_none() && lcn_g.is_none() {
                return Err(eyre!("Need at least one of send and listen sides"));
            }

            Ok(TlsConn::new(
                remote_skaddr,
                lcn_g.take().transpose()?,
                scn_g.take().transpose()?,
            ))
        };
        ready(f())
    }
}

/// Send and receive on unix-stream sockets, corresponding to the TLS tunnel.
///
/// Keep the server and client tunnel process handles around until drop time, when we want to kill the child
/// processes.
pub struct TlsConn<A> {
    listen: Option<Arc<UnixListener>>,
    listen_conns: Arc<Mutex<Vec<UnixStream>>>,
    remote_addr: Option<SocketAddr>,
    send: Option<Arc<Mutex<UnixStream>>>,
    _tls_tunnel_server_handle: Option<ghostunnel::GhostTunnel>,
    _tls_tunnel_client_handle: Option<ghostunnel::GhostTunnel>,
    _phantom: std::marker::PhantomData<A>,
}

impl<A> TlsConn<A> {
    fn new(
        remote_addr: Option<SocketAddr>,
        listen: Option<(UnixListener, ghostunnel::GhostTunnel)>,
        send: Option<(UnixStream, ghostunnel::GhostTunnel)>,
    ) -> Self {
        match (listen, send) {
            (Some((listen, server_handle)), Some((send, client_handle))) => TlsConn {
                remote_addr,
                listen: Some(Arc::new(listen)),
                send: Some(Arc::new(Mutex::new(send))),
                listen_conns: Arc::new(Mutex::new(Vec::new())),
                _tls_tunnel_server_handle: Some(server_handle),
                _tls_tunnel_client_handle: Some(client_handle),
                _phantom: Default::default(),
            },
            (Some((listen, server_handle)), None) => TlsConn {
                remote_addr,
                listen: Some(Arc::new(listen)),
                send: None,
                listen_conns: Arc::new(Mutex::new(Vec::new())),
                _tls_tunnel_server_handle: Some(server_handle),
                _tls_tunnel_client_handle: None,
                _phantom: Default::default(),
            },
            (None, Some((send, client_handle))) => TlsConn {
                remote_addr,
                listen: None,
                send: Some(Arc::new(Mutex::new(send))),
                listen_conns: Arc::new(Mutex::new(Vec::new())),
                _tls_tunnel_server_handle: None,
                _tls_tunnel_client_handle: Some(client_handle),
                _phantom: Default::default(),
            },
            (None, None) => {
                unreachable!("Must specify at least either client side or server side")
            }
        }
    }
}

pub trait GetTlsConnAddr: From<(SocketAddr, TlsConnAddr)> {
    fn as_tlsconn_addr(&self) -> TlsConnAddr;
}

impl From<(SocketAddr, TlsConnAddr)> for TlsConnAddr {
    fn from((_, a): (SocketAddr, TlsConnAddr)) -> TlsConnAddr {
        a
    }
}

impl GetTlsConnAddr for TlsConnAddr {
    fn as_tlsconn_addr(&self) -> TlsConnAddr {
        *self
    }
}

impl GetTlsConnAddr for (SocketAddr, TlsConnAddr) {
    fn as_tlsconn_addr(&self) -> TlsConnAddr {
        self.1
    }
}

/// Decide which connected remote address to send to.
///
/// Passing `Request` will send the message on the outgoing socket to the connected remote. Passing
/// `Response(n)` will send on the stored socket returned by accept().
///
/// On the recv() side, `Request` is returned by responses on the outgoing socket, and
/// `Response(n)` should be passed back to send() to respond on the same socket a request came in
/// on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum TlsConnAddr {
    Request,
    Response(usize),
}

impl<A> ChunnelConnection for TlsConn<A>
where
    A: GetTlsConnAddr,
{
    type Data = (A, Vec<u8>);

    fn send(
        &self,
        (addr, d): Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        use tokio::io::AsyncWriteExt;

        let len = d.len() as u64;
        let send_sk = self.send.as_ref().map(Arc::clone);
        let addr = addr.as_tlsconn_addr();
        let conns = match addr {
            TlsConnAddr::Request => None,
            TlsConnAddr::Response(_) => Some(Arc::clone(&self.listen_conns)),
        };

        Box::pin(
            async move {
                let mut conns_g = if let Some(ref c) = &conns {
                    Some(c.lock().await)
                } else {
                    None
                };

                let mut sk_g = if let Some(ref sk) = send_sk {
                    Some(sk.lock().await)
                } else {
                    None
                };

                let sk = match addr {
                    TlsConnAddr::Request if sk_g.is_some() => sk_g.as_mut().unwrap(),
                    TlsConnAddr::Request => {
                        return Err(eyre!("Request addr mode needs connected remote"))
                    }
                    TlsConnAddr::Response(n) => {
                        let l = conns_g.as_mut().unwrap();
                        if n < l.len() {
                            &mut l[n]
                        } else {
                            return Err(eyre!("Invalid response address, socket not found"));
                        }
                    }
                };

                sk.write_u64(len)
                    .await
                    .wrap_err("error sending (hdr) to tls tunnel entry")?;
                sk.write_all(&d)
                    .await
                    .wrap_err("error sending (body) to tls tunnel entry")?;
                Ok(())
            }
            .instrument(trace_span!("tlsconn-send")),
        )
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        // handle reading the length-framed messages.
        async fn recv_msg(sk: &mut UnixStream) -> Result<Vec<u8>, Report> {
            use tokio::io::AsyncReadExt;
            let len = sk
                .read_u64()
                .await
                .wrap_err("error receiving (hdr) from tls tunnel exit")?;
            let mut buf = Vec::with_capacity(len as usize);
            buf.resize(len as usize, 0);
            sk.read_exact(&mut buf)
                .await
                .wrap_err("error receiving (body) from tls tunnel exit")?;
            Ok(buf)
        }

        let remote_skaddr = self.remote_addr;
        let sk = self.listen.as_ref().map(|c| Arc::clone(c));
        let conns = Arc::clone(&self.listen_conns);
        let send_side = self.send.as_ref().map(|c| Arc::clone(c));
        Box::pin(
            async move {
                let mut to_add: Option<UnixStream> = None;
                let mut to_remove: Option<usize> = None;
                loop {
                    let mut conns_g = conns.lock().await;
                    if let Some(add) = to_add.take() {
                        conns_g.push(add);
                    }
                    if let Some(rem) = to_remove.take() {
                        conns_g.remove(rem);
                    }

                    let num_conns = conns_g.len();
                    let num_listen = num_conns + if send_side.is_some() { 1 } else { 0 };
                    trace!(?num_listen, ?sk, "listening");
                    match (&sk, num_listen) {
                        (Some(ref sk), 0) => {
                            // There is a listener to accept new connections, but no existing
                            // connections to monitor. We do only the accept part.
                            let (new_sk, addr) =
                                sk.accept().await.wrap_err("Error accepting connection")?;
                            debug!(?addr, "new stream");
                            to_add = Some(new_sk);
                        }
                        (Some(ref sk), _) => {
                            // There is a listener to accept new connections, and existing
                            // connections to monitor. We have to select.
                            let conns_futs = Box::pin(futures_util::future::select_all(
                                conns_g
                                    .iter_mut()
                                    .map(|sk| {
                                        Box::pin(recv_msg(sk))
                                            as Pin<Box<dyn Future<Output = _> + Send>>
                                    })
                                    .chain(send_side.iter().map(|s| {
                                        Box::pin(async move {
                                            let sk = &mut *s.lock().await;
                                            recv_msg(sk).await
                                        }) as _
                                    })), // listen to send socket also
                            ));
                            let accept_fut = Box::pin(sk.accept());
                            use futures_util::future::Either;
                            match futures_util::future::select(conns_futs, accept_fut).await {
                                Either::Left(((read_result, fut_num, _), _)) => {
                                    trace!(?fut_num, "msg future completed");
                                    match read_result {
                                        Ok(v) => {
                                            let addr = match fut_num {
                                                x if x == num_conns - 1 && send_side.is_some() => {
                                                    // received on the send socket.
                                                    (remote_skaddr.unwrap(), TlsConnAddr::Request)
                                                }
                                                x => {
                                                    // received on one of the active connection sockets.
                                                    let fake_addr = SocketAddr::from((
                                                        std::net::Ipv4Addr::UNSPECIFIED,
                                                        50000 + x as u16,
                                                    ));
                                                    (fake_addr, TlsConnAddr::Response(x))
                                                }
                                            };
                                            return Ok((addr.into(), v));
                                        }
                                        Err(err) => {
                                            debug!(err = %format!("{:#}", err), "Stream read failed");
                                            to_remove = Some(fut_num);
                                        }
                                    }
                                }
                                Either::Right((res, _)) => {
                                    let (new_sk, addr) = res.wrap_err("Error accepting connection")?;
                                    debug!(?addr, "new stream");
                                    to_add = Some(new_sk);
                                }
                            };
                        }
                        (None, 0) => {
                            // There is no listener to accept new connections, and no existing
                            // connections listening for new messages. It's not possible to make
                            // progress.
                            return Err(eyre!("No listener and no connections"));
                        }
                        (None, 1) => {
                            // There is no listener to accept new connections, but there is at
                            // least one conns_listeners (i.e. 1, the send-side one). So we can
                            // listen on only that.
                            let sk = &mut *send_side
                                .as_ref()
                                .expect("There must be a send-side connection")
                                .lock()
                                .await;
                            let v = recv_msg(sk).await.wrap_err(eyre!("Send-side socket errored"))?;
                            return Ok((
                                (remote_skaddr.unwrap(), TlsConnAddr::Request).into(),
                                v,
                            ));
                        }
                        (None, _) => {
                            // There is no listener to accept new connections, and somehow multiple
                            // connections listening for new messages. This is not possible.
                            unreachable!();
                        }
                    }
                }
            }
            .instrument(trace_span!("tlsconn-recv")),
        )
    }
}

#[cfg(test)]
mod t {
    use super::{TLSChunnel, TlsConnAddr};
    use bertha::{util::NeverCn, Chunnel, ChunnelConnection};
    use color_eyre::eyre::{Report, WrapErr};
    use std::path::PathBuf;
    use tracing::{info, info_span};
    use tracing_error::ErrorLayer;
    use tracing_futures::Instrument;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    #[test]
    #[ignore = "Requires set environment vars"]
    fn encryption_tunnel() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        color_eyre::install().unwrap_or(());

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .unwrap();

        let ghostunnel_path = match std::env::var("GHOSTUNNEL_ROOT") {
            Ok(p) => PathBuf::from(p),
            Err(_) => {
                panic!("GHOSTUNNEL_ROOT env var not set, skpping");
            }
        };

        let mut ghostunnel_binary = ghostunnel_path.clone();
        ghostunnel_binary.push("ghostunnel");

        let mut certs_dir = ghostunnel_path;
        certs_dir.push("test-keys/");

        const ADDR: &str = "127.0.0.1:8443";

        rt.block_on(
            async move {
                let gt_bin = ghostunnel_binary.clone();
                let certd = certs_dir.clone();
                tokio::spawn(
                    async move {
                        info!("starting");
                        // start server-side
                        let mut srv = TLSChunnel::<super::TlsConnAddr>::new(
                            PathBuf::from("/tmp"),
                            gt_bin,
                            certd,
                        )
                        .listen(ADDR.parse().unwrap());
                        let cn = srv.connect_wrap(NeverCn::<()>::default()).await.unwrap();
                        info!("listening");
                        loop {
                            let (a, d): (_, Vec<u8>) =
                                cn.recv().await.wrap_err("call recv").unwrap();
                            info!(addr = ?&a, data = ?&d, "recvd");
                            cn.send((a, d)).await.wrap_err("call send").unwrap();
                        }
                    }
                    .instrument(info_span!("server")),
                );

                info!("starting sender");
                let mut cli = TLSChunnel::new(PathBuf::from("/tmp"), ghostunnel_binary, certs_dir)
                    .connect(ADDR.parse().unwrap());
                let cn = cli
                    .connect_wrap(NeverCn::<()>::default())
                    .await
                    .wrap_err("client connect_wrap")?;
                info!("sending");
                cn.send((TlsConnAddr::Request, vec![0u8; 10]))
                    .await
                    .wrap_err("client send")?;
                let (a, d) = cn.recv().await.wrap_err("client recv")?;
                assert_eq!(a, TlsConnAddr::Request);
                assert_eq!(d, vec![0u8; 10]);
                info!("done");
                Ok::<_, Report>(())
            }
            .instrument(info_span!("encryption-tunnel-test")),
        )
        .unwrap();
    }
}
