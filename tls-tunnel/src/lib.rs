//! TLS encryption tunnel.
//!
//! Internally spawns and maintains ghostunnel client and server processes. Have to use stream
//! sockets, so handles that too.

use bertha::{util::NeverCn, Chunnel, ChunnelConnection, Negotiate};
use color_eyre::eyre::{eyre, Report, WrapErr};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::{future::Future, pin::Pin};
use tokio::{
    net::{UnixListener, UnixStream},
    sync::Mutex,
};
use tracing::debug;

mod ghostunnel;

#[derive(Debug, Clone)]
pub struct TLSChunnel {
    unix_root: PathBuf,
    listen: Option<SocketAddr>,
    remote: Option<SocketAddr>,
    binary_path: PathBuf,
    certs_location: PathBuf,
}

impl<NeverCn> Chunnel<NeverCn> for TLSChunnel {
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Connection = TlsConn;
    type Error = Report;

    fn connect_wrap(&mut self, _: NeverCn) -> Self::Future {
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
            let mut srv_unix = stem.clone();
            srv_unix.push_str("-srv");
            let gt = ghostunnel::GhostTunnel::start_server(
                srv,
                &srv_unix,
                &self.binary_path,
                &self.certs_location,
            );
            let ul = UnixListener::bind(srv_unix);
            (ul, gt)
        });

        let send = self.remote.map(|rem| {
            let mut cli_unix = stem.clone();
            cli_unix.push_str("-cli");
            let gt = ghostunnel::GhostTunnel::start_client(
                rem,
                &cli_unix,
                &self.binary_path,
                &self.certs_location,
            );
            (cli_unix, gt)
        });

        Box::pin(async move {
            let send = if let Some((cli_unix, gt)) = send {
                let uc = UnixStream::connect(cli_unix).await?;
                Some((uc, gt?))
            } else {
                None
            };

            let listen = if let Some((ul, gt)) = listen {
                Some((ul?, gt?))
            } else {
                None
            };

            if send.is_none() && listen.is_none() {
                return Err(eyre!("Need at least one of send and listen sides"));
            }

            Ok(TlsConn::new(listen, send))
        })
    }
}

/// Send and receive on unix-stream sockets, corresponding to the TLS tunnel.
///
/// Keep the server and client tunnel process handles around until drop time, when we want to kill the child
/// processes.
pub struct TlsConn {
    listen: Option<Arc<UnixListener>>,
    listen_conns: Arc<Mutex<Vec<UnixStream>>>,
    send: Option<Arc<UnixStream>>,
    _tls_tunnel_server_handle: Option<ghostunnel::GhostTunnel>,
    _tls_tunnel_client_handle: Option<ghostunnel::GhostTunnel>,
}

impl TlsConn {
    fn new(
        listen: Option<(UnixListener, ghostunnel::GhostTunnel)>,
        send: Option<(UnixStream, ghostunnel::GhostTunnel)>,
    ) -> Self {
        match (listen, send) {
            (Some((listen, server_handle)), Some((send, client_handle))) => TlsConn {
                listen: Some(Arc::new(listen)),
                send: Some(Arc::new(send)),
                listen_conns: Arc::new(Mutex::new(Vec::new())),
                _tls_tunnel_server_handle: Some(server_handle),
                _tls_tunnel_client_handle: Some(client_handle),
            },
            (Some((listen, server_handle)), None) => TlsConn {
                listen: Some(Arc::new(listen)),
                send: None,
                listen_conns: Arc::new(Mutex::new(Vec::new())),
                _tls_tunnel_server_handle: Some(server_handle),
                _tls_tunnel_client_handle: None,
            },
            (None, Some((send, client_handle))) => TlsConn {
                listen: None,
                send: Some(Arc::new(send)),
                listen_conns: Arc::new(Mutex::new(Vec::new())),
                _tls_tunnel_server_handle: None,
                _tls_tunnel_client_handle: Some(client_handle),
            },
            (None, None) => {
                unreachable!("Must specify at least either client side or server side")
            }
        }
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
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum TlsConnAddr {
    Request,
    Response(usize),
}

impl ChunnelConnection for TlsConn {
    type Data = (TlsConnAddr, Vec<u8>);

    fn send(
        &self,
        (addr, d): Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        let len = (d.len() as u64).to_le_bytes();
        let send_sk = self.send.as_ref().map(Arc::clone);
        let conns = match addr {
            TlsConnAddr::Request => None,
            TlsConnAddr::Response(_) => Some(Arc::clone(&self.listen_conns)),
        };

        Box::pin(async move {
            let conns_g = if let Some(ref c) = &conns {
                Some(c.lock().await)
            } else {
                None
            };

            let sk = match addr {
                TlsConnAddr::Request if send_sk.is_some() => send_sk.as_ref().unwrap(),
                TlsConnAddr::Request => {
                    return Err(eyre!("Request addr mode needs connected remote"))
                }
                TlsConnAddr::Response(n) => {
                    let l = conns_g.as_ref().unwrap();
                    if n < l.len() {
                        &l[n]
                    } else {
                        return Err(eyre!("Invalid response address, socket not found"));
                    }
                }
            };

            let d: Vec<_> = len.iter().copied().chain(d.into_iter()).collect();
            loop {
                sk.writable().await?;
                match sk.try_write(&d[..]) {
                    Ok(_) => return Ok(()),
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                    Err(e) => return Err(e).wrap_err("error sending to tls tunnel entry"),
                }
            }
        })
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        let sk = self.listen.as_ref().map(|c| Arc::clone(c));
        let conns = Arc::clone(&self.listen_conns);
        let send_side = self.send.as_ref().map(|c| Arc::clone(c));
        Box::pin(async move {
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
                let conns_listeners = conns_g
                    .iter()
                    .chain(send_side.iter().map(|s| s.as_ref())) // listen to send socket also
                    .map(|sk| {
                        Box::pin(async move {
                            loop {
                                sk.readable().await?;
                                let mut buf = [0u8; 1024];
                                match sk.try_read(&mut buf) {
                                    Ok(0) => return Ok(None),
                                    Ok(n) => {
                                        return Ok(Some(buf[..n].to_vec()));
                                    }
                                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                        continue
                                    }
                                    Err(e) => {
                                        return Err(e)
                                            .wrap_err("error recieving from tls tunnel exit")
                                    }
                                }
                            }
                        })
                    });

                let conns_futs = Box::pin(futures_util::future::select_all(conns_listeners));
                if let Some(ref sk) = sk {
                    let accept_fut = Box::pin(sk.accept());
                    use futures_util::future::Either;
                    match futures_util::future::select(conns_futs, accept_fut).await {
                        Either::Left(((read_result, fut_num, _), _)) => match read_result {
                            Ok(Some(v)) => {
                                let addr = match fut_num {
                                    x if x == num_conns - 1 => {
                                        // received on the send socket.
                                        TlsConnAddr::Request
                                    }
                                    x => {
                                        // received on one of the active connection sockets.
                                        TlsConnAddr::Response(x)
                                    }
                                };
                                return Ok((addr, v));
                            }
                            Ok(None) => {
                                to_remove = Some(fut_num);
                            }
                            Err(err) => {
                                debug!(?err, "Stream read failed");
                                to_remove = Some(fut_num);
                            }
                        },
                        Either::Right((res, _)) => match res {
                            Ok((new_sk, addr)) => {
                                debug!(?addr, "new stream");
                                to_add = Some(new_sk);
                            }
                            Err(e) => return Err(e).wrap_err("Error accepting connection"),
                        },
                    };
                } else {
                    // there is no accept, so conns_listeners is of length 1.
                    let (read_result, fut_num, _) = conns_futs.await;
                    match read_result {
                        Ok(Some(v)) => {
                            assert_eq!(fut_num, 0, "there is only one socket listening");
                            return Ok((TlsConnAddr::Request, v));
                        }
                        Ok(None) => {
                            to_remove = Some(fut_num);
                        }
                        Err(err) => {
                            debug!(?err, "Stream read failed");
                            to_remove = Some(fut_num);
                        }
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod t {
    use color_eyre::eyre::Report;
    use tracing_error::ErrorLayer;
    //use tracing_futures::Instrument;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    #[test]
    fn unix_stream() {
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

        rt.block_on(async move {
            tokio::spawn(async move {
                std::fs::remove_file("/tmp/test").unwrap_or(());
                let ul = tokio::net::UnixListener::bind("/tmp/test").unwrap();
                while let Ok((st, addr)) = ul.accept().await {
                    println!("new listen conn: {:?}", addr);
                    st.readable().await.unwrap();
                    let mut buf = [0u8; 64];
                    let len = st.try_read(&mut buf).unwrap();

                    st.writable().await.unwrap();
                    st.try_write(&buf[0..len]).unwrap();
                }
            });

            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let uc = tokio::net::UnixStream::connect("/tmp/test").await.unwrap();
            println!("{:?}", uc.local_addr());

            uc.writable().await?;
            uc.try_write(&[1, 2, 3, 4, 5, 6])?;

            uc.readable().await?;
            let mut buf = [0u8; 64];
            let len = uc.try_read(&mut buf)?;
            assert_eq!(len, 6);

            Ok::<_, Report>(())
        })
        .unwrap();
    }
}
