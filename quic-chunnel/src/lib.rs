//! QUIC chunnel

use bertha::{Chunnel, ChunnelConnection, Negotiate};
use color_eyre::eyre::{bail, eyre, Report, WrapErr};
use futures_util::future::{ready, Ready};
use quiche::Config;
use std::borrow::BorrowMut;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::{future::Future, pin::Pin};
use std::{
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, Instant},
};
use tcp::Connected;
use tokio::sync::Mutex;
use tracing::debug;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct QuicProto;

impl bertha::negotiate::CapabilitySet for QuicProto {
    fn guid() -> u64 {
        0x789924b36a9bf3b1
    }

    fn universe() -> Option<Vec<Self>> {
        // return None to force both sides to match
        None
    }
}

#[derive(Clone)]
pub enum QuicChunnel {
    Server { cfg: Arc<StdMutex<Config>> },
    Client { cfg: Arc<StdMutex<Config>> },
}

impl Debug for QuicChunnel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Server { .. } => f.debug_struct("QuicChunnelServer"),
            Self::Client { .. } => f.debug_struct("QuicChunnelClient"),
        }
        .finish()
    }
}

impl QuicChunnel {
    pub fn server(mut cfg: Config) -> Self {
        cfg.set_application_protos(&[b"QuicChunnelProto"]).unwrap();
        cfg.enable_dgram(true, 1500, 1500);
        QuicChunnel::Server {
            cfg: Arc::new(StdMutex::new(cfg)),
        }
    }

    pub fn client(mut cfg: Config) -> Self {
        cfg.set_application_protos(&[b"QuicChunnelProto"]).unwrap();
        cfg.enable_dgram(true, 1500, 1500);
        QuicChunnel::Client {
            cfg: Arc::new(StdMutex::new(cfg)),
        }
    }
}

impl Negotiate for QuicChunnel {
    type Capability = QuicProto;

    fn guid() -> u64 {
        0x4c1117e9fc0379b1
    }

    fn capabilities() -> Vec<Self::Capability> {
        vec![QuicProto]
    }
}

impl<T> Chunnel<T> for QuicChunnel
where
    T: ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Connected + Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = QuicConn<T>;
    type Error = Report;

    fn connect_wrap(&mut self, cn: T) -> Self::Future {
        ready((|| {
            let quic_conn_id =
                quiche::ConnectionId::from_ref(&[90, 210, 30, 11, 23, 213, 240, 124, 176]);
            let local_addr = cn.local_addr();
            let peer_addr = cn
                .peer_addr()
                .ok_or_else(|| eyre!("require a connected connection"))?;
            let quic = match self {
                Self::Server { cfg } => quiche::accept(
                    &quic_conn_id,
                    None,
                    local_addr,
                    peer_addr,
                    cfg.lock().unwrap().borrow_mut(),
                )?,
                Self::Client { cfg } => quiche::connect(
                    None,
                    &quic_conn_id,
                    local_addr,
                    peer_addr,
                    cfg.lock().unwrap().borrow_mut(),
                )?,
            };
            Ok(QuicConn::<()>::new(quic, cn))
        })())
    }
}

pub struct QuicConn<T> {
    quic: Arc<Mutex<quiche::Connection>>,
    inner: T,
}

impl<C> QuicConn<C> {
    pub fn new<T>(quic: quiche::Connection, inner: T) -> QuicConn<T> {
        QuicConn {
            quic: Arc::new(Mutex::new(quic)),
            inner,
        }
    }
}

fn get_quic_error(quic_g: &mut quiche::Connection) -> Report {
    if let Some(quiche::ConnectionError {
        is_app,
        error_code,
        reason,
    }) = quic_g.local_error()
    {
        if *is_app {
            eyre!("App-level error")
        } else {
            eyre!("QUIC error code = {:?}, reason = {:?}", error_code, reason)
        }
    } else {
        eyre!("Unknown quic error")
    }
}

impl<C> QuicConn<C>
where
    C: ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Connected + Send + Sync + 'static,
{
    async fn quic_send_drain(&self, quic_g: &mut quiche::Connection) -> Result<(), Report> {
        let mut err = None;
        let mut pacing_exceeded = Duration::from_millis(0);
        self.inner
            .send(std::iter::repeat(()).map_while(|_| {
                let mut out_buf = [0u8; 1460];
                match quic_g.send(&mut out_buf[..]) {
                    Ok((write, quiche::SendInfo { to, at, .. })) => {
                        pacing_exceeded += at - Instant::now();
                        Some((to, out_buf[..write].to_vec()))
                    }
                    Err(quiche::Error::Done) => None,
                    Err(e) => {
                        err = Some(get_quic_error(quic_g).wrap_err(e));
                        None
                    }
                }
            }))
            .await?;
        if let Some(err) = err {
            bail!(err);
        }
        debug!(?pacing_exceeded, "does this grow over time");
        Ok(())
    }

    async fn quic_recv(
        &self,
        quic_g: &mut quiche::Connection,
        mut msgs_buf: &mut [Option<(SocketAddr, Vec<u8>)>],
    ) -> Result<(), Report> {
        let local_addr = self.inner.local_addr();
        let peer_addr = self.inner.peer_addr().unwrap();
        let ms = self.inner.recv(&mut msgs_buf).await?;
        for (_, mut msg) in ms.iter_mut().map_while(Option::take) {
            let mut processed_bytes = 0;
            while processed_bytes < msg.len() {
                processed_bytes += quic_g
                    .recv(
                        &mut msg[processed_bytes..],
                        quiche::RecvInfo {
                            from: peer_addr,
                            to: local_addr,
                        },
                    )
                    .map_err(|e| get_quic_error(quic_g).wrap_err(e))
                    .wrap_err("quic_recv")?;
            }
        }

        Ok(())
    }

    async fn quic_complete_handshake(
        &self,
        quic_g: &mut quiche::Connection,
        msgs_buf: &mut [Option<(SocketAddr, Vec<u8>)>],
    ) -> Result<(), Report> {
        while !quic_g.is_established() {
            self.quic_send_drain(quic_g)
                .await
                .wrap_err("handshake send")?;

            if quic_g.is_established() {
                break;
            }

            self.quic_recv(quic_g, msgs_buf)
                .await
                .wrap_err("handshake recv")?;
        }

        return Ok(());
    }
}

impl<C> ChunnelConnection for QuicConn<C>
where
    C: ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Connected + Send + Sync + 'static,
{
    type Data = (SocketAddr, Vec<u8>);

    fn send<'cn, B>(
        &'cn self,
        burst: B,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'cn>>
    where
        B: IntoIterator<Item = Self::Data> + Send + 'cn,
        <B as IntoIterator>::IntoIter: Send,
    {
        let quic = self.quic.clone();
        Box::pin(async move {
            let mut quic_g = quic.lock().await;
            let mut slot = [None];
            self.quic_complete_handshake(&mut quic_g, &mut slot[..])
                .await
                .wrap_err_with(|| eyre!("error during handshake (send)"))?;

            // since we are sending dgrams, dump all the messages into quic state now.
            // Afterwards we can slurp out actual packets to forward to self.inner
            for (_, msg) in burst.into_iter() {
                quic_g
                    .dgram_send_vec(msg)
                    .wrap_err_with(|| eyre!("QUIC dgram send"))?;
            }

            // now it is sending time.
            self.quic_send_drain(&mut quic_g).await
        })
    }

    fn recv<'cn, 'buf>(
        &'cn self,
        msgs_buf: &'buf mut [Option<Self::Data>],
    ) -> Pin<Box<dyn Future<Output = Result<&'buf mut [Option<Self::Data>], Report>> + Send + 'cn>>
    where
        'buf: 'cn,
    {
        let peer_addr = self.inner.peer_addr();
        let quic = self.quic.clone();
        Box::pin(async move {
            let peer_addr = peer_addr.ok_or_else(|| eyre!("require a connected connection"))?;
            let mut quic_g = quic.lock().await;

            self.quic_complete_handshake(&mut quic_g, &mut msgs_buf[0..1])
                .await?;

            let ret = 'recv: loop {
                if !quic_g.is_readable() {
                    self.quic_recv(&mut quic_g, msgs_buf).await?;
                }

                let mut num_recvd = 0;
                break loop {
                    if num_recvd == msgs_buf.len() {
                        break Ok(&mut msgs_buf[..num_recvd]);
                    }

                    match quic_g.dgram_recv_vec() {
                        Ok(msg) => {
                            msgs_buf[num_recvd] = Some((peer_addr, msg));
                            num_recvd += 1;
                        }
                        Err(quiche::Error::Done) if num_recvd > 0 => {
                            break Ok(&mut msgs_buf[..num_recvd]);
                        }
                        Err(quiche::Error::Done) => {
                            continue 'recv;
                        }

                        Err(e) => {
                            break Err(eyre!(e).wrap_err("Could not get messages from quic"));
                        }
                    }
                };
            };

            // we called quic::Connection::recv and also dgram_recv, and the documentation says we
            // have to call send afterwards.
            self.quic_send_drain(&mut quic_g).await?;
            ret
        })
    }
}

#[cfg(test)]
mod t {
    use crate::QuicChunnel;
    use bertha::udp::{UdpReqChunnel, UdpSkChunnel};
    use bertha::{
        negotiate_client, negotiate_server, ChunnelConnection, ChunnelConnector, ChunnelListener,
    };
    use color_eyre::eyre::{bail, eyre, Report, WrapErr};
    use futures_util::TryStreamExt;
    use rcgen::Certificate;
    use std::io::Write;
    use std::net::SocketAddr;
    use std::sync::{Arc, Once};
    use tcp::Connect;
    use tokio::sync::Barrier;
    use tracing::info;
    use tracing_error::ErrorLayer;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    pub static COLOR_EYRE: Once = Once::new();

    #[tracing::instrument(level = "info", skip(start, cert), err)]
    async fn server(
        start: Arc<Barrier>,
        cert: Certificate,
        addr: SocketAddr,
    ) -> Result<(), Report> {
        let mut cert_file = tempfile::NamedTempFile::new()?;
        let cert_pem = cert.serialize_pem()?;
        cert_file.write_all(cert_pem.as_bytes())?;

        let mut key_file = tempfile::NamedTempFile::new()?;
        let cert_key = cert.serialize_private_key_pem();
        key_file.write_all(cert_key.as_bytes())?;

        let mut server_cfg = quiche::Config::new(quiche::PROTOCOL_VERSION)?;
        server_cfg.verify_peer(false);
        server_cfg.load_cert_chain_from_pem_file(cert_file.path().to_str().unwrap())?;
        server_cfg.load_priv_key_from_pem_file(key_file.path().to_str().unwrap())?;
        server_cfg.set_max_idle_timeout(5000);
        server_cfg.set_max_recv_udp_payload_size(1460);
        server_cfg.set_max_send_udp_payload_size(1460);
        server_cfg.set_initial_max_data(10_000_000);
        server_cfg.set_initial_max_stream_data_bidi_local(1_000_000);
        server_cfg.set_initial_max_stream_data_bidi_remote(1_000_000);
        server_cfg.set_initial_max_streams_bidi(100);
        server_cfg.set_initial_max_streams_uni(100);
        server_cfg.set_disable_active_migration(true);

        let mut udp = UdpReqChunnel;
        let base_st = udp.listen(addr).await?;
        start.wait().await;
        let st = negotiate_server(QuicChunnel::server(server_cfg), base_st).await?;
        st.try_for_each_concurrent(None, |cn| async move {
            info!("established connection");
            let mut slot = [None];
            loop {
                let ms = cn.recv(&mut slot).await?;
                info!(?ms, "received");
                cn.send(ms.into_iter().map_while(Option::take)).await?;
            }
        })
        .await?;
        unreachable!()
    }

    #[tracing::instrument(level = "info", skip(start), err)]
    async fn client(start: Arc<Barrier>) -> Result<(), Report> {
        let mut client_cfg = quiche::Config::new(quiche::PROTOCOL_VERSION)?;
        client_cfg.verify_peer(false);
        client_cfg.set_max_idle_timeout(5000);
        client_cfg.set_max_recv_udp_payload_size(1460);
        client_cfg.set_max_send_udp_payload_size(1460);
        client_cfg.set_initial_max_data(10_000_000);
        client_cfg.set_initial_max_stream_data_bidi_local(1_000_000);
        client_cfg.set_initial_max_stream_data_bidi_remote(1_000_000);
        client_cfg.set_initial_max_streams_bidi(100);
        client_cfg.set_initial_max_streams_uni(100);
        client_cfg.set_disable_active_migration(true);

        let addr = "127.0.0.1:16403".parse().unwrap();

        let mut udp = UdpSkChunnel;
        start.wait().await;
        let base_cn = udp.connect(addr).await?;
        let base_cn = Connect::new(addr, base_cn);
        let cn = negotiate_client(QuicChunnel::client(client_cfg), base_cn, addr)
            .await
            .wrap_err(eyre!("establish conn to {:?}", addr))?;

        cn.send(std::iter::once((addr, (1..16).collect()))).await?;
        let mut slot = [None];
        loop {
            let ms = cn.recv(&mut slot).await?;
            if ms.is_empty() {
                tracing::warn!("recv returned empty slice");
                continue;
            }
            if let Some((_, msg)) = ms[0].take() {
                println!("got {:?}", msg);
                return Ok(());
            } else {
                bail!("did not receive: {:?}", ms)
            }
        }
    }

    #[test]
    fn quic_chunnel() -> Result<(), Report> {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        COLOR_EYRE.call_once(|| color_eyre::install().unwrap_or(()));

        let cert = rcgen::generate_simple_self_signed(["localhost".to_string()])
            .wrap_err("test certificate generation failed")?;

        let addr = "127.0.0.1:16403".parse().unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let start = Arc::new(tokio::sync::Barrier::new(2));
            let server_jh = tokio::spawn(server(start.clone(), cert, addr));
            client(start).await.wrap_err("client")?;
            if server_jh.is_finished() {
                return server_jh.await.unwrap();
            }
            Ok(())
        })
    }
}
