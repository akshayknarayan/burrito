use super::Encryption;
use bertha::{Chunnel, ChunnelConnection, Negotiate};
use color_eyre::eyre::{bail, eyre, Report, WrapErr};
use futures_util::future::{ready, Ready};
use rustls::{
    client::{ClientConnection, ServerName},
    server::ServerConnection,
    Connection,
};
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::{future::Future, pin::Pin};
use tokio::sync::Mutex;
use tracing::{debug, trace};

#[derive(Debug, Clone)]
pub enum TLSChunnel {
    Server {
        cfg: Arc<rustls::ServerConfig>,
    },
    Client {
        cfg: Arc<rustls::ClientConfig>,
        srv_name: ServerName,
    },
}

impl TLSChunnel {
    pub fn server(cfg: rustls::ServerConfig) -> Self {
        Self::Server { cfg: Arc::new(cfg) }
    }

    pub fn client(cfg: rustls::ClientConfig, srv_name: ServerName) -> Self {
        Self::Client {
            cfg: Arc::new(cfg),
            srv_name,
        }
    }
}

impl Negotiate for TLSChunnel {
    type Capability = Encryption;

    fn guid() -> u64 {
        0x5adce65babddbd98
    }

    fn capabilities() -> Vec<Self::Capability> {
        vec![Encryption]
    }
}

impl<T> Chunnel<T> for TLSChunnel
where
    T: ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = TLSConnection<T>;
    type Error = Report;

    fn connect_wrap(&mut self, cn: T) -> Self::Future {
        ready((|| {
            let tls = match self {
                Self::Server { cfg } => {
                    rustls::Connection::Server(ServerConnection::new(cfg.clone()).wrap_err_with(|| eyre!("could not create TLS server connection from config: {:?}", cfg))?)
                }
                Self::Client { cfg, srv_name } => {
                    rustls::Connection::Client(ClientConnection::new(cfg.clone(), srv_name.clone()).wrap_err_with(|| eyre!("could not create TLS client connection to server {:?} from config: {:?}", srv_name, cfg))?)
                }
            };
            Ok(TLSConnection::<()>::new(tls, cn))
        })())
    }
}

pub struct TLSConnection<C> {
    tls: Arc<Mutex<Connection>>,
    inner: C,
}

impl<C> TLSConnection<C> {
    pub fn new<T>(tls: Connection, inner: T) -> TLSConnection<T> {
        TLSConnection {
            tls: Arc::new(Mutex::new(tls)),
            inner,
        }
    }
}

impl<C> TLSConnection<C>
where
    C: ChunnelConnection<Data = (SocketAddr, Vec<u8>)>,
{
    #[tracing::instrument(level = "debug", skip(self, tls), err)]
    async fn complete_handshake(
        &self,
        tls: &mut Connection,
    ) -> Result<Option<rustls::IoState>, Report> {
        let mut ret = None;
        while tls.is_handshaking() {
            debug!("in tls handshake loop");
            let mut msg = Vec::with_capacity(1024);
            // write, then read
            while tls.wants_write() {
                tls.write_tls(&mut msg).wrap_err("write_tls")?;
            }

            if !msg.is_empty() {
                debug!(len = ?msg.len(), "writing");
                self.inner
                    .send(std::iter::once((
                        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0),
                        msg,
                    )))
                    .await
                    .wrap_err("send")?;
            }

            if !tls.is_handshaking() {
                break;
            }

            // now try reading
            trace!("try reading");
            if let Some((_, mut recvd)) = {
                let mut slots = [None];
                let ms = self.inner.recv(&mut slots[..]).await?;
                ms[0].take()
            } {
                debug!("read new TLS handshake packets");
                let mut c = std::io::Cursor::new(&mut recvd[..]);
                tls.read_tls(&mut c).wrap_err("read_tls")?;
                let iostate = tls.process_new_packets().wrap_err("process tls packets")?;
                ret = Some(iostate);
            }
        }

        Ok(ret)
    }
}

impl<C> ChunnelConnection for TLSConnection<C>
where
    C: ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
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
        let tls = self.tls.clone();
        Box::pin(async move {
            let mut tls_g = tls.lock().await;
            self.complete_handshake(&mut tls_g)
                .await
                .wrap_err("tls handshake completion")?;

            let mut err = None;
            let out = burst.into_iter().map_while(|(addr, mut msg)| {
                let mut w = tls_g.writer(); // writer is not Send
                if let Err(e) = w
                    .write(&msg)
                    .wrap_err_with(|| eyre!("Could not write into TLS"))
                {
                    err = Some(e);
                    return None;
                }

                msg.clear();
                while tls_g.wants_write() {
                    if let Err(e) = tls_g.write_tls(&mut msg).wrap_err_with(|| {
                        eyre!("Could not write from TLS to underlying connection")
                    }) {
                        err = Some(e);
                        return None;
                    }
                }

                Some((addr, msg))
            });

            debug!("sending");
            self.inner.send(out).await.wrap_err("tls conn inner recv")?;
            if let Some(e) = err {
                Err(e)
            } else {
                debug!("sent");
                Ok(())
            }
            // drop tls_g here, since inner.send uses it
        })
    }

    fn recv<'cn, 'buf>(
        &'cn self,
        msgs_buf: &'buf mut [Option<Self::Data>],
    ) -> Pin<Box<dyn Future<Output = Result<&'buf mut [Option<Self::Data>], Report>> + Send + 'cn>>
    where
        'buf: 'cn,
    {
        Box::pin(async move {
            debug!("starting recv");
            let mut tls_g = self.tls.lock().await;
            if let Some(iostate) = self
                .complete_handshake(&mut tls_g)
                .await
                .wrap_err_with(|| eyre!("tls handshake completion"))?
            {
                // complete_handshake read at most 1 message
                let next_msg_len = iostate.plaintext_bytes_to_read();
                if next_msg_len > 0 {
                    debug!(?next_msg_len, "already have payload to return");
                    let mut r = tls_g.reader();
                    if let Some((_, ref mut buf)) = msgs_buf[0] {
                        buf.resize(next_msg_len, 0);
                        r.read_exact(buf)?;
                        return Ok(&mut msgs_buf[0..1]);
                    } else {
                        let mut buf = vec![0u8; next_msg_len];
                        r.read_exact(&mut buf)?;
                        msgs_buf[0] = Some((
                            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0),
                            buf,
                        ));
                        return Ok(&mut msgs_buf[0..1]);
                    }
                }
            }

            let mut whole_pkts_received = 0;
            let ms = self
                .inner
                .recv(msgs_buf)
                .await
                .wrap_err_with(|| eyre!("tls conn inner recv"))?;
            debug!("got packets");
            for (_, ref mut raw_pkt) in ms.iter_mut().map_while(Option::as_mut) {
                let mut c = std::io::Cursor::new(&raw_pkt[..]);
                tls_g.read_tls(&mut c)?;
                let tls_io_state = tls_g
                    .process_new_packets()
                    .wrap_err_with(|| eyre!("fatal tls processing error"))?;
                let next_msg_len = tls_io_state.plaintext_bytes_to_read();
                debug!(?next_msg_len, "got payload");
                if next_msg_len > 0 {
                    raw_pkt.resize(next_msg_len, 0);
                    let mut r = tls_g.reader();
                    r.read_exact(raw_pkt)?;
                    whole_pkts_received += 1;
                } else {
                    bail!("TLS packets did not result in payload");
                }
            }

            debug!(?whole_pkts_received, "finished recv");
            Ok(&mut ms[..whole_pkts_received])
        })
    }
}

#[cfg(test)]
mod t {
    use super::TLSChunnel;
    use bertha::{
        negotiate_client, negotiate_server, ChunnelConnection, ChunnelConnector, ChunnelListener,
    };
    use color_eyre::eyre::{bail, eyre, Report, WrapErr};
    use futures_util::TryStreamExt;
    use rustls::{Certificate, ClientConfig, PrivateKey, ServerConfig};
    use std::net::SocketAddr;
    use std::sync::{Arc, Once};
    use tcp::{IgnoreAddr, TcpChunnel};
    use tokio::sync::Barrier;
    use tracing::info;
    use tracing_error::ErrorLayer;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    pub static COLOR_EYRE: Once = Once::new();

    #[tracing::instrument(level = "info", skip(start, cert, private_key), err)]
    async fn server(
        start: Arc<Barrier>,
        cert: Certificate,
        private_key: PrivateKey,
        addr: SocketAddr,
    ) -> Result<(), Report> {
        let server_cfg = ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(vec![cert], private_key)
            .expect("bad certificate/key");

        let mut tcp = TcpChunnel;
        let base_st = tcp
            .listen(addr)
            .await?
            .map_ok(move |cn| IgnoreAddr(addr, cn));
        start.wait().await;
        let st = negotiate_server(TLSChunnel::server(server_cfg), base_st).await?;
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

    #[tracing::instrument(level = "info", skip(start, serv_cert), err)]
    async fn client(start: Arc<Barrier>, serv_cert: &Certificate) -> Result<(), Report> {
        let mut cert_store = rustls::RootCertStore::empty();
        cert_store.add(serv_cert)?;
        let client_cfg = ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(cert_store)
            .with_no_client_auth();

        let addr = "127.0.0.1:12403".parse().unwrap();

        let mut tcp = TcpChunnel;
        start.wait().await;
        let base_cn = IgnoreAddr(
            addr,
            tcp.connect(addr)
                .await
                .wrap_err_with(|| eyre!("establish base connection to {:?}", addr))?,
        );
        let cn = negotiate_client(
            TLSChunnel::client(client_cfg, "localhost".try_into().unwrap()),
            base_cn,
            addr,
        )
        .await?;

        cn.send(std::iter::once((addr, (1..16).collect()))).await?;
        let mut slot = [None];
        let ms = cn.recv(&mut slot).await?;
        if let Some((_, msg)) = ms[0].take() {
            println!("got {:?}", msg);
            return Ok(());
        } else {
            bail!("did not receive: {:?}", ms)
        }
    }

    #[test]
    fn tls_chunnel() -> Result<(), Report> {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        COLOR_EYRE.call_once(|| color_eyre::install().unwrap_or(()));

        let cert = rcgen::generate_simple_self_signed(["localhost".to_string()])
            .wrap_err("test certificate generation failed")?;
        let private_key = PrivateKey(cert.serialize_private_key_der());
        let cert_der = Certificate(cert.serialize_der()?);

        let addr = "127.0.0.1:12403".parse().unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let start = Arc::new(tokio::sync::Barrier::new(2));
            let server_jh =
                tokio::spawn(server(start.clone(), cert_der.clone(), private_key, addr));
            client(start, &cert_der).await.wrap_err("client")?;
            if server_jh.is_finished() {
                return server_jh.await.unwrap();
            }
            Ok(())
        })
    }
}
