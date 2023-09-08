//! Shard-compatible client, similar to KvClient. Support TLS and QUIC encryption options.
//!
//! Produce raw messages for `crate::listen` connections to handle.

use std::{
    error::Error,
    fmt::Display,
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use bertha::{
    bincode::SerializeChunnel,
    negotiate_client,
    udp::UdpSkChunnel,
    util::{Nothing, ProjectLeft},
    ChunnelConnection, ChunnelConnector, CxList, Select,
};
use burrito_shard_ctl::ClientShardChunnelClient;
use color_eyre::eyre::{Report, WrapErr};
use rustls::{ClientConfig, RootCertStore, ServerName};
use tcp::{Connect, TcpChunnelWrapClient};
use tls_tunnel::rustls::TLSChunnel;
use tracing::debug;

use crate::listen::Line;

#[derive(Clone, Copy, Debug)]
struct DontVerify(pub IpAddr);

#[derive(Clone, Debug)]
struct DontVerifyErr(String);

impl Display for DontVerifyErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Error for DontVerifyErr {}

impl rustls::client::ServerCertVerifier for DontVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        server_name: &ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        match server_name {
            ServerName::IpAddress(ip) if *ip == self.0 => (),
            ServerName::DnsName(dns) if dns.as_ref() == "localhost" => (),
            _ => {
                let err = DontVerifyErr(format!(
                    "ServerName {:?} mismatched verifier configured IP {:?} or hostname localhost",
                    server_name, self.0,
                ));
                return Err(rustls::Error::InvalidCertificate(
                    rustls::CertificateError::Other(Arc::new(err)),
                ));
            }
        }

        return Ok(rustls::client::ServerCertVerified::assertion());
    }
}

macro_rules! encr_stack {
    ($connect_addr: expr) => {{
        // either:
        // base |> quic |> serialize
        // base |> tcp |> tls |> serialize
        // base |> serialize (no encryption)

        // tcp |> tls
        let mut tls_client_cfg = ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth();
        let mut dangerous_cfg = tls_client_cfg.dangerous();
        dangerous_cfg.set_certificate_verifier(Arc::new(DontVerify($connect_addr.ip())));
        let sn: ServerName = ServerName::IpAddress($connect_addr.ip());
        let tls_stack = CxList::from(TLSChunnel::client(tls_client_cfg, sn))
            .wrap(TcpChunnelWrapClient::new($connect_addr));

        // quic
        let mut quic_client_cfg = quiche::Config::new(quiche::PROTOCOL_VERSION)?;
        quic_client_cfg.verify_peer(false);
        quic_client_cfg.set_max_idle_timeout(5000);
        quic_client_cfg.set_max_recv_udp_payload_size(1460);
        quic_client_cfg.set_max_send_udp_payload_size(1460);
        quic_client_cfg.set_initial_max_data(10_000_000);
        quic_client_cfg.set_initial_max_stream_data_bidi_local(1_000_000);
        quic_client_cfg.set_initial_max_stream_data_bidi_remote(1_000_000);
        quic_client_cfg.set_initial_max_streams_bidi(100);
        quic_client_cfg.set_initial_max_streams_uni(100);
        quic_client_cfg.set_disable_active_migration(true);
        let quic_stack = quic_chunnel::QuicChunnel::client(quic_client_cfg);

        Ok::<_, Report>(Select::from((quic_stack, tls_stack)))
        //Ok::<_, Report>(tls_stack)
    }};
}

pub async fn connect(
    addr: SocketAddr,
    redis_addr: String,
) -> Result<impl ChunnelConnection<Data = Line> + Send + 'static, Report> {
    let base = UdpSkChunnel.connect(addr).await?;
    let base = Connect::new(addr, base);
    let enc_stack = encr_stack!(addr).wrap_err("creating encryption stack")?;
    let cl_shard = ClientShardChunnelClient::new(addr, &redis_addr)
        .await
        .wrap_err("make ClientShardChunnelClient")?;
    let stack = CxList::from(cl_shard)
        .wrap(SerializeChunnel::default())
        //    .wrap(Select::from((Nothing::<()>::default(), enc_stack)));
        .wrap(enc_stack);
    let cn = negotiate_client(stack, base, addr).await?;
    debug!("returning connection");
    let cn = ProjectLeft::new(addr, cn);
    Ok(cn)
}
