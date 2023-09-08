//! Sharded multi-threaded listener, similar to kvserver.
//!
//! Take in raw messages, parse them with `crate::parse_log`, and produce them.

use std::{
    io::Write,
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use bertha::{
    bincode::SerializeChunnel,
    chan_transport::{Cln, RendezvousChannel, Srv},
    util::Nothing,
    ChunnelConnection, ChunnelListener, CxList, Either, Select,
};
use burrito_shard_ctl::{Kv, ShardInfo, SimpleShardPolicy};
use color_eyre::eyre::{Report, WrapErr};
use futures_util::stream::TryStreamExt;
use rcgen::Certificate;
use rustls::PrivateKey;
use tcp::TcpChunnelWrapServer;
use tls_tunnel::rustls::TLSChunnel;
use tracing::{debug, debug_span, error, info, instrument, trace, trace_span, warn, Instrument};

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct Line(pub String);

impl Kv for Line {
    type Key = String;
    fn key(&self) -> Self::Key {
        self.0.clone()
    }

    type Val = ();
    fn val(&self) -> Self::Val {
        ()
    }
}

pub fn serve(
    listen_addr: SocketAddr,
    hostname: impl ToString,
    num_workers: usize,
    redis_addr: String,
) -> Result<(), Report> {
    let (internal_srv, internal_cli) = RendezvousChannel::<SocketAddr, _, _>::new(100).split();
    let worker_addrs: Vec<_> = (1..=(num_workers as u16))
        .map(|i| SocketAddr::new(listen_addr.ip(), listen_addr.port() + i))
        .collect();
    let si = ShardInfo {
        canonical_addr: listen_addr,
        shard_addrs: worker_addrs.clone(),
        shard_info: SimpleShardPolicy {
            packet_data_offset: 18,
            packet_data_length: 4,
        },
    };

    let cert_rc = Arc::new(
        rcgen::generate_simple_self_signed([hostname.to_string(), listen_addr.ip().to_string()])
            .wrap_err("test certificate generation failed")?,
    );

    // start the workers
    for worker in worker_addrs {
        let int_srv = internal_srv.clone();
        let cert = cert_rc.clone();
        std::thread::spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(r) => r,
                Err(err) => {
                    error!(?err, "Could not start tokio runtime for worker thread");
                    return;
                }
            };
            match rt.block_on(
                single_worker(worker, int_srv, cert)
                    .instrument(debug_span!("worker", addr = ?&worker)),
            ) {
                Ok(_) => (),
                Err(err) => {
                    error!(?err, "Shard errored");
                }
            }
        });
    }

    // start the base address listener
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(serve_canonical(
        si,
        internal_cli,
        listen_addr,
        redis_addr,
        cert_rc,
    ))
}

macro_rules! encr_stack {
    ($listen_addr: expr, $cert_rc: expr) => {{
        // either:
        // base |> quic |> serialize
        // base |> tcp |> tls |> serialize
        // base |> serialize (no encryption)

        // tcp |> tls
        let private_key = PrivateKey($cert_rc.serialize_private_key_der());
        let cert = rustls::Certificate($cert_rc.serialize_der()?);
        let tls_server_cfg = rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(vec![cert], private_key)
            .expect("bad certificate/key");
        let tls_stack = CxList::from(TLSChunnel::server(tls_server_cfg))
            .wrap(TcpChunnelWrapServer::new($listen_addr)?);

        // quic
        let mut quic_server_cfg = quiche::Config::new(quiche::PROTOCOL_VERSION)?;
        quic_server_cfg.verify_peer(false);
        let mut cert_file = tempfile::NamedTempFile::new()?;
        let cert_pem = $cert_rc.serialize_pem()?;
        cert_file.write_all(cert_pem.as_bytes())?;
        quic_server_cfg.load_cert_chain_from_pem_file(cert_file.path().to_str().unwrap())?;
        let mut key_file = tempfile::NamedTempFile::new()?;
        let cert_key = $cert_rc.serialize_private_key_pem();
        key_file.write_all(cert_key.as_bytes())?;
        quic_server_cfg.load_priv_key_from_pem_file(key_file.path().to_str().unwrap())?;
        quic_server_cfg.set_max_idle_timeout(5000);
        quic_server_cfg.set_max_recv_udp_payload_size(1460);
        quic_server_cfg.set_max_send_udp_payload_size(1460);
        quic_server_cfg.set_initial_max_data(10_000_000);
        quic_server_cfg.set_initial_max_stream_data_bidi_local(1_000_000);
        quic_server_cfg.set_initial_max_stream_data_bidi_remote(1_000_000);
        quic_server_cfg.set_initial_max_streams_bidi(100);
        quic_server_cfg.set_initial_max_streams_uni(100);
        quic_server_cfg.set_disable_active_migration(true);
        let quic_stack = quic_chunnel::QuicChunnel::server(quic_server_cfg, [cert_file, key_file]);

        Ok::<_, Report>(Select::from((quic_stack, tls_stack)))
    }};
}

#[instrument(skip(internal_srv, cert), level = "info", err)]
async fn single_worker(
    addr: SocketAddr,
    mut internal_srv: RendezvousChannel<SocketAddr, Vec<u8>, Srv>,
    cert: Arc<Certificate>,
) -> Result<(), Report> {
    info!(?addr, "listening");
    let enc_stack = encr_stack!(addr, cert).wrap_err("creating encryption stack")?;
    let stack = CxList::from(SerializeChunnel::default())
        .wrap(Select::from((Nothing::<()>::default(), enc_stack)));
    let mut base_udp = bertha::udp::UdpReqChunnel::default();
    let negotiation_state = Default::default();
    let external_conn_stream = bertha::negotiate::negotiate_server_shared_state(
        stack,
        base_udp.listen(addr).await?,
        Arc::clone(&negotiation_state),
    )?;
    let internal_conn_stream = internal_srv.listen(addr).await?;
    // negotiating on the internal connection is required because ShardCanonicalServer has to be
    // able to send a StackNonce. we don't want the encryption stuff here.
    let internal_conn_stream = bertha::negotiate::negotiate_server_shared_state(
        SerializeChunnel::default(),
        internal_conn_stream,
        negotiation_state,
    )?;
    let joined_stream = futures_util::stream::select(
        external_conn_stream.map_ok(|cn| Either::Left(cn)),
        internal_conn_stream.map_ok(|cn| Either::Right(cn)),
    );
    let st = std::pin::pin!(joined_stream);
    st.try_for_each_concurrent(None, |cn| serve_one_cn(cn))
        .await?;
    unreachable!()
}

async fn serve_one_cn(
    cn: impl ChunnelConnection<Data = (SocketAddr, Line)> + Send + 'static,
) -> Result<(), Report> {
    let mut slots: Vec<_> = (0..16).map(|_| None).collect();
    debug!("new");
    loop {
        trace!("call recv");
        let msgs = match cn
            .recv(&mut slots)
            .await
            .wrap_err("logparser/worker: Error while processing requests")
        {
            Ok(ms) => ms,
            Err(e) => {
                warn!(err = ?e, "exiting on recv error");
                break Ok(());
            }
        };

        trace!(sz = ?msgs.iter().map_while(|x| x.as_ref().map(|_| 1)).sum::<usize>(), "got batch");

        // TODO do something with this message
    }
}

#[instrument(skip(internal_cli, cert), level = "info", err)]
async fn serve_canonical(
    si: ShardInfo<SocketAddr>,
    internal_cli: RendezvousChannel<SocketAddr, Vec<u8>, Cln>,
    listen_addr: SocketAddr,
    redis_addr: String,
    cert: Arc<Certificate>,
) -> Result<(), Report> {
    let enc_stack = encr_stack!(si.canonical_addr, cert).wrap_err("creating encryption stack")?;
    let cnsrv = burrito_shard_ctl::ShardCanonicalServer::new(
        si.clone(),
        None,
        internal_cli,
        SerializeChunnel::default(),
        None,
        &redis_addr,
    )
    .await
    .wrap_err("Create ShardCanonicalServer")?;
    let stack = CxList::from(cnsrv)
        .wrap(SerializeChunnel::<Line>::default())
        //.wrap(Select::from((Nothing::<()>::default(), enc_stack)));
        .wrap(enc_stack);

    let mut base_udp = bertha::udp::UdpReqChunnel::default();
    info!(shard_info = ?&si, "start canonical server");
    let st = base_udp.listen(listen_addr).await?;
    let st = bertha::negotiate_server(stack, st)
        .await
        .wrap_err("negotiate_server")?;

    let ctr: Arc<AtomicUsize> = Default::default();
    let st = std::pin::pin!(st);
    st.try_for_each_concurrent(None, |r| {
        let ctr = Arc::clone(&ctr);
        let mut slot = [None];
        async move {
            let ctr = ctr.fetch_add(1, Ordering::SeqCst);
            loop {
                match async {
                    r
                    .recv(&mut slot) // ShardCanonicalServerConnection is recv-only
                    .instrument(trace_span!("shard-canonical-server-connection", ?ctr))
                    .await
                    .wrap_err("logparser/server: Error while processing requests")
                }
                .await
                .wrap_err("logparser/server: Error in serving canonical connection")
                {
                    Err(e) => {
                        warn!(err = ?e, ?ctr, "exiting connection loop");
                        break Ok(());
                    }
                    Ok(_) => {}
                }
            }
        }
    })
    .await?;
    unreachable!()
}
