//! Sharded multi-threaded listener, similar to kvserver.
//!
//! Take in raw messages, parse them with `crate::parse_log`, and produce them.

use std::{
    collections::HashMap,
    future::Future,
    io::Write,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use bertha::{
    bincode::SerializeChunnel,
    chan_transport::{Cln, RendezvousChannel, Srv},
    uds::UnixSkChunnel,
    util::{Nothing, ProjectLeft},
    ChunnelConnection, ChunnelListener, CxList, Either, Select, StackNonce,
};
use burrito_localname_ctl::LocalNameChunnel;
use burrito_shard_ctl::ShardInfo;
use color_eyre::eyre::{Report, WrapErr};
use futures_util::{stream::TryStreamExt, Stream};
use rcgen::Certificate;
use rustls::PrivateKey;
use tcp::{Connected, TcpChunnelWrapServer};
use tls_tunnel::rustls::TLSChunnel;
use tokio::runtime::Runtime;
use tracing::{debug, debug_span, error, info, instrument, trace, trace_span, warn, Instrument};

use crate::{
    parse_log::{EstOutputRateHist, EstOutputRateSerializeChunnel, Line},
    EncrSpec,
};

pub trait ProcessLine<L> {
    type Future<'a>: Future<Output = Result<(), Self::Error>>
    where
        Self: 'a,
        L: 'a;
    type Error: Into<Report> + Send + Sync + 'static;
    fn process_lines<'a>(&'a self, line_batch: &'a mut [Option<L>]) -> Self::Future<'a>;
}

pub fn serve(
    listen_addr: SocketAddr,
    hostname: impl ToString,
    num_workers: usize,
    redis_addr: String,
    process_message: impl ProcessLine<(SocketAddr, Line)> + Send + Sync + 'static,
    encr_spec: EncrSpec,
    runtime: Option<Runtime>,
) -> Result<(), Report> {
    let (internal_srv, internal_cli) = RendezvousChannel::<SocketAddr, _, _>::new(100).split();
    let worker_addrs: Vec<_> = (1..=(num_workers as u16))
        .map(|i| SocketAddr::new(listen_addr.ip(), listen_addr.port() + i))
        .collect();
    let si = ShardInfo {
        canonical_addr: listen_addr,
        shard_addrs: worker_addrs.clone(),
    };

    let cert_rc = Arc::new(
        rcgen::generate_simple_self_signed([hostname.to_string(), listen_addr.ip().to_string()])
            .wrap_err("test certificate generation failed")?,
    );

    let line_processor = Arc::new(process_message);
    // start the workers
    for worker in worker_addrs {
        let int_srv = internal_srv.clone();
        let cert = cert_rc.clone();
        let lp = Arc::clone(&line_processor);
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
                single_worker(worker, int_srv, cert, lp, encr_spec)
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
    let rt = if let Some(r) = runtime {
        r
    } else {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
    };
    rt.block_on(serve_canonical(
        si,
        internal_cli,
        listen_addr,
        redis_addr,
        cert_rc,
        encr_spec,
    ))
}

macro_rules! encr_stack {
    (tls => $listen_addr: expr, $cert_rc: expr) => {{
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
        tls_stack
    }};
    (quic => $listen_addr: expr, $cert_rc: expr) => {{
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
        quic_stack
    }};
    ($listen_addr: expr, $cert_rc: expr) => {{
        // either:
        // base |> quic
        // base |> tcp |> tls
        Select::from((encr_stack!(quic => $listen_addr, $cert_rc), encr_stack!(tls => $listen_addr, $cert_rc)))
    }};
}

#[instrument(skip(internal_srv, cert, line_processor), level = "info", err)]
async fn single_worker(
    addr: SocketAddr,
    internal_srv: RendezvousChannel<SocketAddr, Vec<u8>, Srv>,
    cert: Arc<Certificate>,
    line_processor: Arc<impl ProcessLine<(SocketAddr, Line)> + Send + Sync + 'static>,
    encr_spec: EncrSpec,
) -> Result<(), Report> {
    async fn serve_with_external<S, C>(
        addr: SocketAddr,
        external_conn_stream: S,
        negotiation_state: Arc<Mutex<HashMap<SocketAddr, StackNonce>>>,
        mut internal_srv: RendezvousChannel<SocketAddr, Vec<u8>, Srv>,
        line_processor: Arc<impl ProcessLine<(SocketAddr, Line)> + Send + Sync + 'static>,
    ) -> Result<(), Report>
    where
        S: Stream<Item = Result<C, Report>>,
        C: ChunnelConnection<Data = (SocketAddr, Line)> + Send + 'static,
    {
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
        info!(?addr, "ready");
        let st = std::pin::pin!(joined_stream);
        st.try_for_each_concurrent(None, |cn| serve_one_cn(cn, &line_processor))
            .await?;
        unreachable!()
    }

    let mut base_udp = bertha::udp::UdpReqChunnel::default();
    let negotiation_state = Default::default();
    match encr_spec {
        EncrSpec::AllowNone => {
            let enc_stack = encr_stack!(addr, cert);
            let stack = CxList::from(SerializeChunnel::default())
                .wrap(Select::from((Nothing::<()>::default(), enc_stack)));
            let st = bertha::negotiate::negotiate_server_shared_state(
                stack,
                base_udp.listen(addr).await?,
                Arc::clone(&negotiation_state),
            )?;
            serve_with_external(addr, st, negotiation_state, internal_srv, line_processor).await
        }
        EncrSpec::AutoOnly => {
            let enc_stack = encr_stack!(addr, cert);
            let stack = CxList::from(SerializeChunnel::default()).wrap(enc_stack);
            let st = bertha::negotiate::negotiate_server_shared_state(
                stack,
                base_udp.listen(addr).await?,
                Arc::clone(&negotiation_state),
            )?;
            serve_with_external(addr, st, negotiation_state, internal_srv, line_processor).await
        }
        EncrSpec::TlsOnly => {
            let enc_stack = encr_stack!(tls => addr, cert);
            let stack = CxList::from(SerializeChunnel::default()).wrap(enc_stack);
            let st = bertha::negotiate::negotiate_server_shared_state(
                stack,
                base_udp.listen(addr).await?,
                Arc::clone(&negotiation_state),
            )?;
            serve_with_external(addr, st, negotiation_state, internal_srv, line_processor).await
        }
        EncrSpec::QuicOnly => {
            let enc_stack = encr_stack!(quic => addr, cert);
            let stack = CxList::from(SerializeChunnel::default()).wrap(enc_stack);
            let st = bertha::negotiate::negotiate_server_shared_state(
                stack,
                base_udp.listen(addr).await?,
                Arc::clone(&negotiation_state),
            )?;
            serve_with_external(addr, st, negotiation_state, internal_srv, line_processor).await
        }
    }
}

#[instrument(skip(cn, line_processor), level = "info", err)]
async fn serve_one_cn(
    cn: impl ChunnelConnection<Data = (SocketAddr, Line)> + Send + 'static,
    line_processor: &Arc<impl ProcessLine<(SocketAddr, Line)> + Send + Sync + 'static>,
) -> Result<(), Report> {
    let mut slots: Vec<_> = (0..16).map(|_| None).collect();
    let mut acks = Vec::with_capacity(16);
    debug!("new connection");
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
        acks.clear();
        acks.extend(
            msgs.iter()
                .filter_map(|m| m.as_ref().map(|(a, _)| (*a, Line::Ack))),
        );
        line_processor
            .process_lines(msgs)
            .await
            .map_err(Into::into)?;
        cn.send(acks.drain(..)).await?;
        trace!("done processing batch");
    }
}

#[instrument(skip(internal_cli, cert), level = "info", err)]
async fn serve_canonical(
    si: ShardInfo<SocketAddr>,
    internal_cli: RendezvousChannel<SocketAddr, Vec<u8>, Cln>,
    listen_addr: SocketAddr,
    redis_addr: String,
    cert: Arc<Certificate>,
    encr_spec: EncrSpec,
) -> Result<(), Report> {
    async fn inner<S, C>(si: ShardInfo<SocketAddr>, st: S) -> Result<(), Report>
    where
        S: Stream<Item = Result<C, Report>>,
        C: ChunnelConnection<Data = ()> + Send + 'static,
    {
        info!(shard_info = ?&si, "ready");
        let ctr: Arc<AtomicUsize> = Default::default();
        let st = std::pin::pin!(st);
        st.try_for_each_concurrent(None, |r| {
            let ctr = Arc::clone(&ctr);
            let mut slot = [None];
            async move {
                let ctr = ctr.fetch_add(1, Ordering::SeqCst);
                info!(?ctr, "starting shard-canonical-server-connection");
                loop {
                    trace!("calling recv");
                    match r
                        .recv(&mut slot) // ShardCanonicalServerConnection is recv-only
                        .instrument(trace_span!("shard-canonical-server-connection", ?ctr))
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
    let mut base_udp = bertha::udp::UdpReqChunnel::default();
    let st = base_udp.listen(listen_addr).await?;

    match encr_spec {
        EncrSpec::AllowNone => {
            let enc_stack = encr_stack!(si.canonical_addr, cert);
            let stack = CxList::from(cnsrv)
                .wrap(SerializeChunnel::<Line>::default())
                .wrap(Select::from((Nothing::<()>::default(), enc_stack)));
            let st = bertha::negotiate_server(stack, st)
                .await
                .wrap_err("negotiate_server")?;
            inner(si, st).await
        }
        EncrSpec::AutoOnly => {
            let enc_stack = encr_stack!(si.canonical_addr, cert);
            let stack = CxList::from(cnsrv)
                .wrap(SerializeChunnel::<Line>::default())
                .wrap(enc_stack);
            let st = bertha::negotiate_server(stack, st)
                .await
                .wrap_err("negotiate_server")?;
            inner(si, st).await
        }
        EncrSpec::TlsOnly => {
            let enc_stack = encr_stack!(tls => si.canonical_addr, cert);
            let stack = CxList::from(cnsrv)
                .wrap(SerializeChunnel::<Line>::default())
                .wrap(enc_stack);
            let st = bertha::negotiate_server(stack, st)
                .await
                .wrap_err("negotiate_server")?;
            inner(si, st).await
        }
        EncrSpec::QuicOnly => {
            let enc_stack = encr_stack!(quic => si.canonical_addr, cert);
            let stack = CxList::from(cnsrv)
                .wrap(SerializeChunnel::<Line>::default())
                .wrap(enc_stack);
            let st = bertha::negotiate_server(stack, st)
                .await
                .wrap_err("negotiate_server")?;
            inner(si, st).await
        }
    }
}

pub async fn serve_local(
    listen_addr: SocketAddr,
    hostname: impl ToString,
    localname_root: Option<PathBuf>,
    encr_spec: EncrSpec,
    recvs: impl ProcessLine<EstOutputRateHist> + Send + Sync + 'static,
) -> Result<(), Report> {
    let cert = Arc::new(
        rcgen::generate_simple_self_signed([hostname.to_string(), listen_addr.ip().to_string()])
            .wrap_err("test certificate generation failed")?,
    );
    let mut base_udp = bertha::udp::UdpReqChunnel::default();
    let st = base_udp.listen(listen_addr).await?;
    let local_chunnel = if let Some(lr) = localname_root {
        Some(
            LocalNameChunnel::server(
                lr.clone(),
                listen_addr,
                UnixSkChunnel::with_root(lr),
                bertha::CxNil,
            )
            .await?,
        )
    } else {
        None
    };

    macro_rules! serve {
        (local => $stack: expr, $base: expr) => {{
            let st = bertha::negotiate_server($stack, $base)
                .await
                .wrap_err("negotiate_server")?;
            let st = st.map_ok(|cn| {
                let a = cn.peer_addr().unwrap();
                ProjectLeft::new(Either::Left(a), cn)
            });
            serve_local_inner(listen_addr, st, recvs).await
        }};
        (nolocal => $stack: expr, $base: expr) => {{
            let st = bertha::negotiate_server($stack, $base)
                .await
                .wrap_err("negotiate_server")?;
            let st = st.map_ok(|cn| {
                let a = cn.peer_addr().unwrap();
                ProjectLeft::new(a, cn)
            });
            serve_local_inner(listen_addr, st, recvs).await
        }};
    }

    match (encr_spec, local_chunnel) {
        (EncrSpec::AllowNone, Some(lch)) => {
            let enc = encr_stack!(listen_addr, cert);
            let stack = CxList::from(EstOutputRateSerializeChunnel)
                .wrap(lch)
                .wrap(Select::from((Nothing::<()>::default(), enc)));
            serve!(local => stack, st)
        }
        (EncrSpec::AllowNone, None) => {
            let enc = encr_stack!(listen_addr, cert);
            let stack = CxList::from(EstOutputRateSerializeChunnel)
                .wrap(Select::from((Nothing::<()>::default(), enc)));
            serve!(nolocal => stack, st)
        }
        (EncrSpec::AutoOnly, Some(lch)) => {
            let enc = encr_stack!(listen_addr, cert);
            let stack = CxList::from(EstOutputRateSerializeChunnel)
                .wrap(lch)
                .wrap(enc);
            serve!(local => stack, st)
        }
        (EncrSpec::AutoOnly, None) => {
            let enc = encr_stack!(listen_addr, cert);
            let stack = CxList::from(EstOutputRateSerializeChunnel).wrap(enc);
            serve!(nolocal => stack, st)
        }
        (EncrSpec::TlsOnly, Some(lch)) => {
            let enc = encr_stack!(tls => listen_addr, cert);
            let stack = CxList::from(EstOutputRateSerializeChunnel)
                .wrap(lch)
                .wrap(enc);
            serve!(local => stack, st)
        }
        (EncrSpec::TlsOnly, None) => {
            let enc = encr_stack!(tls => listen_addr, cert);
            let stack = CxList::from(EstOutputRateSerializeChunnel).wrap(enc);
            serve!(nolocal => stack, st)
        }
        (EncrSpec::QuicOnly, Some(lch)) => {
            let enc = encr_stack!(quic => listen_addr, cert);
            let stack = CxList::from(EstOutputRateSerializeChunnel)
                .wrap(lch)
                .wrap(enc);
            serve!(local => stack, st)
        }
        (EncrSpec::QuicOnly, None) => {
            let enc = encr_stack!(quic => listen_addr, cert);
            let stack = CxList::from(EstOutputRateSerializeChunnel).wrap(enc);
            serve!(nolocal => stack, st)
        }
    }
}

async fn serve_local_inner<S, C>(
    listen_addr: SocketAddr,
    st: S,
    recvs: impl ProcessLine<EstOutputRateHist> + Send + Sync + 'static,
) -> Result<(), Report>
where
    S: Stream<Item = Result<C, Report>>,
    C: ChunnelConnection<Data = EstOutputRateHist>,
{
    info!(?listen_addr, "ready");
    let st = std::pin::pin!(st);
    let recvs = Arc::new(recvs);
    st.try_for_each_concurrent(None, |cn| {
        let mut slot = [None];
        let recvs = Arc::clone(&recvs);
        async move {
            loop {
                let ms = cn
                    .recv(&mut slot)
                    .await
                    .wrap_err("serve_local: inner connection recv")?;
                if ms.is_empty() || ms[0].is_none() {
                    continue;
                }

                recvs
                    .process_lines(ms)
                    .await
                    .map_err(Into::into)
                    .wrap_err("serve_local: process lines handler")?;
            }
        }
    })
    .await
    .wrap_err("serve_local: stream error")?;
    unreachable!()
}
