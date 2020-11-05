//! Server side.

use crate::kv::Store;
use crate::msg::Msg;
use bertha::{
    bincode::SerializeChunnelProject,
    chan_transport::RendezvousChannel,
    reliable::ReliabilityProjChunnel,
    select::SelectListener,
    tagger::OrderedChunnelProj,
    udp::{UdpReqChunnel, UdpSkChunnel},
    ChunnelConnection, ChunnelConnector, ChunnelListener, CxList, GetOffers,
};
use burrito_shard_ctl::{ShardCanonicalServer, ShardInfo, SimpleShardPolicy};
use color_eyre::eyre::{eyre, Report, WrapErr};
use futures_util::{future::poll_fn, stream::TryStreamExt};
use std::net::{IpAddr, SocketAddr};
use std::sync::{atomic::AtomicUsize, Arc};
use tower_buffer::Buffer;
use tower_service::Service;
use tracing::{debug, debug_span, info, trace, warn};
use tracing_futures::Instrument;

/// Start and serve a `ShardCanonicalServer` and shards.
///
/// `redis_addr`: Address of a redis instance.
/// `srv_port`: Local port to serve on.
/// `num_shards`: Number of shards to start. Shard addresses are selected sequentially after
/// `srv_port`, but this could change.
pub async fn serve(
    redis_addr: SocketAddr,
    srv_ip: IpAddr,
    srv_port: u16,
    num_shards: u16,
    ready: impl Into<Option<tokio::sync::oneshot::Sender<()>>>,
) -> Result<(), Report> {
    let shard_addrs = (1..=num_shards)
        .map(|i| SocketAddr::new(srv_ip, srv_port + i))
        .collect();
    // 1. Define addr.
    let si: ShardInfo<SocketAddr> = ShardInfo {
        canonical_addr: SocketAddr::new(srv_ip, srv_port),
        shard_addrs,
        // TODO fix this
        shard_info: SimpleShardPolicy {
            packet_data_offset: 18,
            packet_data_length: 4,
        },
    };

    // 2. start shard serv
    let (internal_srv, internal_cli) = RendezvousChannel::<SocketAddr, _, _>::new(100).split();
    let rdy = futures_util::stream::FuturesUnordered::new();
    for a in si.clone().shard_addrs {
        info!(addr = ?&a, "start shard");
        let (s, r) = tokio::sync::oneshot::channel();
        let int_srv = internal_srv.clone();
        tokio::spawn(single_shard(a, int_srv, s).instrument(debug_span!("shardsrv", addr = ?&a)));
        rdy.push(r);
    }

    let mut offers: Vec<Vec<Vec<bertha::negotiate::Offer>>> = rdy.try_collect().await.unwrap();

    // 3. start canonical server
    // TODO Ebpf chunnel
    let redis_addr = format!("redis://{}:{}", redis_addr.ip(), redis_addr.port());
    let shards_extern = UdpSkChunnel.connect(()).await.unwrap();
    let cnsrv = ShardCanonicalServer::new(
        si.clone(),
        internal_cli,
        CxList::from(OrderedChunnelProj::default())
            .wrap(ReliabilityProjChunnel::default())
            .wrap(SerializeChunnelProject::default()),
        shards_extern,
        offers.pop().unwrap(),
        &redis_addr,
    )
    .await
    .wrap_err("Create ShardCanonicalServer")?;

    // UdpConn: (SocketAddr, Vec<u8>)
    // ProjectLeft: (SocketAddr, Vec<u8>) -> Vec<u8>
    // SerializeChunnel: Vec<u8> -> (u32, Option<Msg>)
    // ReliabilityChunnel: (u32, Option<Msg>) -> (u32, Msg)
    // OrderedChunnel: (u32, Msg) -> Msg
    // ShardCanonicalServer: Msg -> ()
    let external = CxList::from(cnsrv)
        .wrap(OrderedChunnelProj::default())
        .wrap(ReliabilityProjChunnel::<_, Msg>::default())
        .wrap(SerializeChunnelProject::<_, (u32, Option<Msg>)>::default());
    info!(shard_info = ?&si, "start canonical server");
    let st = UdpReqChunnel::default()
        .listen(si.canonical_addr)
        .await
        .wrap_err("Listen on UdpReqChunnel")?;
    let st = bertha::negotiate::negotiate_server(external, st)
        .instrument(tracing::info_span!("negotiate_server"))
        .await
        .wrap_err("negotiate_server")?;

    if let Some(ready) = ready.into() {
        ready.send(()).unwrap_or_default();
    }

    st.try_for_each_concurrent(None, |r| {
        async move {
            loop {
                r.recv()
                    .instrument(debug_span!("shard-canonical-server-connection"))
                    .await?; // ShardCanonicalServerConnection is recv-only
            }
        }
    })
    .instrument(tracing::info_span!("negotiate_server"))
    .await
    .wrap_err("kvstore/server: Error while processing requests")?;
    unreachable!()
}

/// Start and serve a single shard.
///
/// `addr`: Public address to listen on
/// `internal_srv`: Channel to receive messages from `ShardCanonicalServer` on.
/// `s`: Will send the offers on this channel when ready to listen for connections. This is needed
/// so that `ShardCanonicalServer` can open negotiation with us if it hears from the client first,
/// since we expect a negotiation handshake here.
async fn single_shard(
    addr: SocketAddr,
    internal_srv: RendezvousChannel<SocketAddr, Vec<u8>, bertha::chan_transport::Srv>,
    s: tokio::sync::oneshot::Sender<Vec<Vec<bertha::negotiate::Offer>>>,
) {
    let external = CxList::from(OrderedChunnelProj::default())
        .wrap(ReliabilityProjChunnel::default())
        .wrap(SerializeChunnelProject::default());
    let stack = external.clone();
    info!(addr = ?&addr, "listening");
    let st = SelectListener::new(UdpReqChunnel::default(), internal_srv)
        .listen(addr)
        .await
        .unwrap();
    debug!("got raw connection");
    let st = bertha::negotiate::negotiate_server(external, st)
        .await
        .unwrap();
    s.send(stack.offers()).unwrap();

    // initialize the kv store.
    let store = Buffer::new(Store::default(), 100_000);
    let idx = Arc::new(AtomicUsize::new(0));
    if let Err(e) = st
        .try_for_each_concurrent(None, |cn| {
            let idx = idx.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut store = store.clone();
            async move {
                debug!("new");
                loop {
                    let (a, msg): (_, Msg) =
                        cn.recv().await.wrap_err(eyre!("receive message error"))?;
                    debug!(msg = ?&msg, "got msg");

                    poll_fn(|cx| store.poll_ready(cx))
                        .await
                        .map_err(|e| eyre!(e))?;
                    trace!(msg = ?&msg, "poll_ready for store");
                    let rsp = store.call(msg).await.unwrap();

                    cn.send((a, rsp))
                        .await
                        .wrap_err(eyre!("send response err"))?;
                    debug!("sent response");
                }
            }
            .instrument(debug_span!("shard_connection", idx = ?idx))
        })
        .instrument(debug_span!("negotiate_server"))
        .await
    {
        warn!(shard_addr = ?addr, err = ?e, "Shard errorred");
        panic!(e);
    }
}
