//! Server side.

use crate::kv::Store;
use crate::msg::Msg;
use crate::reliability::KvReliabilityServerChunnel;
use bertha::{
    bincode::SerializeChunnelProject, chan_transport::RendezvousChannel, either::DataEither,
    negotiate::Offer, reliable::ReliabilityProjChunnel, select::SelectListener,
    tagger::OrderedChunnelProj, ChunnelConnection, ChunnelListener, CxList, GetOffers, Select,
};
use burrito_shard_ctl::{ShardInfo, SimpleShardPolicy};
use color_eyre::eyre::{eyre, Report, WrapErr};
use futures_util::stream::{FuturesUnordered, Stream, StreamExt, TryStreamExt};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{atomic::AtomicUsize, Arc};
use tracing::{debug, debug_span, info, trace, warn};
use tracing_futures::Instrument;

/// Start and serve a `ShardCanonicalServer` and shards.
///
/// `raw_listener`: A ChunnelListener that can return `Data = (SocketAddr, Vec<u8>)`
/// `ChunnelConnection`s.
/// `redis_addr`: Address of a redis instance.
/// `srv_ip`: Local ip to serve on.
/// `srv_port`: Local port to serve on.
/// `num_shards`: Number of shards to start. Shard addresses are selected sequentially after
/// `srv_port`, but this could change.
/// `ready`: An optional notification for after setup and negotiation are done and before we start serving.
pub async fn serve(
    mut raw_listener: impl ChunnelListener<
            Addr = SocketAddr,
            Connection = impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
            Error = impl Into<Report> + Send + Sync + 'static,
        > + Clone
        + Send
        + 'static,
    redis_addr: SocketAddr,
    srv_ip: IpAddr,
    srv_port: u16,
    num_shards: u16,
    ready: impl Into<Option<tokio::sync::oneshot::Sender<()>>>,
) -> Result<(), Report> {
    // 1. Define addr.
    let si = make_shardinfo(srv_ip, srv_port, num_shards);

    // 2. start shard serv
    let (internal_srv, internal_cli) = RendezvousChannel::<SocketAddr, _, _>::new(100).split();
    let rdy = futures_util::stream::FuturesUnordered::new();
    for a in si.clone().shard_addrs {
        info!(addr = ?&a, "start shard");
        let (s, r) = tokio::sync::oneshot::channel();
        let int_srv = internal_srv.clone();
        tokio::spawn(
            single_shard(a, raw_listener.clone(), int_srv, s)
                .instrument(debug_span!("shardsrv", addr = ?&a)),
        );
        rdy.push(r);
    }

    let mut offers: Vec<Vec<HashMap<u64, Offer>>> = rdy.try_collect().await.unwrap();

    let st = raw_listener
        .listen(si.canonical_addr)
        .await
        .map_err(Into::into)
        .wrap_err("Listen on UdpReqChunnel")?;
    serve_canonical(
        si,
        st.map_err(Into::into),
        internal_cli,
        redis_addr,
        offers.pop().unwrap(),
        ready,
    )
    .await
}

async fn serve_canonical(
    si: ShardInfo<SocketAddr>,
    st: impl Stream<
            Item = Result<
                impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
                Report,
            >,
        > + Send
        + 'static,
    internal_cli: RendezvousChannel<SocketAddr, Vec<u8>, bertha::chan_transport::Cln>,
    redis_addr: SocketAddr,
    mut offer: Vec<HashMap<u64, Offer>>,
    ready: impl Into<Option<tokio::sync::oneshot::Sender<()>>>,
) -> Result<(), Report> {
    // 3. start canonical server
    let redis_addr = format!("redis://{}:{}", redis_addr.ip(), redis_addr.port());
    let shard_stack = CxList::from(
        Select::from((
            CxList::from(OrderedChunnelProj::default()).wrap(ReliabilityProjChunnel::default()),
            KvReliabilityServerChunnel::default(),
        ))
        .inner_type::<DataEither<bertha::reliable::Pkt<Msg>, Msg>>()
        .prefer_right(),
    )
    .wrap(SerializeChunnelProject::default());

    #[cfg(not(feature = "ebpf"))]
    let cnsrv = burrito_shard_ctl::ShardCanonicalServer::new(
        si.clone(),
        internal_cli,
        shard_stack,
        offer.pop().unwrap(),
        &redis_addr,
    )
    .await
    .wrap_err("Create ShardCanonicalServer")?;

    #[cfg(feature = "ebpf")]
    let cnsrv = burrito_shard_ctl::ShardCanonicalServerEbpf::new(
        si.clone(),
        internal_cli,
        shard_stack,
        offer.pop().unwrap(),
        &redis_addr,
    )
    .await
    .wrap_err("Create ShardCanonicalServer")?;

    let external = CxList::from(cnsrv)
        .wrap(
            Select::from((
                CxList::from(OrderedChunnelProj::default()).wrap(ReliabilityProjChunnel::default()),
                KvReliabilityServerChunnel::default(),
            ))
            .inner_type::<DataEither<bertha::reliable::Pkt<Msg>, Msg>>()
            .prefer_right(),
        )
        .wrap(SerializeChunnelProject::default());
    info!(shard_info = ?&si, "start canonical server");
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
                let _: Option<(_, Msg)> = r
                    .recv()
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
    raw_listener: impl ChunnelListener<
        Addr = SocketAddr,
        Connection = impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
        Error = impl Into<Report> + Send + Sync + 'static,
    >,
    internal_srv: RendezvousChannel<SocketAddr, Vec<u8>, bertha::chan_transport::Srv>,
    s: tokio::sync::oneshot::Sender<Vec<HashMap<u64, Offer>>>,
) {
    let external = CxList::from(
        Select::from((
            CxList::from(OrderedChunnelProj::default()).wrap(ReliabilityProjChunnel::default()),
            KvReliabilityServerChunnel::default(),
        ))
        .inner_type::<DataEither<bertha::reliable::Pkt<Msg>, Msg>>()
        .prefer_right(),
    )
    .wrap(SerializeChunnelProject::default());
    let stack = external.clone();
    info!(addr = ?&addr, "listening");
    let st = SelectListener::new(raw_listener, internal_srv)
        .listen(addr)
        .await
        .unwrap();
    debug!("got raw connection");
    let st = bertha::negotiate::negotiate_server(external, st)
        .await
        .unwrap();
    s.send(stack.offers().collect()).unwrap();

    // initialize the kv store.
    let store = Store::default();
    let idx = Arc::new(AtomicUsize::new(0));
    if let Err(e) = st
        .try_for_each_concurrent(None, |cn| {
            let idx = idx.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let store = store.clone();
            // TODO deduplicate possible spurious retxs by req id
            let mut pending_sends = FuturesUnordered::new();
            async move {
                debug!("new");
                loop {
                    trace!("call recv");
                    tokio::select!(
                        inc = cn.recv() => {
                            let (a, msg): (_, Msg) =
                                inc.wrap_err(eyre!("receive message error"))?;
                            trace!(msg = ?&msg, from=?&a, pending_sends = pending_sends.len(), "got msg");
                            let rsp = store.call(msg);
                            let id = rsp.id;
                            let send_fut = cn.send((a, rsp));
                            pending_sends.push(async move {
                                send_fut.await.wrap_err(eyre!("send response err"))?;
                                trace!(msg_id = id, "sent response");
                                Ok::<_, Report>(())
                            });
                        }
                        Some(send_res) = pending_sends.next() => {
                            send_res?;
                        }
                    );
                }
            }
            .instrument(debug_span!("shard_connection", idx = ?idx))
        })
        .instrument(debug_span!("negotiate_server"))
        .await
    {
        warn!(err = ?e, "Shard errorred");
        panic!("{}", e);
    }
}

fn make_shardinfo(srv_ip: IpAddr, srv_port: u16, num_shards: u16) -> ShardInfo<SocketAddr> {
    let shard_addrs = (1..=num_shards)
        .map(|i| SocketAddr::new(srv_ip, srv_port + i))
        .collect();
    ShardInfo {
        canonical_addr: SocketAddr::new(srv_ip, srv_port),
        shard_addrs,
        // TODO fix this
        shard_info: SimpleShardPolicy {
            packet_data_offset: 18,
            packet_data_length: 4,
        },
    }
}
