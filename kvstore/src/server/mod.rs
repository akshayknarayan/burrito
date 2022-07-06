//! Server side.

use crate::kv::Store;
use crate::msg::Msg;
use crate::reliability::KvReliabilityServerChunnel;
use bertha::{
    bincode::SerializeChunnel, chan_transport::RendezvousChannel, negotiate::StackNonce,
    select::SelectListener, ChunnelConnection, ChunnelListener, CxList, GetOffers,
};
use burrito_shard_ctl::{ShardInfo, SimpleShardPolicy};
use color_eyre::eyre::{Report, WrapErr};
use futures_util::stream::{Stream, TryStreamExt};
use std::fmt::Debug;
use std::net::{IpAddr, SocketAddr};
use std::sync::{atomic::AtomicUsize, Arc};
use tracing::{debug, debug_span, info, info_span, trace, warn};
use tracing_futures::Instrument;

mod serve_lb;
mod udp_to_shard;

pub use serve_lb::serve_lb;
pub use udp_to_shard::UdpToShard;

/// Start and serve a single shard.
///
/// `addr`: Public address to listen on
/// `raw_listener`: Chunnel to receive public messages from clients on.
/// `internal_addr`: Address to listen for forwarded messages from `ShardCanonicalServer` on. If
/// `None`, same as `addr`. Note: be careful of trying to bind twice to the same address.
/// `internal_srv`: Chunnel to receive messages from `ShardCanonicalServer` on.
/// `s`: Will send the offers on this channel when ready to listen for connections. This is needed
/// so that `ShardCanonicalServer` can open negotiation with us if it hears from the client first,
/// since we expect a negotiation handshake here.
pub async fn single_shard(
    addr: SocketAddr,
    raw_listener: impl ChunnelListener<
            Addr = SocketAddr,
            Connection = impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
            Error = impl Into<Report> + Send + Sync + 'static,
        > + Send
        + 'static,
    internal_addr: Option<SocketAddr>,
    internal_srv: impl ChunnelListener<
            Addr = SocketAddr,
            Connection = impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
            Error = impl Into<Report> + Send + Sync + 'static,
        > + Clone
        + Send
        + Sync
        + 'static,
    need_address_embedding: bool,
    s: impl Into<Option<tokio::sync::oneshot::Sender<Vec<StackNonce>>>>,
    no_negotiation: bool,
) {
    let s = s.into();
    let internal_addr = internal_addr.unwrap_or(addr);
    info!(?addr, ?internal_addr, "listening");

    macro_rules! serve_stack {
        ($st_make: path, $stack: expr, $st: expr) => {{
            let offers = $stack.clone().offers().collect();
            let st = $st_make($stack, $st) // bertha::negotiate::negotiate_server
                .await
                .unwrap();

            if let Some(s) = s {
                s.send(offers).unwrap();
            }

            // initialize the kv store.
            let store = Store::default();
            let idx = Arc::new(AtomicUsize::new(0));

            tokio::pin!(st);
            loop {
                let cn = match st
                    .try_next()
                    .instrument(debug_span!("negotiate_server"))
                    .await
                {
                    Ok(Some(cn)) => cn,
                    Err(err) => {
                        warn!(?err, "Could not accept connection");
                        break;
                    }
                    Ok(None) => unreachable!(),
                };

                let idx = idx.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let store = store.clone();
                // TODO deduplicate possible spurious retxs by req id

                tokio::spawn(
                        serve_one(cn, store, idx, 16)
                        .instrument(debug_span!("shard_connection", idx = ?idx)),
                );
            }
        }}
    }

    if need_address_embedding {
        let st = SelectListener::new(raw_listener, udp_to_shard::UdpToShard(internal_srv))
            .separate_addresses()
            .listen((addr, internal_addr))
            .await
            .map_err::<Report, _>(Into::into)
            .unwrap();
        let external =
            CxList::from(KvReliabilityServerChunnel::default()).wrap(SerializeChunnel::default());
        serve_stack!(bertha::negotiate::negotiate_server, external, st)
    } else {
        let st = SelectListener::new(raw_listener, internal_srv)
            .separate_addresses()
            .listen((addr, internal_addr))
            .await
            .map_err::<Report, _>(Into::into)
            .unwrap();
        if no_negotiation {
            let external = SerializeChunnel::default();
            serve_stack!(make_cn, external, st)
        } else {
            let external = CxList::from(KvReliabilityServerChunnel::default())
                .wrap(SerializeChunnel::default());
            serve_stack!(bertha::negotiate::negotiate_server, external, st)
        }
    }

    unreachable!()
}

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
/// `batching`: Which [`BatchMode`] to use.
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
    no_negotiation: bool,
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
            single_shard(
                a,
                raw_listener.clone(),
                None,
                int_srv,
                false,
                s,
                no_negotiation,
            )
            .instrument(debug_span!("shardsrv", addr = ?&a)),
        );
        rdy.push(r);
    }

    if no_negotiation {
        info!("no negotiation: no serve_canonical needed");
        futures_util::future::pending().await
    } else {
        let mut offers: Vec<Vec<StackNonce>> = rdy.try_collect().await.unwrap();

        let st = raw_listener
            .listen(si.canonical_addr)
            .await
            .map_err(Into::into)
            .wrap_err("Listen on raw_listener")?;
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
    mut offer: Vec<StackNonce>,
    ready: impl Into<Option<tokio::sync::oneshot::Sender<()>>>,
) -> Result<(), Report> {
    let redis_addr = format!("redis://{}:{}", redis_addr.ip(), redis_addr.port());

    macro_rules! serve_stack {
        ($negotiator: path, $stack: expr) => {{
            info!(shard_info = ?&si, "start canonical server");
            let st = $negotiator($stack, st) // bertha::negotiate::negotiate_server
                .instrument(info_span!("negotiate_server"))
                .await
                .wrap_err("negotiate_server")?;

            if let Some(ready) = ready.into() {
                ready.send(()).unwrap_or_default();
            }

            let mut ctr = 0usize;
            tokio::pin!(st);
            while let Some(r) = st
                .try_next()
                    .instrument(info_span!("negotiate_server"))
                    .await?
            {
                tokio::spawn(async move {
                    let ctr = ctr;
                    let mut slot = [None];
                    loop {
                        match r
                            .recv(&mut slot) // ShardCanonicalServerConnection is recv-only
                            .instrument(debug_span!("shard-canonical-server-connection", ?ctr))
                            .await
                            .wrap_err("kvstore/server: Error while processing requests")
                            {
                                Ok(_) => {}
                                Err(e) => {
                                    warn!(err = ?e, ?ctr, "exiting");
                                    break;
                                }
                            }
                    }
                });
                ctr += 1;
            }
        }}
    }

    let shard_stack =
        CxList::from(KvReliabilityServerChunnel::default()).wrap(SerializeChunnel::default());

    #[cfg(not(feature = "ebpf"))]
    let cnsrv = burrito_shard_ctl::ShardCanonicalServer::new(
        si.clone(),
        None,
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
        None,
        internal_cli,
        shard_stack,
        offer.pop().unwrap(),
        &redis_addr,
    )
    .await
    .wrap_err("Create ShardCanonicalServer")?;
    let external = CxList::from(cnsrv)
        .wrap(KvReliabilityServerChunnel::default())
        .wrap(SerializeChunnel::<Msg>::default());
    serve_stack!(bertha::negotiate::negotiate_server, external);

    unreachable!() // negotiate_server never returns None
}

async fn serve_one(
    cn: impl ChunnelConnection<Data = (SocketAddr, Msg)> + Send + Sync + 'static,
    store: Store,
    idx: usize,
    batch_size: usize,
) {
    let mut slots: Vec<_> = (0..batch_size).map(|_| None).collect();
    debug!("new");
    loop {
        trace!("call recv");
        let msgs = match cn.recv(&mut slots).await {
            Ok(ms) => ms,
            Err(e) => {
                warn!(err = ?e, ?idx, "exiting on recv error");
                break;
            }
        };

        trace!(sz = ?msgs.iter().map_while(|x| x.as_ref().map(|_| 1)).sum::<usize>(), "got batch");

        match cn
            .send(
                msgs.into_iter()
                    .map_while(Option::take)
                    .map(|(a, msg @ Msg { .. })| {
                        let rsp = store.call(msg);
                        (a, rsp)
                    }),
            )
            .await
        {
            Ok(_) => (),
            Err(e) => {
                warn!(err = ?e, ?idx, "exiting on send error");
                break;
            }
        }
    }
}

use and_then_concurrent::TryStreamAndThenExt;
use bertha::Chunnel;
#[allow(clippy::manual_async_fn)] // we need the + 'static which async fn does not do.
pub fn make_cn<Srv, Sc, Se, C>(
    stack: Srv,
    raw_cn_st: Sc,
) -> impl std::future::Future<
    Output = Result<
        impl Stream<Item = Result<<Srv as Chunnel<C>>::Connection, Report>> + Send + 'static,
        Report,
    >,
> + Send
       + 'static
where
    Sc: Stream<Item = Result<C, Se>> + Send + 'static,
    Se: Into<Report> + Send + Sync + 'static,
    C: ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
    Srv: Chunnel<C> + Clone + Debug + Send + 'static,
    <Srv as Chunnel<C>>::Connection: Send + Sync + 'static,
    <Srv as Chunnel<C>>::Error: Into<Report> + Send + Sync + 'static,
{
    async move {
        // 1. serve (A, Vec<u8>) connections.
        let st = raw_cn_st.map_err(Into::into); // stream of incoming Vec<u8> conns.
        Ok(st
            .map_err(Into::into)
            .and_then_concurrent(move |cn| {
                debug!("make_cn: new cn");
                let mut stack = stack.clone();
                async move { Ok(Some(stack.connect_wrap(cn).await.map_err(Into::into)?)) }
            })
            .try_filter_map(|v| futures_util::future::ready(Ok(v))))
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
