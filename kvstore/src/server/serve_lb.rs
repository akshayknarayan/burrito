use crate::msg::Msg;
use crate::reliability::KvReliabilityServerChunnel;
use crate::server::udp_to_shard;
use bertha::{
    bincode::SerializeChunnel, ChunnelConnection, ChunnelConnector, ChunnelListener, CxList,
    GetOffers,
};
use burrito_shard_ctl::{ShardInfo, SimpleShardPolicy};
use color_eyre::eyre::{Report, WrapErr};
use futures_util::stream::TryStreamExt;
use std::net::SocketAddr;
use tracing::{debug_span, info, info_span, warn};
use tracing_futures::Instrument;

/// Start and serve a load balancer, which just forwards connections to existing shards.
pub async fn serve_lb(
    addr: SocketAddr,
    shards: Vec<SocketAddr>,
    mut raw_listener: impl ChunnelListener<
            Addr = SocketAddr,
            Connection = impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
            Error = impl Into<Report> + Send + Sync + 'static,
        > + Clone
        + Send
        + 'static,
    shards_internal: Vec<SocketAddr>,
    shard_connector: impl ChunnelConnector<
            Addr = (),
            Connection = impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)>
                             + Clone
                             + Send
                             + Sync
                             + 'static,
            Error = impl Into<Report> + Send + Sync + 'static,
        > + Clone
        + Send
        + Sync
        + 'static,
    redis_addr: SocketAddr,
    ready: impl Into<Option<tokio::sync::oneshot::Sender<()>>>,
) -> Result<(), Report> {
    let si = ShardInfo {
        canonical_addr: addr,
        shard_addrs: shards,
        shard_info: SimpleShardPolicy {
            packet_data_offset: 18,
            packet_data_length: 4,
        },
    };

    info!(?si, "starting serve_lb");

    let shard_stack =
        CxList::from(KvReliabilityServerChunnel::default()).wrap(SerializeChunnel::default());
    let mut offer: Vec<_> = shard_stack.offers().collect();
    let redis_addr = format!("redis://{}:{}", redis_addr.ip(), redis_addr.port());

    let cnsrv = burrito_shard_ctl::ShardCanonicalServer::<_, _, _, (SocketAddr, Msg)>::new(
        si.clone(),
        Some(shards_internal),
        udp_to_shard::UdpToShard(shard_connector),
        shard_stack,
        offer.pop().unwrap(),
        &redis_addr,
    )
    .await
    .wrap_err("Create ShardCanonicalServer")?;
    //use crate::opt::SerdeOpt;
    let external = CxList::from(cnsrv)
        .wrap(KvReliabilityServerChunnel::default())
        .wrap(SerializeChunnel::default());
    //let external = external.serde_opt();
    let st = raw_listener
        .listen(si.canonical_addr)
        .await
        .map_err(Into::into)
        .wrap_err("Listen on raw_listener")?;
    let st = bertha::negotiate::negotiate_server(external, st)
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
        tokio::spawn(
            async move {
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
            }
            .instrument(debug_span!("serve_lb_conn")),
        );
        ctr += 1;
    }

    unreachable!() // negotiate_server never returns None
}