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
use tracing::{debug, debug_span, info, info_span, trace, warn};
use tracing_futures::Instrument;

/// Start and serve a load balancer, which just forwards connections to existing shards.
pub async fn serve_lb(
    mut raw_listener: impl ChunnelListener<
            Addr = SocketAddr,
            Connection = impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
            Error = impl Into<Report> + Send + Sync + 'static,
        > + Clone
        + Send
        + 'static,
    addr: SocketAddr,
    shards: Vec<SocketAddr>,
    shards_internal: Vec<SocketAddr>,
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

    let shard_stack = CxList::from(
        Select::from((
            CxList::from(OrderedChunnelProj::default()).wrap(ReliabilityProjChunnel::default()),
            KvReliabilityServerChunnel::default(),
        ))
        .inner_type::<DataEither<bertha::reliable::Pkt<Msg>, Msg>>()
        .prefer_right(),
    )
    .wrap(SerializeChunnelProject::default());
    let mut offer: Vec<_> = shard_stack.offers().collect();
    let redis_addr = format!("redis://{}:{}", redis_addr.ip(), redis_addr.port());
    let cnsrv = burrito_shard_ctl::ShardCanonicalServer::new(
        si.clone(),
        Some(shards_internal),
        udp_to_shard::UdpToShard,
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
    .instrument(info_span!("negotiate_server"))
    .await
    .wrap_err("kvstore/server: Error while processing requests")?;
    unreachable!()
}

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
    internal_srv: Option<
        impl ChunnelListener<
                Addr = SocketAddr,
                Connection = impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)>
                                 + Send
                                 + Sync
                                 + 'static,
                Error = impl Into<Report> + Send + Sync + 'static,
            > + Send
            + 'static,
    >,
    s: tokio::sync::oneshot::Sender<Vec<HashMap<u64, Offer>>>,
) {
    let internal_addr = internal_addr.unwrap_or(addr);
    info!(?addr, ?internal_addr, "listening");

    async fn srv<C, Sc, Se>(st: Sc, s: tokio::sync::oneshot::Sender<Vec<HashMap<u64, Offer>>>)
    where
        Sc: Stream<Item = Result<C, Se>> + Send + 'static,
        C: ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
        Se: Into<Report> + Send + Sync + 'static,
    {
        let external = CxList::from(
            Select::from((
                CxList::from(OrderedChunnelProj::default()).wrap(ReliabilityProjChunnel::default()),
                KvReliabilityServerChunnel::default(),
            ))
            .inner_type::<DataEither<bertha::reliable::Pkt<Msg>, Msg>>()
            .prefer_right(),
        )
        .wrap(SerializeChunnelProject::default());
        let offers = external.clone().offers().collect();
        let st = bertha::negotiate::negotiate_server(external, st)
            .await
            .unwrap();
        s.send(offers).unwrap();

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
        .await {
            warn!(err = ?e, "Shard errorred");
            panic!("{}", e);
        }
    }

    if let Some(is) = internal_srv {
        let st = SelectListener::new(raw_listener, is)
            .separate_addresses()
            .listen((addr, internal_addr))
            .await
            .map_err::<Report, _>(Into::into)
            .unwrap();
        srv(st, s).await
    } else {
        let st = SelectListener::new(raw_listener, udp_to_shard::UdpToShard)
            .separate_addresses()
            .listen((addr, internal_addr))
            .await
            .map_err::<Report, _>(Into::into)
            .unwrap();
        srv(st, s).await
    }
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
            single_shard(a, raw_listener.clone(), None, Some(int_srv), s)
                .instrument(debug_span!("shardsrv", addr = ?&a)),
        );
        rdy.push(r);
    }

    let mut offers: Vec<Vec<HashMap<u64, Offer>>> = rdy.try_collect().await.unwrap();

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
        .instrument(info_span!("negotiate_server"))
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
    .instrument(info_span!("negotiate_server"))
    .await
    .wrap_err("kvstore/server: Error while processing requests")?;
    unreachable!()
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

mod udp_to_shard {
    use bertha::{
        udp::{UdpReqChunnel, UdpSkChunnel},
        util::ProjectLeft,
        ChunnelConnection, ChunnelConnector, ChunnelListener,
    };
    use color_eyre::eyre::{eyre, Report};
    use futures_util::stream::{Stream, StreamExt};
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
    use std::{future::Future, pin::Pin};

    /// Shim address semantics.
    ///
    /// `ShardCanonicalServer` wants a `impl ChunnelConnector<Addr = A, Connection = impl
    /// ChunnelConnection<Data = (A, Vec<u8>)>>`. But, the `A` in the connection is "fake": it's
    /// meant to be passed to the shard, which will echo it, so we know which address to send the
    /// response to.
    ///
    /// So, we `ProjectLeft` a shard address onto the connection, and put the client address into
    /// the data type.
    #[derive(Debug, Clone, Default)]
    pub(crate) struct UdpToShard;

    impl ChunnelConnector for UdpToShard {
        type Addr = SocketAddr;
        type Connection =
            UdpToShardCn<ProjectLeft<SocketAddr, <UdpSkChunnel as ChunnelConnector>::Connection>>;
        type Future =
            Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
        type Error = Report;

        fn connect(&mut self, a: Self::Addr) -> Self::Future {
            Box::pin(async move {
                Ok(UdpToShardCn(ProjectLeft::new(
                    a,
                    UdpSkChunnel::default().connect(()).await?,
                )))
            })
        }
    }

    impl ChunnelListener for UdpToShard {
        type Addr = SocketAddr;
        type Connection =
            UdpToShardCn<ProjectLeft<SocketAddr, <UdpReqChunnel as ChunnelListener>::Connection>>;
        type Future =
            Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
        type Stream =
            Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;
        type Error = Report;

        fn listen(&mut self, a: Self::Addr) -> Self::Future {
            Box::pin(async move {
                let l = UdpReqChunnel::default().listen(a).await?;
                // ProjectLeft a is a dummy, the UdpConn will ignore it and replace with the
                // req-connection source addr.
                Ok(Box::pin(l.map(move |cn| Ok(UdpToShardCn(ProjectLeft::new(a, cn?))))) as _)
            })
        }
    }

    pub(crate) struct UdpToShardCn<C>(C);

    impl<C> ChunnelConnection for UdpToShardCn<C>
    where
        C: ChunnelConnection<Data = Vec<u8>>,
    {
        type Data = (SocketAddr, Vec<u8>);

        fn send(
            &self,
            (addr, mut data): Self::Data,
        ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
            // stick the addr in the front of data.
            let ip = addr.ip();
            let port = addr.port();
            let p = port.to_be_bytes();
            match ip {
                IpAddr::V4(v4) => {
                    let addr_bytes = v4.octets();
                    let addr_bytes_len = addr_bytes.len() as u8; // either 8 or 16 will fit.
                    let i = std::iter::once(addr_bytes_len)
                        .chain(p.iter().copied())
                        .chain(addr_bytes.iter().copied());
                    data.splice(0..0, i);
                }
                IpAddr::V6(v6) => {
                    let addr_bytes = v6.octets();
                    let addr_bytes_len = addr_bytes.len() as u8; // either 8 or 16 will fit.
                    let i = std::iter::once(addr_bytes_len)
                        .chain(p.iter().copied())
                        .chain(addr_bytes.iter().copied());
                    data.splice(0..0, i);
                }
            };

            self.0.send(data)
        }

        fn recv(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
            let f = self.0.recv();
            Box::pin(async move {
                let mut d = f.await?;
                if d.len() < 7 {
                    tracing::warn!("bad payload");
                    return Err(eyre!("Bad payload, no address"));
                }

                let mut p = [0u8; 2];
                p.copy_from_slice(&d[1..3]);
                let port = u16::from_be_bytes(p);
                match d[0] {
                    4 => {
                        let mut a = [0u8; 4];
                        a.copy_from_slice(&d[3..7]);
                        let addr = Ipv4Addr::from(a);
                        let sa = SocketAddr::new(IpAddr::V4(addr), port);
                        d.splice(0..7, std::iter::empty());
                        return Ok((sa, d));
                    }
                    16 => {
                        if d.len() < 19 {
                            return Err(eyre!("Bad payload, no address"));
                        }

                        let mut a = [0u8; 16];
                        a.copy_from_slice(&d[3..19]);
                        let sa = SocketAddr::new(IpAddr::V6(Ipv6Addr::from(a)), port);
                        d.splice(0..19, std::iter::empty());
                        return Ok((sa, d));
                    }
                    _ => {
                        return Err(eyre!("Bad payload, no address"));
                    }
                }
            })
        }
    }
}
