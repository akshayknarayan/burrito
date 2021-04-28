//! Server side.

use crate::kv::Store;
use crate::msg::Msg;
use crate::reliability::KvReliabilityServerChunnel;
use bertha::{
    bincode::SerializeChunnelProject, chan_transport::RendezvousChannel, either::DataEither,
    negotiate::Offer, reliable::ReliabilityProjChunnel, select::SelectListener,
    tagger::OrderedChunnelProj, ChunnelConnection, ChunnelConnector, ChunnelListener, CxList,
    GetOffers, Select,
};
use burrito_shard_ctl::{ShardInfo, SimpleShardPolicy};
use color_eyre::eyre::{Report, WrapErr};
use futures_util::stream::{FuturesUnordered, Stream, StreamExt, TryStreamExt};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{atomic::AtomicUsize, Arc};
use tracing::{debug, debug_span, info, info_span, trace, warn};
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
            Connection = impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
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

    let shard_stack = CxList::from(
        Select::from((
            CxList::from(OrderedChunnelProj::default())
                .wrap(ReliabilityProjChunnel::default())
                .wrap(SerializeChunnelProject::default()),
            CxList::from(KvReliabilityServerChunnel::default())
                .wrap(SerializeChunnelProject::default()),
        ))
        .prefer_right(),
    );
    let mut offer: Vec<_> = shard_stack.offers().collect();
    let redis_addr = format!("redis://{}:{}", redis_addr.ip(), redis_addr.port());

    // TODO write sharding optimization to make this route raw packets.
    // If the ordered |> reliable path is picked, we can't do the optimization because the bytes
    // have semantics. `KvReliabilityServerChunnel` is basically a no-op though, so we can
    // pass-through without the serialization.
    let cnsrv = burrito_shard_ctl::ShardCanonicalServer::new(
        si.clone(),
        Some(shards_internal),
        udp_to_shard::UdpToShard(shard_connector),
        shard_stack,
        offer.pop().unwrap(),
        &redis_addr,
    )
    .await
    .wrap_err("Create ShardCanonicalServer")?;
    let external = CxList::from(cnsrv).wrap(
        Select::from((
            CxList::from(OrderedChunnelProj::default())
                .wrap(ReliabilityProjChunnel::default())
                .wrap(SerializeChunnelProject::default()),
            CxList::from(KvReliabilityServerChunnel::default())
                .wrap(SerializeChunnelProject::default()),
        ))
        .prefer_right(),
    );
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
        tokio::spawn(async move {
            let ctr = ctr;
            loop {
                match r
                    .recv() // ShardCanonicalServerConnection is recv-only
                    .instrument(debug_span!("shard-canonical-server-connection", ?ctr))
                    .await
                    .wrap_err("kvstore/server: Error while processing requests")
                {
                    Ok(x) => {
                        let _: Option<(_, Msg)> = x;
                    }
                    Err(e) => {
                        warn!(err = ?e, ?ctr, "exiting");
                        break;
                    }
                }
            }
        });
        ctr += 1;
    }

    unreachable!() // negotiate_server never returns None
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
    internal_srv: impl ChunnelListener<
            Addr = SocketAddr,
            Connection = impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
            Error = impl Into<Report> + Send + Sync + 'static,
        > + Clone
        + Send
        + Sync
        + 'static,
    need_address_embedding: bool,
    s: impl Into<Option<tokio::sync::oneshot::Sender<Vec<HashMap<u64, Offer>>>>>,
) {
    let s = s.into();
    let internal_addr = internal_addr.unwrap_or(addr);
    info!(?addr, ?internal_addr, "listening");

    async fn srv<C, Sc, Se>(
        st: Sc,
        s: Option<tokio::sync::oneshot::Sender<Vec<HashMap<u64, Offer>>>>,
    ) where
        Sc: Stream<Item = Result<C, Se>> + Send + 'static,
        C: ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
        Se: Into<Report> + Send + Sync + 'static,
    {
        let external = Select::from((
            CxList::from(OrderedChunnelProj::default())
                .wrap(ReliabilityProjChunnel::default())
                .wrap(SerializeChunnelProject::default()),
            CxList::from(KvReliabilityServerChunnel::default())
                .wrap(SerializeChunnelProject::default()),
        ))
        .prefer_right();
        let offers = external.clone().offers().collect();
        let st = bertha::negotiate::negotiate_server(external, st)
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
            let mut pending_sends = FuturesUnordered::new();

            tokio::spawn(async move {
                debug!("new");
                loop {
                    trace!("call recv");
                    use futures_util::future::Either;
                    let recv_val = match futures_util::future::select(pending_sends.next(), cn.recv()).await {
                        Either::Left((Some(Ok(_)), _)) => None,
                        Either::Left((Some(Err(e)), _)) => {
                            warn!(err = ?e, ?idx, "exiting on send error");
                            break;
                        }
                        Either::Left((None, f)) => Some(f.await),
                        Either::Right((inc, _)) => Some(inc),
                    };

                    match recv_val {
                        Some(Ok((a, msg @ Msg {..}))) => {
                            trace!(msg = ?&msg, from=?&a, pending_sends = pending_sends.len(), "got msg");
                            let rsp = store.call(msg);
                            let id = rsp.id;
                            let send_fut = cn.send((a, rsp));
                            pending_sends.push(async move {
                                send_fut.await?;
                                trace!(msg_id = id, "sent response");
                                Ok::<_, Report>(())
                            });
                        }
                        Some(Err(e)) => {
                            warn!(err = ?e, ?idx, "exiting on recv error");
                            break;
                        }
                        None => (),
                    }
                }
            }
            .instrument(debug_span!("shard_connection", idx = ?idx)));
        }

        unreachable!();
    }

    if need_address_embedding {
        let st = SelectListener::new(raw_listener, udp_to_shard::UdpToShard(internal_srv))
            .separate_addresses()
            .listen((addr, internal_addr))
            .await
            .map_err::<Report, _>(Into::into)
            .unwrap();
        srv(st, s).await
    } else {
        let st = SelectListener::new(raw_listener, internal_srv)
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
            single_shard(a, raw_listener.clone(), None, int_srv, false, s)
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
            CxList::from(OrderedChunnelProj::default())
                .wrap(ReliabilityProjChunnel::default())
                .wrap(SerializeChunnelProject::default()),
            CxList::from(KvReliabilityServerChunnel::default())
                .wrap(SerializeChunnelProject::default()),
        ))
        .prefer_right(),
    );

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
    let external = CxList::from(cnsrv).wrap(
        Select::from((
            CxList::from(OrderedChunnelProj::default())
                .wrap(ReliabilityProjChunnel::default())
                .wrap(SerializeChunnelProject::default()),
            CxList::from(KvReliabilityServerChunnel::default())
                .wrap(SerializeChunnelProject::<Msg>::default()),
        ))
        .prefer_right(),
    );
    info!(shard_info = ?&si, "start canonical server");
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
        tokio::spawn(async move {
            let ctr = ctr;
            loop {
                match r
                    .recv() // ShardCanonicalServerConnection is recv-only
                    .instrument(debug_span!("shard-canonical-server-connection", ?ctr))
                    .await
                    .wrap_err("kvstore/server: Error while processing requests")
                {
                    Ok(x) => {
                        let _: Option<(_, Msg)> = x;
                    }
                    Err(e) => {
                        warn!(err = ?e, ?ctr, "exiting");
                        break;
                    }
                }
            }
        });
        ctr += 1;
    }

    unreachable!() // negotiate_server never returns None
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
    use bertha::{util::ProjectLeft, ChunnelConnection, ChunnelConnector, ChunnelListener};
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
    pub struct UdpToShard<I>(pub I);

    impl<I, E> ChunnelConnector for UdpToShard<I>
    where
        I: ChunnelConnector<Addr = (), Error = E> + Clone + Send + Sync + 'static,
        E: Into<Report> + Send + Sync + 'static,
        UdpToShardCn<ProjectLeft<SocketAddr, <I as ChunnelConnector>::Connection>>:
            ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
    {
        type Addr = SocketAddr;
        type Connection =
            UdpToShardCn<ProjectLeft<SocketAddr, <I as ChunnelConnector>::Connection>>;
        type Future =
            Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
        type Error = Report;

        fn connect(&mut self, a: Self::Addr) -> Self::Future {
            let mut ctr = self.0.clone();
            Box::pin(async move {
                Ok(UdpToShardCn(ProjectLeft::new(
                    a,
                    ctr.connect(()).await.map_err(Into::into)?,
                )))
            })
        }
    }

    impl<I, E> ChunnelListener for UdpToShard<I>
    where
        I: ChunnelListener<Addr = SocketAddr, Error = E> + Clone + Send + Sync + 'static,
        E: Into<Report> + Send + Sync + 'static,
        UdpToShardCn<ProjectLeft<SocketAddr, <I as ChunnelListener>::Connection>>:
            ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
    {
        type Addr = SocketAddr;
        type Connection = UdpToShardCn<ProjectLeft<SocketAddr, <I as ChunnelListener>::Connection>>;
        type Future =
            Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
        type Stream =
            Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;
        type Error = Report;

        fn listen(&mut self, a: Self::Addr) -> Self::Future {
            let mut lis = self.0.clone();
            Box::pin(async move {
                let l = lis.listen(a).await.map_err(Into::into)?;
                // ProjectLeft a is a dummy, the UdpConn will ignore it and replace with the
                // req-connection source addr.
                Ok(Box::pin(
                    l.map(move |cn| Ok(UdpToShardCn(ProjectLeft::new(a, cn.map_err(Into::into)?)))),
                ) as _)
            })
        }
    }

    #[derive(Debug, Clone)]
    pub struct UdpToShardCn<C>(C);

    impl<C> ChunnelConnection for UdpToShardCn<C>
    where
        C: ChunnelConnection<Data = Vec<u8>> + Send + Sync + 'static,
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
