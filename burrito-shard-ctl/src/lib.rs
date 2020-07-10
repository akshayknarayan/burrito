mod client;
pub use client::*;
pub mod proto;

pub const CONTROLLER_ADDRESS: &str = "shard-ctl";

#[cfg(feature = "bin")]
pub use srv::*;

#[cfg(feature = "bin")]
mod srv {
    use crate::proto;
    use eyre::{eyre, Error};
    use std::collections::HashMap;
    use std::pin::Pin;
    use std::sync::Arc;
    use tokio::sync::{Mutex, RwLock};
    use tracing::{debug, trace, warn};

    /// Keep track of sharded services.
    #[derive(Clone)]
    pub struct ShardCtl {
        // canonical addr -> shard info
        shard_table: Arc<RwLock<HashMap<proto::Addr, proto::ShardInfo>>>,
        // Interface name -> handles
        #[cfg(feature = "ebpf")]
        ingress_handles: Arc<Mutex<HashMap<String, xdp_shard::BpfHandles<xdp_shard::Ingress>>>>,
        redis_client: redis::Client,
        redis_listen_connection: Arc<Mutex<redis::aio::Connection>>,
        disc_cl: Arc<Mutex<Option<burrito_discovery_ctl::client::DiscoveryClient>>>,
    }

    impl ShardCtl {
        pub async fn new(
            redis_addr: &str,
            root: Option<std::path::PathBuf>,
        ) -> Result<Self, Error> {
            let redis_client = redis::Client::open(redis_addr)?;
            let redis_listen_connection =
                Arc::new(Mutex::new(redis_client.get_async_connection().await?));

            let s = ShardCtl {
                redis_client,
                redis_listen_connection,
                shard_table: Default::default(),
                #[cfg(feature = "ebpf")]
                ingress_handles: Default::default(),
                disc_cl: Default::default(),
            };

            let dcl = s.disc_cl.clone();
            tokio::spawn(async move {
                use burrito_discovery_ctl::client::DiscoveryClient;
                use std::path::Path;
                use tokio::time;

                let root = root.unwrap_or_else(|| Path::new("/tmp/burrito").to_path_buf());

                let start = time::Instant::now();
                loop {
                    time::delay_for(time::Duration::from_millis(100)).await;

                    if start.elapsed() > time::Duration::from_secs(5) {
                        // time out.
                        debug!(root = ?&root, "Timed out connecting to discovery-ctl");
                        return None;
                    }

                    if let Ok(d) = DiscoveryClient::new(&root).await {
                        *dcl.lock().await = Some(d);
                        return Some(());
                    }
                }
            });

            Ok(s)
        }

        pub async fn serve_on<S, E>(
            self,
            sk: S,
            #[cfg_attr(not(feature = "ebpf"), allow(unused_variables))] stats_log: Option<
                std::fs::File,
            >,
        ) -> Result<(), Error>
        where
            S: tokio::stream::Stream<Item = Result<tokio::net::UnixStream, E>>,
            E: std::error::Error,
        {
            use futures_util::{sink::Sink, stream::StreamExt};
            let con = self
                .redis_client
                .get_async_connection()
                .await
                .expect("get redis connection");
            tokio::spawn(listen_updates(self.shard_table.clone(), con));
            #[cfg(feature = "ebpf")]
            tokio::spawn(read_shard_stats(self.ingress_handles.clone(), stats_log));

            sk.for_each_concurrent(None, |st| async {
                let st = st.expect("accept failed");
                let peer = st.peer_addr();
                let st: async_bincode::AsyncBincodeStream<_, proto::Request, proto::Reply, _> =
                    st.into();
                let mut st = st.for_async();
                loop {
                    let req = st.next().await;
                    match req {
                        Some(Err(e)) => {
                            warn!(err = ?e, "Error processing message");
                            break;
                        }
                        None => {
                            trace!("st.next() gave None");
                            break;
                        }
                        _ => (),
                    }

                    let rep = async {
                        match req {
                            Some(Ok(proto::Request::Register(si))) => {
                                let rep = self.register(si).await;
                                proto::Reply::Register(rep)
                            }
                            Some(Ok(proto::Request::Query(sa))) => {
                                let rep = self.query(sa).await;
                                proto::Reply::Query(rep)
                            }
                            _ => unreachable!(),
                        }
                    };

                    let (rep, rdy): (proto::Reply, _) = futures_util::future::join(
                        rep,
                        futures_util::future::poll_fn(|cx| {
                            let pst = Pin::new(&mut st);
                            pst.poll_ready(cx)
                        }),
                    )
                    .await;
                    rdy.expect("poll_ready");

                    trace!(response = ?&rep, "start_send");
                    let pst = Pin::new(&mut st);
                    pst.start_send(rep).expect("start_send");

                    trace!("poll_flush");
                    futures_util::future::poll_fn(|cx| {
                        let pst = Pin::new(&mut st);
                        pst.poll_flush(cx)
                    })
                    .await
                    .expect("poll_flush");
                }

                debug!(client = ?peer, "Stream done");
            })
            .await;
            Ok(())
        }

        // Instead of tower_service::call(), do this, which lets us have async fns
        // instead of fn foo() -> Pin<Box<dyn Future<..>>> everywhere
        pub async fn register(&self, req: proto::ShardInfo) -> proto::RegisterShardReply {
            fn check(si: &proto::ShardInfo) -> Result<(), Error> {
                for a in si.shard_addrs.iter() {
                    match a {
                        proto::Addr::Udp(_) | proto::Addr::Burrito(_) => (),
                        a => {
                            return Err(eyre!("Must pass either name or Udp address: {}", a));
                        }
                    }
                }

                match &si.canonical_addr {
                    proto::Addr::Udp(_) | proto::Addr::Burrito(_) => (),
                    a => {
                        return Err(eyre!("Must pass either name or Udp address: {}", a));
                    }
                };

                Ok(())
            }

            match check(&req) {
                Ok(_) => (),
                Err(e) => return proto::RegisterShardReply::Err(format!("{}", e)),
            }

            let redis_conn = self.redis_listen_connection.clone();
            if let Err(e) = self.shard_table_insert(req.clone()).await {
                return proto::RegisterShardReply::Err(format!(
                    "Could not insert shard-service: {}",
                    e
                ));
            }

            debug!(req = ?&req, "Registered shard");

            let r = req.clone();
            tokio::spawn(async move {
                if let Err(e) = redis_insert(redis_conn, &r).await {
                    warn!(req = ?&r, err = ?e, "Could not do redis insert");
                }
            });

            #[cfg(feature = "ebpf")]
            {
                async fn register_resolved_shardinfo(
                    handles: Arc<Mutex<HashMap<String, xdp_shard::BpfHandles<xdp_shard::Ingress>>>>,
                    si: proto::ShardInfo,
                ) {
                    use tracing::info;

                    let shard_ports: Vec<u16> = si
                        .shard_addrs
                        .iter()
                        .map(|a| match a {
                            proto::Addr::Udp(s) => s.port(),
                            _ => unreachable!(),
                        })
                        .collect();

                    let sk = match si.canonical_addr {
                        proto::Addr::Udp(sk) => sk,
                        _ => unreachable!(),
                    };

                    // see if handles are there already
                    let ifnames = match xdp_shard::get_interface_name(sk.ip()) {
                        Ok(ifnames) => ifnames,
                        Err(e) => {
                            warn!(err = ?e, sk = ?sk, "Error getting interface");
                            return;
                        }
                    };

                    let mut map = handles.lock().await;
                    for ifn in ifnames {
                        if !map.contains_key(&ifn) {
                            let l =
                                xdp_shard::BpfHandles::<xdp_shard::Ingress>::load_on_interface_name(
                                    &ifn,
                                );
                            let handle = match l {
                                Ok(s) => s,
                                Err(e) => {
                                    warn!(err = ?e, ifn = ?&ifn, "Error loading xdp-shard");
                                    return;
                                }
                            };

                            info!(service = ?si.canonical_addr, "Loaded XDP program");

                            map.insert(ifn.clone(), handle);
                        }

                        let h = map.get_mut(&ifn).unwrap();
                        let ok = h.shard_ports(
                            sk.port(),
                            &shard_ports[..],
                            si.shard_info.packet_data_offset,
                            si.shard_info.packet_data_length,
                        );
                        match ok {
                            Ok(_) => (),
                            Err(e) => {
                                warn!(
                                    err = ?e,
                                    ifn = ?&ifn,
                                    port = ?sk.port(),
                                    shard_port = ?&shard_ports[..],
                                    "Error writing xdp-shard ports",
                                );
                                return;
                            }
                        }

                        info!(
                            service = ?si.canonical_addr,
                            interface = ?&ifn,
                            "Registered sharding with XDP program"
                        );
                    }
                }

                use crate::{clear_port, resolve_shardinfo};

                if req.shard_addrs.is_empty() {
                    match &req.canonical_addr {
                        proto::Addr::Udp(sk) | proto::Addr::Tcp(sk) => {
                            clear_port(self.ingress_handles.clone(), *sk).await;
                        }
                        proto::Addr::Burrito(_) => {
                            let h = self.ingress_handles.clone();
                            let dc = self.disc_cl.clone();
                            tokio::spawn(async move {
                                let ca = match resolve_shardinfo(dc, req).await? {
                                    proto::ShardInfo {
                                        canonical_addr: proto::Addr::Udp(sk),
                                        ..
                                    } => sk,
                                    _ => return None,
                                };

                                clear_port(h, ca).await;
                                Some(())
                            });
                        }
                        _ => (),
                    }

                    return proto::RegisterShardReply::Ok;
                }

                let h = self.ingress_handles.clone();
                let dc = self.disc_cl.clone();
                tokio::spawn(async move {
                    let si = resolve_shardinfo(dc, req).await?;
                    register_resolved_shardinfo(h, si).await;
                    Some(())
                });
            }

            proto::RegisterShardReply::Ok
        }

        pub async fn query(&self, req: proto::QueryShardRequest) -> proto::QueryShardReply {
            // TODO if necessary, install client-side xdp program
            if let Some(s) = self.shard_table.read().await.get(&req.canonical_addr) {
                proto::QueryShardReply::Ok(s.clone())
            } else {
                proto::QueryShardReply::Err(format!("Could not find {}", &req.canonical_addr))
            }
        }

        async fn shard_table_insert(&self, serv: proto::ShardInfo) -> Result<(), Error> {
            let sn = serv.canonical_addr.clone();
            if let Some(s) = self.shard_table.write().await.insert(sn.clone(), serv) {
                Err(eyre!(
                    "Shard service address {} already in use at: {}",
                    sn,
                    s.canonical_addr
                ))?;
            };

            Ok(())
        }
    }

    #[cfg(feature = "ebpf")]
    async fn read_shard_stats(
        handles: Arc<Mutex<HashMap<String, xdp_shard::BpfHandles<xdp_shard::Ingress>>>>,
        mut stats_log: Option<std::fs::File>,
    ) -> Result<(), Error> {
        use tracing::info;

        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        let mut first = true;
        let start = std::time::Instant::now();
        loop {
            interval.tick().await;
            for (inf, handle) in handles.lock().await.iter_mut() {
                match handle.get_stats() {
                    Ok((c, p)) => {
                        let mut c = c.get_rxq_cpu_port_count();
                        xdp_shard::diff_maps(&mut c, &p.get_rxq_cpu_port_count());
                        trace!("logging XDP stats");
                        // c: rxq_id -> cpu_id -> port -> count
                        for (rxq_id, cpu_map) in c.iter().enumerate() {
                            for (cpu_id, port_map) in cpu_map.iter().enumerate() {
                                for (port, count) in port_map.iter() {
                                    info!(
                                        interface = ?inf,
                                        rxq = ?rxq_id,
                                        cpu = ?cpu_id,
                                        port = ?port,
                                        count = ?count,
                                        "XDP stats"
                                    );

                                    if let Some(ref mut f) = stats_log {
                                        use std::io::Write;
                                        if first {
                                            if let Err(e) = write!(f, "Time CPU Rxq Port Count\n") {
                                                debug!(err = ?e, "Failed writing to stats_log");
                                                continue;
                                            }

                                            first = false;
                                        }

                                        if let Err(e) = write!(
                                            f,
                                            "{} {} {} {} {}\n",
                                            start.elapsed().as_secs_f32(),
                                            cpu_id,
                                            rxq_id,
                                            port,
                                            count
                                        ) {
                                            debug!(err = ?e, "Failed writing to stats_log");
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        debug!(err = ?e, "Could not get rxq stats");
                    }
                }
            }
        }
    }

    async fn listen_updates(
        shard_table: Arc<RwLock<HashMap<proto::Addr, proto::ShardInfo>>>,
        mut con: redis::aio::Connection,
    ) {
        // usual blah blah warning about polling intervals
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
        loop {
            interval.tick().await;
            if let Err(e) = poll_updates(&shard_table, &mut con).await {
                warn!(err = ?e, "Failed to poll shard-services updates");
            } else {
                trace!("Updated shard-services");
            }
        }
    }

    async fn poll_updates(
        shard_table: &RwLock<HashMap<proto::Addr, proto::ShardInfo>>,
        con: &mut redis::aio::Connection,
    ) -> Result<(), Error> {
        use redis::AsyncCommands;
        let srvs: Vec<String> = con.smembers("shard-services").await?;

        // TODO fast-path check if a write is even necessary?

        let mut tbl = shard_table.write().await;
        for srv in srvs {
            let k = srv.trim_start_matches("shard:");
            if let Ok(s) = k.parse::<proto::Addr>() {
                if !tbl.contains_key(&s) {
                    let sh_info_blob = con.get::<_, Vec<u8>>(&srv).await?;
                    let sh_info: proto::ShardInfo = bincode::deserialize(&sh_info_blob)?;
                    if s == sh_info.canonical_addr {
                        tbl.insert(s.to_owned(), sh_info);
                    } else {
                        warn!(
                            redis_addr = ?s,
                            shardinfo_addr = ?sh_info.canonical_addr,
                            "Mismatch error with redis key <-> ShardInfo",
                        );
                    }
                }
            } else {
                warn!(redis_key = ?srv, addr = ?k, "Parse error with redis key -> Addr");
            }
        }

        Ok(())
    }

    async fn redis_insert(
        conn: Arc<Mutex<redis::aio::Connection>>,
        serv: &proto::ShardInfo,
    ) -> Result<(), Error> {
        let name = format!("shard:{}", serv.canonical_addr);
        let mut r = redis::pipe();
        r.atomic()
            .cmd("SADD")
            .arg("shard-services")
            .arg(&name)
            .ignore();

        let shard_blob = bincode::serialize(serv)?;
        r.cmd("SET").arg(&name).arg(shard_blob).ignore();

        r.query_async(&mut *conn.lock().await).await?;
        Ok(())
    }
}

use eyre::{eyre, Error};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, warn};

#[cfg_attr(not(feature = "ebpf"), allow(unused))]
use std::collections::HashMap;

#[cfg(feature = "ebpf")]
async fn clear_port<T>(
    handles: Arc<Mutex<HashMap<String, xdp_shard::BpfHandles<T>>>>,
    sk: std::net::SocketAddr,
) {
    let mut map = handles.lock().await;
    for h in map.values_mut() {
        if let Err(e) = h.clear_port(sk.port()) {
            debug!(err = ?e, sock = ?sk, "Failed to clear port map");
        }
    }
}

/// Resolve any burrito addresses of a ShardInfo to Udp addresses.
///
/// Return `None` if any of the addresses are not Udp, or if some other error occurred.
pub async fn resolve_shardinfo(
    dcl: Arc<Mutex<Option<burrito_discovery_ctl::client::DiscoveryClient>>>,
    mut si: proto::ShardInfo,
) -> Option<proto::ShardInfo> {
    async fn try_resolve_once(
        dcl: &mut burrito_discovery_ctl::client::DiscoveryClient,
        addr: &mut proto::Addr,
    ) -> Result<bool, Error> {
        match addr {
            proto::Addr::Burrito(name) => {
                if let Ok(m) = dcl.query(name.clone()).await {
                    for srv in m.services {
                        if srv.service == burrito_discovery_ctl::CONTROLLER_ADDRESS {
                            match srv.address {
                                a @ proto::Addr::Udp(_) => {
                                    std::mem::replace(addr, a);
                                    return Ok(true);
                                }
                                // other cases are not possible, so we error
                                a => {
                                    return Err(eyre!(
                                        "Must resolve to Udp address: {} -> {}",
                                        addr,
                                        a
                                    ))
                                }
                            }
                        }
                    }
                }
                return Ok(false);
            }
            _ => Ok(true),
        }
    }

    use tokio::time;
    let start = time::Instant::now();
    loop {
        time::delay_for(time::Duration::from_millis(100)).await;

        if start.elapsed() > time::Duration::from_secs(5) {
            // time out.
            debug!(shard = ?si, "Timed out resolving shardinfo for xdp setup");
            return None;
        }

        if let Some(ref mut dcl) = *dcl.lock().await {
            // 1. first check the canonical_addr
            if let ref mut ca @ proto::Addr::Burrito(_) = si.canonical_addr {
                match try_resolve_once(dcl, ca).await {
                    Ok(true) => {}
                    Ok(false) => continue,
                    Err(e) => {
                        warn!(err = ?e, addr = ?ca, "Error resolving addr");
                        return None;
                    }
                }
            }

            // 2. next check the shard addrs
            for mut sa in si.shard_addrs.iter_mut() {
                if let ref mut sa @ proto::Addr::Burrito(_) = sa {
                    match try_resolve_once(dcl, sa).await {
                        Ok(true) => {}
                        Ok(false) => continue,
                        Err(e) => {
                            warn!(err = ?e, addr = ?sa, "Error resolving addr");
                            return None;
                        }
                    }
                }
            }

            // 3. if we reach here, we are good to go!
            break;
        } else {
            // There is no disc-cl (yet) - just check if we are already done.
            // 1. first check the canonical_addr
            if let proto::Addr::Burrito(_) = si.canonical_addr {
                continue;
            }

            // 2. next check the shard addrs
            for sa in si.shard_addrs.iter_mut() {
                if let proto::Addr::Burrito(_) = sa {
                    continue;
                }
            }

            // 3. if we reach here, we are good to go!
            break;
        }
    }

    Some(si)
}

#[cfg(test)]
mod tests {
    use crate::proto;
    use eyre::Error;
    use slog::debug;
    use std::sync::Arc;
    use test_util::*;
    use tokio::sync::Mutex;

    #[test]
    fn resolve_shardinfo_test() -> Result<(), Error> {
        let log = test_logger();
        let root: std::path::PathBuf = "./tmp-bn-resolve_shardinfo_test".parse().unwrap();
        reset_root_dir(&root, &log);

        let mut rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async move {
            let l = log.clone();
            let redis = start_redis(&l, 28174);
            let redis_addr = redis.get_addr();
            let ra = redis_addr.clone();
            let root2 = root.clone();
            block_for(std::time::Duration::from_millis(400)).await; // wait for redis

            tokio::spawn(async move {
                burrito_discovery_ctl::ctl::serve_ctl(
                    Some(root2.clone()),
                    &ra,
                    vec!["127.0.0.1".parse().unwrap()],
                    true,
                )
                .await
            });
            block_for(std::time::Duration::from_millis(400)).await; // wait for discovery-ctl

            let names: Vec<proto::Addr> = (0..7)
                .map(|i| format!("burrito://shard-{}", i).parse().unwrap())
                .collect();
            let si = proto::ShardInfo {
                canonical_addr: "burrito://shardtest".parse().unwrap(),
                shard_addrs: names[0..].into(),
                shard_info: proto::SimpleShardPolicy {
                    packet_data_offset: 18,
                    packet_data_length: 4,
                },
            };

            // start server
            start_disc_server(root.as_path(), &names[..], 12897).await;
            block_for(std::time::Duration::from_millis(200)).await;

            let adcl = Arc::new(Mutex::new(None));

            let adcl1 = adcl.clone();
            let l2 = log.clone();
            tokio::spawn(async move {
                block_for(std::time::Duration::from_secs(1)).await;
                debug!(&l2, "connecting to burrito controller"; "burrito_root" => ?&root);
                let dcl = burrito_discovery_ctl::client::DiscoveryClient::new(&root)
                    .await
                    .expect("Connect client discoveryclient");
                *adcl1.lock().await = Some(dcl);
            });

            let si = super::resolve_shardinfo(adcl, si).await.unwrap();

            assert!(matches!(
                si,
                proto::ShardInfo {
                    canonical_addr: proto::Addr::Udp(ca),
                    shard_info: proto::SimpleShardPolicy {
                        packet_data_offset: 18,
                        packet_data_length: 4,
                    },
                    ..
                } if ca.ip().is_loopback() && ca.port() == 12897
            ));

            for (p, r) in si
                .shard_addrs
                .iter()
                .map(|a| match a {
                    burrito_shard_ctl::proto::Addr::Udp(sa) if sa.ip().is_loopback() => sa.port(),
                    _ => panic!("shard addresses wrong"),
                })
                .zip(12898..12905)
            {
                assert_eq!(p, r);
            }

            Ok(())
        })
    }

    async fn start_disc_server(root: &std::path::Path, names: &[proto::Addr], start: u16) {
        use burrito_shard_ctl::proto;
        let addrs: Vec<proto::Addr> = (0..8)
            .map(|i| format!("udp://127.0.0.1:{}", start + i).parse().unwrap())
            .collect();

        {
            let mut dcl = burrito_discovery_ctl::client::DiscoveryClient::new(root.clone())
                .await
                .expect("Connect client discoveryclient");

            dcl.register(burrito_discovery_ctl::proto::Service {
                name: "shardtest".to_owned(),
                scope: burrito_discovery_ctl::proto::Scope::Global,
                service: burrito_discovery_ctl::CONTROLLER_ADDRESS.to_owned(),
                address: addrs[0].clone(),
            })
            .await
            .expect("Could not register discovery-ctl");

            // register all the shards
            for (n, a) in names.iter().zip(addrs[1..].iter()) {
                let s = match n {
                    burrito_discovery_ctl::proto::Addr::Burrito(s) => s,
                    _ => unreachable!(),
                };

                dcl.register(burrito_discovery_ctl::proto::Service {
                    name: s.clone(),
                    scope: burrito_discovery_ctl::proto::Scope::Global,
                    service: burrito_discovery_ctl::CONTROLLER_ADDRESS.to_owned(),
                    address: a.clone(),
                })
                .await
                .expect("Could not register discovery-ctl");
            }
        }
    }

    #[test]
    fn already_done_resolve_shardinfo_test() -> Result<(), Error> {
        let mut rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async move {
            let names: Vec<proto::Addr> = (0..7)
                .map(|i| format!("udp://127.0.0.1:{}", 12315 + i).parse().unwrap())
                .collect();
            let si = proto::ShardInfo {
                canonical_addr: "udp://127.0.0.1:12314".parse().unwrap(),
                shard_addrs: names[0..].into(),
                shard_info: proto::SimpleShardPolicy {
                    packet_data_offset: 18,
                    packet_data_length: 4,
                },
            };

            let adcl = Arc::new(Mutex::new(None));
            super::resolve_shardinfo(adcl, si).await.unwrap();
            Ok(())
        })
    }

    #[test]
    fn failed_resolve_shardinfo_test() -> Result<(), Error> {
        let mut rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async move {
            let names: Vec<proto::Addr> = (0..7)
                .map(|i| format!("burrito://shard-{}", i).parse().unwrap())
                .collect();
            let si = proto::ShardInfo {
                canonical_addr: "burrito://shardtest".parse().unwrap(),
                shard_addrs: names[0..].into(),
                shard_info: proto::SimpleShardPolicy {
                    packet_data_offset: 18,
                    packet_data_length: 4,
                },
            };

            let adcl = Arc::new(Mutex::new(None));
            if super::resolve_shardinfo(adcl, si).await.is_some() {
                panic!("failed");
            }

            Ok(())
        })
    }
}
