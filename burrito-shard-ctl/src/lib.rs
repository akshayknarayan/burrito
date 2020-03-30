use anyhow::Error;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, info, trace, warn};

pub const CONTROLLER_ADDRESS: &str = "shard-controller";
mod client;
pub use client::*;
pub mod proto;

/// Keep track of sharded services.
#[derive(Clone)]
pub struct ShardCtl {
    shard_table: Arc<RwLock<HashMap<String, proto::ShardInfo>>>,
    // Interface name -> handles
    handles: Arc<Mutex<HashMap<String, xdp_shard::BpfHandles>>>,
    redis_client: redis::Client,
    redis_listen_connection: Arc<Mutex<redis::aio::Connection>>,
}

impl ShardCtl {
    pub async fn new(redis_addr: &str) -> Result<Self, Error> {
        let redis_client = redis::Client::open(redis_addr)?;
        let redis_listen_connection =
            Arc::new(Mutex::new(redis_client.get_async_connection().await?));

        let s = ShardCtl {
            redis_client,
            redis_listen_connection,
            shard_table: Default::default(),
            handles: Default::default(),
        };

        Ok(s)
    }

    pub async fn serve_on<S, E>(self, sk: S, stats_log: Option<std::fs::File>) -> Result<(), Error>
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
        tokio::spawn(read_shard_stats(self.handles.clone(), stats_log));

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

        if req.shard_addrs.is_empty() {
            if let proto::Addr::Udp(sk) = req.canonical_addr {
                let ifnames = match xdp_shard::get_interface_name(sk.ip()) {
                    Ok(ifnames) => ifnames,
                    Err(e) => {
                        return proto::RegisterShardReply::Err(format!(
                            "Error loading xdp-shard on address: {:?}",
                            e
                        ))
                    }
                };

                let mut map = self.handles.lock().await;
                for ifn in ifnames {
                    if let Some(h) = map.get_mut(&ifn) {
                        if let Err(e) = h.clear_port(sk.port()) {
                            warn!(err = ?e, sock = ?sk, "Failed to clear port map");
                        }
                    }
                }
            }

            return proto::RegisterShardReply::Ok;
        }

        let shard_ports: Result<Vec<u16>, String> = req
            .shard_addrs
            .iter()
            .map(|a| match a {
                proto::Addr::Tcp(s) | proto::Addr::Udp(s) => Ok(s.port()),
                x => Err(format!("Addr must be tcp or udp for sharding: {:?}", x)),
            })
            .collect();
        let shard_ports = match shard_ports {
            Ok(s) => s,
            Err(s) => return proto::RegisterShardReply::Err(s),
        };

        if let proto::Addr::Udp(sk) = req.canonical_addr {
            // see if handles are there already
            let ifnames = match xdp_shard::get_interface_name(sk.ip()) {
                Ok(ifnames) => ifnames,
                Err(e) => {
                    return proto::RegisterShardReply::Err(format!(
                        "Error loading xdp-shard on address: {:?}",
                        e
                    ))
                }
            };

            let mut map = self.handles.lock().await;
            for ifn in ifnames {
                if !map.contains_key(&ifn) {
                    let handle = match xdp_shard::BpfHandles::load_on_interface_name(&ifn) {
                        Ok(s) => s,
                        Err(e) => {
                            return proto::RegisterShardReply::Err(format!(
                                "Error loading xdp-shard on address: {:?}",
                                e
                            ))
                        }
                    };

                    info!(service = ?req.canonical_addr, "Loaded XDP program");

                    map.insert(ifn.clone(), handle);
                }

                let h = map.get_mut(&ifn).unwrap();
                let ok = h.shard_ports(
                    sk.port(),
                    &shard_ports[..],
                    req.shard_info.packet_data_offset,
                    req.shard_info.packet_data_length,
                );
                match ok {
                    Ok(_) => (),
                    Err(e) => {
                        return proto::RegisterShardReply::Err(format!(
                            "Error writing xdp-shard ports: {:?}",
                            e
                        ))
                    }
                }

                info!(
                    service = ?req.canonical_addr,
                    interface = ?&ifn,
                    "Registered sharding with XDP program"
                );
            }
        }

        proto::RegisterShardReply::Ok
    }

    pub async fn query(&self, req: proto::QueryShardRequest) -> proto::QueryShardReply {
        // TODO if necessary, install client-side xdp program
        if let Some(s) = self.shard_table.read().await.get(&req.service_name) {
            proto::QueryShardReply::Ok(s.clone())
        } else {
            proto::QueryShardReply::Err(format!("Could not find {:?}", &req.service_name))
        }
    }

    async fn shard_table_insert(&self, serv: proto::ShardInfo) -> Result<(), Error> {
        let sn = serv.service_name.clone();
        if let Some(s) = self.shard_table.write().await.insert(sn.clone(), serv) {
            Err(anyhow::anyhow!(
                "Shard service address {} already in use at: {:?}",
                sn,
                s.canonical_addr
            ))?;
        };

        Ok(())
    }
}

async fn read_shard_stats(
    handles: Arc<Mutex<HashMap<String, xdp_shard::BpfHandles>>>,
    mut stats_log: Option<std::fs::File>,
) -> Result<(), Error> {
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
    shard_table: Arc<RwLock<HashMap<String, proto::ShardInfo>>>,
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
    shard_table: &RwLock<HashMap<String, proto::ShardInfo>>,
    con: &mut redis::aio::Connection,
) -> Result<(), Error> {
    use redis::AsyncCommands;
    let srvs: Vec<String> = con.smembers("shard-services").await?;

    // TODO fast-path check if a write is even necessary?

    let mut tbl = shard_table.write().await;
    for srv in srvs {
        if !tbl.contains_key(&srv) {
            let sh_info_blob = con.get::<_, Vec<u8>>(&srv).await?;
            let sh_info = bincode::deserialize(&sh_info_blob)?;
            let k = srv.trim_start_matches("shard:");
            tbl.insert(k.to_owned(), sh_info);
        }
    }

    Ok(())
}

async fn redis_insert(
    conn: Arc<Mutex<redis::aio::Connection>>,
    serv: &proto::ShardInfo,
) -> Result<(), Error> {
    let name = format!("shard:{}", serv.service_name);
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
