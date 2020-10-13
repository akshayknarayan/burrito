use super::eyre::Error;
use super::ShardCanonicalServer;
use crate::ShardInfo;
use bertha::{ChunnelConnection, IpPort, Negotiate, Serve};
use futures_util::stream::Stream;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info, trace, warn};
use tracing_futures::Instrument;

/// A chunnel managing a sharded service.
///
/// Forwards incoming messages to one of the internal connections specified by `ShardInfo` after
/// evaluating the sharding function. Also registers with shard-ctl, which will perform other setup
/// (loading XDP program, answering client queries, etc).
#[derive(Clone)]
pub struct ShardCanonicalServerEbpf<A, A2, S, C> {
    inner: ShardCanonicalServer<A, A2, S, C>,
    handles: Arc<Mutex<HashMap<String, xdp_shard::BpfHandles<xdp_shard::Ingress>>>>,
}

impl<A, A2, S, C> ShardCanonicalServerEbpf<A, A2, S, C> {
    /// Inner is a chunnel for the external connection.
    /// Shards is a chunnel for an internal connection to the shards.
    pub async fn new(
        addr: ShardInfo<A>,
        shards_inner: S,
        shards_extern: C,
        shards_extern_nonce: Vec<Vec<bertha::negotiate::Offer>>,
        redis_addr: &str,
    ) -> Result<Self, Error> {
        let inner = ShardCanonicalServer::new(
            addr,
            shards_inner,
            shards_extern,
            shards_extern_nonce,
            redis_addr,
        )
        .await?;

        Ok(ShardCanonicalServerEbpf {
            inner,
            handles: Default::default(),
        })
    }

    /// Future which will poll stats from the ebpf program.
    ///
    /// If file is Some, the stats will be dumped to the file in addition to being logged via
    /// the tracing crate.
    pub async fn log_shard_stats(&self, file: Option<std::fs::File>) {
        read_shard_stats(Arc::clone(&self.handles), file)
            .instrument(tracing::info_span!("read-shard-stats"))
            .await
            .expect("read_shard_stats")
    }
}

impl<A, A2, S, C, Cap> Negotiate for ShardCanonicalServerEbpf<A, A2, S, C>
where
    Cap: bertha::negotiate::CapabilitySet,
    ShardCanonicalServer<A, A2, S, C>: Negotiate<Capability = Cap>,
{
    type Capability = Cap;

    fn capabilities() -> Vec<Cap> {
        ShardCanonicalServer::<A, A2, S, C>::capabilities()
    }

    fn picked<'s>(&mut self, nonce: &'s [u8]) -> Pin<Box<dyn Future<Output = ()> + Send + 's>> {
        self.inner.picked(nonce)
    }
}

impl<I, A, A2, S, C, Ic, Ie> Serve<I> for ShardCanonicalServerEbpf<A, A2, S, C>
where
    A: Clone + IpPort + Sync + Send + 'static,
    ShardCanonicalServer<A, A2, S, C>: Serve<
        I,
        Connection = Ic,
        Error = Ie,
        Stream = Pin<Box<dyn Stream<Item = Result<Ic, Ie>> + Send + 'static>>,
    >,
    Ic: ChunnelConnection + Send + 'static,
    Ie: Send + Sync + 'static,
{
    type Connection = Ic;
    type Error = Ie;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn serve(&mut self, inner: I) -> Self::Future {
        let fut = self.inner.serve(inner);
        let addr = self.inner.addr.clone();
        let handles = Arc::clone(&self.handles);
        Box::pin(async move {
            register_shardinfo(handles, addr.clone())
                .instrument(tracing::debug_span!("register-shardinfo-ebpf"))
                .await;
            fut.await
        })
    }
}

async fn clear_port<T>(handles: Arc<Mutex<HashMap<String, xdp_shard::BpfHandles<T>>>>, port: u16) {
    let mut map = handles.lock().await;
    for h in map.values_mut() {
        if let Err(e) = h.clear_port(port) {
            debug!(err = ?e, port = ?port, "Failed to clear port map");
        }
    }
}

pub(crate) async fn register_shardinfo<A: IpPort + Clone>(
    handles: Arc<Mutex<HashMap<String, xdp_shard::BpfHandles<xdp_shard::Ingress>>>>,
    si: ShardInfo<A>,
) {
    let shard_ports: Vec<u16> = si.shard_addrs.iter().map(|a| a.port()).collect();
    let sk = si.canonical_addr.clone();

    clear_port(handles.clone(), sk.port()).await;

    // see if handles are there already
    let ifnames = match xdp_shard::get_interface_name(sk.ip()) {
        Ok(ifnames) => ifnames,
        Err(e) => {
            warn!(err = ?e, ip = ?sk.ip(), port = ?sk.port(), "Error getting interface");
            return;
        }
    };

    let mut map = handles.lock().await;
    for ifn in ifnames {
        if !map.contains_key(&ifn) {
            let l = xdp_shard::BpfHandles::<xdp_shard::Ingress>::load_on_interface_name(&ifn);
            let handle = match l {
                Ok(s) => s,
                Err(e) => {
                    warn!(err = ?e, ifn = ?&ifn, "Error loading xdp-shard");
                    return;
                }
            };

            info!(service_ip = ?&si.canonical_addr.ip(), service_port = ?&si.canonical_addr.port(), "Loaded XDP program");

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
            service_ip = ?si.canonical_addr.ip(),
            service_port = ?si.canonical_addr.port(),
            interface = ?&ifn,
            "Registered sharding with XDP program"
        );
    }
}

pub(crate) async fn read_shard_stats(
    handles: Arc<Mutex<HashMap<String, xdp_shard::BpfHandles<xdp_shard::Ingress>>>>,
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
