use bertha::{ChunnelConnection, ChunnelConnector, ChunnelListener, Either, IpPort};
use eyre::{eyre, Error, WrapErr};
use futures_util::stream::{Stream, StreamExt};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, trace, warn};
use tracing_futures::Instrument;

pub const CONTROLLER_ADDRESS: &str = "shard-ctl";

/// Request type for servers registering.
///
/// The Addr type is to parameterize by the inner chunnel's addr type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardInfo<Addr> {
    pub canonical_addr: Addr,
    pub shard_addrs: Vec<Addr>,
    pub shard_info: SimpleShardPolicy,
}

impl<A> IpPort for ShardInfo<A>
where
    A: IpPort,
{
    fn ip(&self) -> std::net::IpAddr {
        self.canonical_addr.ip()
    }

    fn port(&self) -> u16 {
        self.canonical_addr.port()
    }
}

/// TODO Replace this with something based on trait Kv.
/// This approach assumes the serialization is known.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SimpleShardPolicy {
    pub packet_data_offset: u8,
    pub packet_data_length: u8,
}

/// Allow the shard chunnel to look into messages.
pub trait Kv {
    type Key;
    fn key(&self) -> Self::Key;

    type Val;
    fn val(&self) -> Self::Val;
}

const FNV1_64_INIT: u64 = 0xcbf29ce484222325u64;
const FNV_64_PRIME: u64 = 0x100000001b3u64;

/// A chunnel managing a sharded service.
///
/// Listens on an external connection, then forwards messages to one of the internal
/// connections after evaluating the sharding function. Also registers with shard-ctl, which
/// will perform other setup (loading XDP program, answering client queries, etc).  Does not
/// implement `connect()`.
pub struct ShardCanonicalServer<C, S> {
    inner: Arc<Mutex<C>>,
    shards_inner: Arc<Mutex<S>>,
    redis_listen_connection: Arc<Mutex<redis::aio::Connection>>,
    #[cfg(feature = "ebpf")]
    handles: Arc<Mutex<HashMap<String, xdp_shard::BpfHandles<xdp_shard::Ingress>>>>,
}

impl<C, S> ShardCanonicalServer<C, S> {
    /// Inner is a chunnel for the external connection.
    /// Shards is a chunnel for an internal connection to the shards.
    pub async fn new<C1, S1>(
        inner: C1,
        shards: S1,
        redis_addr: &str,
    ) -> Result<ShardCanonicalServer<C1, S1>, Error> {
        let redis_client = redis::Client::open(redis_addr)?;
        let redis_listen_connection =
            Arc::new(Mutex::new(redis_client.get_async_connection().await?));

        Ok(ShardCanonicalServer {
            inner: Arc::new(Mutex::new(inner)),
            shards_inner: Arc::new(Mutex::new(shards)),
            redis_listen_connection,
            #[cfg(feature = "ebpf")]
            handles: Default::default(),
        })
    }

    #[cfg(feature = "ebpf")]
    pub async fn log_shard_stats(&self, file: Option<std::fs::File>) {
        ebpf::read_shard_stats(Arc::clone(&self.handles), file).await
    }
}

impl<C, A, S, D> ChunnelListener for ShardCanonicalServer<C, S>
where
    C: ChunnelListener<Addr = A> + 'static,
    C::Connection: ChunnelConnection<Data = D> + Send + Sync + 'static,
    S: ChunnelConnector<Addr = A> + 'static,
    S::Connection: ChunnelConnection<Data = D> + Clone + Send + Sync + 'static,
    D: Kv + Send + Sync + 'static,
    <D as Kv>::Key: AsRef<str>,
    A: IpPort
        + Clone
        + Serialize
        + DeserializeOwned
        + std::fmt::Debug
        + std::fmt::Display
        + 'static,
{
    type Addr = ShardInfo<A>;
    type Connection = ShardCanonicalServerConnection<C::Connection, S::Connection>;

    fn listen(
        &mut self,
        a: Self::Addr,
    ) -> Pin<
        Box<
            dyn Future<
                Output = Pin<Box<dyn Stream<Item = Result<Self::Connection, eyre::Report>>>>,
            >,
        >,
    > {
        let a1 = a.clone();
        let inner = Arc::clone(&self.inner);
        let shards_inner = Arc::clone(&self.shards_inner);
        let redis_conn = Arc::clone(&self.redis_listen_connection);
        #[cfg(feature = "ebpf")]
        let handles = Arc::clone(&self.handles);
        Box::pin(
            async move {
                // redis insert
                if let Err(e) = redis_insert(redis_conn, &a).await {
                    return Box::pin(futures_util::stream::once(async {
                        Err(e.wrap_err("Could not register shard info"))
                    })) as _;
                }

                // if ebpf, ebpf initialization
                #[cfg(feature = "ebpf")]
                {
                    ebpf::register_shardinfo(handles, a).await;
                }

                // serve canonical address
                let num_shards = a.shard_addrs.len();
                let addrs = a.shard_addrs.clone();

                let conns: Vec<Arc<S::Connection>> = match futures_util::future::join_all(
                    addrs.clone().into_iter().map(|addr| async {
                        Ok::<_, eyre::Report>(Arc::new(
                            shards_inner.lock().await.connect(addr).await?,
                        ))
                    }),
                )
                .await
                .into_iter()
                .collect()
                {
                    Ok(a) => a,
                    Err(e) => {
                        return Box::pin(futures_util::stream::once(async {
                            Err(e).wrap_err("Could not connect to shards")
                        })) as _
                    }
                };

                // we are only responsible for the canonical address here.
                Box::pin(
                    inner
                        .lock()
                        .await
                        .listen(a.canonical_addr)
                        .await
                        .map(move |conn| {
                            Ok(ShardCanonicalServerConnection {
                                inner: Arc::new(conn?),
                                shards: conns.clone(),
                                shard_fn: Arc::new(move |d| {
                                    /* xdp_shard version of FNV: take the first 4 bytes of the key
                                    * u64 hash = FNV1_64_INIT;
                                    * // ...
                                    * // value start
                                    * pkt_val = ((u8*) app_data) + offset;

                                    * // compute FNV hash
                                    * #pragma clang loop unroll(full)
                                    * for (i = 0; i < 4; i++) {
                                    *     hash = hash ^ ((u64) pkt_val[i]);
                                    *     hash *= FNV_64_PRIME;
                                    * }

                                    * // map to a shard and assign to that port.
                                    * idx = hash % shards->num;
                                    */
                                    let mut hash = FNV1_64_INIT;
                                    for b in d.key().as_ref().as_bytes()[0..4].iter() {
                                        hash = hash ^ (*b as u64);
                                        hash = u64::wrapping_mul(hash, FNV_64_PRIME);
                                    }

                                    hash as usize % num_shards
                                }),
                            })
                        }),
                ) as _
            }
            .instrument(tracing::debug_span!("listen", addr = ?&a1)),
        )
    }

    fn scope(&self) -> bertha::Scope {
        bertha::Scope::Local
    }

    fn endedness(&self) -> bertha::Endedness {
        bertha::Endedness::Either
    }

    fn implementation_priority(&self) -> usize {
        1
    }
}

/// Chunnel connection type for serving the shard canonical address.
///
/// Does not implement `send()`, since the server does not send messages unprompted.  Similarly,
/// the Data type is `()`, since any received message is forwarded to the appropriate shard.
/// Expected usage is to call `recv()` in a loop.
pub struct ShardCanonicalServerConnection<C, S>
where
    C: ChunnelConnection,
    S: ChunnelConnection,
{
    inner: Arc<C>,
    shards: Vec<Arc<S>>,
    shard_fn: Arc<dyn Fn(&C::Data) -> usize + Send + Sync + 'static>,
}

impl<C, S, D> ChunnelConnection for ShardCanonicalServerConnection<C, S>
where
    C: ChunnelConnection<Data = D> + Send + Sync + 'static,
    S: ChunnelConnection<Data = D> + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Data = ();

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + Sync>> {
        let inner = Arc::clone(&self.inner);
        let shard_fn = Arc::clone(&self.shard_fn);
        let shard_conns = self.shards.clone();
        Box::pin(async move {
            // received a packet on the canonical_addr.
            // need to
            // 1. evaluate the hash fn
            // 2. forward to the right shard,
            //    preserving the src addr so the response goes back to the client

            // 0. receive the packet.
            let data = inner.recv().await?;

            // 1. evaluate the hash fn to determine where to forward to.
            let shard_idx = (shard_fn)(&data);
            let conn = shard_conns[shard_idx].clone();

            conn.send(data).await?;
            Ok(())
        })
    }

    fn send(
        &self,
        _data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + Sync>> {
        unimplemented!()
    }
}

/// A Chunnel for a single shard.
///
/// Listens on an external chunnel, for direct connections from clients, as well as an internal
/// chunnel from the canonical_addr proxy.  Does not implement `connect()`.
pub struct ShardServer<C, S> {
    external: Arc<Mutex<C>>,
    internal: Arc<Mutex<S>>,
}

impl<C, S> ShardServer<C, S> {
    /// internal: A way to listen for messages forwarded from the fallback canonical address listener.
    /// external: Listen for messages over the network.
    pub fn new<C1, S1>(internal: S1, external: C1) -> ShardServer<C1, S1> {
        ShardServer {
            internal: Arc::new(Mutex::new(internal)),
            external: Arc::new(Mutex::new(external)),
        }
    }
}

impl<C, A, S, D> ChunnelListener for ShardServer<C, S>
where
    C: ChunnelListener<Addr = A> + 'static,
    C::Connection: ChunnelConnection<Data = D> + Send + Sync + 'static,
    S: ChunnelListener<Addr = A> + 'static,
    S::Connection: ChunnelConnection<Data = D> + Send + Sync + 'static,
    D: Kv + Send + Sync + 'static,
    <D as Kv>::Key: AsRef<str>,
    A: Clone + 'static,
{
    type Addr = A;
    type Connection = Either<C::Connection, S::Connection>;

    fn listen(
        &mut self,
        a: Self::Addr,
    ) -> Pin<
        Box<
            dyn Future<
                Output = Pin<Box<dyn Stream<Item = Result<Self::Connection, eyre::Report>>>>,
            >,
        >,
    > {
        let ext = Arc::clone(&self.external);
        let int = Arc::clone(&self.internal);
        Box::pin(async move {
            let ext_str = ext
                .lock()
                .await
                .listen(a.clone())
                .await
                .map(|conn| Ok(Either::Left(conn?)));
            let int_str = int
                .lock()
                .await
                .listen(a)
                .await
                .map(|conn| Ok(Either::Right(conn?)));

            Box::pin(futures_util::stream::select(ext_str, int_str)) as _
        })
    }

    fn scope(&self) -> bertha::Scope {
        bertha::Scope::Local
    }

    fn endedness(&self) -> bertha::Endedness {
        bertha::Endedness::Either
    }

    fn implementation_priority(&self) -> usize {
        1
    }
}

/// Client-side sharding chunnel implementation.
///
/// Contacts shard-ctl for sharding information, and does client-side sharding.
/// Does not implement `listen()`, since what would that do?
pub struct ClientShardChunnelClient<C> {
    inner: Arc<Mutex<C>>,
    redis_listen_connection: Arc<Mutex<redis::aio::Connection>>,
}

impl<C> ClientShardChunnelClient<C> {
    pub async fn new<C1, S1>(
        inner: C1,
        redis_addr: &str,
    ) -> Result<ClientShardChunnelClient<C1>, Error> {
        let redis_client = redis::Client::open(redis_addr)?;
        let redis_listen_connection =
            Arc::new(Mutex::new(redis_client.get_async_connection().await?));

        Ok(ClientShardChunnelClient {
            inner: Arc::new(Mutex::new(inner)),
            redis_listen_connection,
        })
    }
}

impl<A, C> ChunnelConnector for ClientShardChunnelClient<C>
where
    C: ChunnelConnector<Addr = A> + 'static,
    C::Connection: Send + Sync + 'static,
    <C::Connection as ChunnelConnection>::Data: Kv + Send + Sync + 'static,
    <<C::Connection as ChunnelConnection>::Data as Kv>::Key: AsRef<str>,
    A: Serialize
        + DeserializeOwned
        + std::cmp::PartialEq
        + std::fmt::Debug
        + std::fmt::Display
        + Sync
        + 'static,
{
    type Addr = A;
    type Connection = ClientShardClientConnection<C::Connection>;

    fn connect(
        &mut self,
        a: Self::Addr,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Connection, eyre::Report>>>> {
        let inner = Arc::clone(&self.inner);
        let redis_conn = Arc::clone(&self.redis_listen_connection);
        Box::pin(
            async move {
                // query redis for si
                trace!(addr = ?&a, "querying redis");
                let si = redis_query(&a, redis_conn.lock().await).await?;

                let addrs = match si {
                    None => vec![a],
                    Some(si) => {
                        let mut addrs = vec![si.canonical_addr];
                        addrs.extend(si.shard_addrs.into_iter());
                        addrs
                    }
                };

                debug!(addrs = ?&addrs, "Decided sharding plan");
                let num_shards = addrs.len();
                let conns: Vec<Arc<C::Connection>> =
                    futures_util::future::join_all(addrs.into_iter().map(|a| async {
                        Ok::<_, eyre::Report>(Arc::new(inner.lock().await.connect(a).await?))
                    }))
                    .await
                    .into_iter()
                    .collect::<Result<_, _>>()?;
                Ok(ClientShardClientConnection {
                    inners: conns,
                    shard_fn: Arc::new(move |d| {
                        /* xdp_shard version of FNV: take the first 4 bytes of the key
                        * u64 hash = FNV1_64_INIT;
                        * // ...
                        * // value start
                        * pkt_val = ((u8*) app_data) + offset;

                        * // compute FNV hash
                        * #pragma clang loop unroll(full)
                        * for (i = 0; i < 4; i++) {
                        *     hash = hash ^ ((u64) pkt_val[i]);
                        *     hash *= FNV_64_PRIME;
                        * }

                        * // map to a shard and assign to that port.
                        * idx = hash % shards->num;
                        */
                        let mut hash = FNV1_64_INIT;
                        for b in d.key().as_ref().as_bytes()[0..4].iter() {
                            hash = hash ^ (*b as u64);
                            hash = u64::wrapping_mul(hash, FNV_64_PRIME);
                        }

                        hash as usize % num_shards
                    }),
                })
            }
            .instrument(tracing::debug_span!("connect")),
        )
    }

    fn scope(&self) -> bertha::Scope {
        bertha::Scope::Local
    }

    fn endedness(&self) -> bertha::Endedness {
        bertha::Endedness::Either
    }

    fn implementation_priority(&self) -> usize {
        1
    }
}

/// `ChunnelConnection` type for ClientShardChunnelClient.
pub struct ClientShardClientConnection<C>
where
    C: ChunnelConnection,
{
    inners: Vec<Arc<C>>,
    shard_fn: Arc<dyn Fn(&C::Data) -> usize>,
}

impl<C> ChunnelConnection for ClientShardClientConnection<C>
where
    C: ChunnelConnection + Send + Sync + 'static,
    C::Data: Send + Sync + 'static,
{
    type Data = C::Data;

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + Sync>> {
        // figure out which shard to send to.
        let shard_idx = (self.shard_fn)(&data);
        let cl = self.inners[shard_idx].clone();
        Box::pin(async move { cl.send(data).await })
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + Sync>> {
        let cls = self.inners.clone();
        Box::pin(async move {
            let (rcv, _, _) =
                futures_util::future::select_all(cls.iter().map(|cl| cl.recv())).await;
            rcv
        })
    }
}

async fn redis_insert<A>(
    conn: Arc<Mutex<redis::aio::Connection>>,
    serv: &ShardInfo<A>,
) -> Result<(), Error>
where
    A: std::fmt::Display + Serialize + DeserializeOwned,
{
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

async fn redis_query<A>(
    canonical_addr: &A,
    mut con: impl std::ops::DerefMut<Target = redis::aio::Connection>,
) -> Result<Option<ShardInfo<A>>, Error>
where
    A: Serialize + DeserializeOwned + std::cmp::PartialEq + std::fmt::Display + Sync + 'static,
{
    use redis::AsyncCommands;

    let a = canonical_addr.to_string();
    let sh_info_blob: Vec<u8> = con.get::<_, Vec<u8>>(&a).await?;
    if sh_info_blob.is_empty() {
        return Ok(None);
    }

    let sh_info: ShardInfo<A> = bincode::deserialize(&sh_info_blob)?;
    if canonical_addr != &sh_info.canonical_addr {
        warn!(
            canonical_addr = %canonical_addr,
            shardinfo_addr = %sh_info.canonical_addr,
            "Mismatch error with redis key <-> ShardInfo",
        );

        return Err(eyre!("redis key mismatched"));
    }

    Ok(Some(sh_info))
}

#[cfg(feature = "ebpf")]
mod ebpf {
    use bertha::IpPort;
    use std::collections::HashMap;
    use tracing::{debug, info, trace};

    async fn clear_port<T>(
        handles: Arc<Mutex<HashMap<String, xdp_shard::BpfHandles<T>>>>,
        port: u16,
    ) {
        let mut map = handles.lock().await;
        for h in map.values_mut() {
            if let Err(e) = h.clear_port(port) {
                debug!(err = ?e, port = ?sk, "Failed to clear port map");
            }
        }
    }

    async fn register_shardinfo<A: IpPort>(
        handles: Arc<Mutex<HashMap<String, xdp_shard::BpfHandles<xdp_shard::Ingress>>>>,
        si: proto::ShardInfo<A>,
    ) {
        let shard_ports: Vec<u16> = si.shard_addrs.iter().map(|a| a.port()).collect();
        let sk = si.canonical_addr;

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
                let l = xdp_shard::BpfHandles::<xdp_shard::Ingress>::load_on_interface_name(&ifn);
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

    async fn read_shard_stats(
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
}
