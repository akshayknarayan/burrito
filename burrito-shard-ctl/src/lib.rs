use bertha::{ChunnelConnection, ChunnelConnector, ChunnelListener, Either, IpPort};
use color_eyre::eyre;
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

impl<T> Kv for Option<T>
where
    T: Kv,
{
    type Key = Option<T::Key>;
    fn key(&self) -> Self::Key {
        self.as_ref().map(|t| t.key())
    }

    type Val = Option<T::Val>;
    fn val(&self) -> Self::Val {
        self.as_ref().map(|t| t.val())
    }
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
}

impl<C, S> ShardCanonicalServer<C, S> {
    /// Inner is a chunnel for the external connection.
    /// Shards is a chunnel for an internal connection to the shards.
    pub async fn new(inner: C, shards: S, redis_addr: &str) -> Result<Self, Error> {
        let redis_client = redis::Client::open(redis_addr)?;
        let redis_listen_connection =
            Arc::new(Mutex::new(redis_client.get_async_connection().await?));

        Ok(ShardCanonicalServer {
            inner: Arc::new(Mutex::new(inner)),
            shards_inner: Arc::new(Mutex::new(shards)),
            redis_listen_connection,
        })
    }
}

impl<C, A, S, D> ChunnelListener for ShardCanonicalServer<C, S>
where
    C: ChunnelListener<Addr = A, Error = Error> + Send + Sync + 'static,
    C::Connection: ChunnelConnection<Data = D> + Send + Sync + 'static,
    S: ChunnelConnector<Addr = A, Error = Error> + Send + Sync + 'static,
    S::Connection: ChunnelConnection<Data = D> + Send + Sync + 'static,
    D: Kv + Send + Sync + 'static,
    <D as Kv>::Key: AsRef<str>,
    A: IpPort
        + Clone
        + std::fmt::Debug
        + std::fmt::Display
        + Send
        + Sync
        + 'static
        + Serialize
        + DeserializeOwned,
{
    type Addr = ShardInfo<A>;
    type Connection = ShardCanonicalServerConnection<C::Connection, S::Connection>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Error;

    fn listen(&mut self, a: Self::Addr) -> Self::Future {
        let a1 = a.clone();
        let inner = Arc::clone(&self.inner);
        let shards_inner = Arc::clone(&self.shards_inner);
        let redis_conn = Arc::clone(&self.redis_listen_connection);
        Box::pin(
            async move {
                // redis insert
                redis_insert(redis_conn, &a)
                    .await
                    .wrap_err("Could not register shard info")?;

                let num_shards = a.shard_addrs.len();
                let addrs = a.shard_addrs.clone();

                // connect to shards
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
                        return Err(e).wrap_err("Could not connect to shards");
                    }
                };

                // serve canonical address
                // we are only responsible for the canonical address here.
                Ok(Box::pin(
                    inner
                        .lock()
                        .await
                        .listen(a.canonical_addr)
                        .await?
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
                ) as _)
            }
            .instrument(tracing::debug_span!("listen", addr = ?&a1)),
        )
    }

    fn scope() -> bertha::Scope {
        bertha::Scope::Global
    }

    fn endedness() -> bertha::Endedness {
        bertha::Endedness::Either
    }

    fn implementation_priority() -> usize {
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
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>> {
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

            // 2. Forward to the shard.
            conn.send(data).await?;

            // 3. Get response from the shard, and send back to client.
            let resp = conn.recv().await?;
            inner.send(resp).await?;

            Ok(())
        })
    }

    fn send(
        &self,
        _data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>> {
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

impl ShardServer<(), ()> {
    /// internal: A way to listen for messages forwarded from the fallback canonical address listener.
    /// external: Listen for messages over the network.
    pub fn new<C, S>(internal: S, external: C) -> ShardServer<C, S> {
        ShardServer {
            internal: Arc::new(Mutex::new(internal)),
            external: Arc::new(Mutex::new(external)),
        }
    }
}

impl<C, A, S, D> ChunnelListener for ShardServer<C, S>
where
    C: ChunnelListener<Addr = A, Error = Error> + Send + Sync + 'static,
    C::Connection: ChunnelConnection<Data = D> + Send + Sync + 'static,
    S: ChunnelListener<Addr = A, Error = Error> + Send + Sync + 'static,
    S::Connection: ChunnelConnection<Data = D> + Send + Sync + 'static,
    D: Kv + Send + Sync + 'static,
    <D as Kv>::Key: AsRef<str>,
    A: IpPort
        + Clone
        + std::fmt::Debug
        + std::fmt::Display
        + Send
        + Sync
        + 'static
        + Serialize
        + DeserializeOwned,
{
    type Addr = A;
    type Connection = Either<C::Connection, S::Connection>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Error;

    fn listen(&mut self, a: Self::Addr) -> Self::Future {
        let ext = Arc::clone(&self.external);
        let int = Arc::clone(&self.internal);
        Box::pin(async move {
            let ext_str = ext
                .lock()
                .await
                .listen(a.clone())
                .await?
                .map(|conn| Ok(Either::Left(conn?)));
            let int_str = int
                .lock()
                .await
                .listen(a)
                .await?
                .map(|conn| Ok(Either::Right(conn?)));

            Ok(Box::pin(futures_util::stream::select(ext_str, int_str)) as _)
        })
    }

    fn scope() -> bertha::Scope {
        bertha::Scope::Global
    }

    fn endedness() -> bertha::Endedness {
        bertha::Endedness::Either
    }

    fn implementation_priority() -> usize {
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
    pub async fn new(inner: C, redis_addr: &str) -> Result<Self, Error> {
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
    C: ChunnelConnector<Addr = A, Error = Error> + Send + Sync + 'static,
    C::Connection: Send + Sync + 'static,
    <C::Connection as ChunnelConnection>::Data: Kv + Send + Sync + 'static,
    <<C::Connection as ChunnelConnection>::Data as Kv>::Key: AsRef<str>,
    A: Serialize
        + DeserializeOwned
        + std::cmp::PartialEq
        + std::fmt::Debug
        + std::fmt::Display
        + Send
        + Sync
        + 'static,
{
    type Addr = A;
    type Connection = ClientShardClientConnection<C::Connection>;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Error;

    fn connect(&mut self, a: Self::Addr) -> Self::Future {
        let inner = Arc::clone(&self.inner);
        let redis_conn = Arc::clone(&self.redis_listen_connection);
        Box::pin(
            async move {
                // query redis for si
                trace!(addr = ?&a, "querying redis");
                let si = redis_query(&a, redis_conn.lock().await)
                    .await
                    .wrap_err("redis query failed")?;

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
                let conns: Vec<Arc<C::Connection>> = futures_util::future::join_all(
                    addrs
                        .into_iter()
                        .map(|a| async { Ok(Arc::new(inner.lock().await.connect(a).await?)) }),
                )
                .await
                .into_iter()
                .collect::<Result<_, Error>>()?;
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

    fn scope() -> bertha::Scope {
        bertha::Scope::Global
    }

    fn endedness() -> bertha::Endedness {
        bertha::Endedness::Either
    }

    fn implementation_priority() -> usize {
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
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>> {
        // figure out which shard to send to.
        let shard_idx = (self.shard_fn)(&data);
        let cl = self.inners[shard_idx].clone();
        Box::pin(async move { cl.send(data).await })
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>> {
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
pub use ebpf::ShardCanonicalServerEbpf;

#[cfg(feature = "ebpf")]
mod ebpf {
    use super::eyre::Error;
    use super::eyre::{Report, WrapErr};
    use super::ShardCanonicalServerConnection;
    use crate::{Kv, ShardInfo};
    use bertha::{ChunnelConnection, ChunnelConnector, ChunnelListener, IpPort};
    use futures_util::stream::{Stream, StreamExt};
    use serde::{de::DeserializeOwned, Serialize};
    use std::collections::HashMap;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use tracing::{debug, info, trace, warn};
    use tracing_futures::Instrument;

    /// A chunnel managing a sharded service.
    ///
    /// Listens on an external connection, then forwards messages to one of the internal
    /// connections after evaluating the sharding function. Also registers with shard-ctl, which
    /// will perform other setup (loading XDP program, answering client queries, etc).  Does not
    /// implement `connect()`.
    pub struct ShardCanonicalServerEbpf<C, S> {
        inner: Arc<Mutex<C>>,
        shards_inner: Arc<Mutex<S>>,
        redis_listen_connection: Arc<Mutex<redis::aio::Connection>>,
        handles: Arc<Mutex<HashMap<String, xdp_shard::BpfHandles<xdp_shard::Ingress>>>>,
    }

    impl<C, S> ShardCanonicalServerEbpf<C, S> {
        /// Inner is a chunnel for the external connection.
        /// Shards is a chunnel for an internal connection to the shards.
        pub async fn new(inner: C, shards: S, redis_addr: &str) -> Result<Self, Report> {
            let redis_client = redis::Client::open(redis_addr)?;
            let redis_listen_connection =
                Arc::new(Mutex::new(redis_client.get_async_connection().await?));

            Ok(ShardCanonicalServerEbpf {
                inner: Arc::new(Mutex::new(inner)),
                shards_inner: Arc::new(Mutex::new(shards)),
                redis_listen_connection,
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

    impl<C, A, S, D> ChunnelListener for ShardCanonicalServerEbpf<C, S>
    where
        C: ChunnelListener<Addr = A, Error = Report> + Send + Sync + 'static,
        C::Connection: ChunnelConnection<Data = D> + Send + Sync + 'static,
        S: ChunnelConnector<Addr = A, Error = Report> + Send + Sync + 'static,
        S::Connection: ChunnelConnection<Data = D> + Send + Sync + 'static,
        D: Kv + Send + Sync + 'static,
        <D as Kv>::Key: AsRef<str>,
        A: IpPort
            + Clone
            + std::fmt::Debug
            + std::fmt::Display
            + Send
            + Sync
            + 'static
            + Serialize
            + DeserializeOwned,
    {
        type Addr = ShardInfo<A>;
        type Connection = ShardCanonicalServerConnection<C::Connection, S::Connection>;
        type Future =
            Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
        type Stream =
            Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;
        type Error = Report;

        fn listen(&mut self, a: Self::Addr) -> Self::Future {
            let a1 = a.clone();
            let inner = Arc::clone(&self.inner);
            let shards_inner = Arc::clone(&self.shards_inner);
            let redis_conn = Arc::clone(&self.redis_listen_connection);
            let handles = Arc::clone(&self.handles);
            Box::pin(
                async move {
                    // redis insert
                    super::redis_insert(redis_conn, &a)
                        .await
                        .wrap_err("Could not register shard info")?;

                    // if ebpf, ebpf initialization
                    register_shardinfo(handles, a.clone())
                        .instrument(tracing::debug_span!("register-shardinfo-ebpf"))
                        .await;

                    let num_shards = a.shard_addrs.len();
                    let addrs = a.shard_addrs.clone();

                    // connect to shards
                    let conns: Vec<Arc<S::Connection>> = match futures_util::future::join_all(
                        addrs.clone().into_iter().map(|addr| async {
                            Ok::<_, Report>(Arc::new(
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
                            return Err(e).wrap_err("Could not connect to shards");
                        }
                    };

                    // serve canonical address
                    // we are only responsible for the canonical address here.
                    Ok(
                        Box::pin(inner.lock().await.listen(a.canonical_addr).await?.map(
                            move |conn| {
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
                                        let mut hash = super::FNV1_64_INIT;
                                        for b in d.key().as_ref().as_bytes()[0..4].iter() {
                                            hash = hash ^ (*b as u64);
                                            hash = u64::wrapping_mul(hash, super::FNV_64_PRIME);
                                        }

                                        hash as usize % num_shards
                                    }),
                                })
                            },
                        )) as _,
                    )
                }
                .instrument(tracing::debug_span!("listen", addr = ?&a1)),
            )
        }

        fn scope() -> bertha::Scope {
            bertha::Scope::Host
        }

        fn endedness() -> bertha::Endedness {
            bertha::Endedness::Either
        }

        fn implementation_priority() -> usize {
            2
        }
    }

    async fn clear_port<T>(
        handles: Arc<Mutex<HashMap<String, xdp_shard::BpfHandles<T>>>>,
        port: u16,
    ) {
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
}

#[cfg(test)]
mod test {
    use super::{ClientShardChunnelClient, ShardCanonicalServer, ShardInfo, ShardServer};
    use bertha::{
        bincode::SerializeChunnel,
        chan_transport::RendezvousChannel,
        reliable::ReliabilityChunnel,
        tagger::TaggerChunnel,
        udp::{UdpReqChunnel, UdpSkChunnel},
        util::{AddrWrap, OptionUnwrap},
        ChunnelConnection, ChunnelConnector, ChunnelListener,
    };
    use color_eyre::eyre;
    use eyre::{eyre, WrapErr};
    use futures_util::TryStreamExt;
    use serde::{Deserialize, Serialize};
    use std::net::SocketAddr;
    use tracing::{debug, info, trace};
    use tracing_futures::Instrument;

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct Msg {
        k: String,
        v: String,
    }

    impl super::Kv for Msg {
        type Key = String;
        fn key(&self) -> Self::Key {
            self.k.clone()
        }
        type Val = String;
        fn val(&self) -> Self::Val {
            self.v.clone()
        }
    }

    #[test]
    fn single_shard() {
        let _guard = tracing_subscriber::fmt::try_init();
        color_eyre::install().unwrap_or_else(|_| ());

        // 0. Make rt.
        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_time()
            .enable_io()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                let addr: SocketAddr = "127.0.0.1:21422".parse().unwrap();
                let (s, r) = tokio::sync::oneshot::channel();

                let (internal_srv, mut internal_cli) =
                    RendezvousChannel::<SocketAddr, Msg, _>::new(100).split();

                info!(addr = ?&addr, "start shard");
                let internal_srv = OptionUnwrap::from(internal_srv);
                let external = SerializeChunnel::<_, Msg>::from(TaggerChunnel::from(
                    ReliabilityChunnel::from(UdpReqChunnel::default()),
                ));

                tokio::spawn(
                    async move {
                        let mut shard = ShardServer::new(internal_srv, external);
                        info!(addr = ?&addr, "listening");
                        let st = shard.listen(addr).await.unwrap();
                        s.send(()).unwrap();

                        match st
                            .try_for_each_concurrent(None, |once| async move {
                                let msg =
                                    once.recv().await.wrap_err(eyre!("receive message error"))?;
                                // just echo.
                                once.send(msg).await.wrap_err(eyre!("send response err"))?;
                                Ok(())
                            })
                            .await
                        {
                            Err(e) => debug!(err = ?e, shard_addr = ?addr,  "Shard errorred"),
                            Ok(_) => (),
                        }
                    }
                    .instrument(tracing::debug_span!("shard thread")),
                );

                let _: () = r.await.wrap_err("shard thread crashed").unwrap();

                // channel connection
                async {
                    debug!("connect to shard");
                    let cn = internal_cli
                        .connect(addr)
                        .await
                        .wrap_err(eyre!("client connect"))
                        .unwrap();
                    trace!("send request");
                    cn.send(Msg {
                        k: "a".to_owned(),
                        v: "b".to_owned(),
                    })
                    .await
                    .unwrap();

                    trace!("await response");
                    let m = cn.recv().await.unwrap();
                    debug!(msg = ?m, "got response");
                    use super::Kv;
                    assert_eq!(m.key(), "a");
                    assert_eq!(m.val(), "b");
                }
                .instrument(tracing::info_span!("chan client"))
                .await;

                // udp connection
                async {
                    debug!("connect to shard");
                    let mut external = SerializeChunnel::<_, Msg>::from(TaggerChunnel::from(
                        ReliabilityChunnel::from(AddrWrap::from(UdpSkChunnel::default())),
                    ));

                    let cn = external
                        .connect(addr)
                        .await
                        .wrap_err(eyre!("client connect"))
                        .unwrap();
                    trace!("send request");
                    cn.send(Msg {
                        k: "c".to_owned(),
                        v: "d".to_owned(),
                    })
                    .await
                    .unwrap();

                    trace!("await response");
                    let m = cn.recv().await.unwrap();
                    debug!(msg = ?m, "got response");
                    use super::Kv;
                    assert_eq!(m.key(), "c");
                    assert_eq!(m.val(), "d");
                }
                .instrument(tracing::info_span!("udp client"))
                .await;
            }
            .instrument(tracing::debug_span!("single_shard")),
        );
    }

    #[test]
    fn shard_test() {
        let _guard = tracing_subscriber::fmt::try_init();
        color_eyre::install().unwrap_or_else(|_| ());

        // 0. Make rt.
        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_time()
            .enable_io()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                // 1. start redis.
                let redis_port = 15215;
                let redis_addr = format!("redis://127.0.0.1:{}", redis_port);
                info!(port = ?redis_port, "start redis");
                let _redis = test_util::start_redis(redis_port);

                // 2. Define addr.
                let si: ShardInfo<SocketAddr> = ShardInfo {
                    canonical_addr: "127.0.0.1:21421".parse().unwrap(),
                    shard_addrs: vec![
                        "127.0.0.1:21422".parse().unwrap(),
                        "127.0.0.1:21423".parse().unwrap(),
                    ],
                    shard_info: super::SimpleShardPolicy {
                        packet_data_offset: 18,
                        packet_data_length: 4,
                    },
                };

                // 3. start shard serv
                let (internal_srv, internal_cli) =
                    RendezvousChannel::<SocketAddr, Msg, _>::new(100).split();

                let rdy = futures_util::stream::FuturesUnordered::new();

                for a in si.clone().shard_addrs {
                    info!(addr = ?&a, "start shard");
                    let internal_srv = OptionUnwrap::from(internal_srv.clone());
                    let external = SerializeChunnel::<_, Msg>::from(TaggerChunnel::from(
                        ReliabilityChunnel::from(UdpReqChunnel::default()),
                    ));

                    let (s, r) = tokio::sync::oneshot::channel();

                    // we use the below block to check that the types work out.
                    //
                    //use bertha::ChunnelConnection;
                    //use futures_util::StreamExt;
                    //let mut foo: Box<dyn ChunnelListener<Addr = SocketAddr, Connection = _>> =
                    //    Box::new(internal_srv) as _;
                    //let foo_c: Box<dyn ChunnelConnection<Data = Msg>> =
                    //    Box::new(foo.listen(a.clone()).await.next().await.unwrap().unwrap()) as _;

                    //let mut bar: Box<dyn ChunnelListener<Addr = SocketAddr, Connection = _>> =
                    //    Box::new(external) as _;
                    //let bar_c: Box<dyn ChunnelConnection<Data = Msg>> =
                    //    Box::new(bar.listen(a.clone()).await.next().await.unwrap().unwrap()) as _;

                    tokio::spawn(async move {
                        let mut shard = ShardServer::new(internal_srv, external);
                        let st = shard.listen(a).await.unwrap();

                        s.send(()).unwrap();

                        match st
                            .try_for_each_concurrent(None, |once| async move {
                                let msg = once.recv().await?;
                                // just echo.
                                once.send(msg).await?;
                                Ok(())
                            })
                            .await
                        {
                            Err(e) => debug!(err = ?e, shard_addr = ?a,  "Shard errorred"),
                            Ok(_) => (),
                        }
                    });

                    rdy.push(r);
                }

                let _: Vec<()> = rdy.try_collect().await.unwrap();

                // 4. start canonical server
                let external = SerializeChunnel::<_, Msg>::from(TaggerChunnel::from(
                    ReliabilityChunnel::from(UdpReqChunnel::default()),
                ));

                // yet more type inference blah
                //
                //let mut foo: Box<dyn ChunnelConnector<Addr = SocketAddr, Connection = _>> =
                //    Box::new(internal_cli) as _;
                //let foo_c: Box<dyn ChunnelConnection<Data = Msg>> =
                //    Box::new(foo.connect(si.canonical_addr).await.unwrap()) as _;
                //
                //use futures_util::stream::StreamExt;
                //let mut bar: Box<dyn ChunnelListener<Addr = SocketAddr, Connection = _>> =
                //    Box::new(external) as _;
                //let bar_c: Box<dyn ChunnelConnection<Data = Msg>> = Box::new(
                //    bar.listen(si.canonical_addr)
                //        .await
                //        .next()
                //        .await
                //        .unwrap()
                //        .unwrap(),
                //) as _;

                info!(shard_info = ?&si, "start canonical server");
                let mut cnsrv = ShardCanonicalServer::new(external, internal_cli, &redis_addr)
                    .await
                    .unwrap();
                let st = cnsrv.listen(si.clone()).await.unwrap();

                tokio::spawn(async move {
                    st.try_for_each_concurrent(None, |r| async move {
                        r.recv().await?;
                        Ok(())
                    })
                    .await
                    .unwrap()
                });

                // 5. make client
                let external = SerializeChunnel::<_, Msg>::from(TaggerChunnel::from(
                    ReliabilityChunnel::from(AddrWrap::from(UdpSkChunnel::default())),
                ));
                let mut cl = ClientShardChunnelClient::new(external, &redis_addr)
                    .await
                    .unwrap();
                info!("make client");
                let cn = cl.connect(si.canonical_addr).await.unwrap();

                // 6. issue a request
                info!("send request");
                cn.send(Msg {
                    k: "aaaaaaaa".to_owned(),
                    v: "bbbbbbbb".to_owned(),
                })
                .await
                .unwrap();

                info!("await response");
                let m = cn.recv().await.unwrap();
                use super::Kv;
                assert_eq!(m.key(), "aaaaaaaa");
                assert_eq!(m.val(), "bbbbbbbb");
            }
            .instrument(tracing::debug_span!("shard_test")),
        );
    }
}
