use bertha::{
    enumerate_enum, ChunnelConnection, ChunnelConnector, Client, ConnectAddress, Either, IpPort,
    Negotiate, Serve,
};
use color_eyre::eyre;
use eyre::{eyre, Error, WrapErr};
use futures_util::stream::{Stream, StreamExt};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, trace};
use tracing_futures::Instrument;

pub const CONTROLLER_ADDRESS: &str = "shard-ctl";

mod redis_util;

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

impl<T, U> Kv for (U, T)
where
    T: Kv,
{
    type Key = T::Key;
    fn key(&self) -> Self::Key {
        self.1.key()
    }

    type Val = T::Val;
    fn val(&self) -> Self::Val {
        self.1.val()
    }
}

const FNV1_64_INIT: u64 = 0xcbf29ce484222325u64;
const FNV_64_PRIME: u64 = 0x100000001b3u64;

enumerate_enum!(pub ShardFns, 0xe898734df758d0c0, Sharding);

/// A chunnel managing a sharded service.
///
/// Forwards incoming messages to one of the internal connections specified by `ShardInfo` after
/// evaluating the sharding function. Also registers with shard-ctl, which will perform other setup
/// (loading XDP program, answering client queries, etc).
#[derive(Clone)]
pub struct ShardCanonicalServer<A, A2, S> {
    addr: ShardInfo<A>,
    shards_inner: S,
    redis_listen_connection: Arc<Mutex<redis::aio::Connection>>,
    _phantom: std::marker::PhantomData<A2>,
}

impl<A, A2, S> ShardCanonicalServer<A, A2, S> {
    /// Inner is a chunnel for the external connection.
    /// Shards is a chunnel for an internal connection to the shards.
    pub async fn new(addr: ShardInfo<A>, shards_inner: S, redis_addr: &str) -> Result<Self, Error> {
        let redis_client = redis::Client::open(redis_addr)?;
        let redis_listen_connection =
            Arc::new(Mutex::new(redis_client.get_async_connection().await?));

        Ok(ShardCanonicalServer {
            addr,
            shards_inner,
            redis_listen_connection,
            _phantom: Default::default(),
        })
    }
}

impl<A, A2, S> Negotiate for ShardCanonicalServer<A, A2, S> {
    type Capability = ShardFns;

    fn capabilities() -> Vec<ShardFns> {
        vec![ShardFns::Sharding]
    }
}

impl<I, Ic, Ie, A, A2, S, D, E> Serve<I> for ShardCanonicalServer<A, A2, S>
where
    I: Stream<Item = Result<Ic, Ie>> + Send + 'static,
    Ic: ChunnelConnection<Data = D> + Send + Sync + 'static,
    Ie: Into<Error> + Send + Sync + 'static,
    A: IpPort
        + Serialize
        + DeserializeOwned
        + Clone
        + std::fmt::Debug
        + std::fmt::Display
        + Send
        + Sync
        + 'static,
    (A, S): Into<A2>,
    A2: ConnectAddress + Send + Sync + 'static,
    <A2 as ConnectAddress>::Connector: Send,
    <<A2 as ConnectAddress>::Connector as ChunnelConnector>::Error:
        Into<Error> + Send + Sync + 'static,
    <<A2 as ConnectAddress>::Connector as ChunnelConnector>::Connection: Send + 'static,
    S: ChunnelConnector<Addr = A2, Error = E> + Clone + Send + Sync + 'static,
    S::Connection: ChunnelConnection<Data = D> + Send + Sync + 'static,
    D: Kv + Send + Sync + 'static,
    <D as Kv>::Key: AsRef<str>,
    E: Into<Error> + Send + Sync + 'static,
{
    type Connection = ShardCanonicalServerConnection<Ic, S::Connection>;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn serve(&mut self, inner: I) -> Self::Future {
        let addr = self.addr.clone();
        let a1 = addr.clone();
        let shards_inner = self.shards_inner.clone();
        let redis_conn = Arc::clone(&self.redis_listen_connection);
        Box::pin(
            async move {
                // redis insert
                redis_util::redis_insert(redis_conn, &addr)
                    .await
                    .wrap_err("Could not register shard info")?;

                let num_shards = addr.shard_addrs.len();
                let addrs = addr.shard_addrs.clone();

                // connect to shards
                let conns: Vec<Arc<S::Connection>> =
                    futures_util::future::join_all(addrs.into_iter().map(|a| async {
                        let a2 = (a, shards_inner.clone()).into();
                        let cn = shards_inner.clone().connect(a2).await.map_err(Into::into)?;
                        Ok::<_, Error>(Arc::new(cn))
                    }))
                    .await
                    .into_iter()
                    .collect::<Result<_, Error>>()
                    .wrap_err("Could not connect to shards")?;

                trace!("connected to shards");

                // serve canonical address
                // we are only responsible for the canonical address here.
                Ok::<_, Error>(Box::pin(inner.map(move |conn| {
                    Ok(ShardCanonicalServerConnection {
                        inner: Arc::new(conn.map_err(Into::into)?),
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
                })) as _)
            }
            .instrument(tracing::debug_span!("serve", addr = ?&a1.clone())),
        )
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
        Box::pin(
            async move {
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
                trace!(shard_idx, "got packet");

                // 2. Forward to the shard.
                conn.send(data).await?;

                // 3. Get response from the shard, and send back to client.
                let resp = conn.recv().await?;
                trace!(shard_idx, "got shard response");
                inner.send(resp).await?;

                Ok(())
            }
            .instrument(tracing::trace_span!("server-shard-recv")),
        )
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
pub struct ShardServer<S> {
    internal: Arc<Mutex<Option<S>>>,
}

impl<S> Clone for ShardServer<S> {
    fn clone(&self) -> Self {
        ShardServer {
            internal: Arc::clone(&self.internal),
        }
    }
}

impl ShardServer<()> {
    /// internal: A way to listen for messages forwarded from the fallback canonical address listener.
    /// external: Listen for messages over the network.
    pub fn new<S>(internal: S) -> ShardServer<S> {
        ShardServer {
            internal: Arc::new(Mutex::new(Some(internal))),
        }
    }
}

impl<S> Negotiate for ShardServer<S> {
    type Capability = ShardFns;
    fn capabilities() -> Vec<ShardFns> {
        vec![]
    }
}

impl<I, Ic, Ie, S, Sc, D, E> Serve<I> for ShardServer<S>
where
    I: Stream<Item = Result<Ic, Ie>> + Send + 'static,
    Ic: ChunnelConnection<Data = D> + Send + Sync + 'static,
    Ie: Into<Error> + Send + Sync + 'static,
    S: Stream<Item = Result<Sc, E>> + Send + 'static,
    Sc: ChunnelConnection<Data = D> + Send + Sync + 'static,
    E: Into<Error> + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Connection = Either<Ic, Sc>;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn serve(&mut self, inner: I) -> Self::Future {
        let int_handle = self.internal.clone();
        Box::pin(async move {
            let mut int_guard = int_handle.lock().await;
            let int = int_guard.take();

            if let None = int {
                // self.internal was given to whoever called serve() first.
                return Err(eyre!("Cannot ShardServer::serve() more than once."));
            }

            let ext_str = inner.map(|conn| Ok(Either::Left(conn.map_err(Into::into)?)));
            let int_str = int
                .unwrap()
                .map(|conn| Ok(Either::Right(conn.map_err(Into::into)?)));

            Ok(Box::pin(futures_util::stream::select(ext_str, int_str))
                as Pin<
                    Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>,
                >)
        })
    }
}

/// Client-side sharding chunnel implementation.
///
/// Contacts shard-ctl for sharding information, and does client-side sharding.
#[derive(Clone)]
pub struct ClientShardChunnelClient<A, A2> {
    addr: A,
    redis_listen_connection: Arc<Mutex<redis::aio::Connection>>,
    _phantom: std::marker::PhantomData<A2>,
}

impl<A, A2> ClientShardChunnelClient<A, A2> {
    pub async fn new(addr: A, redis_addr: &str) -> Result<Self, Error> {
        let redis_client = redis::Client::open(redis_addr)?;
        let redis_listen_connection =
            Arc::new(Mutex::new(redis_client.get_async_connection().await?));

        Ok(ClientShardChunnelClient {
            addr,
            redis_listen_connection,
            _phantom: Default::default(),
        })
    }
}

impl<A, A2> Negotiate for ClientShardChunnelClient<A, A2> {
    type Capability = ShardFns;
    fn capabilities() -> Vec<ShardFns> {
        vec![ShardFns::Sharding]
    }
}

impl<A, A2, I, D> Client<I> for ClientShardChunnelClient<A, A2>
where
    A: Serialize
        + DeserializeOwned
        + Clone
        + std::cmp::PartialEq
        + std::fmt::Debug
        + std::fmt::Display
        + Send
        + Sync
        + 'static,
    A: Into<A2>,
    A2: Clone + std::fmt::Debug + std::fmt::Display + Send + Sync + 'static,
    I: ChunnelConnection<Data = (A2, D)> + Send + Sync + 'static,
    D: Kv + Send + Sync + 'static,
    <D as Kv>::Key: AsRef<str>,
{
    type Connection = ClientShardClientConnection<A2, I, D>;
    type Error = Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    // implementing this is tricky - we have been given one connection with some semantics,
    // but we need n connections, to each shard.
    // 1. ignore the connection (to the canonical_addr) to establish our own, to the shards.
    // 2. force the connection to have (addr, data) semantics, so we can do routing without
    //    establishing our own connections
    //
    //  -> pick #2
    fn connect_wrap(&mut self, inner: I) -> Self::Future {
        let a = self.addr.clone();
        let redis_conn = Arc::clone(&self.redis_listen_connection);
        Box::pin(
            async move {
                // query redis for si
                let si = redis_util::redis_query(&a, redis_conn.lock().await)
                    .await
                    .wrap_err("redis query failed")?;

                let addrs = match si {
                    None => vec![a.into()],
                    Some(si) => {
                        let mut addrs = vec![si.canonical_addr.into()];
                        addrs.extend(si.shard_addrs.into_iter().map(Into::into));
                        addrs
                    }
                };

                debug!(addrs = ?&addrs, "Decided sharding plan");
                let num_shards = addrs.len() - 1;
                Ok(ClientShardClientConnection {
                    inner: Arc::new(inner),
                    shard_addrs: addrs,
                    shard_fn: Arc::new(move |d| {
                        if num_shards == 0 {
                            return 0;
                        }
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

                        (hash as usize % num_shards) + 1
                    }),
                })
            }
            .instrument(tracing::debug_span!("connect")),
        )
    }
}

/// `ChunnelConnection` type for ClientShardChunnelClient.
pub struct ClientShardClientConnection<A, C, D>
where
    C: ChunnelConnection<Data = (A, D)>,
{
    inner: Arc<C>,
    shard_addrs: Vec<A>,
    shard_fn: Arc<dyn Fn(&D) -> usize + Send + Sync + 'static>,
}

impl<A, C, D> ChunnelConnection for ClientShardClientConnection<A, C, D>
where
    A: Clone + Send + Sync + 'static,
    C: ChunnelConnection<Data = (A, D)> + Send + Sync + 'static,
    D: Kv + Send + Sync + 'static,
{
    type Data = D;

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>> {
        let inner = Arc::clone(&self.inner);
        // figure out which shard to send to.
        let shard_idx = (self.shard_fn)(&data);
        let a = self.shard_addrs[shard_idx].clone();
        Box::pin(async move { inner.send((a, data)).await })
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            let (_, d) = inner.recv().await?;
            Ok(d)
        })
    }
}

//#[cfg(feature = "ebpf")]
//pub use ebpf::ShardCanonicalServerEbpf;
//
//#[cfg(feature = "ebpf")]
//mod ebpf {
//    use super::eyre::Error;
//    use super::eyre::{Report, WrapErr};
//    use super::{ShardCanonicalServerConnection, ShardFns};
//    use crate::{Kv, ShardInfo};
//    use bertha::{ChunnelConnection, ChunnelConnector, ChunnelListener, IpPort, Negotiate};
//    use futures_util::stream::{Stream, StreamExt};
//    use serde::{de::DeserializeOwned, Serialize};
//    use std::collections::HashMap;
//    use std::future::Future;
//    use std::pin::Pin;
//    use std::sync::Arc;
//    use tokio::sync::Mutex;
//    use tracing::{debug, info, trace, warn};
//    use tracing_futures::Instrument;
//
//    /// A chunnel managing a sharded service.
//    ///
//    /// Listens on an external connection, then forwards messages to one of the internal
//    /// connections after evaluating the sharding function. Also registers with shard-ctl, which
//    /// will perform other setup (loading XDP program, answering client queries, etc).  Does not
//    /// implement `connect()`.
//    pub struct ShardCanonicalServerEbpf<C, S> {
//        inner: Arc<Mutex<C>>,
//        shards_inner: Arc<Mutex<S>>,
//        redis_listen_connection: Arc<Mutex<redis::aio::Connection>>,
//        handles: Arc<Mutex<HashMap<String, xdp_shard::BpfHandles<xdp_shard::Ingress>>>>,
//    }
//
//    impl<C, S> ShardCanonicalServerEbpf<C, S> {
//        /// Inner is a chunnel for the external connection.
//        /// Shards is a chunnel for an internal connection to the shards.
//        pub async fn new(inner: C, shards: S, redis_addr: &str) -> Result<Self, Report> {
//            let redis_client = redis::Client::open(redis_addr)?;
//            let redis_listen_connection =
//                Arc::new(Mutex::new(redis_client.get_async_connection().await?));
//
//            Ok(ShardCanonicalServerEbpf {
//                inner: Arc::new(Mutex::new(inner)),
//                shards_inner: Arc::new(Mutex::new(shards)),
//                redis_listen_connection,
//                handles: Default::default(),
//            })
//        }
//
//        /// Future which will poll stats from the ebpf program.
//        ///
//        /// If file is Some, the stats will be dumped to the file in addition to being logged via
//        /// the tracing crate.
//        pub async fn log_shard_stats(&self, file: Option<std::fs::File>) {
//            read_shard_stats(Arc::clone(&self.handles), file)
//                .instrument(tracing::info_span!("read-shard-stats"))
//                .await
//                .expect("read_shard_stats")
//        }
//    }
//
//    impl<C, A, S, D, E1, E2> Negotiate<ShardFns> for ShardCanonicalServerEbpf<C, S>
//    where
//        C: ChunnelListener<Addr = A, Error = E1> + Send + Sync + 'static,
//        C::Connection: ChunnelConnection<Data = D> + Send + Sync + 'static,
//        S: ChunnelConnector<Addr = A, Error = E2> + Send + Sync + 'static,
//        S::Connection: ChunnelConnection<Data = D> + Send + Sync + 'static,
//        D: Kv + Send + Sync + 'static,
//        <D as Kv>::Key: AsRef<str>,
//        E1: Into<Error> + Send + Sync + 'static,
//        E2: Into<Error> + Send + Sync + 'static,
//        A: IpPort
//            + Clone
//            + std::fmt::Debug
//            + std::fmt::Display
//            + Send
//            + Sync
//            + 'static
//            + Serialize
//            + DeserializeOwned,
//    {
//        fn capabilities() -> Vec<ShardFns> {
//            vec![ShardFns::Sharding]
//        }
//    }
//
//    impl<C, A, S, D, E1, E2> ChunnelListener for ShardCanonicalServerEbpf<C, S>
//    where
//        C: ChunnelListener<Addr = A, Error = E1> + Send + Sync + 'static,
//        C::Connection: ChunnelConnection<Data = D> + Send + Sync + 'static,
//        S: ChunnelConnector<Addr = A, Error = E2> + Send + Sync + 'static,
//        S::Connection: ChunnelConnection<Data = D> + Send + Sync + 'static,
//        D: Kv + Send + Sync + 'static,
//        <D as Kv>::Key: AsRef<str>,
//        E1: Into<Error> + Send + Sync + 'static,
//        E2: Into<Error> + Send + Sync + 'static,
//        A: IpPort
//            + Clone
//            + std::fmt::Debug
//            + std::fmt::Display
//            + Send
//            + Sync
//            + 'static
//            + Serialize
//            + DeserializeOwned,
//    {
//        type Addr = ShardInfo<A>;
//        type Connection = ShardCanonicalServerConnection<C::Connection, S::Connection>;
//        type Future =
//            Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
//        type Stream =
//            Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;
//        type Error = Report;
//
//        fn listen(&mut self, a: Self::Addr) -> Self::Future {
//            let a1 = a.clone();
//            let inner = Arc::clone(&self.inner);
//            let shards_inner = Arc::clone(&self.shards_inner);
//            let redis_conn = Arc::clone(&self.redis_listen_connection);
//            let handles = Arc::clone(&self.handles);
//            Box::pin(
//                async move {
//                    // redis insert
//                    super::redis_insert(redis_conn, &a)
//                        .await
//                        .wrap_err("Could not register shard info")?;
//
//                    // if ebpf, ebpf initialization
//                    register_shardinfo(handles, a.clone())
//                        .instrument(tracing::debug_span!("register-shardinfo-ebpf"))
//                        .await;
//
//                    let num_shards = a.shard_addrs.len();
//                    let addrs = a.shard_addrs.clone();
//
//                    // connect to shards
//                    let conns: Vec<Arc<S::Connection>> = match futures_util::future::join_all(
//                        addrs.clone().into_iter().map(|addr| async {
//                            Ok::<_, Report>(Arc::new(
//                                shards_inner
//                                    .lock()
//                                    .await
//                                    .connect(addr)
//                                    .await
//                                    .map_err(|e| e.into())?,
//                            ))
//                        }),
//                    )
//                    .await
//                    .into_iter()
//                    .collect()
//                    {
//                        Ok(a) => a,
//                        Err(e) => {
//                            return Err(e).wrap_err("Could not connect to shards");
//                        }
//                    };
//
//                    // serve canonical address
//                    // we are only responsible for the canonical address here.
//                    Ok(Box::pin(
//                        inner
//                            .lock()
//                            .await
//                            .listen(a.canonical_addr)
//                            .await
//                            .map_err(|e| e.into())?
//                            .map(move |conn| {
//                                Ok(ShardCanonicalServerConnection {
//                                    inner: Arc::new(conn.map_err(|e| e.into())?),
//                                    shards: conns.clone(),
//                                    shard_fn: Arc::new(move |d| {
//                                        /* xdp_shard version of FNV: take the first 4 bytes of the key
//                                        * u64 hash = FNV1_64_INIT;
//                                        * // ...
//                                        * // value start
//                                        * pkt_val = ((u8*) app_data) + offset;
//
//                                        * // compute FNV hash
//                                        * #pragma clang loop unroll(full)
//                                        * for (i = 0; i < 4; i++) {
//                                        *     hash = hash ^ ((u64) pkt_val[i]);
//                                        *     hash *= FNV_64_PRIME;
//                                        * }
//
//                                        * // map to a shard and assign to that port.
//                                        * idx = hash % shards->num;
//                                        */
//                                        let mut hash = super::FNV1_64_INIT;
//                                        for b in d.key().as_ref().as_bytes()[0..4].iter() {
//                                            hash = hash ^ (*b as u64);
//                                            hash = u64::wrapping_mul(hash, super::FNV_64_PRIME);
//                                        }
//
//                                        hash as usize % num_shards
//                                    }),
//                                })
//                            }),
//                    ) as _)
//                }
//                .instrument(tracing::debug_span!("listen", addr = ?&a1)),
//            )
//        }
//
//        fn scope() -> bertha::Scope {
//            bertha::Scope::Host
//        }
//
//        fn endedness() -> bertha::Endedness {
//            bertha::Endedness::Either
//        }
//
//        fn implementation_priority() -> usize {
//            2
//        }
//    }
//
//    async fn clear_port<T>(
//        handles: Arc<Mutex<HashMap<String, xdp_shard::BpfHandles<T>>>>,
//        port: u16,
//    ) {
//        let mut map = handles.lock().await;
//        for h in map.values_mut() {
//            if let Err(e) = h.clear_port(port) {
//                debug!(err = ?e, port = ?port, "Failed to clear port map");
//            }
//        }
//    }
//
//    pub(crate) async fn register_shardinfo<A: IpPort + Clone>(
//        handles: Arc<Mutex<HashMap<String, xdp_shard::BpfHandles<xdp_shard::Ingress>>>>,
//        si: ShardInfo<A>,
//    ) {
//        let shard_ports: Vec<u16> = si.shard_addrs.iter().map(|a| a.port()).collect();
//        let sk = si.canonical_addr.clone();
//
//        clear_port(handles.clone(), sk.port()).await;
//
//        // see if handles are there already
//        let ifnames = match xdp_shard::get_interface_name(sk.ip()) {
//            Ok(ifnames) => ifnames,
//            Err(e) => {
//                warn!(err = ?e, ip = ?sk.ip(), port = ?sk.port(), "Error getting interface");
//                return;
//            }
//        };
//
//        let mut map = handles.lock().await;
//        for ifn in ifnames {
//            if !map.contains_key(&ifn) {
//                let l = xdp_shard::BpfHandles::<xdp_shard::Ingress>::load_on_interface_name(&ifn);
//                let handle = match l {
//                    Ok(s) => s,
//                    Err(e) => {
//                        warn!(err = ?e, ifn = ?&ifn, "Error loading xdp-shard");
//                        return;
//                    }
//                };
//
//                info!(service_ip = ?&si.canonical_addr.ip(), service_port = ?&si.canonical_addr.port(), "Loaded XDP program");
//
//                map.insert(ifn.clone(), handle);
//            }
//
//            let h = map.get_mut(&ifn).unwrap();
//            let ok = h.shard_ports(
//                sk.port(),
//                &shard_ports[..],
//                si.shard_info.packet_data_offset,
//                si.shard_info.packet_data_length,
//            );
//            match ok {
//                Ok(_) => (),
//                Err(e) => {
//                    warn!(
//                        err = ?e,
//                        ifn = ?&ifn,
//                        port = ?sk.port(),
//                        shard_port = ?&shard_ports[..],
//                        "Error writing xdp-shard ports",
//                    );
//                    return;
//                }
//            }
//
//            info!(
//                service_ip = ?si.canonical_addr.ip(),
//                service_port = ?si.canonical_addr.port(),
//                interface = ?&ifn,
//                "Registered sharding with XDP program"
//            );
//        }
//    }
//
//    pub(crate) async fn read_shard_stats(
//        handles: Arc<Mutex<HashMap<String, xdp_shard::BpfHandles<xdp_shard::Ingress>>>>,
//        mut stats_log: Option<std::fs::File>,
//    ) -> Result<(), Error> {
//        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
//        let mut first = true;
//        let start = std::time::Instant::now();
//        loop {
//            interval.tick().await;
//            for (inf, handle) in handles.lock().await.iter_mut() {
//                match handle.get_stats() {
//                    Ok((c, p)) => {
//                        let mut c = c.get_rxq_cpu_port_count();
//                        xdp_shard::diff_maps(&mut c, &p.get_rxq_cpu_port_count());
//                        trace!("logging XDP stats");
//                        // c: rxq_id -> cpu_id -> port -> count
//                        for (rxq_id, cpu_map) in c.iter().enumerate() {
//                            for (cpu_id, port_map) in cpu_map.iter().enumerate() {
//                                for (port, count) in port_map.iter() {
//                                    info!(
//                                        interface = ?inf,
//                                        rxq = ?rxq_id,
//                                        cpu = ?cpu_id,
//                                        port = ?port,
//                                        count = ?count,
//                                        "XDP stats"
//                                    );
//
//                                    if let Some(ref mut f) = stats_log {
//                                        use std::io::Write;
//                                        if first {
//                                            if let Err(e) = write!(f, "Time CPU Rxq Port Count\n") {
//                                                debug!(err = ?e, "Failed writing to stats_log");
//                                                continue;
//                                            }
//
//                                            first = false;
//                                        }
//
//                                        if let Err(e) = write!(
//                                            f,
//                                            "{} {} {} {} {}\n",
//                                            start.elapsed().as_secs_f32(),
//                                            cpu_id,
//                                            rxq_id,
//                                            port,
//                                            count
//                                        ) {
//                                            debug!(err = ?e, "Failed writing to stats_log");
//                                        }
//                                    }
//                                }
//                            }
//                        }
//                    }
//                    Err(e) => {
//                        debug!(err = ?e, "Could not get rxq stats");
//                    }
//                }
//            }
//        }
//    }
//}

#[cfg(test)]
mod test {
    use super::{ClientShardChunnelClient, Kv, ShardCanonicalServer, ShardInfo, ShardServer};
    use bertha::{
        bincode::{SerializeChunnel, SerializeChunnelProject},
        chan_transport::{RendezvousChannel, RendezvousChannelAddr},
        reliable::{ReliabilityChunnel, ReliabilityProjChunnel},
        tagger::{TaggerChunnel, TaggerProjChunnel},
        udp::{UdpReqAddr, UdpSkChunnel, UdpSocketAddr},
        util::OptionUnwrap,
        ChunnelConnection, ChunnelConnector, ChunnelListener, Client, ConnectAddress, CxList,
        ListenAddress, Serve,
    };
    use color_eyre::eyre;
    use eyre::{eyre, WrapErr};
    use futures_util::TryStreamExt;
    use serde::{Deserialize, Serialize};
    use std::net::SocketAddr;
    use tracing::{debug, info, trace};
    use tracing_futures::Instrument;

    #[derive(Default, Clone, Debug, PartialEq, Serialize, Deserialize)]
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

                let (mut internal_srv, internal_cli) =
                    RendezvousChannel::<SocketAddr, Msg, _>::new(100).split();

                info!(addr = ?&addr, "start shard");

                tokio::spawn(
                    async move {
                        let internal_st = internal_srv.listen(addr).await.unwrap();
                        let internal_st = OptionUnwrap.serve(internal_st).await.unwrap();
                        let i = ShardServer::new(internal_st);
                        let mut external = CxList::from(i)
                            .wrap(TaggerChunnel)
                            .wrap(ReliabilityChunnel::default())
                            .wrap(SerializeChunnel::default());
                        info!(addr = ?&addr, "listening");
                        let addr: UdpReqAddr = addr.into();
                        let st = addr.listener().listen(addr).await.unwrap();
                        let st = external.serve(st).await.unwrap();
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
                    let a = RendezvousChannelAddr::from((addr, internal_cli));
                    let cn = a
                        .connector()
                        .connect(a)
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
                    assert_eq!(m.key(), "a");
                    assert_eq!(m.val(), "b");
                }
                .instrument(tracing::info_span!("chan client"))
                .await;

                // udp connection
                async {
                    debug!("connect to shard");
                    let mut external = CxList::from(TaggerChunnel)
                        .wrap(ReliabilityChunnel::default())
                        .wrap(SerializeChunnel::default());

                    let a = UdpSocketAddr::from(addr);
                    let cn = a
                        .connector()
                        .connect(a)
                        .await
                        .wrap_err(eyre!("client connect"))
                        .unwrap();
                    let cn = external.connect_wrap(cn).await.unwrap();
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
                    assert_eq!(m.key(), "c");
                    assert_eq!(m.val(), "d");
                }
                .instrument(tracing::info_span!("udp client"))
                .await;
            }
            .instrument(tracing::debug_span!("single_shard")),
        );
    }

    async fn shard_setup(redis_port: u16, srv_port: u16) -> (test_util::Redis, SocketAddr) {
        // 1. start redis.
        let redis_addr = format!("redis://127.0.0.1:{}", redis_port);
        info!(port = ?redis_port, "start redis");
        let redis_guard = test_util::start_redis(redis_port);

        let shard1_port = srv_port + 1;
        let shard2_port = srv_port + 2;
        // 2. Define addr.
        let si: ShardInfo<SocketAddr> = ShardInfo {
            canonical_addr: format!("127.0.0.1:{}", srv_port).parse().unwrap(),
            shard_addrs: vec![
                format!("127.0.0.1:{}", shard1_port).parse().unwrap(),
                format!("127.0.0.1:{}", shard2_port).parse().unwrap(),
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

            let (s, r) = tokio::sync::oneshot::channel();

            let int_srv = internal_srv.clone();
            tokio::spawn(async move {
                let mut internal_srv = int_srv;
                let internal_st = internal_srv.listen(a).await.unwrap();
                let internal_st = OptionUnwrap.serve(internal_st).await.unwrap();
                let i = ShardServer::new(internal_st);
                let mut external = CxList::from(i)
                    .wrap(TaggerChunnel)
                    .wrap(ReliabilityChunnel::default())
                    .wrap(SerializeChunnel::default());
                info!(addr = ?&a, "listening");

                let addr: UdpReqAddr = a.into();
                let st = addr.listener().listen(addr).await.unwrap();
                let st = external.serve(st).await.unwrap();
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
        let cnsrv = ShardCanonicalServer::new(si.clone(), internal_cli, &redis_addr)
            .await
            .unwrap();
        let mut external = CxList::from(cnsrv)
            .wrap(TaggerChunnel)
            .wrap(ReliabilityChunnel::default())
            .wrap(SerializeChunnel::default());
        info!(shard_info = ?&si, "start canonical server");
        let a: UdpReqAddr = si.canonical_addr.into();
        let st = a.listener().listen(a).await.unwrap();

        let st = external.serve(st).await.unwrap();

        tokio::spawn(async move {
            st.try_for_each_concurrent(None, |r| async move {
                r.recv().await?; // ShardCanonicalServerConnection is recv-only
                Ok(())
            })
            .await
            .unwrap()
        });

        (redis_guard, si.canonical_addr)
    }

    #[test]
    fn shard_test_shardclient() {
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
                // 0-4. make shard servers and shard canonical server
                let (redis_h, canonical_addr) = shard_setup(15215, 21421).await;
                let redis_addr = redis_h.get_addr();

                // 5. make client
                info!("make client");
                let cl = ClientShardChunnelClient::new(canonical_addr, &redis_addr)
                    .await
                    .unwrap();
                let cn = UdpSkChunnel::default().connect(()).await.unwrap(); // above: (A, Vec<u8>)
                let mut stack = CxList::from(cl) // D = (u32, Option<D>), above: D, below: (A, D)
                    .wrap(TaggerProjChunnel) // above: D, below: (u32, D)
                    .wrap(ReliabilityProjChunnel::default()) //  above: (u32, D), below: (u32, Option<D>)
                    .wrap(SerializeChunnelProject::default()); // above: (A, D), below: (A, Vec<u8>)
                let cn = stack.connect_wrap(cn).await.unwrap();

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

    #[test]
    fn shard_test_canonicalclient() {
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
                // 0-4. make shard servers and shard canonical server
                let (_redis_h, canonical_addr) = shard_setup(25215, 31421).await;

                // 5. make client
                info!("make client");
                let mut stack = CxList::from(TaggerChunnel)
                    .wrap(ReliabilityChunnel::default())
                    .wrap(SerializeChunnel::default());
                let a = UdpSocketAddr::from(canonical_addr);
                let cn = a
                    .connector()
                    .connect(a)
                    .await
                    .wrap_err(eyre!("client connect"))
                    .unwrap();
                let cn = stack.connect_wrap(cn).await.unwrap();

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

    //#[cfg(feature = "ebpf")]
    //#[test]
    //fn shard_negotiate_test() {
    //    use super::ebpf::ShardCanonicalServerEbpf;

    //    let _guard = tracing_subscriber::fmt::try_init();
    //    color_eyre::install().unwrap_or_else(|_| ());

    //    // 0. Make rt.
    //    let mut rt = tokio::runtime::Builder::new()
    //        .basic_scheduler()
    //        .enable_time()
    //        .enable_io()
    //        .build()
    //        .unwrap();

    //    rt.block_on(
    //        async move {
    //            // 1. start redis.
    //            let redis_port = 12425;
    //            let redis_addr = format!("redis://127.0.0.1:{}", redis_port);
    //            info!(port = ?redis_port, "start redis");
    //            let _redis = test_util::start_redis(redis_port);

    //            // 2. Define addr.
    //            let si: ShardInfo<SocketAddr> = ShardInfo {
    //                canonical_addr: "127.0.0.1:21471".parse().unwrap(),
    //                shard_addrs: vec![
    //                    "127.0.0.1:21472".parse().unwrap(),
    //                    "127.0.0.1:21473".parse().unwrap(),
    //                ],
    //                shard_info: super::SimpleShardPolicy {
    //                    packet_data_offset: 18,
    //                    packet_data_length: 4,
    //                },
    //            };

    //            // 3. start shard serv
    //            let (internal_srv, internal_cli) =
    //                RendezvousChannel::<SocketAddr, Msg, _>::new(100).split();

    //            let rdy = futures_util::stream::FuturesUnordered::new();

    //            for a in si.clone().shard_addrs {
    //                info!(addr = ?&a, "start shard");
    //                let internal_srv = OptionUnwrap::from(internal_srv.clone());
    //                let external = SerializeChunnel::<_, Msg>::from(TaggerChunnel::from(
    //                    ReliabilityChunnel::from(UdpReqChunnel::default()),
    //                ));

    //                let (s, r) = tokio::sync::oneshot::channel();

    //                tokio::spawn(async move {
    //                    let mut shard = ShardServer::new(internal_srv, external);
    //                    let st = shard.listen(a).await.unwrap();

    //                    s.send(()).unwrap();

    //                    match st
    //                        .try_for_each_concurrent(None, |once| async move {
    //                            let msg = once.recv().await?;
    //                            // just echo.
    //                            once.send(msg).await?;
    //                            Ok(())
    //                        })
    //                        .await
    //                    {
    //                        Err(e) => debug!(err = ?e, shard_addr = ?a,  "Shard errorred"),
    //                        Ok(_) => (),
    //                    }
    //                });

    //                rdy.push(r);
    //            }

    //            let _: Vec<()> = rdy.try_collect().await.unwrap();

    //            // 4. start canonical server
    //            let external = SerializeChunnel::<_, Msg>::from(TaggerChunnel::from(
    //                ReliabilityChunnel::from(UdpReqChunnel::default()),
    //            ));

    //            info!(shard_info = ?&si, "start canonical server");
    //            let cnsrv =
    //                ShardCanonicalServer::new(external.clone(), internal_cli.clone(), &redis_addr)
    //                    .await
    //                    .unwrap();
    //            let cnsrv_ebpf = ShardCanonicalServerEbpf::new(external, internal_cli, &redis_addr)
    //                .await
    //                .unwrap();

    //            let mut cnsrv = (cnsrv, cnsrv_ebpf);
    //            let st = cnsrv.listen(si.clone()).await.unwrap();

    //            tokio::spawn(async move {
    //                st.try_for_each_concurrent(None, |r| async move {
    //                    r.recv().await?;
    //                    Ok(())
    //                })
    //                .await
    //                .unwrap()
    //            });

    //            // 5. make client
    //            let external = SerializeChunnel::<_, Msg>::from(TaggerChunnel::from(
    //                ReliabilityChunnel::from(AddrWrap::from(UdpSkChunnel::default())),
    //            ));
    //            let cl = ClientShardChunnelClient::new(external.clone(), &redis_addr)
    //                .await
    //                .unwrap();
    //            let mut cl = (cl, external);

    //            info!("connect client");
    //            let cn = cl.connect(si.canonical_addr).await.unwrap();

    //            // 6. issue a request
    //            info!("send request");
    //            cn.send(Msg {
    //                k: "aaaaaaaa".to_owned(),
    //                v: "bbbbbbbb".to_owned(),
    //            })
    //            .await
    //            .unwrap();

    //            info!("await response");
    //            let m = cn.recv().await.unwrap();
    //            use super::Kv;
    //            assert_eq!(m.key(), "aaaaaaaa");
    //            assert_eq!(m.val(), "bbbbbbbb");
    //        }
    //        .instrument(tracing::debug_span!("negotiate_test")),
    //    );
    //}
}
