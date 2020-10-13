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
use tracing::{debug, trace, warn};
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
/// Forwards incoming messages to one of the internal connections specified by `shards_inner` after
/// evaluating the sharding function.
/// `shards_extern` should be a connection that can talk to a shard with `Data = (A,
/// Vec<u8)` semantics.
#[derive(Clone)]
pub struct ShardCanonicalServer<A, A2, S, C> {
    addr: ShardInfo<A>,
    shards_inner: S,
    shards_extern: Arc<C>,
    shards_extern_nonce: Vec<Vec<bertha::negotiate::Offer>>,
    redis_listen_connection: Arc<Mutex<redis::aio::Connection>>,
    _phantom: std::marker::PhantomData<A2>,
}

impl<A, A2, S, C> ShardCanonicalServer<A, A2, S, C> {
    /// Inner is a chunnel for the external connection.
    /// Shards is a chunnel for an internal connection to the shards.
    pub async fn new(
        addr: ShardInfo<A>,
        shards_inner: S,
        shards_extern: C,
        shards_extern_nonce: Vec<Vec<bertha::negotiate::Offer>>,
        redis_addr: &str,
    ) -> Result<Self, Error> {
        let redis_client = redis::Client::open(redis_addr)?;
        let redis_listen_connection =
            Arc::new(Mutex::new(redis_client.get_async_connection().await?));

        Ok(ShardCanonicalServer {
            addr,
            shards_inner,
            shards_extern: Arc::new(shards_extern),
            shards_extern_nonce,
            redis_listen_connection,
            _phantom: Default::default(),
        })
    }
}

impl<A, A2, A3, S, C> Negotiate for ShardCanonicalServer<A, A2, S, C>
where
    A: Into<A3> + Clone + std::fmt::Debug + Send + Sync + 'static,
    A3: Send + PartialEq,
    C: ChunnelConnection<Data = (A3, Vec<u8>)> + Send + Sync + 'static,
{
    type Capability = ShardFns;

    fn capabilities() -> Vec<ShardFns> {
        vec![ShardFns::Sharding]
    }

    fn picked<'s>(&mut self, nonce: &'s [u8]) -> Pin<Box<dyn Future<Output = ()> + Send + 's>> {
        let addrs = self.addr.shard_addrs.clone();
        let cn = Arc::clone(&self.shards_extern);
        let offer: Vec<bertha::negotiate::Offer> = self
            .shards_extern_nonce
            .iter()
            .map(|o| o[0].clone())
            .collect();
        let msg: bertha::negotiate::NegotiateMsg = match bincode::deserialize(nonce) {
            Err(e) => {
                warn!(err = ?e, "deserialize failed");
                return Box::pin(futures_util::future::ready(()));
            }
            Ok(m) => m,
        };

        let msg = match msg {
            bertha::negotiate::NegotiateMsg::ServerNonce { addr, .. } => {
                bertha::negotiate::NegotiateMsg::ServerNonce {
                    addr,
                    picked: offer,
                }
            }
            _ => {
                warn!("malformed nonce");
                return Box::pin(futures_util::future::ready(()));
            }
        };

        let buf = match bincode::serialize(&msg) {
            Err(e) => {
                warn!(err = ?e, "serialize failed");
                return Box::pin(futures_util::future::ready(()));
            }
            Ok(m) => m,
        };

        Box::pin(async move {
            futures_util::future::join_all(addrs.into_iter().map(|shard| {
                let buf = buf.clone();
                let cn = Arc::clone(&cn);
                async move {
                if let Err(e) = cn.send((shard.clone().into(), buf.clone())).await {
                    warn!(shard = ?&shard, err = ?e, "failed sending negotiation nonce to shard");
                }

                match cn.recv().await {
                    Ok((a, buf)) => match bincode::deserialize(&buf) {
                        Ok(bertha::negotiate::NegotiateMsg::ServerNonceAck) => {
                            if a != shard.clone().into() {
                                warn!(shard = ?&shard, "received from unexpected address");
                            }

                            debug!(shard = ?&shard, "got nonce ack");
                        }
                        Ok(m) => {
                            warn!(shard = ?&shard, msg = ?m, "got unexpected response to nonce");
                        }
                        Err(e) => {
                            warn!(shard = ?&shard, err = ?e, "failed deserializing nonce ack");
                        }
                    },
                    Err(e) => {
                        warn!(shard = ?&shard, err = ?e, "failed waiting for nonce ack");
                    }
                }
            }})).await;
        })
    }
}

impl<I, Ic, Ie, A, A2, S, C, D, E> Serve<I> for ShardCanonicalServer<A, A2, S, C>
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
    A2: ConnectAddress<Connector = S> + Send + Sync + 'static,
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
                trace!("redis_insert");

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
                // TODO this assumes no reordering.
                conn.send(data).await.wrap_err("Forward to shard")?;

                // 3. Get response from the shard, and send back to client.
                let resp = conn.recv().await.wrap_err("Receive from shard")?;
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
/// chunnel from the canonical_addr proxy.  The first caller to call `serve` will get the internal
/// chunnel, and subsequent callers will error.
pub struct ShardServer<S> {
    // The `Arc<Mutex<...>>` is necessary for the clone implementation.
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
                // We CANNOT hand out multiple copies of self.internal by e.g. cloning because the
                // incoming messages on the Stream would get split between the copies.
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

impl<A, A2> std::fmt::Debug for ClientShardChunnelClient<A, A2>
where
    A: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientShardChunnelClient")
            .field("addr", &self.addr)
            .finish()
    }
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
    type Connection = ClientShardClientConnection<A2, D, I>;
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
                Ok(ClientShardClientConnection::new(inner, addrs, move |d| {
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
                }))
            }
            .instrument(tracing::debug_span!("connect")),
        )
    }
}

/// `ChunnelConnection` type for ClientShardChunnelClient.
pub struct ClientShardClientConnection<A, D, C>
where
    C: ChunnelConnection<Data = (A, D)>,
{
    inner: Arc<C>,
    shard_addrs: Vec<A>,
    shard_fn: Arc<dyn Fn(&D) -> usize + Send + Sync + 'static>,
}

impl<A, C, D> ClientShardClientConnection<A, D, C>
where
    C: ChunnelConnection<Data = (A, D)>,
{
    pub fn new(
        inner: C,
        shard_addrs: Vec<A>, // canonical_addr is the first
        shard_fn: impl Fn(&D) -> usize + Send + Sync + 'static,
    ) -> Self {
        ClientShardClientConnection {
            inner: Arc::new(inner),
            shard_addrs,
            shard_fn: Arc::new(shard_fn),
        }
    }
}

impl<A, C, D> ChunnelConnection for ClientShardClientConnection<A, D, C>
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

#[cfg(feature = "ebpf")]
pub use ebpf::ShardCanonicalServerEbpf;

#[cfg(feature = "ebpf")]
mod ebpf;

#[cfg(test)]
mod test {
    use super::{ClientShardChunnelClient, Kv, ShardCanonicalServer, ShardInfo, ShardServer};
    use bertha::{
        bincode::{SerializeChunnel, SerializeChunnelProject},
        chan_transport::{RendezvousChannel, RendezvousChannelAddr},
        reliable::{ReliabilityChunnel, ReliabilityProjChunnel},
        tagger::{TaggerChunnel, TaggerProjChunnel},
        udp::{UdpReqAddr, UdpSkChunnel, UdpSocketAddr},
        util::{OptionUnwrap, ProjectLeft},
        ChunnelConnection, ChunnelConnector, ChunnelListener, Client, ConnectAddress, CxList,
        ListenAddress, Serve,
    };
    use color_eyre::eyre;
    use eyre::{eyre, WrapErr};
    use futures_util::TryStreamExt;
    use serde::{Deserialize, Serialize};
    use std::net::SocketAddr;
    use tracing::{debug, info, trace, warn};
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

    #[derive(Clone, Debug)]
    struct Hole<T>(T);

    use std::{future::Future, pin::Pin};
    impl<T: Clone + Send + 'static> ChunnelConnection for Hole<T> {
        type Data = T;

        fn send(
            &self,
            _: Self::Data,
        ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>> {
            Box::pin(futures_util::future::ready(Ok(())))
        }

        /// Retrieve next incoming message.
        fn recv(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>>
        {
            let dummy = self.0.clone();
            Box::pin(futures_util::future::ready(Ok(dummy)))
        }
    }

    // raw version without any negotiation
    async fn start_shard(
        addr: SocketAddr,
        mut internal_srv: RendezvousChannel<SocketAddr, Msg, bertha::chan_transport::Srv>,
        s: tokio::sync::oneshot::Sender<Vec<Vec<bertha::negotiate::Offer>>>,
    ) {
        let internal_st = internal_srv.listen(addr).await.unwrap();
        let internal_st = CxList::from(OptionUnwrap)
            .wrap(ProjectLeft::from(addr))
            .serve(internal_st)
            .await
            .unwrap();

        let mut external = CxList::from(ShardServer::new(internal_st))
            .wrap(TaggerChunnel)
            .wrap(ReliabilityChunnel::default())
            .wrap(SerializeChunnel::default())
            .wrap(ProjectLeft::from(addr));
        let stack = external.clone();
        info!(addr = ?&addr, "listening");
        let addr: UdpReqAddr = addr.into();
        let st = addr.listener().listen(addr).await.unwrap();
        debug!("got raw connection");
        let st = external.serve(st).await.unwrap();
        use bertha::GetOffers;
        s.send(stack.offers()).unwrap();

        match st
            .try_for_each_concurrent(None, |once| async move {
                let msg = once.recv().await.wrap_err(eyre!("receive message error"))?;
                // just echo.
                once.send(msg).await.wrap_err(eyre!("send response err"))?;
                Ok(())
            })
            .await
        {
            Err(e) => {
                warn!(err = ?e, shard_addr = ?addr,  "Shard errorred");
                panic!(e);
            }
            Ok(_) => (),
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

                let (internal_srv, internal_cli) =
                    RendezvousChannel::<SocketAddr, Msg, _>::new(100).split();

                info!(addr = ?&addr, "start shard");

                tokio::spawn(
                    start_shard(addr, internal_srv, s)
                        .instrument(tracing::debug_span!("shard thread")),
                );

                let _ = r.await.wrap_err("shard thread crashed").unwrap();

                // udp connection
                async {
                    debug!("connect to shard");
                    let a = UdpSocketAddr::from(addr);
                    let mut external = CxList::from(TaggerChunnel)
                        .wrap(ReliabilityChunnel::default())
                        .wrap(SerializeChunnel::default());

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
                    cn.send((
                        addr,
                        Msg {
                            k: "a".to_owned(),
                            v: "b".to_owned(),
                        },
                    ))
                    .await
                    .unwrap();

                    trace!("await response");
                    let (_, m) = cn.recv().await.unwrap();
                    debug!(msg = ?m, "got response");
                    assert_eq!(m.key(), "a");
                    assert_eq!(m.val(), "b");
                }
                .instrument(tracing::info_span!("chan client"))
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
            tokio::spawn(start_shard(a, int_srv, s));
            rdy.push(r);
        }

        let mut offers: Vec<Vec<Vec<bertha::negotiate::Offer>>> = rdy.try_collect().await.unwrap();

        // 4. start canonical server
        let cnsrv = ShardCanonicalServer::new(
            si.clone(),
            internal_cli,
            // use `Hole` connection type as the external connection to the shards, because we
            // don't want any messages to be sent, because this test doesn't use negotiation.
            Hole((si.canonical_addr.clone(), Vec::<u8>::new())),
            offers.pop().unwrap(), // the stack used for the shard
            &redis_addr,
        )
        .await
        .unwrap();
        // UdpConn: (SocketAddr, Vec<u8>)
        // ProjectLeft: (SocketAddr, Vec<u8>) -> Vec<u8>
        // SerializeChunnel: Vec<u8> -> (u32, Option<Msg>)
        // ReliabilityChunnel: (u32, Option<Msg>) -> (u32, Msg)
        // TaggerChunnel: (u32, Msg) -> Msg
        // ShardCanonicalServer: Msg -> ()
        let mut external = CxList::from(cnsrv)
            .wrap(TaggerProjChunnel)
            .wrap(ReliabilityProjChunnel::<_, Msg>::default())
            .wrap(SerializeChunnelProject::<_, (u32, Option<Msg>)>::default());
        info!(shard_info = ?&si, "start canonical server");
        let a: UdpReqAddr = si.canonical_addr.into();
        let st = a.listener().listen(a).await.unwrap();
        let st = external.serve(st).await.unwrap();

        tokio::spawn(async move {
            st.try_for_each_concurrent(None, |r| async move {
                r.recv().await.wrap_err("ShardCanonicalServerConnection")?; // ShardCanonicalServerConnection is recv-only
                Ok(())
            })
            .await
            .unwrap()
        });

        (redis_guard, si.canonical_addr)
    }

    #[test]
    fn shard_shardclient() {
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
                let (redis_h, canonical_addr) = shard_setup(15215, 11421).await;
                let redis_addr = redis_h.get_addr();

                // 5. make client
                info!("make client");
                let cl = ClientShardChunnelClient::new(canonical_addr, &redis_addr)
                    .await
                    .unwrap();
                let raw_cn = UdpSkChunnel::default().connect(()).await.unwrap(); // below: (A, Vec<u8>)

                // UdpSkChunnel: (A, Vec<u8>)
                // SerializeChunnelProject: (A, Vec<u8>) -> (A, (u32, Option<Msg>))
                // ReliabilityProjChunnel: (A, (u32, Option<Msg>)) -> (A, (u32, Msg))
                // TaggerProjChunnel: (A, (u32, Msg)) -> (A, Msg)
                // ClientShardChunnelClient: (A, Msg) -> Msg
                let mut stack = CxList::from(cl)
                    .wrap(TaggerProjChunnel)
                    .wrap(ReliabilityProjChunnel::default())
                    .wrap(SerializeChunnelProject::default());
                let cn = stack.connect_wrap(raw_cn).await.unwrap();

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
    fn shard_canonicalclient() {
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
                let (_redis_h, canonical_addr) = shard_setup(25215, 21421).await;

                // 5. make client
                info!("make client");
                let udp_addr: UdpSocketAddr = canonical_addr.into();

                // UdpSkChunnel: (A, Vec<u8>)
                // SerializeChunnelProject: (A, Vec<u8>) -> (A, (u32, Option<Msg>))
                // ReliabilityProjChunnel: (A, (u32, Option<Msg>)) -> (A, (u32, Msg))
                // TaggerProjChunnel: (A, (u32, Msg)) -> (A, Msg)
                // ProjectLeft: (A, Msg) -> Msg
                let mut stack = CxList::from(ProjectLeft::from(udp_addr))
                    .wrap(TaggerProjChunnel)
                    .wrap(ReliabilityProjChunnel::default())
                    .wrap(SerializeChunnelProject::default());

                // UdpSkChunnel: (A, Vec<u8>)
                // ProjectLeft: (A, Vec<u8>) -> Vec<u8>
                // SerializeChunnel: Vec<u8> -> (u32, Option<Msg>)
                // ReliabilityChunnel: (u32, Option<Msg>) -> (u32, Msg)
                // TaggerChunnel: (u32, Msg) -> Msg
                //let mut stack = CxList::from(TaggerChunnel)
                //    .wrap(ReliabilityChunnel::default())
                //    .wrap(SerializeChunnel::default())
                //    .wrap(ProjectLeft::from(udp_addr.clone()));

                let raw_cn = UdpSkChunnel::default().connect(()).await.unwrap();
                let cn = stack.connect_wrap(raw_cn).await.unwrap();

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

    async fn start_shard_negotiate(
        addr: SocketAddr,
        mut internal_srv: RendezvousChannel<SocketAddr, Msg, bertha::chan_transport::Srv>,
        s: tokio::sync::oneshot::Sender<Vec<Vec<bertha::negotiate::Offer>>>,
    ) {
        let internal_st = internal_srv.listen(addr).await.unwrap();

        // why serve here and not negotiate_server?
        //
        // the underlying raw connection is a channel, and negotiate_server expects (Addr,
        // Vec<u8>). This saves the extra serialization.
        //
        // TODO alternate: add serialization and do negotiation, but make it a mem::transmute version
        // since we know the underlying thing is a channel
        let internal_st = CxList::from(OptionUnwrap)
            .wrap(ProjectLeft::from(addr))
            .serve(internal_st)
            .await
            .unwrap();

        let external = CxList::from(ShardServer::new(internal_st))
            .wrap(TaggerChunnel)
            .wrap(ReliabilityChunnel::default())
            .wrap(SerializeChunnel::default())
            .wrap(ProjectLeft::from(addr));
        let stack = external.clone();
        info!(addr = ?&addr, "listening");
        let addr: UdpReqAddr = addr.into();
        let st = addr.listener().listen(addr).await.unwrap();
        debug!("got raw connection");
        let st = bertha::negotiate::negotiate_server(external, st)
            .await
            .unwrap();
        use bertha::GetOffers;
        s.send(stack.offers()).unwrap();

        match st
            .try_for_each_concurrent(None, |once| {
                async move {
                    debug!("new");
                    let msg = once.recv().await.wrap_err(eyre!("receive message error"))?;
                    debug!(msg = ?&msg, "got msg");
                    // just echo.
                    once.send(msg).await.wrap_err(eyre!("send response err"))?;
                    debug!("sent echo");
                    Ok(())
                }
                .instrument(tracing::debug_span!("shard_connection"))
            })
            .instrument(tracing::debug_span!("negotiate_server"))
            .await
        {
            Err(e) => {
                warn!(shard_addr = ?addr, err = ?e, "Shard errorred");
                panic!(e);
            }
            Ok(_) => (),
        }
    }

    async fn shard_setup_negotiate(
        redis_port: u16,
        srv_port: u16,
    ) -> (test_util::Redis, SocketAddr) {
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
            tokio::spawn(
                start_shard_negotiate(a, int_srv, s)
                    .instrument(tracing::debug_span!("shardsrv", addr = ?&a)),
            );
            rdy.push(r);
        }

        let mut offers: Vec<Vec<Vec<bertha::negotiate::Offer>>> = rdy.try_collect().await.unwrap();

        // 4. start canonical server
        let shards_extern = UdpSkChunnel.connect(()).await.unwrap();
        let cnsrv = ShardCanonicalServer::new(
            si.clone(),
            internal_cli,
            shards_extern,
            offers.pop().unwrap(),
            &redis_addr,
        )
        .await
        .unwrap();
        // UdpConn: (SocketAddr, Vec<u8>)
        // ProjectLeft: (SocketAddr, Vec<u8>) -> Vec<u8>
        // SerializeChunnel: Vec<u8> -> (u32, Option<Msg>)
        // ReliabilityChunnel: (u32, Option<Msg>) -> (u32, Msg)
        // TaggerChunnel: (u32, Msg) -> Msg
        // ShardCanonicalServer: Msg -> ()
        let external = CxList::from(cnsrv)
            .wrap(TaggerProjChunnel)
            .wrap(ReliabilityProjChunnel::<_, Msg>::default())
            .wrap(SerializeChunnelProject::<_, (u32, Option<Msg>)>::default());
        info!(shard_info = ?&si, "start canonical server");
        let a: UdpReqAddr = si.canonical_addr.into();
        let st = a.listener().listen(a).await.unwrap();
        let st = bertha::negotiate::negotiate_server(external, st)
            .instrument(tracing::info_span!("negotiate_server"))
            .await
            .unwrap();

        tokio::spawn(
            async move {
                st.try_for_each_concurrent(None, |r| {
                    async move {
                        r.recv().await?; // ShardCanonicalServerConnection is recv-only
                        Ok(())
                    }
                })
                .instrument(tracing::info_span!("negotiate_server"))
                .await
                .unwrap()
            }
            .instrument(tracing::info_span!("canonicalsrv", addr = ?&si.canonical_addr)),
        );

        (redis_guard, si.canonical_addr)
    }

    #[test]
    fn shard_negotiate_clientonly() {
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
                let (redis_h, canonical_addr) = shard_setup_negotiate(35215, 31421).await;
                let udp_addr: UdpSocketAddr = canonical_addr.into();

                // 5. make client
                info!("make client");
                let redis_addr = redis_h.get_addr();

                let cl = ClientShardChunnelClient::new(canonical_addr, &redis_addr)
                    .await
                    .unwrap();

                let neg_stack = CxList::from(bertha::negotiate::Select(
                    cl,
                    ProjectLeft::from(udp_addr.clone()),
                ))
                .wrap(TaggerProjChunnel)
                .wrap(ReliabilityProjChunnel::default())
                .wrap(SerializeChunnelProject::default());

                let raw_cn = UdpSkChunnel::default().connect(()).await.unwrap();
                let cn = bertha::negotiate::negotiate_client(neg_stack, raw_cn, udp_addr)
                    .await
                    .unwrap();

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
            .instrument(tracing::info_span!("negotiate_clientonly")),
        );
    }

    #[cfg(feature = "ebpf")]
    #[test]
    fn shard_negotiate_bothsides() {
        use super::ebpf::ShardCanonicalServerEbpf;

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
                let redis_port = 42215;
                let redis_addr = format!("redis://127.0.0.1:{}", redis_port);
                info!(port = ?redis_port, "start redis");
                let redis_h = test_util::start_redis(redis_port);

                // 2. Define addr.
                let si: ShardInfo<SocketAddr> = ShardInfo {
                    canonical_addr: "127.0.0.1:41471".parse().unwrap(),
                    shard_addrs: vec![
                        "127.0.0.1:41472".parse().unwrap(),
                        "127.0.0.1:41473".parse().unwrap(),
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
                    tokio::spawn(
                        start_shard_negotiate(a, int_srv, s)
                            .instrument(tracing::info_span!("shard", addr = ?&a)),
                    );

                    rdy.push(r);
                }

                let offers: Vec<Vec<Vec<bertha::negotiate::Offer>>> =
                    rdy.try_collect().await.unwrap();

                // 4. start canonical server
                let shards_extern = UdpSkChunnel.connect(()).await.unwrap();
                let cnsrv = ShardCanonicalServer::new(
                    si.clone(),
                    internal_cli.clone(),
                    shards_extern,
                    offers[0].clone(),
                    &redis_addr,
                )
                .await
                .unwrap();
                let shards_extern = UdpSkChunnel.connect(()).await.unwrap();
                let esrv: ShardCanonicalServerEbpf<
                    _,
                    RendezvousChannelAddr<SocketAddr, Msg>,
                    _,
                    _,
                > = ShardCanonicalServerEbpf::new(
                    si.clone(),
                    internal_cli.clone(),
                    shards_extern,
                    offers[0].clone(),
                    &redis_addr,
                )
                .await
                .unwrap();
                let external = CxList::from(bertha::negotiate::Select(cnsrv, esrv))
                    .wrap(TaggerProjChunnel)
                    .wrap(ReliabilityProjChunnel::<_, Msg>::default())
                    .wrap(SerializeChunnelProject::<_, (u32, Option<Msg>)>::default());
                info!(shard_info = ?&si, "start canonical server");
                let a: UdpReqAddr = si.canonical_addr.into();
                let raw_st: std::pin::Pin<
                    Box<
                        dyn futures_util::stream::Stream<
                                Item = Result<bertha::udp::UdpConn, eyre::Report>,
                            > + Send
                            + 'static,
                    >,
                > = a.listener().listen(a).await.unwrap();
                let st = bertha::negotiate::negotiate_server(external, raw_st)
                    .instrument(tracing::info_span!("negotiate_server"))
                    .await
                    .unwrap();

                tokio::spawn(
                    async move {
                        st.try_for_each_concurrent(None, |r| async move {
                            r.recv().await?;
                            Ok(())
                        })
                        .instrument(tracing::info_span!("negotiate_server"))
                        .await
                        .unwrap()
                    }
                    .instrument(tracing::info_span!("canonicalsrv", addr = ?&si.canonical_addr)),
                );

                // 5. make client
                info!("make client");
                let redis_addr = redis_h.get_addr();

                let cl = ClientShardChunnelClient::new(si.canonical_addr, &redis_addr)
                    .await
                    .unwrap();

                let udp_addr: UdpSocketAddr = si.canonical_addr.into();
                let neg_stack = CxList::from(bertha::negotiate::Select(
                    cl,
                    ProjectLeft::from(udp_addr.clone()),
                ))
                .wrap(TaggerProjChunnel)
                .wrap(ReliabilityProjChunnel::default())
                .wrap(SerializeChunnelProject::default());

                let raw_cn = UdpSkChunnel::default().connect(()).await.unwrap();
                let cn = bertha::negotiate::negotiate_client(neg_stack, raw_cn, udp_addr)
                    .await
                    .unwrap();

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
            .instrument(tracing::debug_span!("negotiate_bothsides")),
        );
    }
}
