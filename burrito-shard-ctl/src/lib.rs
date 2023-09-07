//! Sharding chunnel.

// Pin<Box<...>> is necessary and not worth breaking up
#![allow(clippy::type_complexity)]

use bertha::{
    enumerate_enum,
    negotiate::{Apply, GetOffers, StackNonce},
    Chunnel, ChunnelConnection, ChunnelConnector, IpPort, Negotiate,
};
use color_eyre::eyre;
use eyre::{eyre, Error, WrapErr};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::Mutex;
use tracing::{debug, debug_span, trace, trace_span, warn};
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
pub struct ShardCanonicalServer<A, S, Ss, D> {
    addr: ShardInfo<A>,
    internal_addr: Vec<A>,
    shards_inner: Arc<StdMutex<S>>,
    shards_inner_stack: Ss,
    shards_extern_nonce: StackNonce,
    redis_listen_connection: Arc<Mutex<redis::aio::MultiplexedConnection>>,
    _phantom: std::marker::PhantomData<D>,
}

impl<A, S, Ss, D> Clone for ShardCanonicalServer<A, S, Ss, D>
where
    A: Clone,
    Ss: Clone,
{
    fn clone(&self) -> Self {
        Self {
            addr: self.addr.clone(),
            internal_addr: self.internal_addr.clone(),
            shards_inner: Arc::clone(&self.shards_inner),
            shards_inner_stack: self.shards_inner_stack.clone(),
            shards_extern_nonce: self.shards_extern_nonce.clone(),
            redis_listen_connection: Arc::clone(&self.redis_listen_connection),
            _phantom: Default::default(),
        }
    }
}

impl<A: std::fmt::Debug, S, Ss, D> std::fmt::Debug for ShardCanonicalServer<A, S, Ss, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardCanonicalServer")
            .field("addr", &self.addr)
            .finish()
    }
}

impl<A, S, Ss, D> ShardCanonicalServer<A, S, Ss, D>
where
    A: Clone + std::fmt::Debug,
{
    /// Inner is a chunnel for the external connection.
    /// Shards is a chunnel for an internal connection to the shards.
    pub async fn new(
        addr: ShardInfo<A>,
        internal_addr: Option<Vec<A>>,
        shards_inner: S,
        shards_inner_stack: Ss,
        shards_extern_nonce: StackNonce,
        redis_addr: &str,
    ) -> Result<Self, Error> {
        let redis_client = redis::Client::open(redis_addr)
            .wrap_err_with(|| eyre!("Opening redis connection: {:?}", redis_addr))?;
        let redis_listen_connection = Arc::new(Mutex::new(
            redis_client
                .get_multiplexed_tokio_connection()
                .await
                .wrap_err("Connecting to redis")?,
        ));

        let internal_addr = internal_addr.unwrap_or_else(|| addr.shard_addrs.clone());
        if internal_addr.len() != addr.shard_addrs.len() {
            return Err(eyre!(
                "Shard addresses mismatched between internal and external: {:?} != {:?}",
                internal_addr,
                addr
            ));
        }

        Ok(ShardCanonicalServer {
            addr,
            internal_addr,
            shards_inner: Arc::new(StdMutex::new(shards_inner)),
            shards_inner_stack,
            shards_extern_nonce,
            redis_listen_connection,
            _phantom: Default::default(),
        })
    }
}

impl<A, S, Ss, D> Negotiate for ShardCanonicalServer<A, S, Ss, D>
where
    A: IpPort
        + Serialize
        + DeserializeOwned
        + Clone
        + PartialEq
        + std::fmt::Debug
        + std::fmt::Display
        + Send
        + Sync
        + 'static,
    S: ChunnelConnector<Addr = A> + Send + Sync + 'static,
    <S as ChunnelConnector>::Error: Into<eyre::Report>,
    S::Connection: ChunnelConnection<Data = (A, Vec<u8>)> + Send + Sync + 'static,
{
    type Capability = ShardFns;

    fn guid() -> u64 {
        0xe91d00534cb2b98f
    }

    fn capabilities() -> Vec<ShardFns> {
        vec![ShardFns::Sharding]
    }

    fn picked<'s>(&mut self, nonce: &'s [u8]) -> Pin<Box<dyn Future<Output = ()> + Send + 's>> {
        let addrs = self.internal_addr.clone();
        let ctr = Arc::clone(&self.shards_inner);
        let offer = self.shards_extern_nonce.clone();
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

        let redis_conn = Arc::clone(&self.redis_listen_connection);
        let addr = self.addr.clone();
        Box::pin(async move {
            // redis insert
            if let Err(e) = redis_util::redis_insert(redis_conn, &addr)
                .await
                .wrap_err("Could not register shard info")
            {
                warn!(err = ?e, "failed inserting shard info in redis");
                return;
            }

            futures_util::future::join_all(addrs.into_iter().map(|shard| {
                let buf = buf.clone();
                let shard_addr = shard.clone();
                let ctr = Arc::clone(&ctr);
                async move {
                    let connect_fut = {
                        let mut  ctr_g = ctr.lock().unwrap();
                        ctr_g.connect(shard.clone())
                    };
                    let cn = match connect_fut.await {
                        Ok(c) => c,
                        Err(e) => {
                            let e = e.into();
                            warn!(err = ?e, "failed making connection to shard");
                            return;
                        }
                    };
                    if let Err(e) = cn.send(std::iter::once((shard.clone(), buf.clone()))).await {
                        warn!(err = ?e, "failed sending negotiation nonce to shard");
                        return;
                    }

                    debug!("wait for nonce ack");
                    let mut slot = [None];
                    match cn.recv(&mut slot).await {
                        Ok([Some((a, buf))]) => match bincode::deserialize(buf) {
                            Ok(bertha::negotiate::NegotiateMsg::ServerNonceAck) => {
                                // TODO collect received addresses since we could receive acks from
                                // any shard
                                if *a != shard.clone() {
                                    warn!(addr = ?a, expected = ?shard.clone(), "received from unexpected address");
                                }

                                debug!("got nonce ack");
                            }
                            Ok(m) => {
                                warn!(msg = ?m, shard = ?shard.clone(), "got unexpected response to nonce");
                            }
                            Err(e) => {
                                warn!(err = ?e, shard = ?shard.clone(), "failed deserializing nonce ack");
                            }
                        },
                        Ok(_) => {
                            warn!("got empty response to nonce");
                        }
                        Err(e) => {
                            warn!(err = ?e, shard = ?shard.clone(), "failed waiting for nonce ack");
                        }
                    }
                }
                .instrument(debug_span!("shard-send-nonce", shard = ?&shard_addr))
            }))
            .await;
        })
    }
}

impl<I, A, S, Ss, D, E> Chunnel<I> for ShardCanonicalServer<A, S, Ss, D>
where
    I: ChunnelConnection<Data = D> + Send + Sync + 'static,
    A: IpPort
        + Serialize
        + DeserializeOwned
        + Clone
        + std::fmt::Debug
        + std::fmt::Display
        + Send
        + Sync
        + 'static,
    S: ChunnelConnector<Addr = A, Error = E> + Send + Sync + 'static,
    S::Connection: ChunnelConnection<Data = (A, Vec<u8>)> + Send + Sync + 'static,
    Ss: Apply + GetOffers + Clone + Send + Sync + 'static,
    <Ss as Apply>::Applied:
        Chunnel<S::Connection> + bertha::NegotiatePicked + Clone + Debug + Send + 'static,
    <<Ss as Apply>::Applied as Chunnel<S::Connection>>::Connection:
        ChunnelConnection<Data = D> + Send + Sync + 'static,
    <<Ss as Apply>::Applied as Chunnel<S::Connection>>::Error: Into<Error> + Send + Sync + 'static,
    D: Kv + Send + Sync + 'static,
    <D as Kv>::Key: AsRef<str>,
    E: Into<Error> + Send + Sync + 'static,
{
    type Connection =
        ShardCanonicalServerConnection<I, bertha::negotiate::NegotiatedConn<S::Connection, Ss>, D>;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Error;

    fn connect_wrap(&mut self, conn: I) -> Self::Future {
        let addr = self.addr.clone();
        let internal_addr = self.internal_addr.clone();
        let a1 = addr.clone();
        let shards_inner = Arc::clone(&self.shards_inner);
        let shards_inner_stack = self.shards_inner_stack.clone();
        Box::pin(
            async move {
                let _ = &addr;
                let num_shards = addr.shard_addrs.len();

                // connect to shards
                let conns: Vec<_> =
                    futures_util::future::join_all(internal_addr.into_iter().map(|a| async {
                        debug!(?a, "connecting to shard");
                        let a = a;
                        let connect_fut = {
                            let mut ctr_g = shards_inner.lock().unwrap();
                            ctr_g.connect(a.clone())
                        };
                        let cn = connect_fut
                            .await
                            .map_err(Into::into)
                            .wrap_err_with(|| eyre!("Could not connect to {}", a.clone()))?;
                        let cn = bertha::negotiate::negotiate_client(
                            shards_inner_stack.clone(),
                            cn,
                            a.clone(),
                        )
                        .await
                        .wrap_err("negotiate_client failed")?;
                        Ok::<_, Error>(cn)
                    }))
                    .await
                    .into_iter()
                    .collect::<Result<_, Error>>()
                    .wrap_err("Could not connect to at least one shard")?;
                debug!("connected to shards");

                // serve canonical address
                // we are only responsible for the canonical address here.
                Ok(ShardCanonicalServerConnection {
                    inner: conn,
                    shards: conns,
                    shard_fn: Arc::new(move |d: &D| {
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
                            hash ^= *b as u64;
                            hash = u64::wrapping_mul(hash, FNV_64_PRIME);
                        }

                        hash as usize % num_shards
                    }),
                })
            }
            .instrument(debug_span!("serve", addr = ?&a1)),
        )
    }
}

/// Chunnel connection type for serving the shard canonical address.
///
/// Does not implement `send()`, since the server does not send messages unprompted.  Similarly,
/// the Data type is `()`, since any received message is forwarded to the appropriate shard.
/// Expected usage is to call `recv()` in a loop.
pub struct ShardCanonicalServerConnection<C, S, D> {
    inner: C,
    shards: Vec<S>,
    shard_fn: Arc<dyn Fn(&D) -> usize + Send + Sync + 'static>,
}

impl<C, S, D> ChunnelConnection for ShardCanonicalServerConnection<C, S, D>
where
    C: ChunnelConnection<Data = D> + Send + Sync + 'static,
    S: ChunnelConnection<Data = D> + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Data = ();

    fn recv<'cn, 'buf>(
        &'cn self,
        msgs_buf: &'buf mut [Option<Self::Data>],
    ) -> Pin<
        Box<dyn Future<Output = Result<&'buf mut [Option<Self::Data>], eyre::Report>> + Send + 'cn>,
    >
    where
        'buf: 'cn,
    {
        Box::pin(
            async move {
                // received a packet on the canonical_addr.
                // need to
                // 1. evaluate the hash fn
                // 2. forward to the right shard,
                //    preserving the src addr so the response goes back to the client

                // 0. receive the packet.
                let mut slots: Vec<_> = (0..msgs_buf.len()).map(|_| None).collect();
                let ms = self.inner.recv(&mut slots).await?;
                trace!("got request batch");

                let mut shard_batches: Vec<Vec<D>> =
                    (0..self.shards.len()).map(|_| Vec::new()).collect();
                for data in ms.iter_mut().map_while(Option::take) {
                    // 1. evaluate the hash fn to determine where to forward to.
                    let shard_idx = (self.shard_fn)(&data);
                    trace!(shard_idx, "checked shard");
                    shard_batches[shard_idx].push(data);
                }

                // 2. Forward to the shard.
                // TODO this assumes no reordering.
                let mut shard_completion_slots: Vec<Vec<_>> = shard_batches
                    .iter()
                    .map(|batch| (0..batch.len()).map(|_| None).collect())
                    .collect();
                let shard_completion_futs = self
                    .shards
                    .iter()
                    .zip(shard_batches.into_iter())
                    .zip(&mut shard_completion_slots)
                    .enumerate()
                    .filter_map(|(shard_idx, ((conn, batch), slots))| {
                        if !batch.is_empty() {
                            Some(async move {
                                conn.send(batch).await.wrap_err("Forward to shard")?;
                                // 3. Get response from the shard
                                let resps = conn
                                    .recv(&mut slots[..])
                                    .instrument(trace_span!(
                                        "canonical-server-internal-recv",
                                        ?shard_idx
                                    ))
                                    .await
                                    .wrap_err("receive from shard")?;
                                Ok::<_, eyre::Report>((shard_idx, resps))
                            })
                        } else {
                            None
                        }
                    });
                let resps = futures_util::future::try_join_all(shard_completion_futs).await?;
                trace!("got shard responses");
                for (shard_idx, resp_batch) in resps {
                    // 4. Send back to client.
                    self.inner
                        .send(resp_batch.iter_mut().map_while(Option::take))
                        .await?;
                    trace!(shard_idx, "send response");
                }

                Ok(&mut msgs_buf[0..0])
            }
            .instrument(trace_span!("server-shard-recv")),
        )
    }

    fn send<'cn, B>(
        &self,
        _data: B,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'cn>>
    where
        B: IntoIterator<Item = Self::Data> + Send + 'cn,
        <B as IntoIterator>::IntoIter: Send,
    {
        warn!("Called ShardCanonicalServerConnection.send(), a useless function");
        Box::pin(async move { Ok(()) })
    }
}

/// Client-side sharding chunnel implementation.
///
/// Contacts shard-ctl for sharding information, and does client-side sharding.
#[derive(Clone)]
pub struct ClientShardChunnelClient<A, A2> {
    addr: A,
    redis_listen_connection: Arc<Mutex<redis::aio::MultiplexedConnection>>,
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
        let redis_client = redis::Client::open(redis_addr)
            .wrap_err_with(|| eyre!("Opening redis connection: {:?}", redis_addr))?;
        let redis_listen_connection = Arc::new(Mutex::new(
            redis_client
                .get_multiplexed_tokio_connection()
                .await
                .wrap_err_with(|| eyre!("Opening redis async connection: {:?}", redis_addr))?,
        ));

        Ok(ClientShardChunnelClient {
            addr,
            redis_listen_connection,
            _phantom: Default::default(),
        })
    }
}

impl<A, A2> Negotiate for ClientShardChunnelClient<A, A2> {
    type Capability = ShardFns;

    fn guid() -> u64 {
        0xafb32251f0697831
    }

    fn capabilities() -> Vec<ShardFns> {
        vec![ShardFns::Sharding]
    }
}

impl<A, A2, I, D> Chunnel<I> for ClientShardChunnelClient<A, A2>
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
                trace!("query redis");
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
                        hash ^= *b as u64;
                        hash = u64::wrapping_mul(hash, FNV_64_PRIME);
                    }

                    (hash as usize % num_shards) + 1
                }))
            }
            .instrument(debug_span!("ClientShardChunnelClient::connect")),
        )
    }
}

/// `ChunnelConnection` type for ClientShardChunnelClient.
pub struct ClientShardClientConnection<A, D, C>
where
    C: ChunnelConnection<Data = (A, D)>,
{
    inner: C,
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
            inner,
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
    type Data = (A, D);

    fn send<'cn, B>(
        &'cn self,
        burst: B,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'cn>>
    where
        B: IntoIterator<Item = Self::Data> + Send + 'cn,
        <B as IntoIterator>::IntoIter: Send,
    {
        self.inner.send(burst.into_iter().map(|(_, payload)| {
            let shard_idx = (self.shard_fn)(&payload);
            let a = self.shard_addrs[shard_idx].clone();
            (a, payload)
        }))
    }

    fn recv<'cn, 'buf>(
        &'cn self,
        msgs_buf: &'buf mut [Option<Self::Data>],
    ) -> Pin<
        Box<dyn Future<Output = Result<&'buf mut [Option<Self::Data>], eyre::Report>> + Send + 'cn>,
    >
    where
        'buf: 'cn,
    {
        self.inner.recv(msgs_buf)
    }
}
mod shard_raw;
pub use shard_raw::ShardCanonicalServerRaw;

pub mod static_client {
    use crate::{Kv, ShardFns, ShardInfo, FNV1_64_INIT, FNV_64_PRIME};
    use bertha::{Chunnel, ChunnelConnection, Negotiate};
    use color_eyre::eyre::{self, Error};
    use std::future::Future;
    use std::net::SocketAddr;
    use std::pin::Pin;
    use std::sync::Arc;
    use tracing::{debug, debug_span};
    use tracing_futures::Instrument;

    #[derive(Clone)]
    pub struct ClientShardChunnelClient {
        addr: ShardInfo<SocketAddr>,
    }

    impl std::fmt::Debug for ClientShardChunnelClient {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("ClientShardChunnelClient")
                .field("addr", &self.addr)
                .finish()
        }
    }

    impl ClientShardChunnelClient {
        pub fn new(addr: ShardInfo<SocketAddr>) -> Self {
            ClientShardChunnelClient { addr }
        }
    }

    impl Negotiate for ClientShardChunnelClient {
        type Capability = ShardFns;

        fn guid() -> u64 {
            0xafb32251f0697831
        }

        fn capabilities() -> Vec<ShardFns> {
            vec![super::ShardFns::Sharding]
        }
    }

    impl<I, D> Chunnel<I> for ClientShardChunnelClient
    where
        I: ChunnelConnection<Data = (SocketAddr, D)> + Send + Sync + 'static,
        D: Kv + Send + Sync + 'static,
        <D as Kv>::Key: AsRef<str>,
    {
        type Connection = ClientShardClientConnection<SocketAddr, D, I>;
        type Error = Error;
        type Future =
            Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;

        fn connect_wrap(&mut self, inner: I) -> Self::Future {
            let a = self.addr.clone();
            Box::pin(
                async move {
                    let mut addrs = vec![a.canonical_addr];
                    addrs.extend(a.shard_addrs.into_iter());

                    debug!(addrs = ?&addrs, "Decided sharding plan");
                    let num_shards = addrs.len() - 1;
                    Ok(ClientShardClientConnection::new(inner, addrs, move |d| {
                        if num_shards == 0 {
                            return 0;
                        }

                        let mut hash = FNV1_64_INIT;
                        for b in d.key().as_ref().as_bytes()[0..4].iter() {
                            hash ^= *b as u64;
                            hash = u64::wrapping_mul(hash, FNV_64_PRIME);
                        }

                        (hash as usize % num_shards) + 1
                    }))
                }
                .instrument(debug_span!("ClientShardChunnelClient::connect")),
            )
        }
    }

    /// `ChunnelConnection` type for ClientShardChunnelClient.
    pub struct ClientShardClientConnection<A, D, C>
    where
        C: ChunnelConnection<Data = (A, D)>,
    {
        inner: C,
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
                inner,
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
        type Data = (A, D);

        fn send<'cn, B>(
            &'cn self,
            burst: B,
        ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'cn>>
        where
            B: IntoIterator<Item = Self::Data> + Send + 'cn,
            <B as IntoIterator>::IntoIter: Send,
        {
            self.inner.send(burst.into_iter().map(|(_, payload)| {
                let shard_idx = (self.shard_fn)(&payload);
                let a = self.shard_addrs[shard_idx].clone();
                (a, payload)
            }))
        }

        fn recv<'cn, 'buf>(
            &'cn self,
            msgs_buf: &'buf mut [Option<Self::Data>],
        ) -> Pin<
            Box<
                dyn Future<Output = Result<&'buf mut [Option<Self::Data>], eyre::Report>>
                    + Send
                    + 'cn,
            >,
        >
        where
            'buf: 'cn,
        {
            self.inner.recv(msgs_buf)
        }
    }
}

#[cfg(feature = "ebpf")]
pub use ebpf::ShardCanonicalServerEbpf;

#[cfg(feature = "ebpf")]
mod ebpf;

#[cfg(test)]
mod test {
    use super::{ClientShardChunnelClient, Kv, ShardCanonicalServer, ShardInfo};
    use bertha::{
        bincode::SerializeChunnel,
        chan_transport::RendezvousChannel,
        negotiate::StackNonce,
        select::SelectListener,
        udp::{UdpReqChunnel, UdpSkChunnel},
        util::{Nothing, ProjectLeft},
        ChunnelConnection, ChunnelConnector, ChunnelListener, CxList,
    };
    use color_eyre::eyre;
    use eyre::{eyre, WrapErr};
    use futures_util::TryStreamExt;
    use serde::{Deserialize, Serialize};
    use std::net::SocketAddr;
    use std::sync::Once;
    use tracing::{debug, debug_span, info, trace, warn};
    use tracing_error::ErrorLayer;
    use tracing_futures::Instrument;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    pub static COLOR_EYRE: Once = Once::new();

    #[derive(Default, Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub(crate) struct Msg {
        pub(crate) k: String,
        pub(crate) v: String,
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

    pub(crate) async fn start_shard(
        addr: SocketAddr,
        internal_srv: RendezvousChannel<SocketAddr, Vec<u8>, bertha::chan_transport::Srv>,
        s: tokio::sync::oneshot::Sender<Vec<StackNonce>>,
    ) {
        let external = SerializeChunnel::default();
        let stack = external.clone();
        info!(addr = ?&addr, "listening");
        let st = SelectListener::new(UdpReqChunnel::default(), internal_srv)
            .listen(addr)
            .await
            .unwrap();
        trace!("got raw connection");
        let st = bertha::negotiate::negotiate_server(external, st)
            .await
            .unwrap();
        use bertha::GetOffers;
        s.send(stack.offers().collect()).unwrap();

        if let Err(e) = st
            .try_for_each_concurrent(None, |cn| {
                async move {
                    debug!("new");
                    let mut slots = [None, None, None, None];
                    loop {
                        let ms: &mut [Option<(_, Msg)>] = cn
                            .recv(&mut slots)
                            .await
                            .wrap_err("receive message error")?;
                        debug!("got msg batch");
                        // just echo.
                        cn.send(ms.iter_mut().map_while(Option::take))
                            .await
                            .wrap_err("send response err")?;
                        debug!("sent echo");
                    }
                }
                .instrument(debug_span!("shard_connection"))
            })
            .instrument(debug_span!("negotiate_server"))
            .await
        {
            warn!(shard_addr = ?addr, err = ?e, "Shard errorred");
            panic!("{}", e);
        }
    }

    #[test]
    fn single_shard() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        COLOR_EYRE.call_once(|| color_eyre::install().unwrap_or(()));

        // 0. Make rt.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                let addr: SocketAddr = "127.0.0.1:21422".parse().unwrap();
                let (s, r) = tokio::sync::oneshot::channel();

                let (internal_srv, mut internal_cli) =
                    RendezvousChannel::<SocketAddr, _, _>::new(100).split();

                info!(addr = ?&addr, "start shard");

                tokio::spawn(
                    start_shard(addr, internal_srv, s).instrument(debug_span!("shard thread")),
                );

                let _ = r.await.wrap_err("shard thread crashed").unwrap();
                let stack = SerializeChunnel::default();

                async fn do_msg(cn: impl ChunnelConnection<Data = Msg>) {
                    debug!("send request");
                    cn.send(std::iter::once(Msg {
                        k: "c".to_owned(),
                        v: "d".to_owned(),
                    }))
                    .await
                    .unwrap();

                    debug!("await response");
                    let mut slot = [None];
                    let ms = cn.recv(&mut slot).await.unwrap();
                    let m = ms[0].take().unwrap();
                    debug!(msg = ?m, "got response");
                    assert_eq!(m.key(), "c");
                    assert_eq!(m.val(), "d");
                }

                // udp connection
                async {
                    debug!("connect to shard");
                    let cn = UdpSkChunnel::default()
                        .connect(addr)
                        .await
                        .wrap_err("client connect")
                        .unwrap();
                    let cn = bertha::negotiate::negotiate_client(stack.clone(), cn, addr)
                        .await
                        .unwrap();
                    let cn = ProjectLeft::new(addr, cn);
                    do_msg(cn).await;
                }
                .instrument(tracing::info_span!("udp client"))
                .await;

                // channel connection
                async {
                    debug!("connect to shard");
                    let cn = internal_cli
                        .connect(addr)
                        .await
                        .wrap_err("client connect")
                        .unwrap();
                    let cn = bertha::negotiate::negotiate_client(stack.clone(), cn, addr)
                        .await
                        .unwrap();
                    let cn = ProjectLeft::new(addr, cn);
                    do_msg(cn).await;
                }
                .instrument(tracing::info_span!("chan client"))
                .await;
            }
            .instrument(debug_span!("single_shard")),
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
            RendezvousChannel::<SocketAddr, Vec<u8>, _>::new(100).split();
        let rdy = futures_util::stream::FuturesUnordered::new();
        for a in si.clone().shard_addrs {
            info!(addr = ?&a, "start shard");
            let (s, r) = tokio::sync::oneshot::channel();
            let int_srv = internal_srv.clone();
            tokio::spawn(
                start_shard(a, int_srv, s).instrument(debug_span!("shardsrv", addr = ?&a)),
            );
            rdy.push(r);
        }

        let mut offers: Vec<Vec<StackNonce>> = rdy.try_collect().await.unwrap();

        // 4. start canonical server
        let cnsrv = ShardCanonicalServer::<_, _, _, (_, Msg)>::new(
            si.clone(),
            None,
            internal_cli,
            SerializeChunnel::default(),
            offers.pop().unwrap().pop().unwrap(),
            &redis_addr,
        )
        .await
        .unwrap();
        // UdpConn: (SocketAddr, Vec<u8>)
        // SerializeChunnelProject: (A, Vec<u8>) -> _
        // ReliabilityChunnel: (A, _) -> (A, (u32, Msg))
        // TaggerChunnel: (A, (u32, Msg)) -> (A, Msg)
        // ShardCanonicalServer: (A, Msg) -> ()
        let external = CxList::from(cnsrv).wrap(SerializeChunnel::default());
        info!(shard_info = ?&si, "start canonical server");
        let st = UdpReqChunnel::default()
            .listen(si.canonical_addr)
            .await
            .unwrap();
        let st = bertha::negotiate::negotiate_server(external, st)
            .instrument(tracing::info_span!("negotiate_server"))
            .await
            .unwrap();

        tokio::spawn(
            async move {
                if let Err(e) = st
                    .try_for_each_concurrent(None, |r| {
                        async move {
                            let mut slot = [None];
                            loop {
                                r.recv(&mut slot).await?; // ShardCanonicalServerConnection is recv-only
                            }
                        }
                    })
                    .instrument(tracing::info_span!("negotiate_server"))
                    .await
                {
                    warn!(err = ?e, "canonical server crashed");
                    panic!("{}", e);
                }
            }
            .instrument(tracing::info_span!("canonicalsrv", addr = ?&si.canonical_addr)),
        );

        (redis_guard, si.canonical_addr)
    }

    #[test]
    fn shard_negotiate_basicclient() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        COLOR_EYRE.call_once(|| color_eyre::install().unwrap_or(()));

        // 0. Make rt.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                // 0-4. make shard servers and shard canonical server
                let (_redis_h, canonical_addr) = shard_setup(35215, 31421).await;

                // 5. make client
                info!("make client");

                let neg_stack = SerializeChunnel::default();

                let raw_cn = UdpSkChunnel::default()
                    .connect(canonical_addr)
                    .await
                    .unwrap();
                let cn = bertha::negotiate::negotiate_client(neg_stack, raw_cn, canonical_addr)
                    .await
                    .unwrap();
                let cn = ProjectLeft::new(canonical_addr, cn);

                // 6. issue a request
                info!("send request");
                cn.send(std::iter::once(Msg {
                    k: "aaaaaaaa".to_owned(),
                    v: "bbbbbbbb".to_owned(),
                }))
                .await
                .unwrap();

                info!("await response");
                let mut slot = [None];
                let ms = cn.recv(&mut slot).await.unwrap();
                let m = ms[0].take().unwrap();
                use super::Kv;
                assert_eq!(m.key(), "aaaaaaaa");
                assert_eq!(m.val(), "bbbbbbbb");
            }
            .instrument(tracing::info_span!("negotiate_basicclient")),
        );
    }

    #[test]
    fn shard_negotiate_clientonly() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        COLOR_EYRE.call_once(|| color_eyre::install().unwrap_or(()));

        // 0. Make rt.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                // 0-4. make shard servers and shard canonical server
                let (redis_h, canonical_addr) = shard_setup(25215, 21421).await;

                // 5. make client
                info!("make client");
                let redis_addr = redis_h.get_addr();

                let cl = ClientShardChunnelClient::new(canonical_addr, &redis_addr)
                    .await
                    .unwrap();

                use bertha::negotiate::Select;
                let neg_stack = CxList::from(Select::from((cl, Nothing::<()>::default())))
                    .wrap(SerializeChunnel::default());

                let raw_cn = UdpSkChunnel::default()
                    .connect(canonical_addr)
                    .await
                    .unwrap();
                let cn = bertha::negotiate::negotiate_client(neg_stack, raw_cn, canonical_addr)
                    .await
                    .unwrap();
                let cn = ProjectLeft::new(canonical_addr, cn);

                // 6. issue a request
                info!("send request");
                cn.send(std::iter::once(Msg {
                    k: "aaaaaaaa".to_owned(),
                    v: "bbbbbbbb".to_owned(),
                }))
                .await
                .unwrap();

                info!("await response");
                let mut slot = [None];
                let ms = cn.recv(&mut slot).await.unwrap();
                let m = ms[0].take().unwrap();
                use super::Kv;
                assert_eq!(m.key(), "aaaaaaaa");
                assert_eq!(m.val(), "bbbbbbbb");
            }
            .instrument(tracing::info_span!("negotiate_clientonly")),
        );
    }
}
