use eyre as anyhow;
use eyre::{eyre, Context, Error};
use std::path::Path;
use std::pin::Pin;
use tracing::{debug, trace};

use crate::{proto, CONTROLLER_ADDRESS};

#[derive(Debug, Clone)]
pub enum Shard {
    Addr(proto::Addr),
    Sharded(TreeShardInfo),
}

#[derive(Debug, Clone)]
pub struct TreeShardInfo {
    pub canonical_addr: proto::Addr,
    pub shard_addrs: Vec<Shard>,
    pub shard_info: proto::SimpleShardPolicy,
}

impl From<proto::ShardInfo> for TreeShardInfo {
    fn from(si: proto::ShardInfo) -> Self {
        TreeShardInfo {
            canonical_addr: si.canonical_addr,
            shard_addrs: si.shard_addrs.into_iter().map(|a| Shard::Addr(a)).collect(),
            shard_info: si.shard_info,
        }
    }
}

#[derive(Debug)]
pub struct ShardCtlClient {
    uc: async_bincode::AsyncBincodeStream<
        tokio::net::UnixStream,
        proto::Reply,
        proto::Request,
        async_bincode::AsyncDestination,
    >,
}

impl ShardCtlClient {
    pub async fn new(burrito_root: impl AsRef<Path>) -> Result<Self, Error> {
        let controller_addr = burrito_root.as_ref().join(CONTROLLER_ADDRESS);
        let uc: async_bincode::AsyncBincodeStream<_, proto::Reply, proto::Request, _> =
            tokio::net::UnixStream::connect(controller_addr)
                .await?
                .into();
        let uc = uc.for_async();

        Ok(ShardCtlClient { uc })
    }

    pub async fn register(&mut self, req: proto::ShardInfo) -> Result<(), Error> {
        use futures_util::{sink::Sink, stream::StreamExt};
        futures_util::future::poll_fn(|cx| {
            let pst = Pin::new(&mut self.uc);
            pst.poll_ready(cx)
        })
        .await?;

        let pst = Pin::new(&mut self.uc);
        pst.start_send(proto::Request::Register(req))?;

        futures_util::future::poll_fn(|cx| {
            let pst = Pin::new(&mut self.uc);
            pst.poll_flush(cx)
        })
        .await?;

        trace!("wait for response");

        // now wait for the response
        match self.uc.next().await {
            Some(Ok(proto::Reply::Register(proto::RegisterShardReply::Ok))) => Ok(()),
            Some(Ok(proto::Reply::Register(proto::RegisterShardReply::Err(e)))) => Err(eyre!(e)),
            Some(Err(e)) => Err(Error::from(e)),
            None => Err(eyre!("Stream done")),
            Some(Ok(proto::Reply::Query(_))) => unreachable!(),
        }
    }

    async fn query(&mut self, req: proto::Addr) -> Result<proto::ShardInfo, Error> {
        use futures_util::{sink::Sink, stream::StreamExt};

        trace!("poll_ready");
        futures_util::future::poll_fn(|cx| {
            let pst = Pin::new(&mut self.uc);
            pst.poll_ready(cx)
        })
        .await?;

        let pst = Pin::new(&mut self.uc);
        trace!("start_send");
        pst.start_send(proto::Request::Query(proto::QueryShardRequest {
            canonical_addr: req,
        }))?;

        trace!("poll_flush");
        futures_util::future::poll_fn(|cx| {
            let pst = Pin::new(&mut self.uc);
            pst.poll_flush(cx)
        })
        .await?;

        // now wait for the response
        trace!("wait for response");
        match self.uc.next().await {
            Some(Ok(proto::Reply::Query(proto::QueryShardReply::Ok(si)))) => Ok(si),
            Some(Ok(proto::Reply::Query(proto::QueryShardReply::Err(e)))) => Err(eyre!(e)),
            Some(Err(e)) => Err(Error::from(e)),
            None => Err(eyre!("Stream done")),
            Some(Ok(proto::Reply::Register(_))) => unreachable!(),
        }
    }

    /// If any Addr shard-ctl returns would require discovery-ctl to resolve (Addr::Burrito), this will error.
    /// The correct way to handle that error is to call discovery-ctl via `query_recursive`, so
    /// those addresses can be resolved.
    pub async fn query_shard(&mut self, req: proto::Addr) -> Result<Shard, Error> {
        let si = self.query(req).await.context("Query shard-ctl failed")?;

        fn check(a: &proto::Addr) -> Result<(), Error> {
            match a {
                proto::Addr::Burrito(_) => Err(eyre!("Cannot resolve returned Addr: {}")),
                _ => Ok(()),
            }
        }

        check(&si.canonical_addr)?;
        for s in &si.shard_addrs {
            check(s)?;
        }

        Ok(Shard::Sharded(si.into()))
    }

    /// Helper function for querying shard-ctl and discovery-ctl recursively together.
    pub async fn query_recursive(
        &mut self,
        dcl: &mut burrito_discovery_ctl::client::DiscoveryClient,
        addr: proto::Addr,
    ) -> Result<Shard, Error> {
        async fn ask(
            scl: &mut ShardCtlClient,
            dcl: &mut burrito_discovery_ctl::client::DiscoveryClient,
            s: Shard,
        ) -> Result<Shard, Error> {
            let addr = match s {
                Shard::Addr(crate::proto::Addr::Burrito(b)) => b,
                a => return Ok(a),
            };

            let ba = addr.clone();
            let rsp = dcl.query(addr).await.map_err(|e| {
                // discovery-ctl is on anyhow, so trash the context.
                let e: Box<dyn std::error::Error + Send + Sync + 'static> = e.into();
                eyre!(e)
            })?;

            debug!(services = ?&rsp.services, addr = ?&ba, "got services");

            let mut info: Option<Shard> = None;
            for srv in rsp.services.into_iter() {
                let burrito_discovery_ctl::proto::Service {
                    service, address, ..
                } = srv;
                match service.as_str() {
                    burrito_discovery_ctl::CONTROLLER_ADDRESS => {
                        info = match info {
                            Some(s) => match s {
                                Shard::Addr(_) => unreachable!(),
                                Shard::Sharded(mut t) => {
                                    t.canonical_addr = address;
                                    Some(Shard::Sharded(t))
                                }
                            },
                            None => Some(Shard::Addr(address)),
                        };
                    }
                    crate::CONTROLLER_ADDRESS => {
                        debug!(service_addr = ?&address, "Querying ShardCtl");
                        // this will install the correct offloads for this level of the shard-tree
                        let shards = scl.query(address).await?;
                        info = match info {
                            None => Some(Shard::Sharded(shards.into())),
                            Some(s) => match s {
                                Shard::Addr(a) => {
                                    let mut tsi: TreeShardInfo = shards.into();
                                    tsi.canonical_addr = a;
                                    Some(Shard::Sharded(tsi))
                                }
                                Shard::Sharded(_) => unreachable!(),
                            },
                        }
                    }
                    _ => (),
                }
            }

            Ok(info.ok_or_else(|| anyhow::anyhow!("No information found"))?)
        }

        use std::future::Future;
        fn resolve_shards<'cl>(
            scl: &'cl mut ShardCtlClient,
            dcl: &'cl mut burrito_discovery_ctl::client::DiscoveryClient,
            s: Shard,
        ) -> Pin<Box<dyn Future<Output = Result<Shard, Error>> + 'cl>> {
            Box::pin(async move {
                Ok(match ask(scl, dcl, s).await? {
                    s @ Shard::Addr(_) => s,
                    Shard::Sharded(mut tsi) => {
                        // canonical_addr is guaranteed to be resolved
                        let mut new = Vec::with_capacity(tsi.shard_addrs.len());
                        for sh in tsi.shard_addrs {
                            new.push(resolve_shards(scl, dcl, sh).await?);
                        }

                        tsi.shard_addrs = new;
                        Shard::Sharded(tsi)
                    }
                })
            })
        }

        Ok(resolve_shards(self, dcl, Shard::Addr(addr)).await?)
    }
}

#[cfg(feature = "chunnels")]
pub use chunnels::*;

#[cfg(feature = "chunnels")]
mod chunnels {
    use super::{Shard, ShardCtlClient};
    use crate::proto;
    use bertha::{Chunnel, ChunnelConnection};
    use burrito_discovery_ctl::client::DiscoveryClient;
    use eyre::WrapErr;
    use futures_util::stream::{Stream, StreamExt};
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use tracing::debug;

    const FNV1_64_INIT: u64 = 0xcbf29ce484222325u64;
    const FNV_64_PRIME: u64 = 0x100000001b3u64;

    pub trait Kv {
        type Key;
        fn key(&self) -> Self::Key;

        type Val;
        fn val(&self) -> Self::Val;
    }

    pub struct ShardCanonicalServer<C, S> {
        inner: Arc<Mutex<C>>,
        shards_inner: Arc<Mutex<S>>,
        burrito_root: String,
    }

    impl<C, S, D> Chunnel for ShardCanonicalServer<C, S>
    where
        C: Chunnel<Addr = proto::Addr> + 'static,
        C::Connection: ChunnelConnection<Data = D> + Send + Sync + 'static,
        S: Chunnel<Addr = proto::Addr> + 'static,
        S::Connection: ChunnelConnection<Data = D> + Clone + Send + Sync + 'static,
        D: Kv + Send + Sync + 'static,
        <D as Kv>::Key: AsRef<str>,
    {
        type Addr = proto::ShardInfo;
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
            let root = self.burrito_root.clone();
            let inner = Arc::clone(&self.inner);
            let shards_inner = Arc::clone(&self.shards_inner);
            Box::pin(async move {
                async fn register_shardctl(
                    root: &str,
                    a: proto::ShardInfo,
                ) -> Result<(), eyre::Report> {
                    let mut shardctl = match ShardCtlClient::new(&root).await {
                        Ok(s) => s,
                        Err(e) => Err(eyre::eyre!("Could not contact ShardCtl: err = {}", e))?,
                    };

                    shardctl.register(a).await?;
                    Ok(())
                }

                if let Err(e) = register_shardctl(&root, a.clone()).await {
                    return Box::pin(futures_util::stream::once(async {
                        Err(e).wrap_err("Could not register with shardctl")
                    })) as _;
                }

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
            })
        }

        fn connect(
            &mut self,
            _a: Self::Addr,
        ) -> Pin<Box<dyn Future<Output = Result<Self::Connection, eyre::Report>>>> {
            unimplemented!()
        }

        fn init(&mut self) {}
        fn teardown(&mut self) {}

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
            // the canonical address doesn't send things unprompted.
            unimplemented!()
        }
    }

    pub struct ClientShardChunnelClient<C> {
        inner: Arc<Mutex<C>>,
        burrito_root: String,
    }

    impl<C> Chunnel for ClientShardChunnelClient<C>
    where
        C: Chunnel<Addr = proto::Addr> + 'static,
        C::Connection: Send + Sync + 'static,
        <C::Connection as ChunnelConnection>::Data: Kv + Send + Sync + 'static,
        <<C::Connection as ChunnelConnection>::Data as Kv>::Key: AsRef<str>,
    {
        type Addr = proto::Addr;
        type Connection = ClientShardClientConnection<C::Connection>;

        fn listen(
            &mut self,
            _a: Self::Addr,
        ) -> Pin<
            Box<
                dyn Future<
                    Output = Pin<Box<dyn Stream<Item = Result<Self::Connection, eyre::Report>>>>,
                >,
            >,
        > {
            unimplemented!()
        }

        fn connect(
            &mut self,
            a: Self::Addr,
        ) -> Pin<Box<dyn Future<Output = Result<Self::Connection, eyre::Report>>>> {
            let root = self.burrito_root.clone();
            let inner = Arc::clone(&self.inner);
            Box::pin(async move {
                let mut shardctl = match ShardCtlClient::new(&root).await {
                    Ok(s) => s,
                    Err(e) => Err(eyre::eyre!("Could not contact ShardCtl: err = {}", e))?,
                };

                let si = if let Ok(mut dcl) = DiscoveryClient::new(&root).await {
                    shardctl.query_recursive(&mut dcl, a).await?
                } else {
                    debug!("Could not contact discovery-ctl");
                    shardctl.query_shard(a).await?
                };

                debug!(info = ?&si, "Queried shard");

                let addrs = match si {
                    Shard::Addr(a) => vec![a],
                    Shard::Sharded(si) => {
                        let mut addrs = vec![si.canonical_addr];
                        addrs.extend(
                            si.shard_addrs
                                .into_iter()
                                .map(|s| match s {
                                    Shard::Addr(a) => a,
                                    Shard::Sharded(c) => c.canonical_addr,
                                })
                                .into_iter(),
                        );

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
            })
        }

        fn init(&mut self) {}
        fn teardown(&mut self) {}

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
}
