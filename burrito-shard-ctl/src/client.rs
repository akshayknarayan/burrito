use anyhow::{Context, Error};
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
            Some(Ok(proto::Reply::Register(proto::RegisterShardReply::Err(e)))) => {
                Err(anyhow::anyhow!(e))
            }
            Some(Err(e)) => Err(anyhow::Error::from(e)),
            None => Err(anyhow::anyhow!("Stream done")),
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
            Some(Ok(proto::Reply::Query(proto::QueryShardReply::Err(e)))) => {
                Err(anyhow::anyhow!(e))
            }
            Some(Err(e)) => Err(anyhow::Error::from(e)),
            None => Err(anyhow::anyhow!("Stream done")),
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
                proto::Addr::Burrito(_) => Err(anyhow::anyhow!("Cannot resolve returned Addr: {}")),
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
            let rsp = dcl
                .query(addr)
                .await
                .context("Could not query discovery-ctl")?;

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
