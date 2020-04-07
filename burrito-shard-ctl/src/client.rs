use anyhow::{Context, Error};
use std::path::Path;
use std::pin::Pin;
use tracing::{debug, trace};

use crate::{proto, CONTROLLER_ADDRESS};

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

    pub async fn query(&mut self, req: proto::Addr) -> Result<proto::ShardInfo, Error> {
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

    pub async fn query_recursive(
        &mut self,
        dcl: &mut burrito_discovery_ctl::client::DiscoveryClient,
        name: &str,
    ) -> Result<proto::ShardInfo, Error> {
        let addr = name.parse::<proto::Addr>()?;

        async fn ask(
            scl: &mut ShardCtlClient,
            dcl: &mut burrito_discovery_ctl::client::DiscoveryClient,
            s: crate::proto::Addr,
        ) -> Result<(Option<crate::proto::Addr>, Option<crate::proto::ShardInfo>), Error> {
            let ba = s.clone();
            let addr = match s {
                crate::proto::Addr::Burrito(b) => b,
                a => return Ok((Some(a), None)),
            };

            let rsp = dcl
                .query(addr)
                .await
                .context("Could not query discovery-ctl")?;

            debug!(services = ?&rsp.services, addr = ?&ba, "got services");

            let mut info: (Option<crate::proto::Addr>, Option<crate::proto::ShardInfo>) =
                (None, None);
            for srv in rsp.services.into_iter() {
                let burrito_discovery_ctl::proto::Service {
                    service, address, ..
                } = srv;
                match service.as_str() {
                    burrito_discovery_ctl::CONTROLLER_ADDRESS => {
                        info.0 = info.0.or_else(|| Some(address));
                    }
                    crate::CONTROLLER_ADDRESS => {
                        debug!(service_addr = ?&address, "Querying ShardCtl");
                        if let None = info.1 {
                            let shards = scl.query(address).await?;
                            info.1 = info.1.or_else(|| Some(shards));
                        }
                    }
                    _ => (),
                }
            }

            Ok(info)
        }

        async fn resolve_shards(
            scl: &mut ShardCtlClient,
            dcl: &mut burrito_discovery_ctl::client::DiscoveryClient,
            addr: crate::proto::Addr,
        ) -> Result<Vec<crate::proto::Addr>, Error> {
            let ba = addr.clone();
            let mut remaining = vec![addr];
            let mut done = vec![];
            while !remaining.is_empty() {
                let addr = remaining.pop().unwrap();
                match ask(scl, dcl, addr).await? {
                    (Some(ca), None) => done.push(ca),
                    (_, Some(s)) => {
                        remaining.extend(s.shard_addrs);
                    }
                    (None, None) => Err(anyhow::anyhow!("Could not find addr: {}", ba))?,
                }
            }

            Ok(done)
        }

        let a = addr.clone();
        let (ca, si) = ask(self, dcl, addr).await?;
        let ca = ca.ok_or_else(|| anyhow::anyhow!("Could not find addr: {}", a))?;
        match si {
            Some(mut si) => {
                si.canonical_addr = ca;
                let mut shards = vec![];
                for s in si.shard_addrs {
                    match s {
                        b @ crate::proto::Addr::Burrito(_) => {
                            shards.extend(resolve_shards(self, dcl, b).await?.into_iter())
                        }
                        x => shards.push(x),
                    }
                }

                si.shard_addrs = shards;
                Ok(si)
            }
            None => Err(anyhow::anyhow!("No ShardCtl entry found"))?,
        }
    }
}
