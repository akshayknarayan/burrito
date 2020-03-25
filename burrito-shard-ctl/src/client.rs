use anyhow::Error;
use std::path::Path;
use std::pin::Pin;
use tracing::trace;

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

    pub async fn query(&mut self, req: &str) -> Result<proto::ShardInfo, Error> {
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
            service_name: req.into(),
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
}
