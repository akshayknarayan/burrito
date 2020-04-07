use anyhow::Error;
use std::path::Path;
use std::pin::Pin;
use tracing::trace;

use crate::{proto, CONTROLLER_ADDRESS};

#[derive(Debug)]
pub struct DiscoveryClient {
    uc: async_bincode::AsyncBincodeStream<
        tokio::net::UnixStream,
        proto::Reply,
        proto::Request,
        async_bincode::AsyncDestination,
    >,
}

impl DiscoveryClient {
    pub async fn new(burrito_root: impl AsRef<Path>) -> Result<Self, Error> {
        let controller_addr = burrito_root.as_ref().join(CONTROLLER_ADDRESS);
        let uc: async_bincode::AsyncBincodeStream<_, proto::Reply, proto::Request, _> =
            tokio::net::UnixStream::connect(controller_addr)
                .await?
                .into();
        let uc = uc.for_async();

        Ok(DiscoveryClient { uc })
    }

    pub async fn register(&mut self, req: proto::Service) -> Result<proto::RegisterReplyOk, Error> {
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
            Some(Ok(proto::Reply::Register(r))) => {
                let r: Result<_, _> = r.into();
                let r = r.map_err(|e| anyhow::anyhow!(e));
                Ok(r?)
            }
            Some(Err(e)) => Err(anyhow::Error::from(e)),
            None => Err(anyhow::anyhow!("Stream done")),
            Some(Ok(proto::Reply::Query(_))) => unreachable!(),
        }
    }

    pub async fn query(&mut self, req: String) -> Result<proto::QueryNameReplyOk, Error> {
        use futures_util::{sink::Sink, stream::StreamExt};

        trace!("poll_ready");
        futures_util::future::poll_fn(|cx| {
            let pst = Pin::new(&mut self.uc);
            pst.poll_ready(cx)
        })
        .await?;

        let pst = Pin::new(&mut self.uc);
        trace!("start_send");
        pst.start_send(proto::Request::Query(proto::QueryNameRequest {
            name: req,
            ..Default::default()
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
            Some(Ok(proto::Reply::Query(r))) => {
                let r: Result<_, _> = r.into();
                let r = r.map_err(|e| anyhow::anyhow!(e));
                Ok(r?)
            }
            Some(Err(e)) => Err(anyhow::Error::from(e)),
            None => Err(anyhow::anyhow!("Stream done")),
            Some(Ok(proto::Reply::Register(_))) => unreachable!(),
        }
    }
}
