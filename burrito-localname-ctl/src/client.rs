use crate::{proto, CONTROLLER_ADDRESS};
use color_eyre::eyre::{bail, eyre, Error};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use tokio::net::UnixStream;
use tokio_tower::pipeline;
use tower_service::Service;

#[derive(Debug)]
pub struct LocalNameClient {
    cl: pipeline::Client<
        async_bincode::AsyncBincodeStream<
            UnixStream,
            proto::Reply,
            proto::Request,
            async_bincode::AsyncDestination,
        >,
        Error,
        proto::Request,
    >,
}

impl LocalNameClient {
    pub async fn new(burrito_root: &Path) -> Result<Self, Error> {
        let controller_addr = burrito_root.join(CONTROLLER_ADDRESS);
        let uc: async_bincode::AsyncBincodeStream<_, proto::Reply, proto::Request, _> =
            UnixStream::connect(controller_addr).await?.into();
        let uc = uc.for_async();
        let cl = pipeline::Client::new(uc);

        Ok(LocalNameClient { cl })
    }

    pub async fn register(&mut self, name: SocketAddr) -> Result<PathBuf, Error> {
        tracing::trace!(?name, "registering");
        futures_util::future::poll_fn(|cx| self.cl.poll_ready(cx)).await?;
        match self
            .cl
            .call(proto::Request::Register(proto::RegisterRequest { name }))
            .await
        {
            Ok(proto::Reply::Register(r)) => {
                let r: Result<proto::RegisterReplyOk, String> = r.into();
                r.map_err(|s| eyre!("{}", s)).map(|r| r.local_addr)
            }
            _ => bail!("Reply mismatched request type"),
        }
    }

    pub async fn query(&mut self, req: SocketAddr) -> Result<Option<PathBuf>, Error> {
        futures_util::future::poll_fn(|cx| self.cl.poll_ready(cx)).await?;
        match self.cl.call(proto::Request::Query(req)).await {
            Ok(proto::Reply::Query(r)) => {
                let r: Result<proto::QueryNameReplyOk, String> = r.into();
                r.map_err(|s| eyre!("{}", s)).and_then(
                    |proto::QueryNameReplyOk { addr, local_addr }| {
                        if addr != req {
                            bail!("Reply mismatched request address")
                        }

                        Ok(local_addr)
                    },
                )
            }
            _ => bail!("Reply mismatched request type"),
        }
    }
}
