use eyre::{eyre, Error};
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
                let r = r.map_err(|e| eyre!(e));
                Ok(r?)
            }
            Some(Err(e)) => Err(Error::from(e)),
            None => Err(eyre!("Stream done")),
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
                let r = r.map_err(|e| eyre!(e));
                Ok(r?)
            }
            Some(Err(e)) => Err(Error::from(e)),
            None => Err(eyre!("Stream done")),
            Some(Ok(proto::Reply::Register(_))) => unreachable!(),
        }
    }
}

#[cfg(feature = "chunnels")]
pub use chunnels::*;

#[cfg(feature = "chunnels")]
mod chunnels {
    use super::DiscoveryClient;
    use crate::proto::Addr;
    use bertha::ChunnelListener;
    use eyre::WrapErr;
    use futures_util::stream::Stream;
    use std::future::Future;
    use std::path::PathBuf;
    use std::pin::Pin;
    use std::sync::Arc;

    /// Naming chunnel: translates proto::Addr into a SocketAddr in connect/listen.
    pub struct Naming<C> {
        burrito_root: PathBuf,
        inner: Arc<C>,
    }

    impl<C> ChunnelListener for Naming<C>
    where
        C: ChunnelListener,
    {
        type Addr = Addr;
        type Connection = C::Connection;

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
            Box::pin(async move {
                let cl = DiscoveryClient::new(root).await;

                if let Err(e) = cl {
                    return Box::pin(futures_util::stream::once(async {
                        Err(e).wrap_err("Could not connect to burrito_discovery_ctl")
                    })) as _;
                }

                let cl = cl.unwrap();
                let r = cl.query(a).await;

                if let Err(e) = r {
                    return Box::pin(futures_util::stream::once(async {
                        Err(e).wrap_err("Could not connect to burrito_discovery_ctl")
                    })) as _;
                }

                let r = r.unwrap();

                unimplemented!()
            })
        }

        fn scope(&self) -> bertha::Scope {
            bertha::Scope::Global
        }
        fn endedness(&self) -> bertha::Endedness {
            bertha::Endedness::Either
        }
        fn implementation_priority(&self) -> usize {
            1
        }
        // fn resource_requirements(&self) -> ?;
    }
}
