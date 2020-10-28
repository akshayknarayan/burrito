//! Client side.

use crate::msg::Msg;
use bertha::{
    bincode::SerializeChunnelProject, reliable::ReliabilityProjChunnel, tagger::OrderedChunnelProj,
    udp::UdpSkChunnel, util::ProjectLeft, ChunnelConnection, ChunnelConnector, CxList,
};
use burrito_shard_ctl::ClientShardChunnelClient;
use color_eyre::eyre::{eyre, Report, WrapErr};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use tracing::{debug, debug_span, trace};
use tracing_futures::Instrument;

/// Connect to a Kv service.
#[derive(Debug)]
pub struct KvClient<C>(Arc<C>);

impl<C> Clone for KvClient<C> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl KvClient<()> {
    pub async fn new_basicclient(
        canonical_addr: SocketAddr,
    ) -> Result<KvClient<impl ChunnelConnection<Data = Msg> + 'static>, Report> {
        debug!("make client");
        let neg_stack = CxList::from(ProjectLeft::from(canonical_addr))
            .wrap(OrderedChunnelProj::default())
            .wrap(ReliabilityProjChunnel::default())
            .wrap(SerializeChunnelProject::default());

        let raw_cn = UdpSkChunnel::default().connect(()).await?;
        debug!("negotiation");
        let cn = bertha::negotiate::negotiate_client(neg_stack, raw_cn, canonical_addr)
            .instrument(debug_span!("client_negotiate"))
            .await?;
        Ok(Self::new_from_cn(cn))
    }

    pub async fn new_shardclient(
        redis_addr: SocketAddr,
        canonical_addr: SocketAddr,
    ) -> Result<KvClient<impl ChunnelConnection<Data = Msg> + 'static>, Report> {
        let redis_addr = format!("redis://{}:{}", redis_addr.ip(), redis_addr.port());
        debug!("make client");

        let cl = ClientShardChunnelClient::new(canonical_addr, &redis_addr).await?;

        let neg_stack = CxList::from(bertha::negotiate::Select(
            cl,
            ProjectLeft::from(canonical_addr),
        ))
        .wrap(OrderedChunnelProj::default())
        .wrap(ReliabilityProjChunnel::default())
        .wrap(SerializeChunnelProject::default());

        let raw_cn = UdpSkChunnel::default().connect(()).await?;
        debug!("negotiation");
        let cn = bertha::negotiate::negotiate_client(neg_stack, raw_cn, canonical_addr)
            .instrument(debug_span!("client_negotiate"))
            .await?;
        Ok(Self::new_from_cn(cn))
    }

    fn new_from_cn<C: ChunnelConnection<Data = Msg> + 'static>(inner: C) -> KvClient<C> {
        KvClient(Arc::new(inner))
    }
}

#[tracing::instrument(level = "debug", skip(cn))]
async fn do_req<R, C>(cn: R, req: Msg) -> Result<Option<String>, Report>
where
    R: AsRef<C>,
    C: ChunnelConnection<Data = Msg>,
{
    let cn = cn.as_ref();
    let id = req.id;
    trace!("sending");
    // NOTE assumes in-order delivery! Otherwise it will error.
    cn.send(req).await.wrap_err("Error sending request")?;
    let rsp = cn.recv().await.wrap_err("Error awaiting response")?;
    trace!("received");
    if rsp.id != id {
        return Err(eyre!(
            "Msg id mismatch, check for reordering: {} != {}",
            rsp.id,
            id
        ));
    }

    Ok(rsp.into_kv().1)
}

impl<C> KvClient<C>
where
    C: ChunnelConnection<Data = Msg> + 'static,
{
    fn do_req_fut(
        &self,
        req: Msg,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Option<String>, Report>>>> {
        let cn = Arc::clone(&self.0);
        Box::pin(async move { do_req(cn, req).await })
    }

    pub fn update_fut(
        &self,
        key: String,
        val: String,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Option<String>, Report>>>> {
        let req = Msg::put_req(key, val);
        self.do_req_fut(req)
    }

    pub async fn update(&self, key: String, val: String) -> Result<Option<String>, Report> {
        let req = Msg::put_req(key, val);
        do_req(&self.0, req).await
    }

    pub fn get_fut(
        &self,
        key: String,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Option<String>, Report>>>> {
        let req = Msg::get_req(key);
        self.do_req_fut(req)
    }

    pub async fn get(&self, key: String) -> Result<Option<String>, Report> {
        let req = Msg::get_req(key);
        do_req(&self.0, req).await
    }
}
