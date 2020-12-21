//! Client side.

use crate::msg::Msg;
use bertha::{
    bincode::SerializeChunnelProject,
    reliable::ReliabilityProjChunnel,
    tagger::OrderedChunnelProj,
    util::ProjectLeft,
    util::{MsgIdMatcher, NeverCn, Nothing},
    ChunnelConnection, CxList,
};
use burrito_shard_ctl::ClientShardChunnelClient;
use color_eyre::eyre::{eyre, Report, WrapErr};
use std::net::SocketAddr;
use std::pin::Pin;
use tracing::{debug, debug_span, trace};
use tracing_futures::Instrument;

/// Connect to a Kv service.
pub struct KvClient<C: ChunnelConnection>(MsgIdMatcher<C, Msg>);

impl<C: ChunnelConnection> Clone for KvClient<C> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl KvClient<NeverCn> {
    pub async fn new_basicclient(
        raw_cn: impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
        canonical_addr: SocketAddr,
    ) -> Result<KvClient<impl ChunnelConnection<Data = Msg> + Send + Sync + 'static>, Report> {
        debug!("make client");
        let neg_stack = CxList::from(OrderedChunnelProj::default())
            .wrap(ReliabilityProjChunnel::default())
            .wrap(SerializeChunnelProject::default());

        debug!("negotiation");
        let cn = bertha::negotiate::negotiate_client(neg_stack, raw_cn, canonical_addr)
            .instrument(debug_span!("client_negotiate"))
            .await?;
        let cn = ProjectLeft::new(canonical_addr, cn);
        Ok(Self::new_from_cn(cn))
    }

    pub async fn new_shardclient(
        raw_cn: impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
        redis_addr: SocketAddr,
        canonical_addr: SocketAddr,
    ) -> Result<KvClient<impl ChunnelConnection<Data = Msg> + Send + Sync + 'static>, Report> {
        let redis_addr = format!("redis://{}:{}", redis_addr.ip(), redis_addr.port());
        debug!("make client");

        let cl = ClientShardChunnelClient::new(canonical_addr, &redis_addr).await?;
        let neg_stack = CxList::from(bertha::negotiate::Select(cl, Nothing::default()))
            .wrap(OrderedChunnelProj::default())
            .wrap(ReliabilityProjChunnel::default())
            .wrap(SerializeChunnelProject::default());

        debug!("negotiation");
        let cn = bertha::negotiate::negotiate_client(neg_stack, raw_cn, canonical_addr)
            .instrument(debug_span!("client_negotiate"))
            .await?;
        let cn = ProjectLeft::new(canonical_addr, cn);
        Ok(Self::new_from_cn(cn))
    }

    fn new_from_cn<C2: ChunnelConnection<Data = Msg> + Send + Sync + 'static>(
        inner: C2,
    ) -> KvClient<C2> {
        KvClient(MsgIdMatcher::new(inner))
    }
}

impl<C> KvClient<C>
where
    C: ChunnelConnection<Data = Msg> + Send + Sync + 'static,
{
    #[tracing::instrument(level = "debug", skip(self))]
    async fn do_req(&self, req: Msg) -> Result<Option<String>, Report> {
        let cn = &self.0;
        let id = req.id;
        trace!("sending");
        cn.send_msg(req).await.wrap_err("Error sending request")?;
        let rsp = cn.recv_msg(id).await.wrap_err("Error awaiting response")?;
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
}

impl<C> KvClient<C>
where
    C: ChunnelConnection<Data = Msg> + Send + Sync + 'static,
{
    fn do_req_fut(
        &self,
        req: Msg,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Option<String>, Report>>>> {
        let cl = self.clone();
        Box::pin(async move { cl.do_req(req).await })
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
        self.do_req(req).await
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
        self.do_req(req).await
    }
}
