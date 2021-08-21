//! Client side.

use crate::msg::Msg;
use bertha::{
    bincode::SerializeChunnelProject,
    negotiate::Select,
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
use tracing::{debug, debug_span, instrument, trace};
use tracing_futures::Instrument;

#[derive(Debug, Clone, Copy)]
pub struct KvClientBuilder<const BATCH: bool> {
    canonical_addr: SocketAddr,
}

impl KvClientBuilder<false> {
    pub fn new(canonical_addr: SocketAddr) -> Self {
        KvClientBuilder { canonical_addr }
    }

    pub fn set_batching(self) -> KvClientBuilder<true> {
        KvClientBuilder {
            canonical_addr: self.canonical_addr,
        }
    }
}

macro_rules! basic_client {
    ($raw_cn: expr, $ca: expr) => {{
        let neg_stack = CxList::from(OrderedChunnelProj::default())
            .wrap(ReliabilityProjChunnel::default())
            .wrap(SerializeChunnelProject::default());

        debug!("negotiation");
        let cn = bertha::negotiate::negotiate_client(neg_stack, $raw_cn, $ca)
            .instrument(debug_span!("client_negotiate"))
            .await?;
        ProjectLeft::new($ca, cn)
    }};
}

macro_rules! nonshard {
    ($raw_cn: expr, $ca: expr) => {{
        use crate::reliability::KvReliabilityChunnel;
        let stack = Select::from((
            CxList::from(OrderedChunnelProj::default())
                .wrap(ReliabilityProjChunnel::default())
                .wrap(SerializeChunnelProject::default()),
            CxList::from(KvReliabilityChunnel::default()).wrap(SerializeChunnelProject::default()),
        ))
        .prefer_right();

        debug!("negotiation");
        let cn = bertha::negotiate::negotiate_client(stack, $raw_cn, $ca)
            .instrument(debug_span!("client_negotiate"))
            .await?;
        ProjectLeft::new($ca, cn)
    }};
}

macro_rules! shardcl {
    ($raw_cn: expr, $redis_addr: expr, $ca: expr) => {{
        use crate::reliability::KvReliabilityChunnel;
        let redis_addr = format!("redis://{}:{}", $redis_addr.ip(), $redis_addr.port());
        let cl = ClientShardChunnelClient::new($ca, &redis_addr).await?;
        let sel = Select::from((
            CxList::from(OrderedChunnelProj::default())
                .wrap(ReliabilityProjChunnel::default())
                .wrap(SerializeChunnelProject::default()),
            CxList::from(KvReliabilityChunnel::default()).wrap(SerializeChunnelProject::default()),
        ))
        .prefer_right();
        let stack = CxList::from(Select::from((
            cl,
            Nothing::<burrito_shard_ctl::ShardFns>::default(),
        )))
        .wrap(sel);

        debug!("negotiation");
        let cn = bertha::negotiate::negotiate_client(stack, $raw_cn, $ca)
            .instrument(debug_span!("client_negotiate"))
            .await?;
        ProjectLeft::new($ca, cn)
    }};
}

impl KvClientBuilder<true> {
    #[instrument(skip(raw_cn), err)]
    pub async fn new_basicclient(
        self,
        raw_cn: impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
    ) -> Result<KvClient<impl ChunnelConnection<Data = Msg> + Send + Sync + 'static>, Report> {
        let cn = basic_client!(raw_cn, self.canonical_addr);
        Ok(KvClient::new_from_cn(batcher::Batcher::new(cn)))
    }

    #[instrument(skip(raw_cn), err)]
    pub async fn new_nonshardclient(
        self,
        raw_cn: impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
    ) -> Result<KvClient<impl ChunnelConnection<Data = Msg> + Send + Sync + 'static>, Report> {
        let cn = nonshard!(raw_cn, self.canonical_addr);
        Ok(KvClient::new_from_cn(batcher::Batcher::new(cn)))
    }

    #[instrument(skip(raw_cn), err)]
    pub async fn new_shardclient(
        self,
        raw_cn: impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
        redis_addr: SocketAddr,
    ) -> Result<KvClient<impl ChunnelConnection<Data = Msg> + Send + Sync + 'static>, Report> {
        let cn = shardcl!(raw_cn, redis_addr, self.canonical_addr);
        Ok(KvClient::new_from_cn(batcher::Batcher::new(cn)))
    }
}

impl KvClientBuilder<false> {
    #[instrument(skip(raw_cn), err)]
    pub async fn new_basicclient(
        self,
        raw_cn: impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
    ) -> Result<KvClient<impl ChunnelConnection<Data = Msg> + Send + Sync + 'static>, Report> {
        let cn = basic_client!(raw_cn, self.canonical_addr);
        Ok(KvClient::new_from_cn(cn))
    }

    #[instrument(skip(raw_cn), err)]
    pub async fn new_nonshardclient(
        self,
        raw_cn: impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
    ) -> Result<KvClient<impl ChunnelConnection<Data = Msg> + Send + Sync + 'static>, Report> {
        let cn = nonshard!(raw_cn, self.canonical_addr);
        Ok(KvClient::new_from_cn(cn))
    }

    #[instrument(skip(raw_cn), err)]
    pub async fn new_shardclient(
        self,
        raw_cn: impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
        redis_addr: SocketAddr,
    ) -> Result<KvClient<impl ChunnelConnection<Data = Msg> + Send + Sync + 'static>, Report> {
        let cn = shardcl!(raw_cn, redis_addr, self.canonical_addr);
        Ok(KvClient::new_from_cn(cn))
    }
}

/// Connect to a Kv service.
pub struct KvClient<C: ChunnelConnection>(MsgIdMatcher<C, Msg>);

impl<C: ChunnelConnection> Clone for KvClient<C> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl KvClient<NeverCn> {
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
    #[instrument(level = "trace", skip(self))]
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
