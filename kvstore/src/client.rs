//! Client side.

use crate::msg::Msg;
use crate::reliability::KvReliabilityChunnel;
use bertha::{
    bincode::SerializeChunnelProject,
    negotiate::Select,
    split::Split,
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
pub struct KvClientBuilder<const BATCH: bool, const FRAG: bool> {
    canonical_addr: SocketAddr,
}

impl KvClientBuilder<false, false> {
    pub fn new(canonical_addr: SocketAddr) -> Self {
        KvClientBuilder { canonical_addr }
    }
}

impl<const B: bool, const F: bool> KvClientBuilder<B, F> {
    pub fn set_batching(self) -> KvClientBuilder<true, F> {
        KvClientBuilder {
            canonical_addr: self.canonical_addr,
        }
    }

    pub fn set_stack_fragmentation(self) -> KvClientBuilder<B, true> {
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
    ($raw_cn: expr, $redis_addr: expr, $ca: expr, $sel: expr) => {{
        let redis_addr = format!("redis://{}:{}", $redis_addr.ip(), $redis_addr.port());
        let cl = ClientShardChunnelClient::new($ca, &redis_addr).await?;
        let stack = CxList::from(Select::from((
            cl,
            Nothing::<burrito_shard_ctl::ShardFns>::default(),
        )))
        .wrap($sel);
        debug!("negotiation");
        let cn = bertha::negotiate::negotiate_client(stack, $raw_cn, $ca)
            .instrument(debug_span!("client_negotiate"))
            .await?;
        ProjectLeft::new($ca, cn)
    }};
}

macro_rules! batch_wrap {
    ($cn: expr, $max_batch_size: expr) => {{
        let mut b = batcher::Batcher::new($cn);
        b.set_max_batch_size($max_batch_size);
        Ok(KvClient::new_from_cn(b))
    }};
}

impl<const F: bool> KvClientBuilder<true, F> {
    #[instrument(skip(raw_cn), err)]
    pub async fn new_basicclient(
        self,
        raw_cn: impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
        max_batch_size: usize,
    ) -> Result<KvClient<impl ChunnelConnection<Data = Msg> + Send + Sync + 'static>, Report> {
        let cn = basic_client!(raw_cn, self.canonical_addr);
        batch_wrap!(cn, max_batch_size)
    }

    #[instrument(skip(raw_cn), err)]
    pub async fn new_nonshardclient(
        self,
        raw_cn: impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
        max_batch_size: usize,
    ) -> Result<KvClient<impl ChunnelConnection<Data = Msg> + Send + Sync + 'static>, Report> {
        let cn = nonshard!(raw_cn, self.canonical_addr);
        batch_wrap!(cn, max_batch_size)
    }
}

impl<const F: bool> KvClientBuilder<false, F> {
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
}

macro_rules! stack_no_split {
    () => {
        Select::from((
            CxList::from(OrderedChunnelProj::default())
                .wrap(ReliabilityProjChunnel::default())
                .wrap(SerializeChunnelProject::default()),
            CxList::from(KvReliabilityChunnel::default()).wrap(SerializeChunnelProject::default()),
        ))
        .prefer_right()
    };
}

impl KvClientBuilder<true, false> {
    #[instrument(skip(raw_cn), err)]
    pub async fn new_shardclient(
        self,
        raw_cn: impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
        redis_addr: SocketAddr,
        max_batch_size: usize,
    ) -> Result<KvClient<impl ChunnelConnection<Data = Msg> + Send + Sync + 'static>, Report> {
        let sel = stack_no_split!();
        let cn = shardcl!(raw_cn, redis_addr, self.canonical_addr, sel);
        batch_wrap!(cn, max_batch_size)
    }
}

impl KvClientBuilder<false, false> {
    #[instrument(skip(raw_cn), err)]
    pub async fn new_shardclient(
        self,
        raw_cn: impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
        redis_addr: SocketAddr,
    ) -> Result<KvClient<impl ChunnelConnection<Data = Msg> + Send + Sync + 'static>, Report> {
        let sel = stack_no_split!();
        let cn = shardcl!(raw_cn, redis_addr, self.canonical_addr, sel);
        Ok(KvClient::new_from_cn(cn))
    }
}

macro_rules! stack_with_split {
    () => {
        Select::from((
            CxList::from(OrderedChunnelProj::default())
                .wrap(ReliabilityProjChunnel::default())
                .wrap(SerializeChunnelProject::default()),
            CxList::from(KvReliabilityChunnel::default())
                .wrap(Split::default())
                .wrap(SerializeChunnelProject::default()),
        ))
        .prefer_right()
    };
}

impl KvClientBuilder<true, true> {
    #[instrument(skip(raw_cn), err)]
    pub async fn new_shardclient(
        self,
        raw_cn: impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
        redis_addr: SocketAddr,
        max_batch_size: usize,
    ) -> Result<KvClient<impl ChunnelConnection<Data = Msg> + Send + Sync + 'static>, Report> {
        let sel = stack_with_split!();
        let cn = shardcl!(raw_cn, redis_addr, self.canonical_addr, sel);
        batch_wrap!(cn, max_batch_size)
    }
}

impl KvClientBuilder<false, true> {
    #[instrument(skip(raw_cn), err)]
    pub async fn new_shardclient(
        self,
        raw_cn: impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
        redis_addr: SocketAddr,
    ) -> Result<KvClient<impl ChunnelConnection<Data = Msg> + Send + Sync + 'static>, Report> {
        let sel = stack_with_split!();
        let cn = shardcl!(raw_cn, redis_addr, self.canonical_addr, sel);
        Ok(KvClient::new_from_cn(cn))
    }
}

impl<const T: bool, const B: bool> KvClientBuilder<T, B> {
    #[instrument(skip(raw_cn), err)]
    pub async fn new_fiat_client(
        self,
        raw_cn: impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
        si: burrito_shard_ctl::ShardInfo<SocketAddr>,
    ) -> Result<KvClient<impl ChunnelConnection<Data = Msg> + Send + Sync + 'static>, Report> {
        use bertha::Chunnel;
        let cl = burrito_shard_ctl::static_client::ClientShardChunnelClient::new(si);
        let mut stack = CxList::from(cl)
            .wrap(KvReliabilityChunnel::default())
            .wrap(SerializeChunnelProject::default());
        let cn = stack.connect_wrap(raw_cn).await?;
        let cn = ProjectLeft::new(self.canonical_addr, cn);
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
