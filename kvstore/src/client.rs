//! Client side.

use crate::msg::Msg;
use bertha::{
    bincode::{SerializeChunnelProject, SerializeSelect},
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
        // deliberately don't add any selects
        let neg_stack = CxList::from(SerializeChunnelProject::default())
            .wrap(ReliabilityProjChunnel::default())
            .wrap(OrderedChunnelProj::default());

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

        use crate::reliability::KvReliabilityChunnel;
        use bertha::negotiate::Select;

        let cl = ClientShardChunnelClient::new(canonical_addr, &redis_addr).await?;
        //let neg_stack = CxList::from(SerializeSelect::<
        //    (SocketAddr, bertha::reliable::Pkt<Msg>),
        //    (SocketAddr, Msg),
        //>::default())
        //.wrap(Select(
        //    CxList::from(ReliabilityProjChunnel::default()).wrap(OrderedChunnelProj::default()),
        //    KvReliabilityChunnel::default(),
        //))
        //.wrap(Select(cl, Nothing::<()>::default()));
        let neg_stack = CxList::from(SerializeChunnelProject::default())
            .wrap(ReliabilityProjChunnel::default())
            .wrap(CxList::from(OrderedChunnelProj::default()))
            .wrap(Select(cl, Nothing::default()));

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

#[cfg(test)]
mod t {
    use crate::reliability::KvReliabilityChunnel;
    use bertha::negotiate::Select;
    use bertha::{
        bincode::{SerializeChunnelProject, SerializeSelect},
        negotiate::Offer,
        reliable::ReliabilityProjChunnel,
        tagger::OrderedChunnelProj,
        util::Nothing,
        Chunnel, ChunnelConnection, CxList,
    };
    use burrito_shard_ctl::ClientShardChunnelClient;
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use tracing_error::ErrorLayer;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    #[test]
    fn select_types() {
        // this test won't run, just check compilation
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        color_eyre::install().unwrap_or_else(|_| ());

        // 0. Make rt.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            //let x: SocketAddr = "127.0.0.1:1414".parse().unwrap();
            //let cl = ClientShardChunnelClient::<_, SocketAddr>::new(x, "127.0.0.1:1414")
            //    .await
            //    .unwrap();

            use crate::msg::Msg;
            //let stack = CxList::from(SerializeSelect::<
            //    (SocketAddr, bertha::reliable::Pkt<Msg>),
            //    (SocketAddr, Msg),
            //>::default())
            let stack = CxList::from(SerializeChunnelProject::default()).wrap(Select(
                CxList::from(ReliabilityProjChunnel::default()).wrap(OrderedChunnelProj::default()),
                KvReliabilityChunnel::default(),
            ));
            //.wrap(Select(cl, Nothing::<()>::default()));

            use bertha::{
                negotiate::{Apply, GetOffers},
                ChunnelConnector, CxListReverse,
            };
            //let stack: Box<dyn Apply<(SocketAddr, Vec<u8>), Applied = _>> = Box::new(stack) as _;
            //let stack: Box<dyn GetOffers<Iter = _>> = Box::new(stack) as _;

            let picked = stack.offers();

            fn group_offers(offers: impl IntoIterator<Item = Offer>) -> HashMap<u64, Vec<Offer>> {
                let mut map = HashMap::<_, Vec<Offer>>::new();
                for o in offers {
                    map.entry(o.capability_guid).or_default().push(o);
                }

                map
            }

            let (new_stack, _) = stack.apply(group_offers(picked))?;
            let mut new_stack = new_stack.rev();
            let raw_cn = bertha::udp::UdpSkChunnel::default().connect(()).await?;
            let _cn = new_stack.connect_wrap(raw_cn).await?;

            Ok(())
        });
    }
}
