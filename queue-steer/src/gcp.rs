use crate::{MessageQueueCaps, MessageQueueOrdering, MessageQueueReliability};
use bertha::{util::NeverCn, Chunnel, Negotiate};
use color_eyre::eyre::Report;
use futures_util::future::{ready, Ready};
use gcp_pubsub::{OrderedPubSubChunnel, PubSubChunnel};

#[derive(Debug, Clone)]
pub struct GcpPubSubWrap(PubSubChunnel);
impl From<PubSubChunnel> for GcpPubSubWrap {
    fn from(i: PubSubChunnel) -> Self {
        Self(i)
    }
}

impl From<GcpPubSubWrap> for PubSubChunnel {
    fn from(GcpPubSubWrap(s): GcpPubSubWrap) -> PubSubChunnel {
        s
    }
}

impl Chunnel<NeverCn> for GcpPubSubWrap {
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = PubSubChunnel;
    type Error = Report;

    fn connect_wrap(&mut self, _: NeverCn) -> Self::Future {
        ready(Ok(self.0.clone()))
    }
}

impl Negotiate for GcpPubSubWrap {
    type Capability = MessageQueueCaps;

    fn guid() -> u64 {
        0xf8269884685dc39e
    }

    fn capabilities() -> Vec<Self::Capability> {
        vec![MessageQueueCaps {
            ordering: MessageQueueOrdering::BestEffort,
            reliability: MessageQueueReliability::AtLeastOnce,
        }]
    }
}

#[derive(Debug, Clone)]
pub struct OrderedGcpPubSubWrap(OrderedPubSubChunnel);
impl From<OrderedPubSubChunnel> for OrderedGcpPubSubWrap {
    fn from(i: OrderedPubSubChunnel) -> Self {
        Self(i)
    }
}

impl From<OrderedGcpPubSubWrap> for OrderedPubSubChunnel {
    fn from(OrderedGcpPubSubWrap(s): OrderedGcpPubSubWrap) -> Self {
        s
    }
}

impl OrderedGcpPubSubWrap {
    pub async fn convert(inner: GcpPubSubWrap) -> Result<Self, Report> {
        Ok(Self(OrderedPubSubChunnel::convert(inner.0).await?))
    }
}

impl Chunnel<NeverCn> for OrderedGcpPubSubWrap {
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = OrderedPubSubChunnel;
    type Error = Report;

    fn connect_wrap(&mut self, _: NeverCn) -> Self::Future {
        ready(Ok(self.0.clone()))
    }
}

impl Negotiate for OrderedGcpPubSubWrap {
    type Capability = MessageQueueCaps;

    fn guid() -> u64 {
        0x8521569756866026
    }

    fn capabilities() -> Vec<Self::Capability> {
        vec![MessageQueueCaps {
            ordering: MessageQueueOrdering::Ordered,
            reliability: MessageQueueReliability::AtMostOnce,
        }]
    }
}
