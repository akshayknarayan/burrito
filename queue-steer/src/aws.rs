use crate::{MessageQueueCaps, MessageQueueOrdering, MessageQueueReliability};
use bertha::{util::NeverCn, Chunnel, Negotiate};
use color_eyre::eyre::Report;
use futures_util::future::{ready, Ready};
use sqs::{OrderedSqsChunnel, SqsChunnel};

#[derive(Debug, Clone)]
pub struct SqsChunnelWrap(SqsChunnel);
impl From<SqsChunnel> for SqsChunnelWrap {
    fn from(i: SqsChunnel) -> Self {
        Self(i)
    }
}

impl From<SqsChunnelWrap> for SqsChunnel {
    fn from(SqsChunnelWrap(s): SqsChunnelWrap) -> SqsChunnel {
        s
    }
}

impl Chunnel<NeverCn> for SqsChunnelWrap {
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = SqsChunnel;
    type Error = Report;

    fn connect_wrap(&mut self, _: NeverCn) -> Self::Future {
        ready(Ok(self.0.clone()))
    }
}

impl Negotiate for SqsChunnelWrap {
    type Capability = MessageQueueCaps;

    fn guid() -> u64 {
        0xfe8e872efd74c245
    }

    fn capabilities() -> Vec<Self::Capability> {
        vec![MessageQueueCaps {
            ordering: MessageQueueOrdering::BestEffort,
            reliability: MessageQueueReliability::AtLeastOnce,
        }]
    }
}

#[derive(Debug, Clone)]
pub struct OrderedSqsChunnelWrap(OrderedSqsChunnel);
impl From<OrderedSqsChunnel> for OrderedSqsChunnelWrap {
    fn from(i: OrderedSqsChunnel) -> Self {
        Self(i)
    }
}

impl From<OrderedSqsChunnelWrap> for OrderedSqsChunnel {
    fn from(OrderedSqsChunnelWrap(s): OrderedSqsChunnelWrap) -> Self {
        s
    }
}

impl From<SqsChunnelWrap> for OrderedSqsChunnelWrap {
    fn from(SqsChunnelWrap(i): SqsChunnelWrap) -> Self {
        let i: OrderedSqsChunnel = i.into();
        i.into()
    }
}

impl Chunnel<NeverCn> for OrderedSqsChunnelWrap {
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = OrderedSqsChunnel;
    type Error = Report;

    fn connect_wrap(&mut self, _: NeverCn) -> Self::Future {
        ready(Ok(self.0.clone()))
    }
}

impl Negotiate for OrderedSqsChunnelWrap {
    type Capability = MessageQueueCaps;

    fn guid() -> u64 {
        0xc0fd9c1303caab54
    }

    fn capabilities() -> Vec<Self::Capability> {
        vec![MessageQueueCaps {
            ordering: MessageQueueOrdering::Ordered,
            reliability: MessageQueueReliability::AtMostOnce,
        }]
    }
}
