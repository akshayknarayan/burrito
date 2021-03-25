use crate::{MessageQueueCaps, MessageQueueOrdering, MessageQueueReliability};
use bertha::{util::NeverCn, Chunnel, Negotiate};
use color_eyre::eyre::Report;
use futures_util::future::{ready, Ready};
use kafka::KafkaChunnel;

#[derive(Debug, Clone)]
pub struct KafkaChunnelWrap(KafkaChunnel);
impl From<KafkaChunnel> for KafkaChunnelWrap {
    fn from(i: KafkaChunnel) -> Self {
        Self(i)
    }
}

impl From<KafkaChunnelWrap> for KafkaChunnel {
    fn from(KafkaChunnelWrap(s): KafkaChunnelWrap) -> Self {
        s
    }
}

impl Chunnel<NeverCn> for KafkaChunnelWrap {
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = KafkaChunnel;
    type Error = Report;

    fn connect_wrap(&mut self, _: NeverCn) -> Self::Future {
        ready(Ok(self.0.clone()))
    }
}

impl Negotiate for KafkaChunnelWrap {
    type Capability = MessageQueueCaps;

    fn guid() -> u64 {
        0x982a6af1c899fe15
    }

    fn capabilities() -> Vec<Self::Capability> {
        vec![MessageQueueCaps {
            ordering: MessageQueueOrdering::Ordered,
            reliability: MessageQueueReliability::AtMostOnce,
        }]
    }
}
