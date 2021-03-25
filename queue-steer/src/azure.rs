use crate::{MessageQueueCaps, MessageQueueOrdering, MessageQueueReliability};
use az_queues::AzStorageQueueChunnel;
use bertha::{util::NeverCn, Chunnel, Negotiate};
use color_eyre::eyre::Report;
use futures_util::future::{ready, Ready};

#[derive(Debug, Clone)]
pub struct AzQueueChunnelWrap(AzStorageQueueChunnel);
impl From<AzStorageQueueChunnel> for AzQueueChunnelWrap {
    fn from(i: AzStorageQueueChunnel) -> Self {
        Self(i)
    }
}

impl From<AzQueueChunnelWrap> for AzStorageQueueChunnel {
    fn from(AzQueueChunnelWrap(s): AzQueueChunnelWrap) -> AzStorageQueueChunnel {
        s
    }
}

impl Chunnel<NeverCn> for AzQueueChunnelWrap {
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = AzStorageQueueChunnel;
    type Error = Report;

    fn connect_wrap(&mut self, _: NeverCn) -> Self::Future {
        ready(Ok(self.0.clone()))
    }
}

impl Negotiate for AzQueueChunnelWrap {
    type Capability = MessageQueueCaps;

    fn guid() -> u64 {
        0xaa29a8c2eeb6ed03
    }

    fn capabilities() -> Vec<Self::Capability> {
        vec![MessageQueueCaps {
            ordering: MessageQueueOrdering::BestEffort,
            reliability: MessageQueueReliability::AtLeastOnce,
        }]
    }
}
