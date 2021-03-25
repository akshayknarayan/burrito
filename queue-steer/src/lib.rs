//! Steer messages to the right queue.

use bertha::{
    atmostonce::{AtMostOnceChunnel, AtMostOnceCn},
    tagger::{OrderedChunnelProj, OrderedProj},
    Chunnel, ChunnelConnection, Negotiate,
};
use futures_util::future::{ready, Ready};
use std::hash::Hash;

#[cfg(feature = "bin")]
pub mod bin_help;

mod aws;
pub use aws::{OrderedSqsChunnelWrap, SqsChunnelWrap};
mod azure;
pub use azure::AzQueueChunnelWrap;
mod gcp;
pub use gcp::{GcpPubSubWrap, OrderedGcpPubSubWrap};
mod kafka_ch;
pub use kafka_ch::KafkaChunnelWrap;
mod set_group;
pub use set_group::{FakeSetGroup, FakeSetGroupAddr, FakeSetGroupCn, SetGroup};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub enum MessageQueueOrdering {
    BestEffort,
    Ordered,
    ArbitraryDependencies(String),
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub enum MessageQueueReliability {
    AtLeastOnce,
    AtMostOnce,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct MessageQueueCaps {
    ordering: MessageQueueOrdering,
    reliability: MessageQueueReliability,
}

impl bertha::negotiate::CapabilitySet for MessageQueueCaps {
    fn guid() -> u64 {
        0x917be1a7e2a5c907
    }

    fn universe() -> Option<Vec<Self>> {
        // return None to force both sides to match
        None // TODO is this correct?
    }
}

/// Newtype [`bertha::tagger::OrderedChunnelProj`] to impl `Negotiate` on `MessageQueueCaps`
/// semantics.
#[derive(Debug, Clone, Default)]
pub struct Ordered(OrderedChunnelProj);

impl From<OrderedChunnelProj> for Ordered {
    fn from(i: OrderedChunnelProj) -> Self {
        Self(i)
    }
}

impl<A, D, InC> Chunnel<InC> for Ordered
where
    InC: ChunnelConnection<Data = (A, (u32, A, D))> + Send + Sync + 'static,
    A: serde::Serialize
        + serde::de::DeserializeOwned
        + Clone
        + std::fmt::Debug
        + Eq
        + Hash
        + Send
        + Sync
        + 'static,
    D: Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = OrderedProj<A, InC, D>;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        ready(Ok(OrderedProj::new(cn, self.0.hole_thresh)))
    }
}

impl Negotiate for Ordered {
    type Capability = MessageQueueCaps;

    fn guid() -> u64 {
        0xd0dcf8a1cfb0319f
    }

    fn capabilities() -> Vec<Self::Capability> {
        vec![MessageQueueCaps {
            ordering: MessageQueueOrdering::Ordered,
            reliability: MessageQueueReliability::AtMostOnce,
        }]
    }
}

/// Newtype [`bertha::atmostonce::AtMostOnceChunnel`] to implement `Negotiate` on
/// `MessageQueueCaps`.
#[derive(Debug, Clone, Default)]
pub struct AtMostOnce(AtMostOnceChunnel);

impl From<AtMostOnceChunnel> for AtMostOnce {
    fn from(i: AtMostOnceChunnel) -> Self {
        Self(i)
    }
}

impl<A, D, InC> Chunnel<InC> for AtMostOnce
where
    InC: ChunnelConnection<Data = (A, (u32, A, D))> + Send + Sync + 'static,
    A: serde::Serialize
        + serde::de::DeserializeOwned
        + Clone
        + std::fmt::Debug
        + Eq
        + Hash
        + Send
        + Sync
        + 'static,
    D: Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = AtMostOnceCn<InC, A>;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        ready(Ok(AtMostOnceCn::new(cn, self.0.sunset)))
    }
}

impl Negotiate for AtMostOnce {
    type Capability = MessageQueueCaps;

    fn guid() -> u64 {
        0xb6d905613efd7758
    }

    fn capabilities() -> Vec<Self::Capability> {
        vec![MessageQueueCaps {
            ordering: MessageQueueOrdering::BestEffort,
            reliability: MessageQueueReliability::AtMostOnce,
        }]
    }
}
