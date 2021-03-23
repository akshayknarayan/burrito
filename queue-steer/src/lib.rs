//! Steer messages to the right queue.

use az_queues::AzStorageQueueChunnel;
use bertha::{
    atmostonce::{AtMostOnceChunnel, AtMostOnceCn},
    tagger::{OrderedChunnelProj, OrderedProj},
    util::NeverCn,
    Chunnel, ChunnelConnection, Negotiate,
};
use color_eyre::eyre::{bail, eyre, Report};
use futures_util::future::{ready, Ready};
use gcp_pubsub::{OrderedPubSubChunnel, PubSubAddr, PubSubChunnel};
use kafka::{KafkaAddr, KafkaChunnel};
use sqs::{OrderedSqsChunnel, SqsAddr, SqsChunnel};
use std::hash::Hash;

#[cfg(feature = "bin")]
pub mod bin_help;

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum QueueAddr {
    Aws(SqsAddr),
    Azure(String, String),
    Gcp(PubSubAddr),
}

impl std::str::FromStr for QueueAddr {
    type Err = Report;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let idx = s
            .find(':')
            .ok_or_else(|| eyre!("Malformed QueueAddr: {:?}", s))?;
        let sp = [&s[0..idx], &s[idx + 1..]];
        Ok(match sp {
            ["aws", g] | ["AWS", g] => QueueAddr::Aws(SqsAddr {
                queue_id: g.to_string(),
                group: None,
            }),
            ["az", g] | ["Azure", g] => QueueAddr::Azure(g.to_string(), String::new()),
            ["gcp", g] | ["GCP", g] => QueueAddr::Gcp(PubSubAddr {
                topic_id: g.to_string(),
                group: None,
            }),
            _ => bail!("Unkown addr {:?} -> {:?}", s, sp),
        })
    }
}

impl QueueAddr {
    pub fn provider(&self) -> &str {
        match self {
            QueueAddr::Aws(_) => "aws",
            QueueAddr::Azure(_, _) => "azure",
            QueueAddr::Gcp(_) => "gcp",
        }
    }
}

pub trait SetGroup {
    fn set_group(&mut self, group: String);
}

impl SetGroup for QueueAddr {
    fn set_group(&mut self, group: String) {
        match self {
            QueueAddr::Aws(ref mut a) => a.set_group(group),
            QueueAddr::Gcp(ref mut a) => a.set_group(group),
            QueueAddr::Azure(_, ref mut s) => {
                *s = group;
            }
        }
    }
}

impl SetGroup for sqs::SqsAddr {
    fn set_group(&mut self, group: String) {
        self.group = Some(group);
    }
}

impl SetGroup for gcp_pubsub::PubSubAddr {
    fn set_group(&mut self, group: String) {
        self.group = Some(group);
    }
}

impl SetGroup for KafkaAddr {
    fn set_group(&mut self, group: String) {
        self.group = Some(group);
    }
}

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
