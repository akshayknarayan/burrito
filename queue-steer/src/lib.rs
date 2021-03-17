//! Steer messages to the right queue.

use az_queues::AzStorageQueueChunnel;
use bertha::{
    atmostonce::{AtMostOnceChunnel, AtMostOnceCn},
    tagger::{OrderedChunnelProj, OrderedProj},
    Chunnel, ChunnelConnection, Negotiate,
};
use color_eyre::eyre::{bail, eyre, Report};
use futures_util::future::{ready, Ready};
use gcp_pubsub::{PubSubAddr, PubSubChunnel};
use sqs::{SqsAddr, SqsChunnel};
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;

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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub enum MessageQueueOrdering {
    BestEffort,
    Ordered,
    GroupOrdered { num_groups: usize },
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

/// Wraps basic chunnel connections.
///
/// Since this is a wrapper, it can impl `Chunnel` and `Negotiate`.
#[derive(Debug, Clone)]
pub struct BaseQueueChunnel;

impl Negotiate for BaseQueueChunnel {
    type Capability = MessageQueueCaps;

    fn guid() -> u64 {
        0xc59e844359deaaa3
    }

    fn capabilities() -> Vec<Self::Capability> {
        vec![MessageQueueCaps {
            ordering: MessageQueueOrdering::BestEffort,
            reliability: MessageQueueReliability::AtLeastOnce,
        }]
    }
}

impl Chunnel<SqsChunnel> for BaseQueueChunnel {
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = BaseQueue;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: SqsChunnel) -> Self::Future {
        ready(Ok(cn.into()))
    }
}

impl Chunnel<AzStorageQueueChunnel> for BaseQueueChunnel {
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = BaseQueue;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: AzStorageQueueChunnel) -> Self::Future {
        ready(Ok(cn.into()))
    }
}

impl Chunnel<PubSubChunnel> for BaseQueueChunnel {
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = BaseQueue;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: PubSubChunnel) -> Self::Future {
        ready(Ok(cn.into()))
    }
}

#[derive(Debug, Clone)]
pub enum BaseQueue {
    Aws(SqsChunnel),
    Azure(AzStorageQueueChunnel),
    Gcp(PubSubChunnel),
}

impl From<SqsChunnel> for BaseQueue {
    fn from(s: SqsChunnel) -> Self {
        BaseQueue::Aws(s)
    }
}

impl From<AzStorageQueueChunnel> for BaseQueue {
    fn from(s: AzStorageQueueChunnel) -> Self {
        BaseQueue::Azure(s)
    }
}

impl From<PubSubChunnel> for BaseQueue {
    fn from(s: PubSubChunnel) -> Self {
        BaseQueue::Gcp(s)
    }
}

impl ChunnelConnection for BaseQueue {
    type Data = (QueueAddr, String);

    fn send(
        &self,
        (addr, data): Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        match self {
            BaseQueue::Aws(this) => {
                let addr = match addr {
                    QueueAddr::Aws(a) => a,
                    x => {
                        return Box::pin(ready(Err(eyre!(
                            "Wrong address type for underlying queue: {:?}",
                            x
                        ))))
                    }
                };
                this.send((addr, data))
            }
            BaseQueue::Azure(this) => {
                let addr = match addr {
                    QueueAddr::Azure(a, _) => a,
                    x => {
                        return Box::pin(ready(Err(eyre!(
                            "Wrong address type for underlying queue: {:?}",
                            x
                        ))))
                    }
                };
                this.send((addr, data))
            }
            BaseQueue::Gcp(this) => {
                let addr = match addr {
                    QueueAddr::Gcp(a) => a,
                    x => {
                        return Box::pin(ready(Err(eyre!(
                            "Wrong address type for underlying queue: {:?}",
                            x
                        ))))
                    }
                };
                this.send((addr, data))
            }
        }
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        match self {
            BaseQueue::Aws(this) => {
                let fut = this.recv();
                Box::pin(async move {
                    let (addr, data) = fut.await?;
                    Ok((QueueAddr::Aws(addr), data))
                })
            }
            BaseQueue::Azure(this) => {
                let fut = this.recv();
                Box::pin(async move {
                    let (addr, data) = fut.await?;
                    Ok((QueueAddr::Azure(addr, String::new()), data))
                })
            }
            BaseQueue::Gcp(this) => {
                let fut = this.recv();
                Box::pin(async move {
                    let (addr, data) = fut.await?;
                    Ok((QueueAddr::Gcp(addr), data))
                })
            }
        }
    }
}
