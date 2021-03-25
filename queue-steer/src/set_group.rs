use bertha::{Chunnel, ChunnelConnection, Negotiate};
use color_eyre::eyre::Report;
use futures_util::future::{ready, Ready};
use std::{future::Future, pin::Pin};

pub trait SetGroup {
    fn set_group(&mut self, group: String);
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

impl SetGroup for kafka::KafkaAddr {
    fn set_group(&mut self, group: String) {
        self.group = Some(group);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct FakeSetGroupAddr(String, String);
impl From<String> for FakeSetGroupAddr {
    fn from(s: String) -> Self {
        Self(s, String::new())
    }
}

impl SetGroup for FakeSetGroupAddr {
    fn set_group(&mut self, group: String) {
        self.1 = group;
    }
}

#[derive(Debug, Clone, Default)]
pub struct FakeSetGroup;

impl Negotiate for FakeSetGroup {
    type Capability = ();
    fn guid() -> u64 {
        0x8850250c3bbd66ed
    }
}

impl<InC> Chunnel<InC> for FakeSetGroup
where
    InC: ChunnelConnection<Data = (String, String)> + Send + 'static,
{
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = FakeSetGroupCn<InC>;
    type Error = Report;

    fn connect_wrap(&mut self, inner: InC) -> Self::Future {
        ready(Ok(FakeSetGroupCn(inner)))
    }
}

#[derive(Debug, Clone)]
pub struct FakeSetGroupCn<C>(C);
impl<C> From<C> for FakeSetGroupCn<C> {
    fn from(c: C) -> Self {
        Self(c)
    }
}

impl<C> ChunnelConnection for FakeSetGroupCn<C>
where
    C: ChunnelConnection<Data = (String, String)>,
{
    type Data = (FakeSetGroupAddr, String);

    fn send(
        &self,
        (FakeSetGroupAddr(addr, _), body): Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        self.0.send((addr, body))
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        let fut = self.0.recv();
        Box::pin(async move {
            let (a, d) = fut.await?;
            Ok((FakeSetGroupAddr(a, String::new()), d))
        })
    }
}
