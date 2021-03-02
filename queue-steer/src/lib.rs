//! Steer messages to the right queue.

use az_queues::AzStorageQueueChunnel;
use bertha::ChunnelConnection;
use color_eyre::eyre::{bail, eyre, Report, WrapErr};
use gcp_pubsub::PubSubChunnel;
use sqs::SqsChunnel;
use std::future::Future;
use std::pin::Pin;

#[derive(Debug, Clone)]
pub enum QueueAddr {
    Aws(String),
    Azure(String),
    Gcp(String),
}

impl std::str::FromStr for QueueAddr {
    type Err = Report;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let idx = s
            .find(':')
            .ok_or_else(|| eyre!("Malformed QueueAddr: {:?}", s))?;
        let sp = [&s[0..idx], &s[idx + 1..]];
        match sp {
            ["aws", g] | ["AWS", g] => Ok(QueueAddr::Aws(g.to_string())),
            ["az", g] | ["Azure", g] => Ok(QueueAddr::Azure(g.to_string())),
            ["gcp", g] | ["GCP", g] => Ok(QueueAddr::Gcp(g.to_string())),
            _ => bail!("Unkown addr {:?} -> {:?}", s, sp),
        }
    }
}

/// A ChunnelConnection that selects on the 3 cloud provider services.
#[derive(Clone)]
pub struct QueueCn {
    aws: SqsChunnel,
    gcp: PubSubChunnel,
    az: AzStorageQueueChunnel,
}

impl ChunnelConnection for QueueCn {
    type Data = (QueueAddr, String);

    fn send(
        &self,
        (addr, body): Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        match addr {
            QueueAddr::Aws(q) => self.aws.send((q, body)),
            QueueAddr::Azure(q) => self.az.send((q, body)),
            QueueAddr::Gcp(q) => self.gcp.send((q, body)),
        }
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        let aws_fut = self.aws.recv();
        let az_fut = self.az.recv();
        let gcp_fut = self.gcp.recv();
        Box::pin(async move {
            tokio::select! {
                aws_res = aws_fut => {
                    aws_res.map(|(from, body)| (QueueAddr::Aws(from), body)).wrap_err("QueueCn AWS client")
                }
                az_res = az_fut => {
                    az_res.map(|(from, body)| (QueueAddr::Azure(from), body)).wrap_err("QueueCn Azure client")
                }
                gcp_res = gcp_fut => {
                    gcp_res.map(|(from, body)| (QueueAddr::Gcp(from), body)).wrap_err("QueueCn Gcloud client")
                }
            }
        })
    }
}

///// Abstract away the messaging provider behind a name, and allow dynamic migrations of a chunnel between
///// providers.
/////
///// How? Have a meta channel that announces where each service is?
//pub struct MessageChunnel;
//
//impl<A, InC> Chunnel<InC> for MessageChunnel
//where
//    A: std::fmt::Debug + Send + Sync + 'static,
//    InC: ChunnelConnection<Data = (A, String)> + Send + Sync + 'static,
//{
//    type Future =
//        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
//    type Connection = MessageChunnelCn<InC>;
//    type Error = std::convert::Infallible;
//
//    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
//        Box::pin(async move {
//            let r = MessageChunnelCn::from(cn);
//            Ok(r)
//        })
//    }
//}
//
//pub struct MessageChunnelCn<C> {
//    inner: C,
//}
//
//impl<C> From<C> for MessageChunnelCn<C> {
//    fn from(inner: C) -> Self {
//        Self { inner }
//    }
//}
//
//impl<C, A> ChunnelConnection for MessageChunnelCn<C>
//where
//    C: ChunnelConnection<Data = (A, String)>,
//{
//    type Data = (A, String);
//
//    fn send(
//        &self,
//        (addr, body): Self::Data,
//    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
//        unimplemented!()
//    }
//
//    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
//        unimplemented!()
//    }
//}
