//! Steer messages to the right queue.
//!
//! akshay:aerial_tramway:  12:06 yeah maybe routing topics across providers
//! 12:06 based on something
//!
//! panda:panda_face:  12:06 Yeah. That could be cool too
//! 12:06 Just based on the topic?
//!
//! akshay:aerial_tramway:  12:07 I was thinking like you provide a function to do the routing
//! 12:07 could be based on arbitrary stuff like pricing (edited)
//!
//! panda:panda_face:  12:07 Oh yeah. Sure. Topics is just a very simple.function in this scheme right?
//!
//! akshay:aerial_tramway:  12:08 wait what do you mean by topics
//! 12:08 because google calls a queue a topic
//!
//! panda:panda_face:  12:08 Oh so like I have 5 services
//! 12:08 And 3 of them run in AWS
//! 12:08 And 2 in Google
//! 12:09 Client sends messages
//! 12:09 The router looks at the message to.decide which service it is intended for
//! 12:09 And then sends it to the right one

use az_queues::AzStorageQueueChunnel;
use bertha::ChunnelConnection;
use color_eyre::eyre::{eyre, Report, WrapErr};
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
