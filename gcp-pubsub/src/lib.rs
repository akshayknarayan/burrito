//! An GCP PubSub wrapper for Bertha, allowing it to be used as transport for chunnels.
//! This can't impl `Chunnel` because it can't wrap another transport; it is the transport.
//!
//! Chunnel data type = (String, String) -> (queue URL, msg_string)

use bertha::ChunnelConnection;
use color_eyre::eyre::{bail, eyre, Report, WrapErr};
use google_cloud::pubsub::{Client, Subscription, SubscriptionConfig};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

pub struct PubSubChunnel {
    ps_client: Client,
    subscriptions: HashMap<String, Subscription>,
}

impl PubSubChunnel {
    pub async fn new<'a>(
        ps_client: Client,
        recv_topics: impl IntoIterator<Item = &'a str>,
    ) -> Result<Self, Report> {
        let subscriptions =
            futures_util::future::try_join_all(recv_topics.into_iter().map(|topic_id| {
                let mut ps_client = ps_client.clone();
                async move {
                    let topic = ps_client
                        .topic(topic_id)
                        .await
                        .wrap_err(eyre!("get topic {:?}", &topic_id))?;
                    if topic.is_none() {
                        bail!("Topic not found: {:?}", &topic_id);
                    }

                    let mut topic = topic.unwrap(); // checked above
                    Ok((
                        topic.id().to_owned(),
                        topic
                            .create_subscription(
                                topic_id,
                                SubscriptionConfig::default()
                                    .ack_deadline(chrono::Duration::seconds(15)),
                            )
                            .await
                            .wrap_err(eyre!("create_subscription {:?}", &topic_id))?,
                    ))
                }
            }))
            .await?
            .into_iter()
            .collect();

        Ok(PubSubChunnel {
            ps_client,
            subscriptions,
        })
    }
}

impl ChunnelConnection for PubSubChunnel {
    type Data = (String, String);

    fn send(
        &self,
        (topic_id, body): Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        let mut client = self.ps_client.clone();
        Box::pin(async move {
            // TODO cache topic handles?
            // This call basically just checks the topic exists, which could be done without the
            // call by directly calling Topic::new?
            let topic = client
                .topic(&topic_id)
                .await
                .wrap_err(eyre!("get topic {:?}", &topic_id))?;
            if topic.is_none() {
                bail!("Topic not found: {:?}", &topic_id);
            }

            let mut topic = topic.unwrap(); // checked above
            topic
                .publish(body.as_bytes())
                .await
                .wrap_err(eyre!("Send on topic: {:?}", &topic_id))
        })
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        let mut subs = self.subscriptions.clone();
        Box::pin(async move {
            let futs = subs.iter_mut().map(|(topic, sub)| {
                Box::pin(async move {
                    let msg = sub
                        .receive()
                        .await
                        .ok_or_else(|| eyre!("Should not receive None"))?;
                    Ok::<_, Report>((topic.clone(), msg))
                })
            });

            let ((topic, mut msg), _) = futures_util::future::select_ok(futs).await?;
            msg.ack()
                .await
                .wrap_err(eyre!("ACKing message {:?}", msg.id()))?;
            let body = std::str::from_utf8(msg.data())?.to_owned();
            Ok((topic, body))
        })
    }
}
