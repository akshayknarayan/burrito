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

#[cfg(test)]
mod test {
    use super::PubSubChunnel;
    use bertha::ChunnelConnection;
    use color_eyre::eyre::WrapErr;
    use color_eyre::Report;
    use google_cloud::pubsub::Client;
    use tracing_error::ErrorLayer;
    use tracing_futures::Instrument;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    #[test]
    fn pubsub_send_recv() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        color_eyre::install().unwrap_or(());

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                let project_name =
                    std::env::var("GCLOUD_PROJECT_NAME").wrap_err("GCLOUD_PROJECT_NAME env var")?;
                let gcloud_client = Client::new(project_name).await.wrap_err("make client")?;
                const TEST_TOPIC_URL: &'static str = "my-topic"; // projects/arctic-plate-305119/topics/my-topic ?
                let ch = PubSubChunnel::new(gcloud_client, vec![TEST_TOPIC_URL]).await?;

                ch.send((TEST_TOPIC_URL.to_string(), "test message".to_string()))
                    .await
                    .wrap_err("sqs send")?;
                let (q, msg) = ch.recv().await.wrap_err("recv")?;
                assert_eq!(q, TEST_TOPIC_URL);
                assert_eq!(&msg, "test message");
                Ok::<_, Report>(())
            }
            .instrument(tracing::info_span!("pubsub_send_recv")),
        )
        .unwrap();
    }
}
