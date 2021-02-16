//! An AWS SQS wrapper for Bertha, allowing it to be used as transport for chunnels.
//! This can't impl `Chunnel` because it can't wrap another transport; it is the transport.
//!
//! Chunnel data type = (String, String) -> (queue URL, msg_string)

use bertha::ChunnelConnection;
use color_eyre::eyre::{eyre, Report, WrapErr};
use rusoto_sqs::{
    DeleteMessageRequest, Message, ReceiveMessageRequest, ReceiveMessageResult, SendMessageRequest,
    Sqs, SqsClient,
};
use std::future::Future;
use std::pin::Pin;

pub struct SqsChunnel {
    sqs_client: SqsClient,
    recv_queue_urls: Vec<String>,
}

impl SqsChunnel {
    pub fn new<'a>(
        sqs_client: SqsClient,
        recv_queue_urls: impl IntoIterator<Item = &'a str>,
    ) -> Self {
        SqsChunnel {
            sqs_client,
            recv_queue_urls: recv_queue_urls.into_iter().map(str::to_owned).collect(),
        }
    }
}

impl ChunnelConnection for SqsChunnel {
    type Data = (String, String);

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        let (qid, body) = data;
        let sqs = self.sqs_client.clone();
        Box::pin(async move {
            sqs.send_message(SendMessageRequest {
                message_body: body,
                queue_url: qid.clone(),
                ..Default::default()
            })
            .await
            .wrap_err(eyre!("sqs.send_message on {:?}", qid))?;
            Ok(())
        })
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        let sqs = self.sqs_client.clone();
        let recv_queue_urls = self.recv_queue_urls.clone();
        // How to receive an SQS message:
        // 1. call receive_message on the queue
        // 2. call has some timeout. if we got no message, goto 1.
        // 3. once we have the message, explicitly delete it. Otherwise, it will show up in future
        //    receive_message requests.
        Box::pin(async move {
            tokio::pin!(sqs);
            let mut futs = vec![];
            let (
                qid,
                Message {
                    body,
                    receipt_handle,
                    ..
                },
            ) = loop {
                if futs.is_empty() {
                    futs = recv_queue_urls
                        .iter()
                        .map(|q_url| {
                            let qu = q_url.to_string();
                            let sqs = &sqs; // don't move sqs, we need to use it in the other concurrent reqs
                            Box::pin(async move {
                                Ok::<_, Report>((
                                    sqs.receive_message(ReceiveMessageRequest {
                                        max_number_of_messages: Some(1),
                                        queue_url: qu.clone(),
                                        visibility_timeout: Some(15),
                                        wait_time_seconds: Some(1),
                                        ..Default::default()
                                    })
                                    .await
                                    .wrap_err(eyre!("sqs.receive_message on {:?}", qu))?,
                                    qu,
                                ))
                            })
                        })
                        .collect();
                }

                let (
                    (
                        ReceiveMessageResult {
                            messages: recvd_message,
                        },
                        qid,
                    ),
                    leftover,
                ) = futures_util::future::select_ok(futs).await?;

                match recvd_message {
                    None => {
                        // try again with the rest of the futs.
                        futs = leftover;
                        continue;
                    }
                    Some(msg) if msg.is_empty() => {
                        // try again with the rest of the futs.
                        futs = leftover;
                        continue;
                    }
                    Some(msg) if msg.len() > 1 => {
                        // this shouldn't be possible, we asked for a limit of 1
                        unreachable!("SQS receive_message limit exceeded");
                    }
                    Some(mut msg) => {
                        // done. can drop leftover. if the leftover futures happen to successfully
                        // resolve, that is fine, we're not going to clear them from the queue.
                        break (qid, msg.pop().unwrap());
                    }
                }
            };

            // delete the message that we got
            sqs.delete_message(DeleteMessageRequest {
                queue_url: qid.clone(),
                receipt_handle: receipt_handle.unwrap(),
            })
            .await
            .wrap_err(eyre!("sqs.delete_message on {:?}", qid))?;
            Ok((qid, body.unwrap()))
        })
    }
}

#[cfg(test)]
mod test {
    use super::SqsChunnel;
    use bertha::ChunnelConnection;
    use color_eyre::eyre::WrapErr;
    use color_eyre::Report;
    use rusoto_sqs::SqsClient;
    use tracing_error::ErrorLayer;
    use tracing_futures::Instrument;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    #[test]
    fn sqs_send_recv() {
        // relies on SQS queue "test" being available.
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
                let sqs_client = SqsClient::new(rusoto_core::Region::UsEast1);
                const TEST_QUEUE_URL: &'static str =
                    "https://sqs.us-east-1.amazonaws.com/413104736560/test";
                let ch = SqsChunnel::new(sqs_client, vec![TEST_QUEUE_URL]);

                ch.send((TEST_QUEUE_URL.to_string(), "test message".to_string()))
                    .await
                    .wrap_err("sqs send")?;
                let (q, msg) = ch.recv().await.wrap_err("sqs recv")?;
                assert_eq!(q, TEST_QUEUE_URL);
                assert_eq!(&msg, "test message");
                Ok::<_, Report>(())
            }
            .instrument(tracing::info_span!("sqs_send_recv")),
        )
        .unwrap();
    }
}
