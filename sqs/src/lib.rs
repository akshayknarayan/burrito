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
    pub fn new(sqs_client: SqsClient, recv_queue_urls: impl Iterator<Item = String>) -> Self {
        SqsChunnel {
            sqs_client,
            recv_queue_urls: recv_queue_urls.collect(),
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
            let mut futs = recv_queue_urls
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

            let (
                qid,
                Message {
                    body,
                    receipt_handle,
                    ..
                },
            ) = loop {
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
