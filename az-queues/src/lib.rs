//! An Azure Storage Queues wrapper for Bertha, allowing it to be used as transport for chunnels.
//! This can't impl `Chunnel` because it can't wrap another transport; it is the transport.
//!
//! Chunnel data type = (String, String) -> (queue URL, msg_string)

use azure_storage::clients::{AsStorageClient, StorageAccountClient, StorageClient};
use azure_storage::queue::{
    responses::{GetMessagesResponse, PutMessageResponse},
    AsPopReceiptClient, AsQueueClient,
};
use bertha::ChunnelConnection;
use color_eyre::eyre::{eyre, Report, WrapErr};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tracing::debug;

/// Uses account name and key.
///
/// For fancy config, call [`StorageAccountClient::new`], etc, yourself.
#[derive(Debug)]
pub struct AzureAccountBuilder {
    name: Result<String, Report>,
    key: Result<String, Report>,
}

impl Default for AzureAccountBuilder {
    fn default() -> Self {
        AzureAccountBuilder {
            name: Err(eyre!("Azure account name not specified")),
            key: Err(eyre!("Azure account key not specified")),
        }
    }
}

impl AzureAccountBuilder {
    pub fn with_name(self, name: impl Into<String>) -> Self {
        Self {
            name: Ok(name.into()),
            ..self
        }
    }

    pub fn with_key(self, key: impl Into<String>) -> Self {
        Self {
            key: Ok(key.into()),
            ..self
        }
    }

    /// Reads environment variable `AZ_STORAGE_ACCOUNT_NAME`.
    pub fn name_env_var(self) -> Self {
        Self {
            name: std::env::var("AZ_STORAGE_ACCOUNT_NAME")
                .wrap_err("AZ_STORAGE_ACCOUNT_NAME env var"),
            ..self
        }
    }

    /// Reads environment variable `AZ_STORAGE_ACCOUNT_KEY`.
    pub fn key_env_var(self) -> Self {
        Self {
            key: std::env::var("AZ_STORAGE_ACCOUNT_KEY").wrap_err("AZ_STORAGE_ACCOUNT_KEY env var"),
            ..self
        }
    }

    pub fn from_env_vars(self) -> Self {
        self.name_env_var().key_env_var()
    }

    pub fn finish(self) -> Result<Arc<StorageAccountClient>, Report> {
        Ok(StorageAccountClient::new_access_key(
            Arc::new(Box::new(reqwest::Client::new())),
            self.name?,
            self.key?,
        ))
    }
}

/// Get an Azure storage account client.
///
/// Requires environment variables `AZ_STORAGE_ACCOUNT_NAME` and `AZ_STORAGE_ACCOUNT_KEY` to be set.
/// For fancy config, call [`StorageAccountClient::new`], etc, yourself.
pub fn default_azure_storage_client() -> Result<Arc<StorageAccountClient>, Report> {
    AzureAccountBuilder::default().from_env_vars().finish()
}

#[derive(Clone)]
pub struct AzStorageQueueChunnel {
    az_client: Arc<StorageClient>,
    queues: Vec<String>,
}

impl AzStorageQueueChunnel {
    pub fn new<'a>(
        az_client: Arc<StorageAccountClient>,
        recv_queues: impl IntoIterator<Item = &'a str>,
    ) -> Self {
        let client = az_client.as_storage_client();
        AzStorageQueueChunnel {
            az_client: client,
            queues: recv_queues.into_iter().map(str::to_owned).collect(),
        }
    }
}

impl ChunnelConnection for AzStorageQueueChunnel {
    type Data = (String, String);

    fn send(
        &self,
        (queue_id, body): Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        let queue_client = self.az_client.as_queue_client(queue_id);
        Box::pin(async move {
            let PutMessageResponse { queue_message, .. } = queue_client
                .put_message()
                .execute(&body)
                .await
                .map_err(|e| eyre!("put_message err: {:?}", e))?;
            debug!(
                msg_id = ?&queue_message.message_id,
                insert_time = ?&queue_message.insertion_time,
                "inserted message"
            );
            Ok(())
        })
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        let queue_clients: Vec<_> = self
            .queues
            .iter()
            .map(|qid| self.az_client.as_queue_client(qid))
            .collect();
        Box::pin(async move {
            let mut futs = vec![];
            let (cl, msg) = loop {
                if futs.is_empty() {
                    futs = queue_clients
                        .iter()
                        .map(|cl| {
                            Box::pin(async move {
                                let GetMessagesResponse { messages, .. } = cl
                                    .get_messages()
                                    .visibility_timeout(std::time::Duration::from_secs(15))
                                    .number_of_messages(1) // TODO get more and cache them?
                                    .execute()
                                    .await
                                    .map_err(|e| eyre!("get_message err: {:?}", e))?;
                                Ok::<_, Report>((cl, messages))
                            })
                        })
                        .collect();
                }

                let ((cl, mut msg), leftover_futs) = futures_util::future::select_ok(futs).await?;
                if !msg.is_empty() {
                    assert!(
                        msg.len() == 1,
                        "Asked for only 1 message in GetMessagesRequest"
                    );
                    break ((cl, msg.pop().unwrap()));
                } else {
                    futs = leftover_futs;
                }
            };

            cl.as_pop_receipt_client(msg.pop_receipt)
                .delete()
                .execute()
                .await
                .map_err(|e| eyre!("delete_message err: {:?}", e))?;

            let body = msg.message_text;
            let recvd_from = cl.queue_name().to_owned();
            Ok((recvd_from, body))
        })
    }
}

#[cfg(test)]
mod test {
    use super::AzStorageQueueChunnel;
    use azure_storage::clients::StorageAccountClient;
    use bertha::ChunnelConnection;
    use color_eyre::eyre::WrapErr;
    use color_eyre::Report;
    use std::sync::Arc;
    use tracing_error::ErrorLayer;
    use tracing_futures::Instrument;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    #[test]
    fn azqueue_send_recv() {
        // relies on Azure queue "test-queue" being available.
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
                let account_name = std::env::var("AZ_STORAGE_ACCOUNT_NAME")
                    .wrap_err("AZ_STORAGE_ACCOUNT_NAME env var")?;
                let account_key = std::env::var("AZ_STORAGE_ACCOUNT_KEY")
                    .wrap_err("AZ_STORAGE_ACCOUNT_KEY env var")?;
                let az_client = StorageAccountClient::new_access_key(
                    Arc::new(Box::new(reqwest::Client::new())),
                    account_name,
                    account_key,
                );
                const TEST_QUEUE_URL: &'static str =
                    "https://berthaproject.queue.core.windows.net/test-queue";
                let ch = AzStorageQueueChunnel::new(az_client, vec![TEST_QUEUE_URL]);

                ch.send((TEST_QUEUE_URL.to_string(), "test message".to_string()))
                    .await
                    .wrap_err("az queue send")?;
                let (q, msg) = ch.recv().await.wrap_err("az queue recv")?;
                assert_eq!(q, TEST_QUEUE_URL);
                assert_eq!(&msg, "test message");
                Ok::<_, Report>(())
            }
            .instrument(tracing::info_span!("azqueue_send_recv")),
        )
        .unwrap();
    }
}
