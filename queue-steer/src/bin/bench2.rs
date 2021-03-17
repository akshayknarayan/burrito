//! Measure message throughput, latency, and ordered-ness for an experiment [`Mode`].
//!
//! This program is both the producer and the consumer (in different threads).
//! The chunnel stacks it benchmarks should support the (addr, data) = (QueueAddr, String)
//! datatype.

use az_queues::{AsStorageClient, AzStorageQueueChunnel};
use bertha::{
    bincode::{Base64Chunnel, SerializeChunnelProject},
    Chunnel, CxList,
};
use color_eyre::Report;
use gcp_pubsub::{PubSubAddr, PubSubChunnel};
use queue_steer::bin_help::{
    do_atmostonce_exp, do_best_effort_exp, do_ordered_groups_exp, dump_results, Mode,
};
use queue_steer::{AtMostOnce, BaseQueue, Ordered, QueueAddr};
use sqs::{SqsAddr, SqsChunnel};
use std::fmt::Debug;
use std::iter::once;
use std::sync::Arc;
use structopt::StructOpt;
use tracing::{debug, info};
use tracing_error::ErrorLayer;
use tracing_subscriber::prelude::*;

#[derive(Clone, Debug, StructOpt)]
struct Opt {
    #[structopt(short, long)]
    mode: Mode,
    #[structopt(short, long)]
    num_reqs: usize,
    #[structopt(short, long)]
    queue: Option<String>,
    #[structopt(short, long)]
    inter_request_ms: u64,
    #[structopt(short, long)]
    out_file: std::path::PathBuf,

    #[structopt(subcommand)]
    provider: Provider,
}

#[derive(Debug, Clone, StructOpt)]
pub enum Provider {
    Aws {
        #[structopt(long)]
        aws_access_key_id: String,
        #[structopt(long)]
        aws_secret_access_key: String,
    },
    Azure {
        #[structopt(long)]
        az_account_name: String,
        #[structopt(long)]
        az_key: String,
    },
    Gcp {
        #[structopt(long)]
        gcp_key_file: std::path::PathBuf,
        #[structopt(long)]
        gcp_project_name: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Report> {
    color_eyre::install()?;
    let subscriber = tracing_subscriber::registry();
    let subscriber = subscriber
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(ErrorLayer::default());
    let d = tracing::Dispatch::new(subscriber);
    d.init();
    let Opt {
        mode,
        num_reqs,
        inter_request_ms,
        out_file,
        queue,
        provider,
    } = Opt::from_args();
    info!(?mode, ?num_reqs, ?inter_request_ms, provider = ?provider.provider(), "starting");
    let (ch, addr, provider_cleanup) = provider.into_base_queue(queue).await?;

    let (msgs, elapsed) = match mode {
        Mode::BestEffort => {
            let mut stack =
                CxList::from(SerializeChunnelProject::default()).wrap(Base64Chunnel::default());
            let ch = stack.connect_wrap(ch).await?;
            do_best_effort_exp(ch, addr.clone(), num_reqs, inter_request_ms).await?
        }
        Mode::Ordered { num_groups: None } => {
            // at most once.
            let mut stack = CxList::from(AtMostOnce::default())
                .wrap(SerializeChunnelProject::default())
                .wrap(Base64Chunnel::default());
            let ch = stack.connect_wrap(ch).await?;
            //let _ch: Box<
            //    dyn ChunnelConnection<Data = (QueueAddr, (u32, queue_steer::bin_help::Msg))>,
            //> = Box::new(ch);
            do_atmostonce_exp(ch, addr.clone(), num_reqs, inter_request_ms).await?
        }
        Mode::Ordered {
            num_groups: Some(n),
        } => {
            // ordered, with potentially many groups
            let mut stack = CxList::from(Ordered::default())
                .wrap(SerializeChunnelProject::default())
                .wrap(Base64Chunnel::default());
            let ch = stack.connect_wrap(ch).await?;
            do_ordered_groups_exp(ch, addr.clone(), num_reqs, n, inter_request_ms).await?
        }
    };

    if let Some(cleanup) = provider_cleanup {
        cleanup.cleanup().await?;
    }

    let mut latencies: Vec<_> = msgs.iter().map(|m| m.elapsed).collect();
    latencies.sort_unstable();
    let len = latencies.len() as f64;
    let quantile_idxs = [0.25, 0.5, 0.75, 0.95];
    let quantiles: Vec<_> = quantile_idxs
        .iter()
        .map(|q| (len * q) as usize)
        .map(|i| latencies[i])
        .collect();

    info!(
        ?elapsed,
        ?num_reqs,
        p25 = ?quantiles[0],
        p50 = ?quantiles[1],
        p75 = ?quantiles[2],
        p95 = ?quantiles[3],
        "done",
    );

    dump_results(out_file, msgs, elapsed, mode, inter_request_ms, &addr)?;
    Ok(())
}

pub struct ProviderCleanup {
    queue: String,
    inner: ProviderCleanupInner,
}

impl ProviderCleanup {
    async fn cleanup(self) -> Result<(), Report> {
        use ProviderCleanupInner::*;
        let queue = self.queue;
        debug!(?queue, provider = ?self.inner.provider(), "deleting queue");
        match self.inner {
            Aws(c) => sqs::delete_queue(&c, queue).await,
            Azure(c) => az_queues::delete_queue(&c, queue).await,
            Gcp(mut c) => gcp_pubsub::delete_topic(&mut c, queue).await,
        }
    }
}

impl From<(String, sqs::SqsClient)> for ProviderCleanup {
    fn from((queue, inner): (String, sqs::SqsClient)) -> Self {
        Self {
            queue,
            inner: ProviderCleanupInner::Aws(inner),
        }
    }
}
impl From<(String, Arc<az_queues::StorageClient>)> for ProviderCleanup {
    fn from((queue, inner): (String, Arc<az_queues::StorageClient>)) -> Self {
        Self {
            queue,
            inner: ProviderCleanupInner::Azure(inner),
        }
    }
}
impl From<(String, gcp_pubsub::GcpClient)> for ProviderCleanup {
    fn from((queue, inner): (String, gcp_pubsub::GcpClient)) -> Self {
        Self {
            queue,
            inner: ProviderCleanupInner::Gcp(inner),
        }
    }
}

enum ProviderCleanupInner {
    Aws(sqs::SqsClient),
    Azure(Arc<az_queues::StorageClient>),
    Gcp(gcp_pubsub::GcpClient),
}

impl ProviderCleanupInner {
    fn provider(&self) -> &str {
        match self {
            ProviderCleanupInner::Aws(_) => "aws",
            ProviderCleanupInner::Azure(_) => "azure",
            ProviderCleanupInner::Gcp(_) => "gcp",
        }
    }
}

impl Provider {
    pub fn provider(&self) -> &str {
        match self {
            Provider::Aws { .. } => "aws",
            Provider::Azure { .. } => "azure",
            Provider::Gcp { .. } => "gcp",
        }
    }

    pub async fn into_base_queue(
        self,
        queue: Option<String>,
    ) -> Result<(BaseQueue, QueueAddr, Option<ProviderCleanup>), Report> {
        let mut generated = false;
        let queue: String = queue.unwrap_or_else(|| {
            generated = true;
            use rand::Rng;
            let rng = rand::thread_rng();
            "bertha-"
                .chars()
                .chain(
                    rng.sample_iter(&rand::distributions::Alphanumeric)
                        .take(10)
                        .flat_map(char::to_lowercase),
                )
                .collect()
        });

        use Provider::*;
        match self {
            Aws {
                aws_access_key_id,
                aws_secret_access_key,
            } => {
                let sqs_client =
                    sqs::sqs_client_from_creds(aws_access_key_id, aws_secret_access_key)?;
                let (queue, generated) = if generated {
                    // what if we want to optimize to a fifo queue?
                    let queue = sqs::make_be_queue(&sqs_client, queue.clone()).await?;
                    (queue.clone(), Some((queue, sqs_client.clone()).into()))
                } else {
                    (queue, None)
                };

                debug!(?queue, "AWS queue");
                let cn = SqsChunnel::new(sqs_client, once(queue.as_str()));
                Ok((
                    cn.into(),
                    QueueAddr::Aws(SqsAddr {
                        queue_id: queue,
                        group: None,
                    }),
                    generated,
                ))
            }
            Azure {
                az_account_name,
                az_key,
            } => {
                let az_client = az_queues::AzureAccountBuilder::default()
                    .with_name(az_account_name)
                    .with_key(az_key)
                    .finish()?;

                let generated = if generated {
                    az_queues::make_queue(&az_client.as_storage_client(), queue.clone()).await?;
                    Some((queue.clone(), az_client.as_storage_client()).into())
                } else {
                    None
                };

                debug!(?queue, "Azure queue");
                let cn = AzStorageQueueChunnel::new(az_client, once(queue.as_str()));
                Ok((cn.into(), QueueAddr::Azure(queue, String::new()), generated))
            }
            Gcp {
                gcp_key_file,
                gcp_project_name,
            } => {
                let mut gcp_client = gcp_pubsub::GcpCreds::default()
                    .with_creds_path(gcp_key_file)
                    .with_project_name(gcp_project_name)
                    .finish()
                    .await?;

                let (queue, generated) = if generated {
                    let queue = gcp_pubsub::make_topic(&mut gcp_client, queue.clone()).await?;
                    (queue.clone(), Some((queue, gcp_client.clone()).into()))
                } else {
                    (queue, None)
                };

                debug!(?queue, "GCP queue");
                let cn = PubSubChunnel::new(gcp_client, once(queue.as_str())).await?;
                Ok((
                    cn.into(),
                    QueueAddr::Gcp(PubSubAddr {
                        topic_id: queue,
                        group: None,
                    }),
                    generated,
                ))
            }
        }
    }
}
