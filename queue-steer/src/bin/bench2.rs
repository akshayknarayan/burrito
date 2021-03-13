//! Measure message throughput, latency, and ordered-ness for an experiment [`Mode`].
//!
//! This program is both the producer and the consumer (in different threads).
//! The chunnel stacks it benchmarks should support the (addr, data) = (QueueAddr, String)
//! datatype.

use az_queues::{AsStorageClient, AzStorageQueueChunnel};
use bertha::{
    bincode::{Base64Chunnel, SerializeChunnelProject},
    util::ProjectLeft,
    Chunnel, ChunnelConnection, CxList,
};
use color_eyre::{
    eyre::{bail, ensure, eyre},
    Report,
};
use futures_util::stream::{iter, StreamExt, TryStreamExt};
use gcp_pubsub::{OrderedPubSubChunnel, PubSubAddr, PubSubChunnel};
use queue_steer::bin_help::Mode;
use queue_steer::{BaseQueue, QueueAddr};
use sqs::{OrderedSqsChunnel, SqsAddr, SqsChunnel};
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::io::Write;
use std::iter::once;
use std::str::FromStr;
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use structopt::StructOpt;
use tracing::{debug, info, info_span, instrument, trace};
use tracing_error::ErrorLayer;
use tracing_futures::Instrument;
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
    let mut opt = Opt::from_args();

    info!(?opt, "starting");

    //let (msgs, elapsed) = do_exp(&mut opt).await?;

    //let order: Vec<_> = msgs.iter().map(|m| m.req_num).collect();

    //let mut latencies: Vec<_> = msgs.iter().map(|m| m.elapsed).collect();
    //latencies.sort_unstable();
    //let len = latencies.len() as f64;
    //let quantile_idxs = [0.25, 0.5, 0.75, 0.95];
    //let quantiles: Vec<_> = quantile_idxs
    //    .iter()
    //    .map(|q| (len * q) as usize)
    //    .map(|i| latencies[i])
    //    .collect();

    //info!(
    //    ?elapsed,
    //    ?opt.num_reqs,
    //    p25 = ?quantiles[0],
    //    p50 = ?quantiles[1],
    //    p75 = ?quantiles[2],
    //    p95 = ?quantiles[3],
    //    "done",
    //);

    //dump_results(
    //    opt.out_file,
    //    msgs,
    //    elapsed,
    //    opt.mode,
    //    opt.inter_request_ms,
    //    &opt.queue,
    //)?;
    Ok(())
}

async fn make_base_chunnel(addr: Provider) -> BaseQueue {
    unimplemented!()
}

impl Provider {
    pub fn provider(&self) -> &str {
        match self {
            Provider::Aws { .. } => "aws",
            Provider::Azure { .. } => "azure",
            Provider::Gcp { .. } => "gcp",
        }
    }

    pub async fn into_queue_addr(self, queue: Option<String>) -> Result<(QueueAddr, bool), Report> {
        let mut generated = false;
        let queue: String = queue.unwrap_or_else(|| {
            generated = true;
            use rand::Rng;
            let rng = rand::thread_rng();
            "bertha-"
                .chars()
                .chain(rng.sample_iter(&rand::distributions::Alphanumeric).take(10))
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
                let queue = if generated {
                    sqs::make_be_queue(&sqs_client, queue).await?
                } else {
                    queue
                };

                Ok((
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

                if generated {
                    az_queues::make_queue(&az_client.as_storage_client(), queue.clone()).await?;
                }

                Ok((QueueAddr::Azure(queue), generated))
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

                let queue = if generated {
                    gcp_pubsub::make_topic(&mut gcp_client, queue).await?
                } else {
                    queue
                };

                Ok((
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
