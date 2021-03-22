//! Measure message throughput, latency, and ordered-ness for an experiment [`Mode`].
//!
//! This program is both the producer and the consumer (in different threads).
//! The chunnel stacks it benchmarks should support the (addr, data) = (String, String)
//! datatype.

use az_queues::AzStorageQueueChunnel;
use bertha::{
    bincode::{Base64Chunnel, SerializeChunnelProject},
    Chunnel, ChunnelConnection, CxList,
};
use color_eyre::Report;
use gcp_pubsub::{OrderedPubSubChunnel, PubSubAddr, PubSubChunnel};
use queue_steer::bin_help::{
    do_atmostonce_exp, do_best_effort_exp, do_ordered_groups_exp, dump_results, Mode, Msg, RecvdMsg,
};
use queue_steer::QueueAddr;
use sqs::{OrderedSqsChunnel, SqsAddr, SqsChunnel};
use std::fmt::Debug;
use std::iter::once;
use std::time::Duration;
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
    queue: QueueAddr,
    #[structopt(short, long)]
    inter_request_ms: u64,
    #[structopt(short, long)]
    out_file: std::path::PathBuf,

    #[structopt(long)]
    gcp_key_file: Option<std::path::PathBuf>,
    #[structopt(long)]
    gcp_project_name: Option<String>,
    #[structopt(long)]
    az_account_name: Option<String>,
    #[structopt(long)]
    az_key: Option<String>,
    #[structopt(long)]
    aws_access_key_id: Option<String>,
    #[structopt(long)]
    aws_secret_access_key: Option<String>,
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
    let (msgs, elapsed) = do_exp(&mut opt).await?;

    let order: Vec<_> = msgs.iter().map(|m| m.req_num).collect();
    let orderedness_val = orderedness(&order);
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
        ?opt.num_reqs,
        p25 = ?quantiles[0],
        p50 = ?quantiles[1],
        p75 = ?quantiles[2],
        p95 = ?quantiles[3],
        ?orderedness_val,
        "done",
    );

    dump_results(
        opt.out_file,
        msgs,
        elapsed,
        opt.mode,
        opt.inter_request_ms,
        &opt.queue.provider(),
    )?;
    Ok(())
}

async fn do_exp(opt: &mut Opt) -> Result<(Vec<RecvdMsg>, Duration), Report> {
    let get_az_client = |opt: &Opt| match opt {
        Opt {
            az_account_name: Some(name),
            az_key: Some(key),
            ..
        } => az_queues::AzureAccountBuilder::default()
            .with_name(name)
            .with_key(key)
            .finish(),
        Opt { .. } => az_queues::AzureAccountBuilder::default()
            .with_env_vars()
            .finish(),
    };

    let get_gcp_client = |opt: &Opt| match opt {
        Opt {
            gcp_key_file: Some(file),
            gcp_project_name: Some(name),
            ..
        } => gcp_pubsub::GcpCreds::default()
            .with_creds_path(file)
            .with_project_name(name)
            .finish(),
        Opt { .. } => gcp_pubsub::GcpCreds::default().with_env_vars().finish(),
    };

    let get_aws_client = |opt: &Opt| match opt {
        Opt {
            aws_access_key_id: Some(key_id),
            aws_secret_access_key: Some(secret),
            ..
        } => {
            debug!("Using provided AWS credentials");
            sqs::sqs_client_from_creds(key_id.clone(), secret.clone())
        }
        Opt { .. } => {
            debug!("Using default AWS credentials");
            Ok(sqs::default_sqs_client()) // us-east-1
        }
    };

    let o = opt.clone();
    Ok(match opt.mode {
        Mode::BestEffort => match opt.queue {
            QueueAddr::Aws(ref mut a) => {
                let mut delete_queue = false;
                let sqs_client = get_aws_client(&o)?;
                if a.queue_id == "gen" {
                    use rand::Rng;
                    let rng = rand::thread_rng();
                    let name = "bertha-"
                        .chars()
                        .chain(rng.sample_iter(&rand::distributions::Alphanumeric).take(10))
                        .collect();
                    debug!(?name, "making aws best-effort queue");
                    *a = SqsAddr {
                        queue_id: sqs::make_be_queue(&sqs_client, name).await?,
                        group: None,
                    };
                    delete_queue = true;
                }

                let cn = SqsChunnel::new(sqs_client, once(a.queue_id.as_str()));
                let r = do_best_effort_exp(
                    make_conn(cn).await?,
                    a.clone(),
                    opt.num_reqs,
                    opt.inter_request_ms,
                )
                .await;
                if delete_queue {
                    let sqs_client = get_aws_client(&o)?;
                    sqs::delete_queue(&sqs_client, a.queue_id.clone()).await?;
                }
                r?
            }
            QueueAddr::Azure(ref a, _) => {
                let az_client = get_az_client(&opt)?;
                let cn = AzStorageQueueChunnel::new(az_client, once(a.as_str()));
                do_best_effort_exp(
                    make_conn(cn).await?,
                    a.to_string(),
                    opt.num_reqs,
                    opt.inter_request_ms,
                )
                .await?
            }
            QueueAddr::Gcp(ref mut a) => {
                let mut delete_queue = false;
                let mut gcp_client = get_gcp_client(&o).await?;
                if a.topic_id == "gen" {
                    use rand::Rng;
                    let rng = rand::thread_rng();
                    let name = "bertha-"
                        .chars()
                        .chain(rng.sample_iter(&rand::distributions::Alphanumeric).take(10))
                        .collect();
                    debug!(?name, "making gcp best-effort queue");
                    *a = PubSubAddr {
                        topic_id: gcp_pubsub::make_topic(&mut gcp_client, name).await?,
                        group: None,
                    };
                    delete_queue = true;
                }

                let cn = PubSubChunnel::new(gcp_client, once(a.topic_id.as_str())).await?;
                let r = do_best_effort_exp(
                    make_conn(cn).await?,
                    a.clone(),
                    opt.num_reqs,
                    opt.inter_request_ms,
                )
                .await;
                if delete_queue {
                    let mut gcp_client = get_gcp_client(&o).await?;
                    gcp_pubsub::delete_topic(&mut gcp_client, a.topic_id.clone()).await?;
                }
                r?
            }
        },
        Mode::Ordered {
            num_groups: Some(n), // n == 1: total ordering
        } => {
            // offloaded impls
            match opt.queue {
                QueueAddr::Azure(_, _) => {
                    unimplemented!("no offloaded azure ordering implementaion")
                }
                QueueAddr::Aws(ref mut a) => {
                    let mut delete_queue = false;
                    let sqs_client = get_aws_client(&o)?;
                    if a.queue_id == "gen" {
                        use rand::Rng;
                        let rng = rand::thread_rng();
                        let name = "bertha-"
                            .chars()
                            .chain(rng.sample_iter(&rand::distributions::Alphanumeric).take(10))
                            .chain(".fifo".chars())
                            .collect();
                        debug!(?name, "making aws fifo queue");
                        *a = SqsAddr {
                            queue_id: sqs::make_fifo_queue(&sqs_client, name).await?,
                            group: None,
                        };
                        delete_queue = true;
                    }

                    let ch = OrderedSqsChunnel::new(sqs_client, once(a.queue_id.as_str()))?;
                    let r = do_ordered_groups_exp(
                        make_conn(ch).await?,
                        a.clone(),
                        opt.num_reqs,
                        n,
                        opt.inter_request_ms,
                    )
                    .await;
                    if delete_queue {
                        let sqs_client = get_aws_client(&o)?;
                        sqs::delete_queue(&sqs_client, a.queue_id.clone()).await?;
                    }
                    r?
                }
                QueueAddr::Gcp(ref mut a) => {
                    let mut delete_queue = false;
                    let mut gcp_client = get_gcp_client(&o).await?;
                    if a.topic_id == "gen" {
                        use rand::Rng;
                        let rng = rand::thread_rng();
                        let name = "bertha-"
                            .chars()
                            .chain(rng.sample_iter(&rand::distributions::Alphanumeric).take(10))
                            .collect();
                        debug!(?name, "making gcp ordered queue");
                        *a = PubSubAddr {
                            topic_id: gcp_pubsub::make_topic(&mut gcp_client, name).await?,
                            group: None,
                        };
                        delete_queue = true;
                    }
                    let ch =
                        OrderedPubSubChunnel::new(gcp_client, once(a.topic_id.as_str())).await?;
                    let r = do_ordered_groups_exp(
                        make_conn(ch).await?,
                        a.clone(),
                        opt.num_reqs,
                        n,
                        opt.inter_request_ms,
                    )
                    .await;
                    if delete_queue {
                        let mut gcp_client = get_gcp_client(&o).await?;
                        gcp_pubsub::delete_topic(&mut gcp_client, a.topic_id.clone()).await?;
                    }
                    r?
                }
            }
        }
        // at-most-once mode.
        Mode::Ordered { num_groups: None } => match opt.queue {
            QueueAddr::Azure(_, _) => unimplemented!("no offloaded azure ordering implementaion"),
            QueueAddr::Aws(ref mut a) => {
                let mut delete_queue = false;
                let sqs_client = get_aws_client(&o)?;
                if a.queue_id == "gen" {
                    use rand::Rng;
                    let rng = rand::thread_rng();
                    let name = "bertha-"
                        .chars()
                        .chain(rng.sample_iter(&rand::distributions::Alphanumeric).take(10))
                        .chain(".fifo".chars())
                        .collect();
                    debug!(?name, "making aws fifo queue");
                    *a = SqsAddr {
                        queue_id: sqs::make_fifo_queue(&sqs_client, name).await?,
                        group: None,
                    };
                    delete_queue = true;
                }

                let ch = OrderedSqsChunnel::new(sqs_client, once(a.queue_id.as_str()))?;
                let r = do_atmostonce_exp(
                    make_conn(ch).await?,
                    a.clone(),
                    opt.num_reqs,
                    opt.inter_request_ms,
                )
                .await;
                if delete_queue {
                    let sqs_client = get_aws_client(&o)?;
                    sqs::delete_queue(&sqs_client, a.queue_id.clone()).await?;
                }
                r?
            }
            QueueAddr::Gcp(ref mut a) => {
                let mut delete_queue = false;
                let mut gcp_client = get_gcp_client(&o).await?;
                if a.topic_id == "gen" {
                    use rand::Rng;
                    let rng = rand::thread_rng();
                    let name = "bertha-"
                        .chars()
                        .chain(rng.sample_iter(&rand::distributions::Alphanumeric).take(10))
                        .collect();
                    debug!(?name, "making gcp ordered queue");
                    *a = PubSubAddr {
                        topic_id: gcp_pubsub::make_topic(&mut gcp_client, name).await?,
                        group: None,
                    };
                    delete_queue = true;
                }
                let ch = OrderedPubSubChunnel::new(gcp_client, once(a.topic_id.as_str())).await?;
                let r = do_atmostonce_exp(
                    make_conn(ch).await?,
                    a.clone(),
                    opt.num_reqs,
                    opt.inter_request_ms,
                )
                .await;
                if delete_queue {
                    let mut gcp_client = get_gcp_client(&o).await?;
                    gcp_pubsub::delete_topic(&mut gcp_client, a.topic_id.clone()).await?;
                }
                r?
            }
        },
    })
}

/// How many places out of place was each element
fn orderedness(receive_order: &[usize]) -> usize {
    receive_order
        .iter()
        .enumerate()
        .map(|(i, req_num)| (i as isize - *req_num as isize).abs() as usize)
        .sum()
}

async fn make_conn<A: Send + Sync + 'static>(
    inner: impl ChunnelConnection<Data = (A, String)> + Send + Sync + 'static,
) -> Result<impl ChunnelConnection<Data = (A, Msg)>, Report> {
    let mut stack = CxList::from(SerializeChunnelProject::default()).wrap(Base64Chunnel::default());
    stack.connect_wrap(inner).await
}
