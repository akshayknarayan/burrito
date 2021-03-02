//! Measure message throughput, latency, and ordered-ness for an experiment [`Mode`].
//!
//! This program is both the producer and the consumer (in different threads).
//! The chunnel stacks it benchmarks should support the (addr, data) = (String, String)
//! datatype. Any ordering semantics should be offered per-`ChunnelConnection`.

use az_queues::AzStorageQueueChunnel;
use bertha::{
    bincode::{Base64Chunnel, SerializeChunnelProject},
    util::ProjectLeft,
    Chunnel, ChunnelConnection, CxList,
};
use color_eyre::{eyre::bail, Report};
use gcp_pubsub::PubSubChunnel;
use queue_steer::QueueAddr;
use sqs::SqsChunnel;
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

#[derive(Clone, Copy, Debug)]
enum Mode {
    BestEffort,
    /// if num_groups = Some(1), all keys are in the same group, so there is total ordering.  
    ///
    /// if num_groups = Some(n > 1), number of client threads = number of ordering groups (measure
    /// effect on throughput).  
    ///
    /// if num_groups = None, each key is in its own ordering group (so there can be is
    /// at-most-once delivery without ordering).
    ///
    /// Some(0) is invalid.
    Ordered {
        num_groups: Option<usize>,
    },
}

impl FromStr for Mode {
    type Err = Report;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let sp: Vec<_> = s.split(':').collect();
        match sp.as_slice() {
            ["BestEffort"] | ["besteffort"] | ["be"] => Ok(Mode::BestEffort),
            ["Ordered", g] | ["ordered", g] | ["ord", g] => {
                let g: usize = g.parse()?;
                Ok(Mode::Ordered {
                    num_groups: if g == 0 { None } else { Some(g) },
                })
            }
            _ => bail!("Unkown mode {:?}", s),
        }
    }
}

#[derive(Clone, Debug, StructOpt)]
struct Opt {
    #[structopt(short, long)]
    mode: Mode,
    #[structopt(short, long)]
    num_reqs: usize,
    #[structopt(short, long)]
    queue: QueueAddr,
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
    let opt = Opt::from_args();
    info!(?opt, "starting");
    let (msgs, elapsed) = match opt.mode {
        Mode::BestEffort => {
            match opt.queue {
                QueueAddr::Aws(a) => {
                    let sqs_client = sqs::default_sqs_client(); // us-east-1
                    let send_cn = SqsChunnel::new(sqs_client, once(a.as_str()));
                    let sqs_client = sqs::default_sqs_client();
                    let recv_cn = SqsChunnel::new(sqs_client, once(a.as_str()));
                    do_best_effort_exp(send_cn, recv_cn, a, opt.num_reqs).await?
                }
                QueueAddr::Azure(a) => {
                    let az_client = az_queues::default_azure_storage_client()?;
                    let send_cn = AzStorageQueueChunnel::new(az_client, once(a.as_str()));
                    let az_client = az_queues::default_azure_storage_client()?;
                    let recv_cn = AzStorageQueueChunnel::new(az_client, once(a.as_str()));
                    do_best_effort_exp(send_cn, recv_cn, a, opt.num_reqs).await?
                }
                QueueAddr::Gcp(a) => {
                    let gcp_client = gcp_pubsub::default_gcloud_client().await?;
                    let send_cn = PubSubChunnel::new(gcp_client, once(a.as_str())).await?;
                    let gcp_client = gcp_pubsub::default_gcloud_client().await?;
                    let recv_cn = PubSubChunnel::new(gcp_client, once(a.as_str())).await?;
                    do_best_effort_exp(send_cn, recv_cn, a, opt.num_reqs).await?
                }
            }
        }
        Mode::Ordered { .. } => unimplemented!(),
    };

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
    Ok(())
}

#[instrument(skip(send_cn, recv_cn))]
async fn do_best_effort_exp(
    send_cn: impl ChunnelConnection<Data = (String, String)> + Send + Sync + 'static,
    recv_cn: impl ChunnelConnection<Data = (String, String)> + Send + Sync + 'static,
    addr: String,
    num_reqs: usize,
) -> Result<(Vec<RecvdMsg>, Duration), Report> {
    let send_cn = ProjectLeft::new(addr.clone(), make_conn(send_cn).await?);
    let recv_cn = make_conn(recv_cn).await?;

    let start = std::time::Instant::now();
    let receive_handle = tokio::spawn(receive_reqs(start, num_reqs, once(recv_cn)));
    send_reqs(start, num_reqs, &[send_cn]).await?;
    let (msgs, elapsed) = receive_handle.await??;
    if msgs.iter().any(|(from, _)| from != &addr) {
        bail!("Received from unexpected queue");
    }

    let msgs = msgs.into_iter().map(|(_, m)| m).collect();
    Ok((msgs, elapsed))
}

async fn make_conn(
    inner: impl ChunnelConnection<Data = (String, String)> + Send + Sync + 'static,
) -> Result<impl ChunnelConnection<Data = (String, Msg)>, Report> {
    let mut stack = CxList::from(SerializeChunnelProject::default()).wrap(Base64Chunnel::default());
    stack.connect_wrap(inner).await
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
struct Msg {
    send_time: Duration,
    req_num: usize,
}

#[derive(Debug, Clone, Copy)]
struct RecvdMsg {
    elapsed: Duration,
    req_num: usize,
}

impl RecvdMsg {
    fn from_start(start: std::time::Instant, msg: Msg) -> Self {
        Self {
            elapsed: start.elapsed() - msg.send_time,
            req_num: msg.req_num,
        }
    }
}

/// Each of the `send_chunnels` is an ordering group. Messages are distributed round-robin across
/// groups.
#[instrument(skip(send_chunnels))]
async fn send_reqs(
    start: std::time::Instant,
    num_reqs: usize,
    send_chunnels: &[impl ChunnelConnection<Data = Msg>],
) -> Result<(), Report> {
    debug!("starting sends");
    let mut curr_group: usize = 0;
    for req_num in 0..num_reqs {
        trace!(?curr_group, ?req_num, "sending request");
        send_chunnels[curr_group]
            .send(Msg {
                send_time: start.elapsed(),
                req_num,
            })
            .await?;
        curr_group = curr_group + 1 % send_chunnels.len();
    }

    debug!("finished sends");
    Ok(())
}

/// Each of `receive_chunnels` corresponds to one ordering group. We receive on all until a total
/// of `num_reqs` messages.
#[instrument(skip(receive_chunnels))]
async fn receive_reqs(
    start: std::time::Instant,
    num_reqs: usize,
    receive_chunnels: impl IntoIterator<Item = impl ChunnelConnection<Data = (String, Msg)>>,
) -> Result<(Vec<(String, RecvdMsg)>, Duration), Report> {
    let tot_rcvd = Arc::new(AtomicUsize::new(0));
    let first_recv_time = Arc::new(AtomicU64::new(0));
    let groups = futures_util::future::try_join_all(receive_chunnels.into_iter().enumerate().map(
        |(i, rch)| {
            let rcvd = Arc::clone(&tot_rcvd);
            let first_time = Arc::clone(&first_recv_time);
            let mut msgs = Vec::new();
            async move {
                loop {
                    let (a, msg) = rch.recv().await?;
                    msgs.push((a, RecvdMsg::from_start(start, msg)));
                    // fetch_add returns the old value, so we re-apply the +1 locally.
                    let tot = rcvd.fetch_add(1, Ordering::SeqCst) + 1;
                    trace!(num = ?msg.req_num, ?tot, local = ?msgs.len(), "got msg");

                    if msgs.len() == 1 {
                        let possible_first_time = start.elapsed();
                        match first_time.compare_exchange(
                            0,
                            possible_first_time.as_micros() as u64,
                            Ordering::SeqCst,
                            Ordering::Relaxed,
                        ) {
                            Ok(0) => {
                                debug!(recv_begin = ?&possible_first_time, "Receives started");
                            }
                            Ok(_) => unreachable!(),
                            Err(_) => (), // we're not the first.
                        }
                    }

                    if tot >= num_reqs {
                        return Ok::<_, Report>(msgs);
                    }
                }
            }
            .instrument(info_span!("receive thread", num=?i))
        },
    ))
    .await?;
    let first_recv_time = Duration::from_micros(first_recv_time.load(Ordering::SeqCst));
    let last_recv_time = start.elapsed();
    Ok((
        groups.into_iter().fold(vec![], |mut acc, x| {
            acc.extend(x);
            acc
        }),
        last_recv_time - first_recv_time,
    ))
}

/// How many places out of place was each element
fn orderedness(receive_order: &[usize]) -> usize {
    receive_order
        .iter()
        .enumerate()
        .map(|(i, req_num)| (i as isize - *req_num as isize).abs() as usize)
        .sum()
}
