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
use color_eyre::{
    eyre::{bail, ensure},
    Report,
};
use futures_util::stream::{iter, StreamExt, TryStreamExt};
use gcp_pubsub::{OrderedPubSubChunnel, PubSubAddr, PubSubChunnel};
use queue_steer::QueueAddr;
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
    let opt = Opt::from_args();
    info!(?opt, "starting");

    let (msgs, elapsed) = do_exp(&opt).await?;

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
        &opt.queue,
    )?;
    Ok(())
}

async fn do_exp(opt: &Opt) -> Result<(Vec<RecvdMsg>, Duration), Report> {
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
            .from_env_vars()
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
        Opt { .. } => gcp_pubsub::GcpCreds::default().from_env_vars().finish(),
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

    Ok(match opt.mode {
        Mode::BestEffort => match opt.queue {
            QueueAddr::Aws(ref a) => {
                let sqs_client = get_aws_client(&opt)?;
                let cn = SqsChunnel::new(sqs_client, once(a.as_str()));
                do_best_effort_exp(
                    cn,
                    SqsAddr {
                        queue_id: a.to_owned(),
                        group: None,
                    },
                    opt.num_reqs,
                    opt.inter_request_ms,
                )
                .await?
            }
            QueueAddr::Azure(ref a) => {
                let az_client = get_az_client(&opt)?;
                let cn = AzStorageQueueChunnel::new(az_client, once(a.as_str()));
                do_best_effort_exp(cn, a.to_string(), opt.num_reqs, opt.inter_request_ms).await?
            }
            QueueAddr::Gcp(ref a) => {
                let gcp_client = get_gcp_client(&opt).await?;
                let cn = PubSubChunnel::new(gcp_client, once(a.as_str())).await?;
                do_best_effort_exp(
                    cn,
                    PubSubAddr {
                        topic_id: a.to_string(),
                        group: None,
                    },
                    opt.num_reqs,
                    opt.inter_request_ms,
                )
                .await?
            }
        },
        Mode::Ordered {
            num_groups: Some(n), // n == 1: total ordering
        } => {
            // offloaded impls
            match opt.queue {
                QueueAddr::Azure(_) => unimplemented!("no offloaded azure ordering implementaion"),
                QueueAddr::Aws(ref a) => {
                    let sqs_client = get_aws_client(&opt)?;
                    let ch = OrderedSqsChunnel::new(sqs_client, once(a.as_str()))?;
                    do_ordered_groups_exp(
                        ch,
                        SqsAddr {
                            queue_id: a.to_owned(),
                            group: None,
                        },
                        opt.num_reqs,
                        n,
                        opt.inter_request_ms,
                    )
                    .await?
                }
                QueueAddr::Gcp(ref a) => {
                    let gcp_client = get_gcp_client(&opt).await?;
                    let ch = OrderedPubSubChunnel::new(gcp_client, once(a.as_str())).await?;
                    do_ordered_groups_exp(
                        ch,
                        PubSubAddr {
                            topic_id: a.to_string(),
                            group: None,
                        },
                        opt.num_reqs,
                        n,
                        opt.inter_request_ms,
                    )
                    .await?
                }
            }
        }
        // at-most-once mode.
        Mode::Ordered { num_groups: None } => {
            match opt.queue {
                QueueAddr::Azure(_) => unimplemented!("no offloaded azure ordering implementaion"),
                QueueAddr::Aws(ref a) => {
                    let sqs_client = sqs::default_sqs_client(); // us-east-1
                    let ch = OrderedSqsChunnel::new(sqs_client, once(a.as_str()))?;
                    do_ordered_groups_exp(
                        ch,
                        SqsAddr {
                            queue_id: a.to_owned(),
                            group: None,
                        },
                        opt.num_reqs,
                        n,
                        opt.inter_request_ms,
                    )
                    .await?
                }
                QueueAddr::Gcp(ref a) => {
                    let gcp_client = get_gcp_client(&opt).await?;
                    let ch = OrderedPubSubChunnel::new(gcp_client, once(a.as_str())).await?;
                    do_ordered_groups_exp(
                        ch,
                        PubSubAddr {
                            topic_id: a.to_string(),
                            group: None,
                        },
                        opt.num_reqs,
                        n,
                        opt.inter_request_ms,
                    )
                    .await?
                }
            }
        }
    })
}

#[instrument(skip(cn, addr))]
async fn do_atmostonce_exp<A>(
    cn: impl ChunnelConnection<Data = (A, String)> + Send + Sync + 'static,
    addr: A,
    num_reqs: usize,
    inter_request_ms: u64,
) -> Result<(Vec<RecvdMsg>, Duration), Report>
where
    A: Clone + queue_steer::SetGroup + Hash + Debug + Eq + Send + Sync + 'static,
{
    let cn = Arc::new(cn);
    let recv_cn = make_conn(cn).await?;
    let start = std::time::Instant::now();
    let receive_handle = tokio::spawn(receive_reqs(start, num_reqs, once(recv_cn)));

    // our local version of send_reqs, which uses a unique ordering key
    debug!("starting sends");
    let mut curr_group: usize = 0;
    for req_num in 0..num_reqs {
        trace!(?curr_group, ?req_num, "sending request");
        let mut addr = addr.clone();
        addr.set_group(req_num.to_string());
        cn.send((
            addr,
            Msg {
                send_time: start.elapsed(),
                req_num,
            },
        ))
        .await?;
        if inter_request.as_nanos() > 0 {
            tokio::time::sleep(inter_request).await;
        }
    }

    debug!("finished sends");

    let (msgs, elapsed) = receive_handle.await??;
    let msgs = msgs.into_iter().map(|(_, m)| m).collect();
    Ok((msgs, elapsed))
}

#[instrument(skip(cn, addr))]
async fn do_ordered_groups_exp<A>(
    cn: impl ChunnelConnection<Data = (A, String)> + Send + Sync + 'static,
    addr: A,
    num_reqs: usize,
    num_groups: usize,
    inter_request_ms: u64,
) -> Result<(Vec<RecvdMsg>, Duration), Report>
where
    A: Clone + queue_steer::SetGroup + Hash + Debug + Eq + Send + Sync + 'static,
{
    let cn = Arc::new(cn);
    let send_cns: Vec<_> = iter(0..num_groups)
        .then(|i| {
            let mut a = addr.clone();
            let cn = &cn;
            async move {
                a.set_group(i.to_string());
                Ok::<_, Report>(ProjectLeft::new(a, make_conn(Arc::clone(cn)).await?))
            }
        })
        .try_collect()
        .await?;
    let recv_cn = make_conn(cn).await?;

    let start = std::time::Instant::now();
    let receive_handle = tokio::spawn(receive_reqs(start, num_reqs, once(recv_cn)));
    send_reqs(
        start,
        num_reqs,
        Duration::from_millis(inter_request_ms),
        &send_cns,
    )
    .await?;
    let (msgs, elapsed) = receive_handle.await??;
    // check order
    let mut groups = HashMap::new();
    for (a, m) in msgs.iter() {
        let old_num = groups.entry(a).or_insert(m.req_num);
        ensure!(
            *old_num <= m.req_num,
            "Group order semantics not met: {:?}",
            msgs
        );
        *old_num = m.req_num;
    }
    let msgs = msgs.into_iter().map(|(_, m)| m).collect();
    Ok((msgs, elapsed))
}

#[instrument(skip(cn, addr))]
async fn do_best_effort_exp<A: Clone + Debug + PartialEq + Send + Sync + 'static>(
    cn: impl ChunnelConnection<Data = (A, String)> + Send + Sync + 'static,
    addr: A,
    num_reqs: usize,
    inter_request_ms: u64,
) -> Result<(Vec<RecvdMsg>, Duration), Report> {
    let cn = Arc::new(cn);
    let send_cn = ProjectLeft::new(addr.clone(), make_conn(Arc::clone(&cn)).await?);
    let recv_cn = make_conn(cn).await?;

    let start = std::time::Instant::now();
    let receive_handle = tokio::spawn(receive_reqs(start, num_reqs, once(recv_cn)));
    send_reqs(
        start,
        num_reqs,
        Duration::from_millis(inter_request_ms),
        &[send_cn],
    )
    .await?;
    let (msgs, elapsed) = receive_handle.await??;
    let msgs = msgs.into_iter().map(|(_, m)| m).collect();
    Ok((msgs, elapsed))
}

async fn make_conn<A: Send + Sync + 'static>(
    inner: impl ChunnelConnection<Data = (A, String)> + Send + Sync + 'static,
) -> Result<impl ChunnelConnection<Data = (A, Msg)>, Report> {
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
#[instrument(skip(send_chunnels, start))]
async fn send_reqs(
    start: std::time::Instant,
    num_reqs: usize,
    inter_request: Duration,
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
        curr_group = (curr_group + 1) % send_chunnels.len();
        if inter_request.as_nanos() > 0 {
            tokio::time::sleep(inter_request).await;
        }
    }

    debug!("finished sends");
    Ok(())
}

/// Each of `receive_chunnels` corresponds to one ordering group. We receive on all until a total
/// of `num_reqs` messages.
#[instrument(skip(receive_chunnels))]
async fn receive_reqs<A>(
    start: std::time::Instant,
    num_reqs: usize,
    receive_chunnels: impl IntoIterator<Item = impl ChunnelConnection<Data = (A, Msg)>>,
) -> Result<(Vec<(A, RecvdMsg)>, Duration), Report> {
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

impl std::fmt::Display for Mode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            Mode::BestEffort => f.write_str("BestEffort")?,
            Mode::Ordered {
                num_groups: Some(n),
            } => write!(f, "Ordered:{}", n)?,
            Mode::Ordered { num_groups: None } => f.write_str("AtMostOnce")?,
        };

        Ok(())
    }
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

fn dump_results(
    path: std::path::PathBuf,
    msgs: Vec<RecvdMsg>,
    recv_span: Duration,
    mode: Mode,
    inter_request_ms: u64,
    queue_addr: &QueueAddr,
) -> Result<(), Report> {
    let mut f = std::fs::File::create(path)?;
    writeln!(
        &mut f,
        "mode provider inter_request_ms num_msgs elapsed_us req_latency_us req_orderedness"
    )?;
    let num_msgs = msgs.len();
    for (i, m) in msgs.into_iter().enumerate() {
        let orderedness = ((i as isize) - (m.req_num as isize)).abs() as f32 / num_msgs as f32;
        writeln!(
            &mut f,
            "{} {} {} {} {} {} {}",
            mode,
            queue_addr.provider(),
            inter_request_ms,
            num_msgs,
            recv_span.as_micros(),
            m.elapsed.as_micros(),
            orderedness,
        )?;
    }

    Ok(())
}
