use bertha::{util::ProjectLeft, ChunnelConnection};
use color_eyre::eyre::{bail, eyre, Report};
use futures_util::stream::{iter, StreamExt, TryStreamExt};
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::str::FromStr;
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tracing::{debug, info_span, instrument};
use tracing_futures::Instrument;

#[derive(Clone, Copy, Debug)]
pub enum Mode {
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
                // strip off the optional 'g' at the end
                let g = g.trim_end_matches('g');
                let g: usize = g.parse()?;
                Ok(Mode::Ordered {
                    num_groups: if g == 0 { None } else { Some(g) },
                })
            }
            _ => bail!("Unkown mode {:?}", s),
        }
    }
}

#[instrument(skip(cn, addr))]
pub async fn do_best_effort_exp<A: Clone + Debug + PartialEq + Send + Sync + 'static>(
    cn: impl ChunnelConnection<Data = (A, Msg)> + Send + Sync + 'static,
    addr: A,
    num_reqs: usize,
    inter_request_ms: u64,
    num_receivers: usize,
    batch_size: usize,
) -> Result<(Vec<RecvdMsg>, Duration), Report>
where
    A: Clone + crate::SetGroup + Hash + Debug + Eq + Send + Sync + 'static,
{
    let cn = Arc::new(cn);
    let send_cn = ProjectLeft::new(addr.clone(), Arc::clone(&cn));
    let recv_cns = (0..num_receivers).map(move |_| Arc::clone(&cn));

    let start = std::time::Instant::now();
    let receive_handle = tokio::spawn(receive_reqs(start, num_reqs, batch_size, recv_cns, false));
    send_reqs(
        start,
        num_reqs,
        batch_size,
        Duration::from_millis(inter_request_ms),
        &[send_cn],
    )
    .await?;
    let (msgs, elapsed) = receive_handle.await??;
    let msgs = msgs.into_iter().map(|(_, m)| m).collect();
    Ok((msgs, elapsed))
}

#[instrument(skip(cn, addr))]
pub async fn do_atmostonce_exp<A>(
    cn: impl ChunnelConnection<Data = (A, Msg)> + Send + Sync + 'static,
    addr: A,
    num_reqs: usize,
    inter_request_ms: u64,
    num_receivers: usize,
    batch_size: usize,
) -> Result<(Vec<RecvdMsg>, Duration), Report>
where
    A: Clone + crate::SetGroup + Hash + Debug + Eq + Send + Sync + 'static,
{
    let inter_request = Duration::from_millis(inter_request_ms);
    let cn = Arc::new(cn);
    let send_cn = Arc::clone(&cn);
    let recv_cns = (0..num_receivers).map(move |_| Arc::clone(&cn));

    let start = std::time::Instant::now();
    let receive_handle = tokio::spawn(receive_reqs(start, num_reqs, batch_size, recv_cns, false));

    // our local version of send_reqs, which uses a unique ordering key
    debug!("starting sends");
    let cn = send_cn;
    let num_batches = num_reqs / batch_size + 1;
    let inter_request = inter_request * batch_size as _;
    for batch_num in 0..num_batches {
        debug!(?batch_num, "sending batch");
        cn.send_batch((0..batch_size).map(|i| {
            let mut addr = addr.clone();
            let req_num = batch_num * batch_size + i;
            addr.set_group(req_num.to_string());

            (
                addr,
                Msg {
                    send_time: start.elapsed(),
                    req_num,
                },
            )
        }))
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
pub async fn do_ordered_groups_exp<A>(
    cn: impl ChunnelConnection<Data = (A, Msg)> + Send + Sync + 'static,
    addr: A,
    num_reqs: usize,
    num_groups: usize,
    inter_request_ms: u64,
    num_receivers: usize,
    batch_size: usize,
) -> Result<(Vec<RecvdMsg>, Duration), Report>
where
    A: Clone + crate::SetGroup + Hash + Debug + Eq + Send + Sync + 'static,
{
    let cn = Arc::new(cn);
    let send_cns: Vec<_> = iter(0..num_groups)
        .then(|i| {
            let mut a = addr.clone();
            let cn = &cn;
            async move {
                a.set_group(i.to_string());
                Ok::<_, Report>(ProjectLeft::new(a, Arc::clone(cn)))
            }
        })
        .try_collect()
        .await?;
    let recv_cns = (0..num_receivers).map(move |_| Arc::clone(&cn));

    let start = std::time::Instant::now();
    let receive_handle = tokio::spawn(receive_reqs(start, num_reqs, batch_size, recv_cns, true));
    send_reqs(
        start,
        num_reqs,
        batch_size,
        Duration::from_millis(inter_request_ms),
        &send_cns,
    )
    .await?;
    let (msgs, elapsed) = receive_handle.await??;
    let msgs = msgs.into_iter().map(|(_, m)| m).collect();
    Ok((msgs, elapsed))
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct Msg {
    pub send_time: Duration,
    pub req_num: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct RecvdMsg {
    pub send_time: Duration,
    pub elapsed: Duration,
    pub req_num: usize,
}

impl RecvdMsg {
    pub fn from_start(start: std::time::Instant, msg: Msg) -> Self {
        Self {
            send_time: msg.send_time,
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
    batch_size: usize,
    inter_request: Duration,
    send_chunnels: &[impl ChunnelConnection<Data = Msg> + Sync],
) -> Result<(), Report> {
    debug!("starting sends");
    let mut curr_group: usize = 0;
    let num_batches = num_reqs / batch_size + 1;
    let inter_request = inter_request * batch_size as _;
    for batch_num in 0..num_batches {
        debug!(?curr_group, ?batch_num, "sending batch");
        send_chunnels[curr_group]
            .send_batch((0..batch_size).map(|i| Msg {
                send_time: start.elapsed(),
                req_num: batch_num * batch_size + i,
            }))
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
#[instrument(skip(start, receive_chunnels))]
async fn receive_reqs<A>(
    start: std::time::Instant,
    num_reqs: usize,
    batch_size: usize,
    receive_chunnels: impl IntoIterator<Item = impl ChunnelConnection<Data = (A, Msg)> + Sync>,
    check_ordering: bool,
) -> Result<(Vec<(A, RecvdMsg)>, Duration), Report>
where
    A: Clone + crate::SetGroup + Hash + Debug + Eq + Send + Sync + 'static,
{
    let tot_rcvd = Arc::new(AtomicUsize::new(0));
    let first_recv_time = Arc::new(AtomicU64::new(0));
    let groups = futures_util::future::try_join_all(receive_chunnels.into_iter().enumerate().map(
        |(i, rch)| {
            let rcvd = Arc::clone(&tot_rcvd);
            let first_time = Arc::clone(&first_recv_time);
            let mut msgs = Vec::new();
            let mut groups = HashMap::new();
            async move {
                let mut f = rch.recv_batch(batch_size);
                loop {
                    tokio::select!(
                        r = &mut f => {
                            let recvd_batch: Result<Vec<_>, _> = r?.into_iter().map(|(a, msg)| {
                                // check order
                                if check_ordering {
                                    let old_num = groups.entry(a.clone()).or_insert(msg.req_num);
                                    if *old_num <= msg.req_num {
                                        *old_num = msg.req_num;
                                    } else {
                                        let on = *old_num; // so we can end the borrow of groups
                                        return Err(eyre!(
                                           "Group order semantics not met in group {:?}: {:?} > {:?}: {:?}", 
                                           a, on, msg.req_num, groups
                                        ));
                                    }
                                }

                                Ok((a, RecvdMsg::from_start(start, msg)))
                            }).collect();
                            let recvd_batch = recvd_batch?;
                            if msgs.is_empty() {
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

                            // fetch_add returns the old value, so we re-apply the increment locally.
                            let tot = rcvd.fetch_add(recvd_batch.len(), Ordering::SeqCst) + recvd_batch.len();
                            debug!(batch_start = ?recvd_batch[0].1.req_num, batch_end=?recvd_batch[recvd_batch.len()-1].1.req_num, ?tot, local = ?msgs.len(), "got batch");
                            msgs.extend(recvd_batch);

                            if tot >= num_reqs {
                                return Ok::<_, Report>(msgs);
                            }

                            // get a new recv future.
                            f = rch.recv_batch(batch_size);
                        }
                        _ = tokio::time::sleep(Duration::from_millis(100)) => {
                            // check tot
                            let tot = rcvd.load(Ordering::SeqCst);
                            if tot >= num_reqs {
                                return Ok::<_, Report>(msgs);
                            }

                            debug!(?tot, local=?msgs.len(), "No recv for 100ms");
                        }
                    )

                }
            }
            .instrument(info_span!("receive-reqs", num=?i))
        }
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

pub fn dump_results(
    path: std::path::PathBuf,
    msgs: Vec<RecvdMsg>,
    recv_span: Duration,
    mode: Mode,
    inter_request_ms: u64,
    provider: &str,
) -> Result<(), Report> {
    use std::io::prelude::*;
    let mut f = std::fs::File::create(path)?;
    writeln!(
        &mut f,
        "mode provider inter_request_ms num_msgs elapsed_us msg_send_time req_latency_us req_orderedness"
    )?;
    let num_msgs = msgs.len();
    for (i, m) in msgs.into_iter().enumerate() {
        let orderedness = ((i as isize) - (m.req_num as isize)).abs() as f32 / num_msgs as f32;
        writeln!(
            &mut f,
            "{} {} {} {} {} {} {} {}",
            mode,
            provider,
            inter_request_ms,
            num_msgs,
            recv_span.as_micros(),
            m.send_time.as_micros(),
            m.elapsed.as_micros(),
            orderedness,
        )?;
    }

    Ok(())
}
