use color_eyre::eyre::{Report, WrapErr};
use kvstore::KvClient;
use kvstore_ycsb::{ops, Op};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use structopt::StructOpt;
use tracing::{debug, info, info_span, instrument, trace};
use tracing_error::ErrorLayer;
use tracing_futures::Instrument;
use tracing_subscriber::prelude::*;

#[derive(Debug, StructOpt)]
#[structopt(name = "kvclient")]
struct Opt {
    #[structopt(long)]
    addr: SocketAddr,

    #[structopt(long)]
    redis_addr: SocketAddr,

    #[structopt(short, long)]
    num_shards_thresh: Option<usize>,

    #[structopt(short, long)]
    connections_per_client: bool,

    #[structopt(short, long)]
    interarrival_client_micros: usize,

    #[structopt(long)]
    accesses: PathBuf,

    #[structopt(short, long)]
    burrito_root: Option<String>,

    #[structopt(short, long)]
    out_file: Option<PathBuf>,
}

#[tokio::main(core_threads = 8, max_threads = 32)]
async fn main() -> Result<(), Report> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(ErrorLayer::default())
        .init();

    color_eyre::install()?;
    let opt = Opt::from_args();

    info!("reading workload");
    let loads = ops(opt.accesses.with_extension("load"))?;
    let accesses = ops(opt.accesses)?;
    info!(num_ops = ?accesses.len(), "done reading workload");

    info!("make clients");
    let mut access_by_client = HashMap::default();
    for (cid, ops) in group_by_client(accesses).into_iter() {
        let client = KvClient::new_shardclient(opt.redis_addr, opt.addr)
            .instrument(info_span!("make kvclient", client_id = ?cid))
            .await
            .wrap_err("make KvClient")?;
        access_by_client.insert(cid, (client, ops));
    }

    let num_clients = access_by_client.len();
    let mut basic_client = KvClient::new_basicclient(opt.addr)
        .instrument(info_span!("make kvclient"))
        .await?;
    do_loads(&mut basic_client, loads)
        .instrument(info_span!("loads"))
        .await?;
    let (durs, time) = do_requests(access_by_client, opt.interarrival_client_micros as _).await?;

    // done
    write_results(
        durs,
        time,
        num_clients,
        opt.interarrival_client_micros,
        opt.out_file,
    );
    Ok(())
}

/// Issue a workload of requests, divided by client worker.
///
/// Each client issues its requests open-loop. So we get one future per client, resolving to a
/// Result<Vec<durations, _>>.
/// Terminate once the first client finishes, since the load characteristics would change
/// otherwise.
///
/// Have to measure from the time the request leaves the queue.
async fn do_requests<S>(
    access_by_client: HashMap<usize, (KvClient<S>, Vec<Op>)>,
    interarrival_micros: u64,
) -> Result<(Vec<Duration>, Duration), Report>
where
    S: bertha::ChunnelConnection<Data = kvstore::Msg> + Send + Sync + 'static,
{
    use futures_util::stream::{FuturesOrdered, FuturesUnordered, StreamExt, TryStreamExt};

    async fn time_req(
        cl: KvClient<impl bertha::ChunnelConnection<Data = kvstore::Msg> + Send + Sync + 'static>,
        op: Op,
    ) -> Result<Duration, Report> {
        let now = tokio::time::Instant::now();
        let _ycsb_result = op.exec(&cl).await?;
        Ok::<_, Report>(now.elapsed())
    }

    #[instrument(level = "info", skip(cl, ops, done))]
    async fn req_loop(
        cl: KvClient<impl bertha::ChunnelConnection<Data = kvstore::Msg> + Send + Sync + 'static>,
        mut ops: impl futures_util::stream::Stream<Item = (usize, Op)> + Unpin + 'static,
        done: tokio::sync::watch::Receiver<bool>,
        client_id: usize,
    ) -> Result<Vec<Duration>, Report> {
        let mut durs = vec![];
        let mut inflight = FuturesOrdered::new();

        debug!("starting");
        loop {
            tokio::select!(
                Some((remaining_cnt, o)) = ops.next() => {
                    if remaining_cnt > 0 {
                        inflight.push(time_req(cl.clone(), o));
                        trace!(remaining_cnt, inflight = inflight.len(), "new request");
                    } else {
                        info!(completed = durs.len(), "finished requests");
                        break;
                    }
                }
                Some(r) = inflight.next() => {
                    match r {
                        Ok(d) => {
                            trace!(inflight = inflight.len(), "request done");
                            durs.push(d);
                        }
                        Err(e) => {
                            debug!(err = ?e, "request failed");
                            return Err(e);
                        }
                    }
                }
            );

            // This can't be inside the select because then the else clause would never be
            // triggered.
            if *done.borrow() {
                debug!(completed = durs.len(), "stopping");
                break; // the first client finished. stop.
            }
        }

        Ok::<_, Report>(durs)
    }

    let (done_tx, done_rx) = tokio::sync::watch::channel::<bool>(false);
    let mut reqs: FuturesUnordered<_> = access_by_client
        .into_iter()
        .map(move |(client_id, (cl, ops))| {
            assert!(!ops.is_empty());
            let ops = paced_ops_stream(ops, interarrival_micros, client_id);
            req_loop(cl, ops, done_rx.clone(), client_id)
        })
        .collect();

    let access_start = tokio::time::Instant::now();
    // do the accesses until the first client is done.
    let mut durs: Vec<_> = reqs.try_next().await?.expect("durs");
    assert!(!durs.is_empty());
    let access_end = access_start.elapsed();
    if !reqs.is_empty() {
        info!("broadcasting done");
        done_tx
            .broadcast(true)
            .wrap_err("failed to broadcast experiment termination")?;

        // collect all the requests that have completed.
        let rest_durs: Vec<Vec<_>> = reqs.try_collect().await?;
        assert!(!rest_durs.is_empty());
        info!("all clients reported");
        durs.extend(rest_durs.into_iter().flat_map(|x| x.into_iter()));
    }

    Ok((durs, access_end))
}

fn paced_ops_stream(
    ops: Vec<Op>,
    interarrival_micros: u64,
    client_id: usize,
) -> impl futures_util::stream::Stream<Item = (usize, Op)> {
    let len = ops.len();
    use futures_util::stream::StreamExt;
    //let mut ops = tokio::time::interval(Duration::from_micros(interarrival_micros as u64))
    //use async_timer as hrtimer;
    //let tkr = hrtimer::interval(Duration::from_micros(interarrival_micros as u64));
    let tkr = poisson_ticker::SpinTicker::new_with_log_id(
        Duration::from_micros(interarrival_micros as u64),
        client_id,
    )
    .zip(futures_util::stream::iter((0..len).rev()));
    tkr.zip(futures_util::stream::iter(ops))
        .map(|((_, i), o)| (i, o))
}

async fn do_loads<C>(cl: &mut KvClient<C>, loads: Vec<Op>) -> Result<(), Report>
where
    C: bertha::ChunnelConnection<Data = kvstore::Msg> + Send + Sync + 'static,
{
    info!("starting");
    // don't need to time the loads.
    for o in loads {
        trace!("starting load");
        match o {
            Op::Get(_, k) => cl.get(k).await?,
            Op::Update(_, k, v) => cl.update(k, v).await?,
        };

        trace!("finished load");
    }

    info!("done");
    Ok(())
}

// accesses group_by client
fn group_by_client(accesses: Vec<Op>) -> HashMap<usize, Vec<Op>> {
    let mut access_by_client: HashMap<usize, Vec<Op>> = Default::default();
    for o in accesses {
        let k = o.client_id();
        access_by_client.entry(k).or_default().push(o);
    }

    access_by_client
}

fn write_results(
    mut durs: Vec<Duration>,
    time: Duration,
    num_clients: usize,
    interarrival_client_micros: usize,
    out_file: Option<PathBuf>,
) {
    durs.sort();
    let len = durs.len() as f64;
    let quantile_idxs = [0.25, 0.5, 0.75, 0.95];
    let quantiles: Vec<_> = quantile_idxs
        .iter()
        .map(|q| (len * q) as usize)
        .map(|i| durs[i])
        .collect();
    let num = durs.len() as f64;
    let achieved_load_req_per_sec = num / time.as_secs_f64();
    let offered_load_req_per_sec = num_clients as f64 / (interarrival_client_micros as f64 / 1e6);
    info!(
        num = ?&durs.len(), elapsed = ?time, ?achieved_load_req_per_sec, ?offered_load_req_per_sec,
        min = ?durs[0], p25 = ?quantiles[0], p50 = ?quantiles[1],
        p75 = ?quantiles[2], p95 = ?quantiles[3], max = ?durs[durs.len() - 1],
        "Did accesses"
    );

    if let Some(f) = out_file {
        let mut f = std::fs::File::create(f).expect("Open out file");
        use std::io::Write;
        writeln!(&mut f, "Interarrival_us NumOps Completion_ms Latency_us").expect("write");
        let len = durs.len();
        for d in durs {
            writeln!(
                &mut f,
                "{} {} {} {}",
                interarrival_client_micros,
                len,
                time.as_millis(),
                d.as_micros()
            )
            .expect("write");
        }
    }
}
