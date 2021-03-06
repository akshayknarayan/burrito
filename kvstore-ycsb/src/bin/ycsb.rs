use bertha::{ChunnelConnection, ChunnelConnector};
use color_eyre::eyre::{eyre, Report, WrapErr};
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
    interarrival_client_micros: usize,

    #[structopt(long)]
    accesses: PathBuf,

    #[structopt(short, long)]
    shenango_config: Option<PathBuf>,

    #[structopt(long)]
    use_basicclient: bool,

    #[structopt(long)]
    use_clientsharding: bool,

    #[structopt(short, long)]
    logging: bool,

    #[structopt(short, long)]
    tracing: bool,

    #[structopt(long)]
    loads_only: bool,

    #[structopt(long)]
    skip_loads: bool,

    #[structopt(short, long)]
    out_file: Option<PathBuf>,
}

#[cfg(feature = "use-shenango")]
fn get_raw_connector(
    path: Option<PathBuf>,
) -> Result<
    impl ChunnelConnector<
            Addr = (),
            Connection = impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)>,
            Error = impl Into<Report> + Send + Sync + 'static,
        > + Clone,
    Report,
> {
    let path = path
        .ok_or_else(|| eyre!("If shenango feature is enabled, shenango_cfg must be specified"))?;
    Ok(shenango_chunnel::ShenangoUdpSkChunnel::new(&path))
}

#[cfg(not(feature = "use-shenango"))]
fn get_raw_connector(
    p: Option<PathBuf>,
) -> Result<
    impl ChunnelConnector<
            Addr = (),
            Connection = impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)>,
            Error = impl Into<Report> + Send + Sync + 'static,
        > + Clone,
    Report,
> {
    if p.is_some() {
        tracing::warn!(cfg_file = ?p, "Shenango is disabled, ignoring config");
    }

    Ok(bertha::udp::UdpSkChunnel::default())
}

fn main() -> Result<(), Report> {
    let opt = Opt::from_args();
    let tracing = if opt.logging && opt.tracing {
        let timing_layer = tracing_timing::Builder::default()
            .no_span_recursion()
            .layer(|| tracing_timing::Histogram::new_with_max(100_000_000, 3).unwrap());
        let timing_downcaster = timing_layer.downcaster();
        let subscriber = tracing_subscriber::registry()
            .with(timing_layer)
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let d = tracing::Dispatch::new(subscriber);
        d.clone().init();
        color_eyre::install()?;
        Some((timing_downcaster, d))
    } else if opt.tracing {
        let timing_layer = tracing_timing::Builder::default()
            .no_span_recursion()
            .layer(|| tracing_timing::Histogram::new_with_max(100_000_000, 3).unwrap());
        let timing_downcaster = timing_layer.downcaster();
        let subscriber = tracing_subscriber::registry()
            .with(timing_layer)
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let d = tracing::Dispatch::new(subscriber);
        d.clone().init();
        color_eyre::install()?;
        Some((timing_downcaster, d))
    } else if opt.logging {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let d = tracing::Dispatch::new(subscriber);
        d.clone().init();
        color_eyre::install()?;
        None
    } else {
        None
    };

    if opt.loads_only && opt.skip_loads {
        return Err(eyre!("Must do either loads or skip them"));
    }

    info!("reading workload");
    let loads = ops(opt.accesses.with_extension("load")).wrap_err("Reading loads")?;
    let accesses = ops(opt.accesses.clone()).wrap_err("Reading accesses")?;
    info!(num_ops = ?accesses.len(), "done reading workload");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;
    let of = opt.out_file.clone();
    rt.block_on(async move {
        let ctr = get_raw_connector(opt.shenango_config)?;
        if !opt.skip_loads {
            let mut basic_client = KvClient::new_nonshardclient(
                ctr.clone().connect(()).await.map_err(Into::into)?,
                opt.addr,
            )
            .instrument(info_span!("make kvclient", client_id = "loads_client"))
            .await?;
            do_loads(&mut basic_client, loads)
                .instrument(info_span!("loads"))
                .await?;
            if opt.loads_only {
                info!("doing only loads, done");
                return Ok(());
            }
        } else {
            info!("skipping loads");
        }

        let (num_clients, (durs, remaining_inflight, time)) = if !opt.use_clientsharding {
            if !opt.use_basicclient {
                let mut access_by_client = HashMap::default();
                info!(mode = "nonshardclient", "make clients");
                for (cid, ops) in group_by_client(accesses).into_iter() {
                    let client = KvClient::new_nonshardclient(
                        ctr.clone().connect(()).await.map_err(Into::into)?,
                        opt.addr,
                    )
                    .instrument(info_span!("make kvclient", client_id = ?cid))
                    .await
                    .wrap_err("make KvClient")?;
                    access_by_client.insert(cid, (client, ops));
                }
                let num_clients = access_by_client.len();
                (
                    num_clients,
                    do_requests(access_by_client, opt.interarrival_client_micros as _).await?,
                )
            } else {
                let mut access_by_client = HashMap::default();
                info!(mode = "basicclient", "make clients");
                for (cid, ops) in group_by_client(accesses).into_iter() {
                    let client = KvClient::new_basicclient(
                        ctr.clone().connect(()).await.map_err(Into::into)?,
                        opt.addr,
                    )
                    .instrument(info_span!("make kvclient", client_id = ?cid))
                    .await
                    .wrap_err("make KvClient")?;
                    access_by_client.insert(cid, (client, ops));
                }
                let num_clients = access_by_client.len();
                (
                    num_clients,
                    do_requests(access_by_client, opt.interarrival_client_micros as _).await?,
                )
            }
        } else {
            let mut access_by_client = HashMap::default();
            info!(mode = "shardclient", "make clients");
            for (cid, ops) in group_by_client(accesses).into_iter() {
                let client = KvClient::new_shardclient(
                    ctr.clone().connect(()).await.map_err(Into::into)?,
                    opt.redis_addr,
                    opt.addr,
                )
                .instrument(info_span!("make kvclient", client_id = ?cid))
                .await
                .wrap_err("make KvClient")?;
                access_by_client.insert(cid, (client, ops));
            }
            let num_clients = access_by_client.len();
            (
                num_clients,
                do_requests(access_by_client, opt.interarrival_client_micros as _).await?,
            )
        };

        // done
        write_results(
            durs,
            remaining_inflight,
            time,
            num_clients,
            opt.interarrival_client_micros,
            opt.out_file,
        );

        Ok::<_, Report>(())
    })?;

    if let Some((td, d)) = tracing {
        if let Some(of) = of {
            let timing = td.downcast(&d).expect("downcast timing layer");
            let fname = of.with_extension("trace");
            let mut f = std::fs::File::create(&fname).unwrap();
            dump_tracing(&timing, &mut f)?;
        }
    }

    Ok(())
}

fn dump_tracing(
    timing: &'_ tracing_timing::TimingLayer,
    f: &mut std::fs::File,
) -> Result<(), Report> {
    use std::io::prelude::*;
    timing.force_synchronize();
    timing.with_histograms(|hs| {
        for (span_group, hs) in hs {
            for (event_group, h) in hs {
                let tag = format!("{}.{}", span_group, event_group);
                write!(
                    f,
                    "tracing: \
                        event = {}, \
                        min   = {}, \
                        p25   = {}, \
                        p50   = {}, \
                        p75   = {}, \
                        p95   = {}, \
                        max   = {}, \
                        cnt   = {} \
                        ",
                    &tag,
                    h.min(),
                    h.value_at_quantile(0.25),
                    h.value_at_quantile(0.5),
                    h.value_at_quantile(0.75),
                    h.value_at_quantile(0.95),
                    h.max(),
                    h.len(),
                )?;
            }
        }

        Ok::<_, Report>(())
    })?;

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
) -> Result<(Vec<Duration>, usize, Duration), Report>
where
    S: bertha::ChunnelConnection<Data = kvstore::Msg> + Send + Sync + 'static,
{
    use futures_util::{
        future::{select, Either},
        stream::{FuturesUnordered, Stream, StreamExt, TryStreamExt},
    };

    info!(?interarrival_micros, "starting requests");

    async fn time_req(
        cl: KvClient<impl bertha::ChunnelConnection<Data = kvstore::Msg> + Send + Sync + 'static>,
        op: Op,
    ) -> Result<Duration, Report> {
        let now = tokio::time::Instant::now();
        let _ycsb_result = op.exec(&cl).await?;
        Ok::<_, Report>(now.elapsed())
    }

    #[instrument(level = "info", skip(cl, ops, done), err)]
    async fn req_loop(
        cl: KvClient<impl bertha::ChunnelConnection<Data = kvstore::Msg> + Send + Sync + 'static>,
        mut ops: impl Stream<Item = (usize, Op)> + Unpin + Send + 'static,
        done: tokio::sync::watch::Receiver<bool>,
        client_id: usize,
    ) -> Result<(Vec<Duration>, usize), Report> {
        let mut durs = vec![];
        let mut inflight = FuturesUnordered::new();
        let mut arrv = std::time::Instant::now();

        debug!("starting");
        loop {
            // first check for a finished request.
            let ops_val = match select(ops.next(), inflight.next()).await {
                Either::Right((Some(Ok(d)), _)) => {
                    trace!(inflight = inflight.len(), "request done");
                    durs.push(d);
                    None
                }
                Either::Right((Some(Err(e)), _)) => return Err(e),
                Either::Right((None, f)) => Some(f.await),
                Either::Left((x, _)) => Some(x),
            };

            // if after the above, something happened in incoming request stream -- either the
            // stream directly yielded, or inflight gave us None and we then waited for a request -- then handle that.
            if let Some(ov) = ops_val {
                match ov {
                    Some((remaining_cnt, o)) if remaining_cnt > 0 => {
                        inflight.push(time_req(cl.clone(), o));
                        let interarrv = arrv.elapsed();
                        arrv = std::time::Instant::now();
                        trace!(
                            remaining_cnt,
                            inflight = inflight.len(),
                            ?interarrv,
                            "new request"
                        );
                    }
                    _ => {
                        info!(completed = durs.len(), "finished requests");
                        break;
                    }
                }
            }

            // This can't be inside the select because then the else clause would never be
            // triggered.
            if *done.borrow() {
                debug!(completed = durs.len(), "stopping");
                break; // the first client finished. stop.
            }
        }

        Ok::<_, Report>((durs, inflight.len()))
    }

    let (done_tx, done_rx) = tokio::sync::watch::channel::<bool>(false);
    let mut reqs: FuturesUnordered<_> = access_by_client
        .into_iter()
        .map(move |(client_id, (cl, ops))| {
            assert!(!ops.is_empty());
            let ops = paced_ops_stream(ops, interarrival_micros, client_id);
            let jh = tokio::spawn(req_loop(cl, ops, done_rx.clone(), client_id));
            async move { jh.await.unwrap() }
        })
        .collect();

    let access_start = tokio::time::Instant::now();
    // do the accesses until the first client is done.
    let (mut durs, mut remaining_inflight) = reqs.try_next().await?.expect("durs");
    assert!(!durs.is_empty());
    let access_end = access_start.elapsed();
    if !reqs.is_empty() {
        info!("broadcasting done");
        done_tx
            .send(true)
            .wrap_err("failed to broadcast experiment termination")?;

        // collect all the requests that have completed.
        let rest_durs: Vec<(_, _)> = reqs.try_collect().await?;
        let (rest_durs, rest_left_inflight): (Vec<_>, Vec<_>) = rest_durs.into_iter().unzip();
        assert!(!rest_durs.is_empty());
        info!("all clients reported");
        durs.extend(rest_durs.into_iter().flat_map(|x| x.into_iter()));
        remaining_inflight += rest_left_inflight.into_iter().sum::<usize>();
    }

    Ok((durs, remaining_inflight, access_end))
}

fn paced_ops_stream(
    ops: Vec<Op>,
    interarrival_micros: u64,
    client_id: usize,
) -> impl futures_util::stream::Stream<Item = (usize, Op)> + Send {
    let len = ops.len();
    use futures_util::stream::StreamExt;
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
    remaining_inflight: usize,
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
    let achieved_load_req_per_sec = (num as f64) / time.as_secs_f64();
    let offered_load_req_per_sec = (num + remaining_inflight as f64) / time.as_secs_f64();
    let attempted_load_req_per_sec = num_clients as f64 / (interarrival_client_micros as f64 / 1e6);
    info!(
        num = ?&durs.len(), elapsed = ?time, ?remaining_inflight,
        ?achieved_load_req_per_sec, ?offered_load_req_per_sec, ?attempted_load_req_per_sec,
        min = ?durs[0], p25 = ?quantiles[0], p50 = ?quantiles[1],
        p75 = ?quantiles[2], p95 = ?quantiles[3], max = ?durs[durs.len() - 1],
        "Did accesses"
    );

    println!(
        "Did accesses:\
        num = {:?},\
        elapsed_sec = {:?},\
        remaining_inflight = {:?},\
        achieved_load_req_per_sec = {:?},\
        offered_load_req_per_sec = {:?},\
        attempted_load_req_per_sec = {:?},\
        min_us = {:?},\
        p25_us = {:?},\
        p50_us = {:?},\
        p75_us = {:?},\
        p95_us = {:?},\
        max_us = {:?}",
        durs.len(),
        time.as_secs_f64(),
        remaining_inflight,
        achieved_load_req_per_sec,
        offered_load_req_per_sec,
        attempted_load_req_per_sec,
        durs[0].as_micros(),
        quantiles[0].as_micros(),
        quantiles[1].as_micros(),
        quantiles[2].as_micros(),
        quantiles[3].as_micros(),
        durs[durs.len() - 1].as_micros(),
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
