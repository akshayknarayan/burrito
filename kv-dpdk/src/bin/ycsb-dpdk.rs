use bertha::{util::ProjectLeft, ChunnelConnection, ChunnelConnector};
use color_eyre::{
    eyre::{ensure, eyre, WrapErr},
    Report,
};
use dpdk_direct::{DpdkInlineChunnel, DpdkInlineCn};
use flume::Sender;
use futures_util::{
    future::{select, Either},
    stream::{FuturesUnordered, Stream, StreamExt, TryStreamExt},
};
use kvstore::Msg;
use kvstore_ycsb::{
    const_paced_ops_stream, group_by_client, ops, poisson_paced_ops_stream, write_results, Op,
};
use std::collections::HashMap;
use std::net::{SocketAddr, SocketAddrV4};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use structopt::StructOpt;
use tracing::{debug, info, instrument, trace};

#[derive(Debug, StructOpt)]
#[structopt(name = "kvclient")]
struct Opt {
    #[structopt(long)]
    addr: SocketAddrV4,

    #[structopt(long)]
    num_shards: u16,

    #[structopt(long, default_value = "4")]
    num_threads: u16,

    #[structopt(short, long)]
    interarrival_client_micros: usize,

    #[structopt(long)]
    poisson_arrivals: bool,

    #[structopt(long)]
    accesses: PathBuf,

    #[structopt(short, long)]
    config: PathBuf,

    #[structopt(long)]
    loads_only: bool,

    #[structopt(long)]
    skip_loads: bool,

    #[structopt(short, long)]
    out_file: Option<PathBuf>,
}

fn main() -> Result<(), Report> {
    color_eyre::install()?;
    tracing_subscriber::fmt::init();
    let opt = Opt::from_args();

    if opt.loads_only && opt.skip_loads {
        return Err(eyre!("Must do either loads or skip them"));
    }

    info!("reading workload");
    let loads = ops(opt.accesses.with_extension("load")).wrap_err("Reading loads")?;
    let accesses = ops(opt.accesses.clone()).wrap_err("Reading accesses")?;
    info!(num_ops = ?accesses.len(), "done reading workload");

    let shard_addrs: Vec<_> = (0..opt.num_shards)
        .map(|p| SocketAddrV4::new(*opt.addr.ip(), opt.addr.port() + p + 1))
        .collect();
    let mut ch = DpdkInlineChunnel::new(opt.config, opt.num_threads as _)?;

    if !opt.skip_loads {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let c = &mut ch;
        let sa = shard_addrs.clone();
        rt.block_on(async move {
            let mut cl = RawKvClient::new(c, sa);
            do_loads(&mut cl, loads).await
        })?;
        if opt.loads_only {
            info!("doing only loads, done");
            return Ok(());
        }
    } else {
        info!("skipping loads");
    }

    info!(
        mode = "shardclient, no chunnels",
        ?shard_addrs,
        "make clients"
    );

    let ExpResult {
        durs,
        remaining_inflight,
        tot_time,
        num_clients,
    } = do_requests(
        ch,
        accesses,
        opt.num_threads as _,
        opt.interarrival_client_micros as _,
        opt.poisson_arrivals,
        shard_addrs,
    )
    .unwrap();

    // done
    write_results(
        durs,
        remaining_inflight,
        tot_time,
        num_clients,
        opt.interarrival_client_micros,
        opt.out_file,
    );

    Ok(())
}

struct ExpResult {
    durs: Vec<Duration>,
    remaining_inflight: usize,
    tot_time: Duration,
    num_clients: usize,
}

/// Issue a workload of requests, divided by client worker.
///
/// Each client issues its requests open-loop. So we get one future per client, resolving to a
/// Result<Vec<durations, _>>.
/// Terminate once the first client finishes, since the load characteristics would change
/// otherwise.
///
/// Have to measure from the time the request leaves the queue.
fn do_requests(
    ch: DpdkInlineChunnel,
    accesses: Vec<Op>, //HashMap<usize, (KvClient<S>, Vec<Op>)>,
    num_threads: usize,
    interarrival_micros: u64,
    poisson_arrivals: bool,
    shard_addrs: Vec<SocketAddrV4>,
) -> Result<ExpResult, Report> {
    let access_by_client = group_by_client(accesses);
    let num_clients = access_by_client.len();
    let mut access_by_thread = {
        let mut threads = vec![vec![]; num_threads];
        for (client_id, ops) in access_by_client {
            threads[client_id % num_threads].push((client_id, ops));
        }

        threads
    };

    info!(?interarrival_micros, "starting requests");
    let (start_tx, start_rx) = tokio::sync::watch::channel::<bool>(false);
    let (done_tx, done_rx) = tokio::sync::watch::channel::<bool>(false);
    let done_tx = Arc::new(done_tx);
    let mut threads = Vec::with_capacity(num_threads);
    for thread_id in 1..num_threads {
        let done_tx = done_tx.clone();
        let done_rx = done_rx.clone();
        let start_rx = start_rx.clone();
        let access_by_client = std::mem::take(&mut access_by_thread[thread_id]);
        let sa = shard_addrs.clone();
        let ch = ch.clone();
        let thread_jh = std::thread::spawn(move || {
            run_thread(
                thread_id,
                done_tx,
                done_rx,
                start_rx,
                access_by_client,
                interarrival_micros,
                poisson_arrivals,
                ch,
                sa,
            )
        });

        threads.push(thread_jh);
    }

    let access_start = tokio::time::Instant::now();
    start_tx.send(true).expect("Could not send start signal");
    // local thread
    let (mut durs, mut remaining_inflight) = run_thread(
        0,
        done_tx,
        done_rx,
        start_rx,
        std::mem::take(&mut access_by_thread[0]),
        interarrival_micros,
        poisson_arrivals,
        ch,
        shard_addrs,
    )?;

    for t in threads {
        match t.join() {
            Ok(thread_res) => {
                let (thread_durs, thread_remaining) = thread_res?;
                durs.extend(thread_durs);
                remaining_inflight += thread_remaining;
            }
            Err(err) => std::panic::resume_unwind(err),
        }
    }

    let access_end = access_start.elapsed();
    Ok(ExpResult {
        durs,
        remaining_inflight,
        tot_time: access_end,
        num_clients,
    })
}

async fn time_req(cl: &RawKvClient, op: Op) -> Result<Duration, Report> {
    let now = tokio::time::Instant::now();
    //let _ycsb_result = op.exec(&cl).await?;
    let _ycsb_result = match op {
        Op::Get(_, k) => cl.get(k).await,
        Op::Update(_, k, v) => cl.update(k, v).await,
    };
    Ok::<_, Report>(now.elapsed())
}

#[instrument(level = "info", skip(cl, ops, done), err)]
async fn req_loop(
    cl: RawKvClient,
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
                    inflight.push(time_req(&cl, o));
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

fn run_thread(
    thread_id: usize,
    done_tx: Arc<tokio::sync::watch::Sender<bool>>,
    done_rx: tokio::sync::watch::Receiver<bool>,
    mut start_rx: tokio::sync::watch::Receiver<bool>,
    access_by_client: Vec<(usize, Vec<Op>)>,
    interarrival_micros: u64,
    poisson_arrivals: bool,
    ch: DpdkInlineChunnel,
    shard_addrs: Vec<SocketAddrV4>,
) -> Result<(Vec<Duration>, usize), Report> {
    if access_by_client.is_empty() {
        return Ok((Vec::new(), 0));
    }

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async move {
        let mut reqs: FuturesUnordered<_> = access_by_client
            .into_iter()
            .map(|(client_id, ops)| {
                assert!(!ops.is_empty());
                let done_rx = done_rx.clone();
                let mut ch = ch.clone();
                let sa = shard_addrs.clone();
                async move {
                    let cl = RawKvClient::new(&mut ch, sa);
                    if poisson_arrivals {
                        let ops = poisson_paced_ops_stream(ops, interarrival_micros, client_id);
                        req_loop(cl, ops, done_rx.clone(), client_id).await
                    } else {
                        let ops = const_paced_ops_stream(ops, interarrival_micros, client_id);
                        req_loop(cl, ops, done_rx.clone(), client_id).await
                    }
                }
            })
            .collect();

        // wait for start signal
        start_rx.changed().await.expect("awaiting start signal");

        // do the accesses until the first client is done.
        let (mut durs, mut remaining_inflight) = reqs
            .try_next()
            .await
            .wrap_err("error driving request loop")?
            .expect("No clients?");
        ensure!(!durs.is_empty(), "No requests finished");
        if !reqs.is_empty() {
            info!(?thread_id, "broadcasting done");
            done_tx
                .send(true)
                .wrap_err("failed to broadcast experiment termination")?;

            // collect all the requests that have completed.
            let rest_durs: Vec<(_, _)> = reqs
                .try_collect()
                .await
                .wrap_err("error driving request loop")?;
            let (rest_durs, rest_left_inflight): (Vec<_>, Vec<_>) = rest_durs.into_iter().unzip();
            assert!(!rest_durs.is_empty());
            info!(?thread_id, "all clients reported");
            durs.extend(rest_durs.into_iter().flat_map(|x| x.into_iter()));
            remaining_inflight += rest_left_inflight.into_iter().sum::<usize>();
        }

        Ok((durs, remaining_inflight))
    })
}

async fn do_loads(cl: &mut RawKvClient, loads: Vec<Op>) -> Result<(), Report> {
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

#[derive(Clone)]
pub struct RawKvClient {
    inner: Vec<Arc<ProjectLeft<SocketAddr, DpdkInlineCn>>>,
    inflight: Arc<Mutex<HashMap<usize, Sender<Msg>>>>,
}

impl RawKvClient {
    pub fn new(ch: &mut DpdkInlineChunnel, shard_addrs: Vec<SocketAddrV4>) -> Self {
        RawKvClient {
            inner: shard_addrs
                .into_iter()
                .map(SocketAddr::V4)
                .map(|sa| ProjectLeft::new(sa, ch.connect(sa).into_inner().unwrap()))
                .map(Arc::new)
                .collect(),
            inflight: Default::default(),
        }
    }

    pub async fn update(&self, key: String, val: String) -> Result<Option<String>, Report> {
        let req = Msg::put_req(key, val);
        self.do_req(req).await
    }

    pub async fn get(&self, key: String) -> Result<Option<String>, Report> {
        let req = Msg::get_req(key);
        self.do_req(req).await
    }

    async fn do_req(&self, req: Msg) -> Result<Option<String>, Report> {
        let shard = shardfn(&req, self.inner.len());
        let cn = &self.inner[shard];
        let msg_id = req.id();
        let mut send_buf = vec![0u8; 2048];
        let sz = bincode::serialized_size(&req)? as usize;
        bincode::serialize_into(&mut send_buf[..sz], &req)?;
        let (s, r) = flume::bounded(1);

        cn.send(std::iter::once(send_buf.clone())).await?;

        self.inflight.lock().unwrap().insert(msg_id, s);
        let mut msgs_buf: Vec<_> = (0..16).map(|_| None).collect();

        loop {
            match select(
                Box::pin(tokio::time::timeout(
                    Duration::from_millis(20),
                    r.recv_async(),
                )),
                cn.recv(&mut msgs_buf),
            )
            .await
            {
                Either::Left((Ok(Ok(m)), _)) => {
                    // done.
                    return Ok(m.val());
                }
                // timed out
                Either::Left((Err(_), _)) => {
                    cn.send(std::iter::once(send_buf.clone())).await?;
                }
                // got messages.
                Either::Right((ms, _)) => {
                    let ms = ms?;
                    let mut inflight_g = self.inflight.lock().unwrap();
                    for buf in ms.iter_mut().map_while(Option::take) {
                        let msg: Msg = bincode::deserialize(&buf[..]).unwrap();
                        let id = msg.id();
                        inflight_g.remove(&id).unwrap().send(msg).unwrap();
                    }
                }
                // sender won't drop
                Either::Left((Ok(Err(_)), _)) => unreachable!(),
            }
        }
    }
}

fn shardfn(m: &Msg, num_shards: usize) -> usize {
    const FNV1_64_INIT: u64 = 0xcbf29ce484222325u64;
    const FNV_64_PRIME: u64 = 0x100000001b3u64;
    if num_shards == 0 {
        return 0;
    }

    let mut hash = FNV1_64_INIT;
    for b in m.key().as_bytes()[0..4].iter() {
        hash ^= *b as u64;
        hash = u64::wrapping_mul(hash, FNV_64_PRIME);
    }

    hash as usize % num_shards
}
