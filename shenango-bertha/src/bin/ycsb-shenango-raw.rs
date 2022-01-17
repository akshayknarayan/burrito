use color_eyre::eyre::{eyre, Report, WrapErr};
use kvstore_ycsb::{ops, Op};
use shenango::sync::{Mutex, WaitGroup};
use shenango_bertha::{ChunnelConnection, KvClient, KvClientBuilder};
use std::collections::HashMap;
use std::net::SocketAddrV4;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use structopt::StructOpt;
use tracing::{debug, info, trace};

#[derive(Debug, StructOpt)]
#[structopt(name = "kvclient")]
struct Opt {
    #[structopt(long)]
    addr: SocketAddrV4,

    #[structopt(short, long)]
    interarrival_client_micros: usize,

    #[structopt(long)]
    poisson_arrivals: bool,

    #[structopt(long)]
    accesses: PathBuf,

    #[structopt(short, long)]
    shenango_config: PathBuf,

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

    shenango::runtime_init(
        opt.shenango_config.to_str().unwrap().to_owned(),
        move || {
            if !opt.skip_loads {
                let mut basic_client = KvClientBuilder::new(opt.addr).new_shardclient().unwrap();
                do_loads(&mut basic_client, loads).unwrap();
                if opt.loads_only {
                    info!("doing only loads, done");
                    return;
                }
            } else {
                info!("skipping loads");
            }

            info!(mode = "shardclient", "make clients");

            let mut access_by_client = HashMap::default();
            for (cid, ops) in group_by_client(accesses).into_iter() {
                let cl = KvClientBuilder::new(opt.addr)
                    .new_shardclient()
                    .wrap_err("make KvClient")
                    .unwrap();
                access_by_client.insert(cid, (cl, ops));
            }
            let num_clients = access_by_client.len();
            let (durs, remaining_inflight, time) = do_requests(
                access_by_client,
                opt.interarrival_client_micros as _,
                opt.poisson_arrivals,
            )
            .unwrap();

            // done
            write_results(
                durs,
                remaining_inflight,
                time,
                num_clients,
                opt.interarrival_client_micros,
                opt.out_file,
            );

            std::process::exit(0);
        },
    )
    .unwrap();
    unreachable!()
}

use kvstore::Msg;

fn exec_op(
    op: Op,
    cl: &KvClient<impl ChunnelConnection<Data = Msg> + Send + Sync + 'static>,
) -> Result<Option<String>, Report> {
    match op {
        Op::Get(_, k) => cl.get(k),
        Op::Update(_, k, v) => cl.update(k, v),
    }
}

/// Issue a workload of requests, divided by client worker.
///
/// Each client issues its requests open-loop. So we get one future per client, resolving to a
/// Result<Vec<durations, _>>.
/// Terminate once the first client finishes, since the load characteristics would change
/// otherwise.
///
/// Have to measure from the time the request leaves the queue.
fn do_requests<S>(
    access_by_client: HashMap<usize, (KvClient<S>, Vec<Op>)>,
    interarrival_micros: u64,
    _poisson_arrivals: bool,
) -> Result<(Vec<Duration>, usize, Duration), Report>
where
    S: ChunnelConnection<Data = Msg> + Send + Sync + 'static,
{
    info!(?interarrival_micros, "starting requests");

    fn time_req(
        cl: &KvClient<impl ChunnelConnection<Data = Msg> + Send + Sync + 'static>,
        op: Op,
    ) -> Result<Duration, Report> {
        let then = shenango::microtime();
        let _ycsb_result = exec_op(op, cl)?;
        let now = shenango::microtime();
        Ok::<_, Report>(Duration::from_micros(now - then))
    }

    struct Timer {
        interarrival: Duration,
        deficit: Duration,
    }

    impl Timer {
        fn wait(&mut self) {
            let start = shenango::microtime();
            //let mut rng = rand::thread_rng();
            if self.deficit > self.interarrival {
                self.deficit -= self.interarrival;
                //trace!(deficit = ?self.deficit, "returning immediately from deficit");
                return;
            }

            //let next_interarrival_ns = self.distr.sample(&mut rng) as u64;
            let interarrival_us = self.interarrival.as_micros() as u64;
            let target = start + interarrival_us;

            loop {
                let now = shenango::microtime();
                if now >= target {
                    break;
                }

                if target - now > 10 {
                    shenango::sleep(Duration::from_micros(5));
                } else {
                    shenango::thread::thread_yield();
                }
            }

            let elapsed = shenango::microtime() - start;
            if elapsed > interarrival_us {
                self.deficit += Duration::from_micros(elapsed - interarrival_us);
            }

            //trace!(
            //    ?elapsed,
            //    ?self.deficit,
            //    sampled_wait_us = ?interarrival_us,
            //    "waited"
            //);
        }
    }

    fn req_loop(
        client_id: usize,
        cl: KvClient<impl ChunnelConnection<Data = kvstore::Msg> + Send + Sync + 'static>,
        ops: Vec<Op>,
        interarrival: Duration,
        done: Arc<AtomicBool>,
    ) -> Result<(Vec<Duration>, usize), Report> {
        let mut durs = vec![];
        debug!(?client_id, "starting");
        let paced_ops = Mutex::new(std::collections::VecDeque::new());

        // paced ops
        let paced_ops_putter = paced_ops.clone();
        let paced_ops_done = Arc::clone(&done);
        shenango::thread::spawn(move || {
            let done = paced_ops_done;
            let mut timer = Timer {
                interarrival,
                deficit: Duration::from_nanos(0),
            };
            for o in ops {
                if done.load(Ordering::SeqCst) {
                    break;
                }

                // hold the lock across the wait, so the other side ends up queueing on the mutex
                let mut putter_g = paced_ops_putter.lock();
                timer.wait();
                putter_g.push_back(o);
                //trace!(?client_id, num_pending_ops = ?putter_g.len(), "pushed op");
                // drop putter_g, releasing the lock
            }

            done.store(true, Ordering::SeqCst);
            debug!(?client_id, "ops done");
        });

        let concurrent_ops = WaitGroup::new();
        let durs = loop {
            if done.load(Ordering::SeqCst) {
                debug!(?client_id, "exiting");
                break Ok::<_, Report>(durs);
            }

            // get ops, but don't hold the lock.
            let now_ops: Vec<_> = paced_ops.lock().drain(..).collect();
            if now_ops.is_empty() {
                shenango::thread::thread_yield();
            } else {
                trace!(?client_id, num_now_ops = ?now_ops.len(), "drained ops");
                concurrent_ops.add(now_ops.len() as _);
                let op_batch: Vec<_> = now_ops
                    .into_iter()
                    .map(|o| {
                        let op_done = concurrent_ops.clone();
                        let cl = cl.clone();
                        shenango::thread::spawn(move || {
                            let r = time_req(&cl, o);
                            op_done.done();
                            r
                        })
                    })
                    .collect();

                concurrent_ops.wait();
                trace!(?client_id, "ops batch completed");
                for o in op_batch {
                    durs.push(o.join().unwrap()?);
                }
            }
        }?;

        Ok((durs, 0))
    }

    let wg = WaitGroup::new();
    let done = Arc::new(AtomicBool::default());
    let access_start = shenango::microtime();
    let clients: Vec<_> = access_by_client
        .into_iter()
        .map(|(client_id, (cl, ops))| {
            assert!(!ops.is_empty());
            let wg = wg.clone();
            wg.add(1);
            let done = Arc::clone(&done);
            shenango::thread::spawn(move || {
                let d = req_loop(
                    client_id,
                    cl,
                    ops,
                    Duration::from_micros(interarrival_micros),
                    done,
                );
                wg.done();
                d
            })
        })
        .collect();

    wg.wait();
    info!("requests done");
    let access_end = Duration::from_micros(shenango::microtime() - access_start);
    let mut all_durs = vec![];
    let mut remaining_inflight = 0;
    for c in clients {
        let (durs, remaining) = c.join().unwrap()?;
        assert!(!durs.is_empty());
        all_durs.extend(durs);
        remaining_inflight += remaining;
    }

    Ok((all_durs, remaining_inflight, access_end))
}

fn do_loads<C>(cl: &mut KvClient<C>, loads: Vec<Op>) -> Result<(), Report>
where
    C: ChunnelConnection<Data = Msg> + Send + Sync + 'static,
{
    info!("starting loads");
    // don't need to time the loads.
    for o in loads {
        trace!("starting load");
        match o {
            Op::Get(_, k) => cl.get(k)?,
            Op::Update(_, k, v) => cl.update(k, v)?,
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
