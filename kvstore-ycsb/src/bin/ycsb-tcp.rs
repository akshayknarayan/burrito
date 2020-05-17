use kvstore::Msg;
use kvstore_ycsb::{ops, Op};
use rand_distr::{Distribution, Exp};
use std::collections::HashMap;
use std::error::Error;
use std::io::Read;
use std::sync::Arc;
use structopt::StructOpt;
use tracing::{debug, info, trace};

type StdError = Box<dyn Error + Send + Sync + 'static>;

#[derive(Debug, StructOpt)]
#[structopt(name = "kvclient-sync-tcp")]
struct Opt {
    #[structopt(short, long)]
    burrito_root: Option<String>,

    #[structopt(long)]
    addr: String,

    #[structopt(short, long)]
    num_shards_thresh: Option<usize>,

    #[structopt(long)]
    accesses: std::path::PathBuf,

    #[structopt(short, long)]
    out_file: Option<std::path::PathBuf>,

    #[structopt(short, long)]
    interarrival_client_micros: usize,
}

fn main() -> Result<(), StdError> {
    let opt = Opt::from_args();

    tracing_subscriber::fmt::init();

    debug!(file = ?&opt.accesses.with_extension("load"), "reading workload");
    let loads = ops(opt.accesses.with_extension("load"))?;
    debug!(file = ?&opt.accesses, "reading workload");
    let accesses = ops(opt.accesses)?;
    debug!(num_ops = ?accesses.len(), "done reading workload");

    let cls = {
        let root = opt.burrito_root.as_ref().unwrap().clone();
        debug!(root = ?&root, "Burrito mode");

        let addr = opt.addr;

        let mut rt = tokio::runtime::Runtime::new()?;
        let si = rt.block_on(async {
            let mut shardctl = match burrito_shard_ctl::ShardCtlClient::new(&root).await {
                Ok(s) => s,
                Err(e) => Err(format!("Could not contact ShardCtl: err = {}", e))?,
            };
            let si = if let Ok(mut dcl) =
                burrito_discovery_ctl::client::DiscoveryClient::new(&root).await
            {
                shardctl.query_recursive(&mut dcl, addr.parse()?).await?
            } else {
                debug!("Could not contact discovery-ctl");
                shardctl.query_shard(addr.parse()?).await?
            };

            Ok::<_, StdError>(si)
        })?;

        debug!(info = ?&si, "Queried shard");

        use burrito_shard_ctl::Shard;
        let addrs = match si {
            Shard::Addr(a) => vec![a],
            Shard::Sharded(si) => {
                let mut addrs = vec![si.canonical_addr];

                // decide managed_sharding or not.
                // Some(0): client-always
                // Some(x): server until x, after that client
                // None: server-always
                match opt.num_shards_thresh {
                    Some(thresh) if thresh < si.shard_addrs.len() => {
                        // we will manage the sharding ourselves
                        addrs.extend(
                            si.shard_addrs
                                .into_iter()
                                .map(|s| match s {
                                    Shard::Addr(a) => a,
                                    Shard::Sharded(c) => c.canonical_addr,
                                })
                                .into_iter(),
                        );
                    }
                    _ => (),
                };

                addrs
            }
        };

        debug!(addrs = ?&addrs, "Decided sharding plan");

        // make clients
        let mut cls = Vec::with_capacity(addrs.len());
        for a in addrs {
            cls.push(match a {
                burrito_shard_ctl::proto::Addr::Tcp(sa) => sa,
                _ => unimplemented!(),
            });
        }

        cls
    };

    let (mut durs, time) = do_exp(cls, opt.interarrival_client_micros, loads, accesses)?;

    // done
    durs.sort();
    let len = durs.len() as f64;
    let quantile_idxs = [0.25, 0.5, 0.75, 0.95];
    let quantiles: Vec<_> = quantile_idxs
        .iter()
        .map(|q| (len * q) as usize)
        .map(|i| durs[i])
        .collect();
    info!(
        num = ?&durs.len(), elapsed = ?time, min = ?durs[0],
        p25 = ?quantiles[0], p50 = ?quantiles[1], p75 = ?quantiles[2],
        p95 = ?quantiles[3], max = ?durs[durs.len() - 1],
        "Did accesses"
    );

    if let Some(f) = opt.out_file {
        let mut f = std::fs::File::create(f).expect("Open out file");
        use std::io::Write;
        write!(&mut f, "Interarrival_us NumOps Completion_ms Latency_us\n").expect("write");
        let len = durs.len();
        for d in durs {
            write!(
                &mut f,
                "{} {} {} {}\n",
                opt.interarrival_client_micros,
                len,
                time.as_millis(),
                d.as_micros()
            )
            .expect("write");
        }
    }

    Ok(())
}

fn do_exp(
    shard_addrs: Vec<std::net::SocketAddr>,
    interarrival_micros: usize,
    loads: Vec<Op>,
    accesses: Vec<Op>,
) -> Result<(Vec<std::time::Duration>, std::time::Duration), StdError> {
    if shard_addrs.is_empty() {
        Err(String::from("Need at least one client."))?;
    } else if shard_addrs.len() < 3 {
        // passing only one address means no sharding, which is equivalent to server sharding.
        // two addresses is a sharder and one shard, which is the same thing.
        let mut addrs = shard_addrs;
        return managed_sharding(addrs.swap_remove(0), interarrival_micros, loads, accesses);
    };

    // client-side sharding
    unimplemented!();
}

fn managed_sharding(
    a: std::net::SocketAddr,
    interarrival_micros: usize,
    loads: Vec<Op>,
    accesses: Vec<Op>,
) -> Result<(Vec<std::time::Duration>, std::time::Duration), StdError> {
    let mut cl = std::net::TcpStream::connect(a)?;
    cl.set_read_timeout(Some(std::time::Duration::from_secs(10)))?;
    cl.set_nodelay(true)?;
    do_loads(&mut cl, loads)?;

    let wg = crossbeam::sync::WaitGroup::new();
    let (s, r) = crossbeam::channel::unbounded();

    let start = std::time::Instant::now();
    for (id, ops) in group_by_client(accesses) {
        let wg = wg.clone();
        let s = s.clone();

        let cl = std::net::TcpStream::connect(a)?;
        cl.set_read_timeout(Some(std::time::Duration::from_secs(10)))?;
        cl.set_nodelay(true)?;

        use chashmap::CHashMap;
        let num_ops = ops.len();
        let reqs: Arc<CHashMap<usize, Inflight>> = Arc::new(CHashMap::with_capacity(num_ops));

        // send paced requests
        let send_cl = cl.try_clone()?;
        let send_reqs = Arc::clone(&reqs);
        std::thread::spawn(move || {
            let mut cl = send_cl;
            let reqs = send_reqs;

            let mean_ns: f64 = interarrival_micros as f64 * 1e3;
            let lambda = 1. / mean_ns;
            let r = Exp::new(lambda).expect("Make exponential distr");
            let mut rng = rand::thread_rng();

            info!(id, "send starting");

            for o in ops {
                let mut next_interarrival_ns = r.sample(&mut rng) as u64;
                if next_interarrival_ns == 0 {
                    next_interarrival_ns = 1;
                }

                let next_interarrival = std::time::Duration::from_nanos(next_interarrival_ns);
                let next_req_start = std::time::Instant::now() + next_interarrival;
                let sleep_time = std::time::Duration::from_micros(25);
                while std::time::Instant::now() < next_req_start - sleep_time {
                    std::thread::sleep(sleep_time);
                }

                // send the request
                let req = match o {
                    Op::Get(_, k) => Msg::get_req(k),
                    Op::Update(_, k, v) => Msg::put_req(k, v),
                };

                trace!(id, req_id = req.id(), "sending request");
                let mut c = bincode::config();
                let c = c.limit(u32::max_value() as u64);
                let c = c.big_endian();
                let msg_len = c.serialized_size(&req)? as u32;
                c.serialize_into(&mut cl, &msg_len)?;
                bincode::serialize_into(&mut cl, &req)?;

                reqs.insert(req.id(), Inflight::new(req.id()));
            }

            info!(id, "sending done");
            Ok::<_, StdError>(())
        });

        // receive responses
        let recv_cl = cl.try_clone()?;
        let recv_reqs = Arc::clone(&reqs);
        std::thread::spawn(move || {
            let mut cl = recv_cl;
            let reqs = recv_reqs;
            let mut buf = [0u8; 1024];
            let mut c = bincode::config();
            let c = c.limit(u32::max_value() as u64);
            let c = c.big_endian();

            let mut durs = Vec::with_capacity(num_ops);
            debug!(id, num_ops, "receving");
            while durs.len() < num_ops {
                cl.read(&mut buf[0..4])?;
                let resp_len: u32 = c.deserialize(&buf[0..4])?;
                let read_len = cl.read(&mut buf[0..resp_len as usize])?;
                let resp: Msg = bincode::deserialize(&mut buf[0..read_len])?;
                trace!(id, resp_id = resp.id(), "got response");

                let mut m = reqs.remove(&resp.id()).unwrap();
                m.finish();
                trace!(id, req_id = resp.id(), dur = ?m.dur(), "got response");
                durs.push(m.dur());
            }

            info!(id, "client done");
            std::mem::drop(wg);

            s.send(durs)?;
            Ok::<_, StdError>(())
        });
    }

    std::mem::drop(s);
    wg.wait();
    let cls: Vec<Vec<std::time::Duration>> = r.into_iter().collect();
    let durs: Vec<std::time::Duration> = cls.into_iter().flat_map(|x| x).collect();
    Ok((durs, start.elapsed()))
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

fn do_loads(cl: &mut std::net::TcpStream, loads: Vec<Op>) -> Result<(), StdError> {
    debug!("starting loads");
    // don't need to time or pace the loads.
    for o in loads {
        //std::thread::sleep(std::time::Duration::from_millis(10));
        trace!("starting load");
        do_op(cl, 0, o)?;
        trace!("finished load");
    }
    debug!("finished loads");

    Ok(())
}

fn do_op(mut cl: &mut std::net::TcpStream, id: usize, o: Op) -> Result<(), StdError> {
    let req = match o {
        Op::Get(_, k) => Msg::get_req(k),
        Op::Update(_, k, v) => Msg::put_req(k, v),
    };

    trace!(id, req_id = req.id(), "sending request");
    let mut c = bincode::config();
    let c = c.limit(u32::max_value() as u64);
    let c = c.big_endian();
    let msg_len = c.serialized_size(&req)? as u32;
    c.serialize_into(&mut cl, &msg_len)?;
    bincode::serialize_into(&mut cl, &req)?;
    trace!(id, req_id = req.id(), "wait for response");
    let mut buf = [0u8; 1024];
    cl.read(&mut buf[0..4])?;
    let resp_len: u32 = c.deserialize(&buf[0..4])?;
    trace!(id, req_id = req.id(), resp_len, "response len");
    let read_len = cl.read(&mut buf[0..resp_len as usize])?;
    let resp: Msg = bincode::deserialize(&mut buf[0..read_len])?;
    trace!(id, req_id = req.id(), "got response");
    if resp.id() != req.id() {
        Err(format!(
            "Msg ids don't match: {} != {}",
            resp.id(),
            req.id()
        ))?;
    }

    Ok(())
}

#[derive(Debug, Clone)]
enum Inflight {
    Sent(usize, std::time::Instant),
    Done(usize, std::time::Duration),
}

impl Inflight {
    fn new(id: usize) -> Self {
        Self::Sent(id, std::time::Instant::now())
    }

    //fn is_finished(&self) -> bool {
    //    match self {
    //        Self::Done(_, _) => true,
    //        _ => false,
    //    }
    //}

    fn dur(&self) -> std::time::Duration {
        match self {
            Inflight::Done(_, t) => *t,
            _ => unreachable!(),
        }
    }

    fn finish(&mut self) {
        let s = match self {
            Inflight::Sent(i, s) => Inflight::Done(*i, s.elapsed()),
            _ => self.clone(),
        };

        *self = s;
    }
}
