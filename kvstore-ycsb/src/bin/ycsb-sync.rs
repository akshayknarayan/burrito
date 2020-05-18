use kvstore::Msg;
use kvstore_ycsb::{ops, Op};
use rand_distr::{Distribution, Exp};
use std::collections::HashMap;
use std::error::Error;
use structopt::StructOpt;
use tracing::{debug, info, trace};

type StdError = Box<dyn Error + Send + Sync + 'static>;

const FNV1_64_INIT: u64 = 0xcbf29ce484222325u64;
const FNV_64_PRIME: u64 = 0x100000001b3u64;

#[derive(Debug, StructOpt)]
#[structopt(name = "kvclient-sync")]
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

    debug!("reading workload");
    let loads = ops(opt.accesses.with_extension("load"))?;
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
                burrito_shard_ctl::proto::Addr::Udp(sa) => sa,
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

    let mut cl = std::net::UdpSocket::bind("0.0.0.0:0")?;
    cl.set_read_timeout(Some(std::time::Duration::from_secs(10)))?;
    do_loads(&mut cl, shard_addrs[0], loads)?;

    // take off the canonical_addr
    let shard_addrs = shard_addrs[1..].to_vec();

    // now, measure the accesses.
    let num_shards = shard_addrs.len();
    let shard_fn = move |o: &Op| {
        /* xdp_shard version of FNV: take the first 4 bytes of the key
        * u64 hash = FNV1_64_INIT;
        * // ...
        * // value start
        * pkt_val = ((u8*) app_data) + offset;

        * // compute FNV hash
        * #pragma clang loop unroll(full)
        * for (i = 0; i < 4; i++) {
        *     hash = hash ^ ((u64) pkt_val[i]);
        *     hash *= FNV_64_PRIME;
        * }

        * // map to a shard and assign to that port.
        * idx = hash % shards->num;
        */
        let mut hash = FNV1_64_INIT;
        for b in o.key().as_bytes()[0..4].iter() {
            hash = hash ^ (*b as u64);
            hash = u64::wrapping_mul(hash, FNV_64_PRIME);
        }

        hash as usize % num_shards
    };

    let wg = crossbeam::sync::WaitGroup::new();
    let (s, r) = crossbeam::channel::unbounded();

    let start = std::time::Instant::now();
    for (id, ops) in group_by_client(accesses) {
        let wg = wg.clone();
        let s = s.clone();
        let shard_addrs = shard_addrs.clone();
        std::thread::spawn(move || {
            let res = (|| {
                let mean_ns: f64 = interarrival_micros as f64 * 1e3;
                let lambda = 1. / mean_ns;
                let r = Exp::new(lambda).expect("Make exponential distr");
                let mut rng = rand::thread_rng();

                info!(id, "client starting");
                let cls: Result<Vec<std::net::UdpSocket>, StdError> = shard_addrs
                    .iter()
                    .map(|_| {
                        let cl = std::net::UdpSocket::bind("0.0.0.0:0")?;
                        cl.set_read_timeout(Some(std::time::Duration::from_secs(10)))?;
                        Ok::<_, StdError>(cl)
                    })
                    .collect();
                let mut cls = cls?;
                let mut durs = Vec::with_capacity(ops.len());
                for o in ops {
                    let mut next_interarrival_ns = r.sample(&mut rng) as u64;
                    if next_interarrival_ns == 0 {
                        next_interarrival_ns = 1;
                    }

                    let next_interarrival = std::time::Duration::from_nanos(next_interarrival_ns);
                    let req_start = std::time::Instant::now();
                    let next_req_start = req_start + next_interarrival;

                    let shard_idx = shard_fn(&o);
                    let cl = &mut cls[shard_idx];
                    do_op(cl, shard_addrs[shard_idx], id, o)?;

                    let req_end = std::time::Instant::now();
                    let duration = req_end - req_start;
                    if req_end > next_req_start {
                        debug!(
                            id,
                            wanted_interarrival = ?next_interarrival,
                            duration = ?duration,
                            "Request took too long for next request interarrival"
                        );
                    } else {
                        let sleep_time = std::time::Duration::from_micros(50);
                        while std::time::Instant::now() < next_req_start - sleep_time {
                            std::thread::sleep(sleep_time);
                        }
                    }

                    durs.push(duration);
                }

                info!(id, "client done");
                Ok::<_, StdError>(durs)
            })();

            s.send(res).unwrap();
            std::mem::drop(wg);
        });
    }

    std::mem::drop(s);
    wg.wait();
    let cls: Result<Vec<Vec<std::time::Duration>>, StdError> = r.into_iter().collect();
    let durs: Vec<std::time::Duration> = cls?.into_iter().flat_map(|x| x).collect();
    Ok((durs, start.elapsed()))
}

fn managed_sharding(
    a: std::net::SocketAddr,
    interarrival_micros: usize,
    loads: Vec<Op>,
    accesses: Vec<Op>,
) -> Result<(Vec<std::time::Duration>, std::time::Duration), StdError> {
    let mut cl = std::net::UdpSocket::bind("0.0.0.0:0")?;
    cl.set_read_timeout(Some(std::time::Duration::from_secs(10)))?;
    do_loads(&mut cl, a, loads)?;

    let wg = crossbeam::sync::WaitGroup::new();
    let (s, r) = crossbeam::channel::unbounded();

    let start = std::time::Instant::now();
    for (id, ops) in group_by_client(accesses) {
        let wg = wg.clone();
        let s = s.clone();
        std::thread::spawn(move || {
            let res = (|| {
                let mean_ns: f64 = interarrival_micros as f64 * 1e3;
                let lambda = 1. / mean_ns;
                let r = Exp::new(lambda).expect("Make exponential distr");
                let mut rng = rand::thread_rng();

                info!(id, "client starting");
                let mut cl = std::net::UdpSocket::bind("0.0.0.0:0")?;
                cl.set_read_timeout(Some(std::time::Duration::from_secs(10)))?;

                let mut durs = Vec::with_capacity(ops.len());
                for o in ops {
                    let mut next_interarrival_ns = r.sample(&mut rng) as u64;
                    if next_interarrival_ns == 0 {
                        next_interarrival_ns = 1;
                    }

                    let next_interarrival = std::time::Duration::from_nanos(next_interarrival_ns);
                    let req_start = std::time::Instant::now();
                    let next_req_start = req_start + next_interarrival;

                    do_op(&mut cl, a, id, o)?;

                    let req_end = std::time::Instant::now();
                    let duration = req_end - req_start;
                    if req_end > next_req_start {
                        debug!(
                            id,
                            wanted_interarrival = ?next_interarrival,
                            duration = ?duration,
                            "Request took too long for next request interarrival"
                        );
                    } else {
                        let sleep_time = std::time::Duration::from_micros(50);
                        while std::time::Instant::now() < next_req_start - sleep_time {
                            std::thread::sleep(sleep_time);
                        }
                    }

                    durs.push(duration);
                }

                info!(id, "client done");
                Ok::<_, StdError>(durs)
            })();

            s.send(res).unwrap();
            std::mem::drop(wg);
        });
    }

    std::mem::drop(s);
    wg.wait();
    let cls: Result<Vec<Vec<std::time::Duration>>, StdError> = r.into_iter().collect();
    let durs: Vec<std::time::Duration> = cls?.into_iter().flat_map(|x| x).collect();
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

fn do_loads(
    cl: &mut std::net::UdpSocket,
    addr: std::net::SocketAddr,
    loads: Vec<Op>,
) -> Result<(), StdError> {
    debug!("starting loads");
    // don't need to time or pace the loads.
    for o in loads {
        //std::thread::sleep(std::time::Duration::from_millis(10));
        trace!("starting load");
        do_op(cl, addr, 0, o)?;
        trace!("finished load");
    }
    debug!("finished loads");

    Ok(())
}

fn do_op(
    cl: &mut std::net::UdpSocket,
    addr: std::net::SocketAddr,
    id: usize,
    o: Op,
) -> Result<(), StdError> {
    let req = match o {
        Op::Get(_, k) => Msg::get_req(k),
        Op::Update(_, k, v) => Msg::put_req(k, v),
    };

    let msg = bincode::serialize(&req)?;
    trace!(id, req_id = req.id(), "sending request");
    cl.send_to(&msg, addr)?;

    let mut buf = [0u8; 1024];
    trace!(id, req_id = req.id(), "wait for response");
    let (len, _) = cl.recv_from(&mut buf)?;
    trace!(id, req_id = req.id(), "got response");
    let resp: Msg = bincode::deserialize(&buf[..len])?;
    if resp.id() != req.id() {
        Err(format!(
            "Msg ids don't match: {} != {}",
            resp.id(),
            req.id()
        ))?;
    }

    Ok(())
}
