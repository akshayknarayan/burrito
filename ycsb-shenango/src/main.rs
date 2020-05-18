use anyhow::{anyhow, Context, Error};
use kvstore::Msg;
use kvstore_ycsb::{ops, Op};
use rand_distr::{Distribution, Exp};
use std::collections::HashMap;
use std::sync::Arc;
use structopt::StructOpt;
use tracing::{debug, info, trace, warn};

const FNV1_64_INIT: u64 = 0xcbf29ce484222325u64;
const FNV_64_PRIME: u64 = 0x100000001b3u64;

#[derive(Debug, StructOpt)]
#[structopt(name = "kvclient-shenango")]
struct Opt {
    #[structopt(short, long)]
    burrito_root: Option<String>,

    #[structopt(long)]
    addr: String,

    #[structopt(long)]
    shenango_config: std::path::PathBuf,

    #[structopt(short, long)]
    num_shards_thresh: Option<usize>,

    #[structopt(long)]
    accesses: std::path::PathBuf,

    #[structopt(short, long)]
    out_file: Option<std::path::PathBuf>,

    #[structopt(short, long)]
    interarrival_client_micros: usize,
}

fn main() -> Result<(), Error> {
    // shenango setup otherwise done by linker

    let opt = Opt::from_args();

    tracing_subscriber::fmt::init();

    let load_file = opt.accesses.with_extension("load");
    debug!(file = ?load_file, "reading workload");
    let loads = ops(load_file).context("load ops")?;
    let accesses = ops(opt.accesses).context("access ops")?;
    debug!(num_ops = ?accesses.len(), "done reading workload");

    let cls = {
        let root = opt.burrito_root.as_ref().unwrap().clone();
        debug!(root = ?&root, "Burrito mode");

        let addr = opt.addr;

        let mut rt = tokio::runtime::Runtime::new()?;
        let si = rt.block_on(async {
            let mut shardctl = match burrito_shard_ctl::ShardCtlClient::new(&root).await {
                Ok(s) => s,
                Err(e) => Err(anyhow!(
                    "Could not contact ShardCtl: err = {} path = {}",
                    e,
                    root
                ))?,
            };
            let si = if let Ok(mut dcl) =
                burrito_discovery_ctl::client::DiscoveryClient::new(&root).await
            {
                shardctl.query_recursive(&mut dcl, addr.parse()?).await?
            } else {
                debug!("Could not contact discovery-ctl");
                shardctl.query_shard(addr.parse()?).await?
            };

            Ok::<_, Error>(si)
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

    let inter = opt.interarrival_client_micros;
    let out = opt.out_file;
    shenango::runtime_init(
        opt.shenango_config.to_str().unwrap().to_owned(),
        move || {
            let mut cl = shenango::udp::UdpConnection::listen("0.0.0.0:64347".parse().unwrap())
                .context("Failed to make udp socket")
                .expect("make udp conn");
            do_loads(&mut cl, cls[0], loads).unwrap();
            std::mem::drop(cl);

            do_exp(cls, inter, accesses, out);
        },
    )
    .map_err(|i| anyhow!("shenango runtime error: {}", i))
    .unwrap();

    Ok(())
}

fn done(
    mut durs: Vec<std::time::Duration>,
    elapsed: std::time::Duration,
    interarrival_client_micros: usize,
    out_file: Option<std::path::PathBuf>,
) {
    if durs.is_empty() {
        return;
    }

    durs.sort();
    let len = durs.len() as f64;
    let quantile_idxs = [0.25, 0.5, 0.75, 0.95];
    let quantiles: Vec<_> = quantile_idxs
        .iter()
        .map(|q| (len * q) as usize)
        .map(|i| durs[i])
        .collect();
    info!(
        num = ?&durs.len(), elapsed = ?elapsed, min = ?durs[0],
        p25 = ?quantiles[0], p50 = ?quantiles[1], p75 = ?quantiles[2],
        p95 = ?quantiles[3], max = ?durs[durs.len() - 1],
        "Did accesses"
    );

    if let Some(f) = out_file {
        let mut f = std::fs::File::create(f).expect("Open out file");
        use std::io::Write;
        write!(&mut f, "Interarrival_us NumOps Completion_ms Latency_us\n").expect("write");
        let len = durs.len();
        for d in durs {
            write!(
                &mut f,
                "{} {} {} {}\n",
                interarrival_client_micros,
                len,
                elapsed.as_millis(),
                d.as_micros()
            )
            .expect("write");
        }
    }
}

#[derive(Debug, Clone)]
enum Inflight {
    Sent(usize, std::time::Instant),
    Rcvd(usize, std::time::Instant),
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

    fn sent_time(&self) -> std::time::Instant {
        match self {
            Inflight::Sent(_, t) => *t,
            _ => unreachable!(),
        }
    }

    fn dur(&self) -> std::time::Duration {
        match self {
            Inflight::Done(_, t) => *t,
            _ => unreachable!(),
        }
    }

    fn retro_finish(&mut self, sent: std::time::Instant) {
        let s = match self {
            Inflight::Rcvd(i, s) => Inflight::Done(*i, *s - sent),
            _ => self.clone(),
        };

        *self = s;
    }

    fn finish(&mut self) {
        let s = match self {
            Inflight::Sent(i, s) => Inflight::Done(*i, s.elapsed()),
            _ => self.clone(),
        };

        *self = s;
    }
}

fn do_exp(
    shard_addrs: Vec<std::net::SocketAddr>,
    interarrival_micros: usize,
    accesses: Vec<Op>,
    out_file: Option<std::path::PathBuf>,
) {
    if shard_addrs.is_empty() {
        panic!("Need at least one client.");
    } else if shard_addrs.len() < 3 {
        // passing only one address means no sharding, which is equivalent to server sharding.
        // two addresses is a sharder and one shard, which is the same thing.
        return managed_sharding(shard_addrs[0], interarrival_micros, accesses, out_file);
    };

    debug!(addr = ?shard_addrs, "Client sharding");
    let start = std::time::Instant::now();

    // take off the canonical_addr
    let shard_addrs = shard_addrs[1..].to_vec();
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

    //for (id, ops) in group_by_client(accesses.clone()) {
    //    let ops_len = ops.len();
    //    debug!(id, num_ops = ops_len, "client");
    //    let mut shard_ctrs: Vec<_> = (0..shard_addrs.len()).map(|_| 0).collect();
    //    for o in ops {
    //        trace!(key = &o.key()[0..4], "sharding on key");
    //        let idx = shard_fn(&o);
    //        shard_ctrs[idx] += 1;
    //    }

    //    debug!(id, num_ops = ops_len, shards = ?shard_ctrs, "client shard counts");
    //}

    // now, measure the accesses.
    let mut workers = vec![];
    for (id, ops) in group_by_client(accesses) {
        let shard_addrs = shard_addrs.clone();
        let jh = shenango::thread::spawn(move || {
            let res = (|| {
                let mean_ns: f64 = interarrival_micros as f64 * 1e3;
                let lambda = 1. / mean_ns;
                let r = Exp::new(lambda).expect("Make exponential distr");
                let num_ops = ops.len();

                info!(id, num_ops, shards = shard_addrs.len(), "client starting");
                let cls: Result<Vec<Arc<shenango::udp::UdpConnection>>, Error> = shard_addrs
                    .iter()
                    .enumerate()
                    .map(|(i, _)| {
                        let p = 60000 + 100 * id + i;
                        let cl = shenango::udp::UdpConnection::listen(std::net::SocketAddrV4::new(
                            std::net::Ipv4Addr::new(0, 0, 0, 0),
                            p as _,
                        ))
                        .context("Failed to make udp socket")?;
                        Ok::<_, Error>(Arc::new(cl))
                    })
                    .collect();
                let cls = Arc::new(cls?);

                use chashmap::CHashMap;

                let inflight: Arc<CHashMap<usize, Inflight>> =
                    Arc::new(CHashMap::with_capacity(num_ops));
                let done: Arc<CHashMap<usize, std::time::Duration>> =
                    Arc::new(CHashMap::with_capacity(num_ops));

                // sending thread
                let send_inflight = Arc::clone(&inflight);
                let send_done = Arc::clone(&done);
                let send_cls = cls.clone();
                shenango::thread::spawn(move || {
                    let inflight = send_inflight;
                    let done = send_done;
                    let cls = send_cls;
                    let mut rng = rand::thread_rng();

                    let mut shard_ctrs: Vec<_> = (0..cls.len()).map(|_| 0).collect();

                    for o in ops {
                        // pace the request
                        let mut next_interarrival_ns = r.sample(&mut rng) as u64;
                        if next_interarrival_ns == 0 {
                            next_interarrival_ns = 1;
                        }

                        let next_interarrival =
                            std::time::Duration::from_nanos(next_interarrival_ns);
                        let start = std::time::Instant::now();
                        let next_req_start = start + next_interarrival;
                        let sleep_time = std::time::Duration::from_micros(10);
                        while std::time::Instant::now() < next_req_start - sleep_time {
                            shenango::delay_us(sleep_time.as_micros() as _);
                        }

                        trace!(
                            id,
                            wanted = ?next_interarrival,
                            actual = ?start.elapsed(),
                            "interarrival",
                        );

                        let shard_idx = shard_fn(&o);
                        let cl = &cls[shard_idx];

                        shard_ctrs[shard_idx] += 1;
                        // send the request
                        let req = match o {
                            Op::Get(_, k) => Msg::get_req(k),
                            Op::Update(_, k, v) => Msg::put_req(k, v),
                        };

                        let addr = unwrap_ipv4(shard_addrs[shard_idx]);
                        let msg = bincode::serialize(&req).context("serialize")?;
                        trace!(
                            id,
                            req_id = req.id(),
                            shard_id = shard_idx,
                            "sending request"
                        );
                        let req_inf = Inflight::new(req.id());
                        cl.write_to(&msg, addr).context("socket write")?;

                        if inflight.contains_key(&req.id()) {
                            let mut ent = inflight.remove(&req.id()).unwrap();
                            ent.retro_finish(req_inf.sent_time());
                            done.insert(req.id(), ent.dur());
                        } else {
                            inflight.insert(req.id(), req_inf);
                        }
                    }

                    debug!(
                        id,
                        inflight = inflight.len(),
                        done = done.len(),
                        shard_ctrs = ?shard_ctrs,
                        "done writing requests"
                    );
                    Ok::<_, Error>(())
                });

                // start a thread per cl
                let mut receivers = vec![];
                let recv_cls = cls.clone();
                let mut shard_id = 0;
                for cl in recv_cls.iter() {
                    let recv_shard_id = shard_id;
                    shard_id += 1;
                    let recv_inflight = Arc::clone(&inflight);
                    let recv_done = Arc::clone(&done);
                    let recv_cl = cl.clone();
                    let receive_on_shard = shenango::thread::spawn(move || {
                        let inflight = recv_inflight;
                        let done = recv_done;
                        let cl = recv_cl;
                        let mut ctr = 0;
                        let shard_id = recv_shard_id;

                        let mut buf = [0u8; 1024];
                        loop {
                            let (len, _) = match cl
                                .read_from_timeout(std::time::Duration::from_secs(10), &mut buf)
                            {
                                Err(_) if done.len() == num_ops => {
                                    debug!(
                                        id,
                                        inflight = inflight.len(),
                                        done = done.len(),
                                        shard_id,
                                        receiver_ctr = ctr,
                                        "receiver done"
                                    );
                                    break;
                                }
                                r @ Err(_) => {
                                    debug!(
                                        id,
                                        inflight = inflight.len(),
                                        done = done.len(),
                                        shard_id,
                                        "read timed out"
                                    );
                                    r.context("timed out")?
                                }
                                r @ Ok(_) => r.unwrap(),
                            };
                            let now = std::time::Instant::now();
                            let resp: Msg =
                                bincode::deserialize(&buf[..len]).context("deserialize")?;
                            let entry = inflight.remove(&resp.id());
                            if let Some(mut i) = entry {
                                i.finish();
                                done.insert(resp.id(), i.dur());
                            } else {
                                inflight.insert(resp.id(), Inflight::Rcvd(resp.id(), now));
                            }

                            ctr += 1;
                            trace!(
                                id,
                                resp_id = resp.id(),
                                inflight = inflight.len(),
                                done = done.len(),
                                shard_id,
                                "got response"
                            );

                            if done.len() == num_ops {
                                debug!(
                                    id,
                                    inflight = inflight.len(),
                                    done = done.len(),
                                    shard_id,
                                    receiver_ctr = ctr,
                                    "receiver done"
                                );
                                break;
                            }
                        }

                        Ok::<_, Error>(start.elapsed())
                    });

                    receivers.push(receive_on_shard);
                }

                debug!(id, receivers = receivers.len(), "join receive sockets");
                let durs: Result<Vec<_>, _> = receivers
                    .into_iter()
                    .map(|r| r.join().unwrap().context("receiver thread"))
                    .collect();
                let tot = durs?
                    .iter()
                    .min()
                    .expect("receivers can't be empty")
                    .clone();

                // necessary to allow the Arc to be dropped...
                while Arc::strong_count(&done) > 1 {
                    shenango::delay_us(100);
                }

                info!(id, duration = ?tot, "client done");
                let done = Arc::try_unwrap(done)
                    .map_err(|_| anyhow!("arc failed"))
                    .unwrap();
                let durs = done.into_iter().map(|(_, d)| d).collect();
                Ok::<_, Error>((durs, tot))
            })();

            match &res {
                Err(e) => warn!(id, err = ?e, "client error"),
                _ => (),
            };

            res
        });

        workers.push(jh);
    }

    debug!("joining clients");
    // wait on the client threads
    let workers: Vec<Result<_, _>> = workers.into_iter().map(|jh| jh.join().unwrap()).collect();
    debug!("done joining");
    let workers: Result<Vec<(Vec<_>, _)>, _> = workers.into_iter().collect();
    let workers = workers.context("duration results for clients").unwrap();

    let (durs, times): (Vec<Vec<_>>, Vec<_>) = workers.into_iter().unzip();
    let time = times.into_iter().max().expect("times can't be empty");

    let durs: Vec<_> = durs.into_iter().flat_map(|x| x).collect();

    done(durs, time, interarrival_micros, out_file);
}

fn managed_sharding(
    a: std::net::SocketAddr,
    interarrival_micros: usize,
    accesses: Vec<Op>,
    out_file: Option<std::path::PathBuf>,
) {
    debug!(addr = ?a, "Managed sharding");
    let start = std::time::Instant::now();
    let mut workers = vec![];
    for (id, ops) in group_by_client(accesses) {
        let jh = shenango::thread::spawn(move || {
            let res = (|| {
                let mean_ns: f64 = interarrival_micros as f64 * 1e3;
                let lambda = 1. / mean_ns;
                let r = Exp::new(lambda).expect("Make exponential distr");

                info!(id, "client starting");
                let p = 64347 + id;
                let cl = Arc::new(shenango::udp::UdpConnection::listen(
                    std::net::SocketAddrV4::new(std::net::Ipv4Addr::new(0, 0, 0, 0), p as _),
                )?);

                use chashmap::CHashMap;

                let num_ops = ops.len();
                let inflight: Arc<CHashMap<usize, Inflight>> =
                    Arc::new(CHashMap::with_capacity(num_ops));
                let done: Arc<CHashMap<usize, std::time::Duration>> =
                    Arc::new(CHashMap::with_capacity(num_ops));

                let send_inflight = inflight.clone();
                let send_done = done.clone();
                let send_cl = cl.clone();
                shenango::thread::spawn(move || {
                    let inflight = send_inflight;
                    let done = send_done;
                    let cl = send_cl;
                    let mut rng = rand::thread_rng();

                    for o in ops {
                        // pace the request
                        let mut next_interarrival_ns = r.sample(&mut rng) as u64;
                        if next_interarrival_ns == 0 {
                            next_interarrival_ns = 1;
                        }

                        let next_interarrival =
                            std::time::Duration::from_nanos(next_interarrival_ns);
                        let start = std::time::Instant::now();
                        let next_req_start = start + next_interarrival;
                        let sleep_time = std::time::Duration::from_micros(10);
                        while std::time::Instant::now() < next_req_start - sleep_time {
                            shenango::delay_us(sleep_time.as_micros() as _);
                        }

                        trace!(
                            id,
                            wanted = ?next_interarrival,
                            actual = ?start.elapsed(),
                            "interarrival",
                        );

                        // send the request
                        let req = match o {
                            Op::Get(_, k) => Msg::get_req(k),
                            Op::Update(_, k, v) => Msg::put_req(k, v),
                        };

                        let addr = unwrap_ipv4(a);
                        let msg = bincode::serialize(&req)?;
                        trace!(id, req_id = req.id(), "sending request");
                        let req_inf = Inflight::new(req.id());
                        cl.write_to(&msg, addr)?;

                        {
                            if inflight.contains_key(&req.id()) {
                                let mut ent = inflight.remove(&req.id()).unwrap();
                                ent.retro_finish(req_inf.sent_time());
                                done.insert(req.id(), ent.dur());
                            } else {
                                inflight.insert(req.id(), req_inf);
                            }
                        }
                    }

                    debug!(id, "done writing requests");
                    Ok::<_, Error>(())
                });

                let mut timeout_start = None;
                let mut buf = [0u8; 1024];
                loop {
                    let (len, _) = cl
                        .read_from_timeout(std::time::Duration::from_secs(10), &mut buf)
                        .context("read timed out")?;
                    let now = std::time::Instant::now();
                    let resp: Msg = bincode::deserialize(&buf[..len]).context("deserialize")?;
                    trace!(id, resp_id = resp.id(), "got response");
                    let entry = inflight.remove(&resp.id());
                    if let Some(mut i) = entry {
                        i.finish();
                        done.insert(resp.id(), i.dur());
                    } else {
                        inflight.insert(resp.id(), Inflight::Rcvd(resp.id(), now));
                    }

                    if inflight.is_empty() && !done.is_empty() {
                        timeout_start = Some(std::time::Instant::now());
                    }

                    if done.len() == num_ops {
                        break;
                    }

                    if let Some(to) = timeout_start {
                        if to.elapsed() > std::time::Duration::from_secs(15) {
                            return Err(anyhow!("Timing out 15s after last request sent"));
                        }
                    }
                }

                // necessary to allow the Arc to be dropped...
                while Arc::strong_count(&done) > 1 {
                    shenango::delay_us(100);
                }

                info!(id, "client done");
                let done = Arc::try_unwrap(done)
                    .map_err(|_| anyhow!("arc failed"))
                    .unwrap();
                let durs = done.into_iter().map(|(_, d)| d).collect();
                Ok::<_, Error>(durs)
            })();

            match &res {
                Err(e) => warn!(id, err = ?e, "client error"),
                _ => (),
            };

            res
        });

        workers.push(jh);
    }

    // wait on the client threads
    let durs: Vec<Result<Result<Vec<_>, _>, _>> = workers.into_iter().map(|jh| jh.join()).collect();
    let durs: Result<Vec<Vec<_>>, _> = durs.into_iter().flat_map(|x| x).collect();
    let durs: Result<Vec<_>, _> = durs.and_then(|vv| Ok(vv.into_iter().flat_map(|x| x).collect()));

    done(
        durs.context("Experiment results").unwrap(),
        start.elapsed(),
        interarrival_micros,
        out_file,
    );
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

fn do_op(
    cl: &mut shenango::udp::UdpConnection,
    addr: std::net::SocketAddr,
    id: usize,
    o: Op,
) -> Result<(), Error> {
    let req = match o {
        Op::Get(_, k) => Msg::get_req(k),
        Op::Update(_, k, v) => Msg::put_req(k, v),
    };

    let addr = unwrap_ipv4(addr);
    let msg = bincode::serialize(&req)?;
    trace!(id, req_id = req.id(), "sending request");
    cl.write_to(&msg, addr)?;

    let mut buf = [0u8; 1024];
    trace!(id, req_id = req.id(), "wait for response");
    let (len, _) = cl.read_from_timeout(std::time::Duration::from_secs(10), &mut buf)?;
    trace!(id, req_id = req.id(), "got response");
    let resp: Msg = bincode::deserialize(&buf[..len])?;
    if resp.id() != req.id() {
        Err(anyhow!(
            "Msg ids don't match: {} != {}",
            resp.id(),
            req.id()
        ))?;
    }

    Ok(())
}

fn do_loads(
    cl: &mut shenango::udp::UdpConnection,
    addr: std::net::SocketAddr,
    loads: Vec<Op>,
) -> Result<(), Error> {
    debug!(addr = ?addr, num = loads.len(), "starting loads");
    // don't need to time or pace the loads.
    for o in loads {
        //std::thread::sleep(std::time::Duration::from_millis(10));
        trace!("starting load");
        do_op(cl, addr, 0, o)?;
        trace!("finished load");
    }
    debug!(addr = ?addr, "finished loads");

    Ok(())
}

fn unwrap_ipv4(sk: std::net::SocketAddr) -> std::net::SocketAddrV4 {
    match sk {
        std::net::SocketAddr::V4(a) => a,
        _ => panic!("Socket address not ipv4: {}", sk),
    }
}
