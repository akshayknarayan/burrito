use anyhow::{anyhow, Context, Error};
use chashmap::CHashMap;
use kvstore::Msg;
use kvstore_ycsb::{ops, Op};
use rand_distr::{Distribution, Exp};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
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
    Sent(Msg, usize, std::time::Instant, std::time::Instant),
    Rcvd(Msg, std::time::Instant),
    Done(Msg, std::time::Duration),
}

impl Inflight {
    fn new(m: Msg, shard_id: usize) -> Self {
        let t = std::time::Instant::now();
        Self::Sent(m, shard_id, t, t)
    }

    //fn is_finished(&self) -> bool {
    //    match self {
    //        Self::Done(_, _) => true,
    //        _ => false,
    //    }
    //}

    fn sent_time(&self) -> std::time::Instant {
        match self {
            Inflight::Sent(_, _, t, _) => *t,
            _ => unreachable!(),
        }
    }

    fn shard_id(&self) -> usize {
        match self {
            Inflight::Sent(_, s, _, _) => *s,
            _ => unreachable!(),
        }
    }

    fn retx_time(&self) -> std::time::Instant {
        match self {
            Inflight::Sent(_, _, _, t) => *t,
            e => {
                warn!(err = ?e, "invalid format");
                unreachable!();
            }
        }
    }

    fn retx(&mut self, now: Instant) {
        match self {
            Inflight::Sent(_, _, _, ref mut t) => *t = now,
            _ => unreachable!(),
        }
    }

    fn msg(&self) -> Msg {
        match self {
            Inflight::Sent(m, _, _, _) | Inflight::Rcvd(m, _) | Inflight::Done(m, _) => m.clone(),
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
            Inflight::Rcvd(i, s) => Inflight::Done(i.clone(), *s - sent),
            _ => self.clone(),
        };

        *self = s;
    }

    fn finish(&mut self) {
        let s = match self {
            Inflight::Sent(i, _, s, _) => Inflight::Done(i.clone(), s.elapsed()),
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
        debug!(addr = ?shard_addrs, "Managed sharding");
        return do_requests(
            vec![shard_addrs[0]],
            interarrival_micros,
            accesses,
            out_file,
            |_| 0,
        );
    };

    debug!(addr = ?shard_addrs, "Client sharding");

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

    do_requests(
        shard_addrs,
        interarrival_micros,
        accesses,
        out_file,
        shard_fn,
    );
}

fn do_requests(
    shard_addrs: Vec<std::net::SocketAddr>,
    interarrival_micros: usize,
    accesses: Vec<Op>,
    out_file: Option<std::path::PathBuf>,
    shard_fn: impl Fn(&Op) -> usize + Send + Sync + 'static,
) {
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

    let start = std::time::Instant::now();
    let shard_fn = Arc::new(shard_fn);
    // now, measure the accesses.
    let mut workers = vec![];
    for (id, ops) in group_by_client(accesses) {
        let shard_addrs = shard_addrs.clone();
        let shard_fn = shard_fn.clone();
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

                let inflight: Arc<CHashMap<usize, Inflight>> =
                    Arc::new(CHashMap::with_capacity(num_ops));
                let inflight_idx: Arc<CHashMap<usize, BTreeMap<std::time::Instant, usize>>> =
                    Arc::new(
                        shard_addrs
                            .iter()
                            .map(|_| Default::default())
                            .enumerate()
                            .collect(),
                    );
                let done: Arc<CHashMap<usize, std::time::Duration>> =
                    Arc::new(CHashMap::with_capacity(num_ops));

                let send_inflight = Arc::clone(&inflight);
                let send_inflight_idx = Arc::clone(&inflight_idx);
                let send_done = done.clone();
                let send_cls = cls.clone();
                let send_shard_addrs = shard_addrs.clone();
                shenango::thread::spawn(move || {
                    let inflight = send_inflight;
                    let inflight_idx = send_inflight_idx;
                    let shard_addrs = send_shard_addrs;
                    let done = send_done;
                    let cls = send_cls;
                    let mut rng = rand::thread_rng();

                    let mut shard_ctrs: Vec<_> = (0..cls.len()).map(|_| 0).collect();
                    let mut interarrivals = vec![];

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

                        let inter = start.elapsed();
                        //trace!(
                        //    id,
                        //    wanted = ?next_interarrival,
                        //    actual = ?inter,
                        //    "interarrival",
                        //);

                        interarrivals.push((next_interarrival, inter));

                        let shard_idx = shard_fn(&o);
                        let cl = &cls[shard_idx];

                        shard_ctrs[shard_idx] += 1;
                        // send the request
                        let req = match o {
                            Op::Get(_, k) => Msg::get_req(k),
                            Op::Update(_, k, v) => Msg::put_req(k, v),
                        };

                        let rid = req.id();
                        let addr = unwrap_ipv4(shard_addrs[shard_idx]);
                        let msg = bincode::serialize(&req).context("serialize")?;
                        trace!(
                            id,
                            req_id = req.id(),
                            shard_id = shard_idx,
                            "sending request"
                        );
                        let req_inf = Inflight::new(req.clone(), shard_idx);
                        cl.write_to(&msg, addr).context("socket write")?;

                        if inflight.contains_key(&rid) {
                            let mut ent = inflight.remove(&rid).unwrap();
                            ent.retro_finish(req_inf.sent_time());
                            done.insert(rid, ent.dur());
                        } else {
                            let time = req_inf.sent_time();
                            inflight.insert_new(rid, req_inf);
                            let mut inf = inflight_idx.get_mut(&shard_idx).unwrap();
                            inf.insert(time, rid);
                        }
                    }

                    fn quantiles(mut xs: Vec<Duration>, log: &str, id: usize) {
                        if xs.is_empty() {
                            return;
                        }

                        xs.sort();
                        let len = xs.len() as f64;
                        let quantile_idxs = [0.25, 0.5, 0.75, 0.95];
                        let quantiles: Vec<_> = quantile_idxs
                            .iter()
                            .map(|q| (len * q) as usize)
                            .map(|i| xs[i])
                            .collect();
                        info!(id,
                            num = ?&xs.len(), min = ?xs[0],
                            p25 = ?quantiles[0], p50 = ?quantiles[1], p75 = ?quantiles[2],
                            p95 = ?quantiles[3], max = ?xs[xs.len() - 1],
                            log
                        );
                    }

                    debug!(
                        id,
                        inflight = inflight.len(),
                        done = done.len(),
                        shard_ctrs = ?shard_ctrs,
                        "done writing requests"
                    );
                    let (wanted, actual) = interarrivals.into_iter().unzip();
                    quantiles(wanted, "wanted interarrivals", id);
                    quantiles(actual, "actual interarrivals", id);
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
                    let recv_inflight_idx = Arc::clone(&inflight_idx);
                    let recv_done = Arc::clone(&done);
                    let recv_cl = cl.clone();
                    let recv_shard_addrs = shard_addrs.clone();
                    let receive_on_shard = shenango::thread::spawn(move || {
                        let inflight = recv_inflight;
                        let inflight_idx = recv_inflight_idx;
                        let done = recv_done;
                        let cl = recv_cl;
                        let shard_addrs = recv_shard_addrs;
                        let shard_id = recv_shard_id;
                        let mut retxs = 0;
                        let mut waste = 0;
                        let mut timeout_time: Option<Instant> = None;

                        let mut buf = [0u8; 1024];
                        loop {
                            if let Ok((len, _)) = cl
                                .read_from_timeout(std::time::Duration::from_millis(100), &mut buf)
                                .context("read timed out")
                            {
                                let now = std::time::Instant::now();
                                let resp: Msg =
                                    bincode::deserialize(&buf[..len]).context("deserialize")?;
                                let entry = inflight.remove(&resp.id());
                                if let Some(mut i) = entry {
                                    //let time = i.retx_time();
                                    i.finish();
                                    trace!(
                                        id,
                                        shard_id,
                                        resp_id = resp.id(),
                                        dur = ?i.dur(),
                                        "got response"
                                    );
                                    done.insert(resp.id(), i.dur());
                                //let mut inf = inflight_idx.get_mut(&shard_id).unwrap();
                                //// best effort, there's another sweep in the retx check.
                                //inf.remove(&time).unwrap_or_else(|| 0);
                                } else if !done.contains_key(&resp.id()) {
                                    trace!(id, shard_id, resp_id = resp.id(), "got response");
                                    inflight.insert(resp.id(), Inflight::Rcvd(resp.clone(), now));
                                } else {
                                    waste += 1;
                                }
                            } else {
                                debug!(
                                    id,
                                    shard_id,
                                    remaining = num_ops - done.len(),
                                    retransmits = retxs,
                                    wasted_reqs = waste,
                                    "read timed out"
                                );
                            }

                            if inflight.len() + done.len() == num_ops {
                                if done.len() == num_ops {
                                    break;
                                }

                                if timeout_time.is_none() {
                                    timeout_time = Some(Instant::now());
                                } else if timeout_time.as_ref().unwrap().elapsed()
                                    > Duration::from_secs(15)
                                {
                                    warn!(
                                        id,
                                        shard_id,
                                        remaining = num_ops - done.len(),
                                        retransmits = retxs,
                                        wasted_reqs = waste,
                                        "client timed out"
                                    );

                                    panic!("client timing out");
                                }
                            }

                            // retransmits
                            let cutoff = Instant::now() - Duration::from_millis(100);
                            let expired_entries = {
                                let mut inf = inflight_idx.get_mut(&shard_id).unwrap();
                                // split_off returns entries after the cutoff.
                                let mut still_inflight = inf.split_off(&cutoff);
                                // we want to change the entries *before*, so swap the pointers so that inf
                                // contains the after-cutoff values
                                use std::ops::DerefMut;
                                std::mem::swap(&mut still_inflight, inf.deref_mut());
                                // still_inflight now contains before-cutoff values, so rename it.
                                let expired_entries = still_inflight;
                                expired_entries
                            };

                            let curr_expired = expired_entries.len();

                            let mut retx_entries = expired_entries
                                .into_iter()
                                .enumerate()
                                .filter_map(|(i, (t, idx))| {
                                    if let Some(mut ent) = inflight.get_mut(&idx) {
                                        if i < 1 {
                                            let r = ent.msg();
                                            let shard_idx = ent.shard_id();
                                            assert_eq!(shard_idx, shard_id);

                                            let msg = bincode::serialize(&r).unwrap();
                                            retxs += 1;
                                            trace!(
                                                id,
                                                shard_id,
                                                req_id = r.id(),
                                                curr_expired,
                                                "retransmitting request"
                                            );
                                            let addr = unwrap_ipv4(shard_addrs[shard_idx]);
                                            cl.write_to(&msg, addr).unwrap();
                                            let now = Instant::now();
                                            ent.retx(now);
                                            Some((now, r.id()))
                                        } else {
                                            Some((t, idx))
                                        }
                                    } else {
                                        None
                                    }
                                })
                                .collect();

                            {
                                let mut inf = inflight_idx.get_mut(&shard_id).unwrap();
                                inf.append(&mut retx_entries);
                            }
                        }

                        debug!(
                            id,
                            shard_id,
                            retransmits = retxs,
                            wasted_reqs = waste,
                            "receiver done"
                        );
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
