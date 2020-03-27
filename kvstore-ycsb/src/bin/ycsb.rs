use async_timer as hrtimer;
use core::task::{Context, Poll};
use kvstore_ycsb::{ops, Op};
use std::collections::HashMap;
use std::error::Error;
use std::pin::Pin;
use structopt::StructOpt;
use tracing::{debug, info, trace};

type StdError = Box<dyn Error + Send + Sync + 'static>;

#[derive(Debug, StructOpt)]
#[structopt(name = "kvclient")]
struct Opt {
    #[structopt(short, long)]
    burrito_root: Option<String>,
    #[structopt(long, default_value = "flatbuf")]
    burrito_proto: String,

    #[structopt(long)]
    addr: String,

    #[structopt(short, long)]
    num_shards: Option<usize>,

    #[structopt(short, long)]
    connections_per_client: bool,

    #[structopt(long)]
    accesses: std::path::PathBuf,

    #[structopt(short, long)]
    out_file: Option<std::path::PathBuf>,

    #[structopt(short, long)]
    interarrival_client_micros: usize,
}

fn sk_shard_addrs(num_shards: usize, base_addr: String) -> Vec<std::net::SocketAddr> {
    use std::net::ToSocketAddrs;
    let parts: Vec<&str> = base_addr.split(":").collect();
    assert_eq!(parts.len(), 2);
    let base_port: u16 = parts[1].parse().expect("invalid port number");
    (0..num_shards + 1)
        .map(|i| format!("{}:{}", parts[0], base_port + (i as u16)))
        .map(|s| s.to_socket_addrs().expect("valid sockaddr").next().unwrap())
        .collect()
}

#[tokio::main]
async fn main() -> Result<(), StdError> {
    let opt = Opt::from_args();

    tracing_subscriber::fmt::init();

    trace!("reading workload");
    let loads = ops(opt.accesses.with_extension("load"))?;
    let accesses = ops(opt.accesses)?;
    debug!(num_ops = ?accesses.len(), "done reading workload");

    let num_shards = match opt.num_shards {
        None | Some(1) => 0, // having 1 shard is pointless, same as 0, might as well avoid the extra channel sends.
        Some(x) => x,
    };

    let cls = if let Some(root) = opt.burrito_root {
        debug!(root = ?&root, "Burrito mode");
        // first query shard-ctl
        let mut shardctl = burrito_shard_ctl::ShardCtlClient::new(root).await?;
        debug!(service_addr = ?&opt.addr, "Querying");
        let mut si = shardctl.query(&opt.addr).await?;

        // TODO assume for now that shard_info doesn't contain burrito addresses to be resolved

        // decide managed_sharding or not.
        if num_shards < 1 {
            // we are going to manage the sharding ourselves.
            si.shard_addrs.clear();
        }

        let mut addrs = vec![si.canonical_addr];
        addrs.extend(si.shard_addrs.into_iter());

        debug!(addrs = ?&addrs, num_shards, "Queried shard");

        // make clients
        let mut cls = Vec::with_capacity(addrs.len());
        for a in addrs {
            cls.push(match a {
                burrito_shard_ctl::proto::Addr::Udp(sa) => {
                    let sk = tokio::net::UdpSocket::bind("0.0.0.0:0").await.unwrap();
                    kvstore::Client::from(kvstore::UdpClientService::new(sk, sa).await?)
                }
                _ => unimplemented!(),
            });
        }

        cls
    } else {
        // there is no shardctl
        // "guess" that the ports are sequential
        let addrs = sk_shard_addrs(num_shards, opt.addr);
        // make clients
        let mut cls = Vec::with_capacity(addrs.len());
        for sa in addrs {
            cls.push(kvstore::Client::from({
                let sk = tokio::net::UdpSocket::bind("0.0.0.0:0").await.unwrap();
                kvstore::UdpClientService::new(sk, sa).await?
            }));
        }

        cls
    };

    let (mut durs, time) = do_exp(
        cls,
        opt.connections_per_client,
        opt.interarrival_client_micros,
        loads,
        accesses,
    )
    .await?;

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

async fn do_loads<S>(cl: &mut kvstore::Client<S>, loads: Vec<Op>) -> Result<(), StdError>
where
    S: tower_service::Service<kvstore::Msg, Response = kvstore::Msg, Error = StdError>,
    S::Future: 'static,
{
    // don't need to time the loads.
    for o in loads {
        trace!("starting load");
        match o {
            Op::Get(_, k) => cl.get(k).await?,
            Op::Update(_, k, v) => cl.update(k, v).await?,
        };

        trace!("finished load");
    }

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

fn paced_ops_stream(
    ops: Vec<Op>,
    interarrival_micros: u64,
) -> impl futures_util::stream::Stream<Item = ((), Op)> {
    use futures_util::stream::StreamExt;
    //let mut ops = tokio::time::interval(std::time::Duration::from_micros(interarrival_micros as u64))
    let tkr = hrtimer::interval(std::time::Duration::from_micros(interarrival_micros as u64));
    tkr.zip(futures_util::stream::iter(ops))
}

// Uncommenting the following causes:
// error: reached the type-length limit while instantiating
//     `std::pin::Pin::<&mut std::future...}[1]::poll[0]::{{closure}}[0])]>`
//     |
//     = note: consider adding a `#![type_length_limit="1606073"]` attribute to your crate
//#[tracing::instrument(level = "debug", skip(resolver, loads, accesses))]
async fn managed_sharding<S>(
    mut cl: kvstore::Client<S>,
    interarrival_micros: usize,
    loads: Vec<Op>,
    accesses: Vec<Op>,
) -> Result<(Vec<std::time::Duration>, std::time::Duration), StdError>
where
    S: tower_service::Service<kvstore::Msg, Response = kvstore::Msg, Error = StdError>
        + Clone
        + Unpin,
    S::Future: 'static + Unpin,
{
    do_loads(&mut cl, loads).await?;

    // now, measure the accesses.
    let access_by_client = group_by_client(accesses)
        .into_iter()
        .map(move |(id, ops)| (id, (vec![cl.clone()], ops)))
        .collect();
    do_requests(access_by_client, interarrival_micros as u64, |_| 0).await
}

//#[tracing::instrument(level = "debug", skip(resolver, loads, accesses))]
async fn do_exp<S>(
    mut cls: Vec<kvstore::Client<S>>,
    connections_per_client: bool,
    interarrival_micros: usize,
    loads: Vec<Op>,
    accesses: Vec<Op>,
) -> Result<(Vec<std::time::Duration>, std::time::Duration), StdError>
where
    S: tower_service::Service<kvstore::Msg, Response = kvstore::Msg, Error = StdError>
        + Clone
        + Unpin,
    S::Future: 'static + Unpin,
{
    if cls.is_empty() {
        Err(String::from("Need at least one client."))?;
    } else if cls.len() < 3 {
        // passing only one address means no sharding, which is equivalent to server sharding.
        // two addresses is a sharder and one shard, which is the same thing.
        return managed_sharding(cls.swap_remove(0), interarrival_micros, loads, accesses).await;
    };

    do_loads(&mut cls[0], loads).await?;

    // now, measure the accesses.
    let num_shards = cls.len() - 1;
    let shard_fn = move |o: &Op| {
        // TODO update to match FNV function in xdp program
        use std::hash::{Hash, Hasher};
        let mut hasher = ahash::AHasher::default();
        o.key().hash(&mut hasher);
        hasher.finish() as usize % num_shards
    };

    if !connections_per_client {
        // one connection per shard
        let access_by_client = group_by_client(accesses)
            .into_iter()
            .map(|(id, ops)| (id, (cls.clone(), ops)))
            .collect();
        do_requests(access_by_client, interarrival_micros as u64, shard_fn).await
    } else {
        // one conenction per shard per client
        // accesses group_by client
        let mut access_by_client: HashMap<usize, (Vec<kvstore::Client<_>>, Vec<Op>)> =
            Default::default();
        for o in accesses {
            let k = o.client_id();
            if !access_by_client.contains_key(&k) {
                access_by_client.insert(k, (cls.clone(), vec![o]));
            } else {
                let v = access_by_client.get_mut(&k).unwrap();
                v.1.push(o);
            }
        }

        do_requests(access_by_client, interarrival_micros as u64, shard_fn).await
    }
}

use std::collections::VecDeque;

/// As clients become ready, send return futures which will make requests on them.
///
/// Have to poll only the clients with pending requests. Otherwise, we could
/// end up repeatedly calling poll_ready on a client with no requests.
#[pin_project::pin_project]
struct PendingServices<S> {
    cls: Vec<kvstore::Client<S>>,
    pending: Vec<VecDeque<(tokio::time::Instant, Op)>>,
    pending_now: Vec<usize>,
}

impl<S> PendingServices<S>
where
    S: tower_service::Service<kvstore::Msg, Response = kvstore::Msg, Error = StdError>,
    S::Future: 'static,
{
    fn new(cls: Vec<kvstore::Client<S>>) -> Self {
        let pending = cls.iter().map(|_| Default::default()).collect();
        Self {
            cls,
            pending,
            pending_now: vec![],
        }
    }

    fn push(&mut self, idx: usize, o: Op) {
        if self.pending[idx].is_empty() {
            self.pending_now.push(idx);
        }

        self.pending[idx].push_back((tokio::time::Instant::now(), o));
    }

    fn len(&self, idx: usize) -> usize {
        self.pending[idx].len()
    }
}

impl<S> futures_util::stream::Stream for PendingServices<S>
where
    S: tower_service::Service<kvstore::Msg, Response = kvstore::Msg, Error = StdError>,
    S::Future: 'static,
{
    type Item = Result<
        (
            tokio::time::Instant,
            Pin<Box<dyn std::future::Future<Output = Result<Option<String>, StdError>>>>,
        ),
        StdError,
    >;

    #[pin_project::project]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        // project to split the borrow
        let mut this = self.project();
        let pending: &mut _ = &mut this.pending;
        let pending_now: &mut _ = &mut this.pending_now;
        let cls: &mut _ = &mut this.cls;

        if pending_now.is_empty() {
            return Poll::Ready(None);
        }

        // rotate for poll fairness
        pending_now.rotate_left(1);
        // see if any of them are ready
        let mut ready_now: Option<usize> = None;
        let mut ready_now_idx = 0;
        for i in 0..pending_now.len() {
            let idx = pending_now[i];
            match cls[idx].poll_ready(cx) {
                Poll::Pending => continue,
                Poll::Ready(Ok(())) => {
                    ready_now.replace(idx);
                    ready_now_idx = i;
                    break;
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e.into()))),
            }
        }

        match ready_now {
            None => return Poll::Pending, // there are waiting requests, but all of the clients are pending.
            Some(idx) => {
                // cls[idx] is ready.
                let cl = &mut cls[idx];
                let (then, o) = pending[idx].pop_front().expect("only polled active conns");
                let fut = match o {
                    Op::Get(_, k) => cl.get_fut(k),
                    Op::Update(_, k, v) => cl.update_fut(k, v),
                };

                if pending[idx].is_empty() {
                    // no more messages, yank from pending_now
                    pending_now.remove(ready_now_idx);
                }

                Poll::Ready(Some(Ok((then, fut))))
            }
        }
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
async fn do_requests<S>(
    access_by_client: HashMap<usize, (Vec<kvstore::Client<S>>, Vec<Op>)>,
    interarrival_micros: u64,
    shard_fn: impl Fn(&Op) -> usize,
) -> Result<(Vec<std::time::Duration>, std::time::Duration), StdError>
where
    S: tower_service::Service<kvstore::Msg, Response = kvstore::Msg, Error = StdError> + Unpin,
    S::Future: 'static + Unpin,
{
    use futures_util::stream::{FuturesOrdered, FuturesUnordered, StreamExt, TryStreamExt};
    let (done_tx, done_rx) = tokio::sync::watch::channel::<bool>(false);
    let mut reqs: FuturesUnordered<_> = access_by_client
        .into_iter()
        .map(|(client_id, (cls, ops))| {
            let mut inflight = FuturesOrdered::new();
            let mut durs = vec![];
            let done = done_rx.clone();
            assert!(!ops.is_empty());
            let mut ops = paced_ops_stream(ops, interarrival_micros);
            let mut pending = PendingServices::new(cls);
            let shard_fn = &shard_fn;
            async move {
                debug!(id = client_id, "starting");
                loop {
                    tokio::select!(
                        Some((_, o)) = ops.next() => {
                            let shard = shard_fn(&o);
                            pending.push(shard, o);
                            trace!(id = client_id, inflight = inflight.len(), shard_id = shard, pending = pending.len(shard), "new request");
                        }
                        Some(Ok((then, fut))) = pending.next() => {
                            inflight.push(async move {
                                fut.await?;
                                Ok::<_, StdError>(then.elapsed())
                            });
                        }
                        Some(Ok(d)) = inflight.next() => {
                            trace!(id = client_id, inflight = inflight.len(), "request done");
                            durs.push(d);
                        }
                        else => {
                            info!(id = client_id, completed = durs.len(), "finished requests");
                            break;
                        }
                    );

                    // This can't be inside the select because then else would never be
                    // triggered.
                    if *done.borrow() {
                        debug!(id = client_id, completed = durs.len(), "stopping");
                        break; // the first client finished. stop.
                    }
                }

                Ok::<_, StdError>(durs)
            }
        })
        .collect();

    let access_start = tokio::time::Instant::now();
    // do the accesses until the first client is done.
    let mut durs: Vec<_> = reqs.try_next().await?.expect("durs");
    assert!(!durs.is_empty());
    let access_end = access_start.elapsed();
    info!("broadcasting done");
    done_tx.broadcast(true)?;

    // collect all the requests that have completed.
    let rest_durs: Vec<Vec<_>> = reqs.try_collect().await?;
    assert!(!rest_durs.is_empty());
    info!("all clients reported");
    durs.extend(rest_durs.into_iter().flat_map(|x| x.into_iter()));
    Ok((durs, access_end))
}
