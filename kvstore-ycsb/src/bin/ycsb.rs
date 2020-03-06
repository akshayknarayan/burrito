use async_timer as hrtimer;
use core::task::{Context, Poll};
use kvstore_ycsb::{ops, Op};
use slog::info;
use std::collections::HashMap;
use std::error::Error;
use std::pin::Pin;
use std::str::FromStr;
use structopt::StructOpt;

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

fn shard_addrs(
    num_shards: usize,
    base_addr: &str,
    to_uri: impl Fn(&str) -> hyper::Uri,
) -> Vec<hyper::Uri> {
    let mut addrs = vec![base_addr.to_owned()];
    addrs.extend((1..num_shards + 1).map(|i| format!("{}-shard{}", base_addr, i)));
    addrs.into_iter().map(|s| to_uri(&s)).collect()
}

fn tcp_shard_addrs(num_shards: usize, base_addr: String) -> Vec<hyper::Uri> {
    // strip off http://
    let base = &base_addr[7..];
    let parts: Vec<&str> = base.split(":").collect();
    assert_eq!(parts.len(), 2);
    let base_port: u16 = parts[1].parse().expect("invalid port number");
    (0..num_shards + 1)
        .map(|i| format!("http://{}:{}", parts[0], base_port + (i as u16)))
        .map(|s| hyper::Uri::from_str(&s).expect("valid uri"))
        .collect()
}

#[tokio::main]
async fn main() -> Result<(), StdError> {
    let log = burrito_ctl::logger();
    let opt = Opt::from_args();

    if let None = opt.out_file {
        tracing_subscriber::fmt::init();
    }

    let loads = ops(opt.accesses.with_extension("load"))?;
    let accesses = ops(opt.accesses)?;

    let num_shards = match opt.num_shards {
        None | Some(1) => 0, // having 1 shard is pointless, same as 0, might as well avoid the extra channel sends.
        Some(x) => x,
    };

    let (mut durs, time) = if let Some(root) = opt.burrito_root {
        // burrito mode
        if num_shards < 2 {
            let addr: hyper::Uri = burrito_addr::Uri::new(&opt.addr).into();
            info!(&log, "burrito mode"; "sharding" => "server", "proto" => &opt.burrito_proto, "burrito_root" => ?root, "addr" => ?&opt.addr);
            match opt.burrito_proto {
                x if x == "tonic" => {
                    let cl = burrito_addr::tonic::Client::new(root).await?;
                    server_side_sharding(cl, addr, opt.interarrival_client_micros, loads, accesses)
                        .await?
                }
                x if x == "bincode" => {
                    let cl =
                        burrito_addr::bincode::StaticClient::new(std::path::PathBuf::from(root))
                            .await;
                    server_side_sharding(cl, addr, opt.interarrival_client_micros, loads, accesses)
                        .await?
                }
                x if x == "flatbuf" => {
                    let cl =
                        burrito_addr::flatbuf::Client::new(std::path::PathBuf::from(root)).await?;
                    server_side_sharding(cl, addr, opt.interarrival_client_micros, loads, accesses)
                        .await?
                }
                x => Err(format!("Unknown burrito protocol {:?}", &x))?,
            }
        } else {
            let addrs = shard_addrs(num_shards, &opt.addr, |a| burrito_addr::Uri::new(a).into());
            info!(&log, "burrito mode"; "sharding" => "client", "proto" => &opt.burrito_proto, "burrito_root" => ?root, "addr" => ?&addrs);
            match opt.burrito_proto {
                x if x == "tonic" => {
                    let cl = burrito_addr::tonic::Client::new(root).await?;
                    client_side_sharding(
                        cl,
                        addrs,
                        opt.connections_per_client,
                        opt.interarrival_client_micros,
                        loads,
                        accesses,
                    )
                    .await?
                }
                x if x == "bincode" => {
                    let cl =
                        burrito_addr::bincode::StaticClient::new(std::path::PathBuf::from(root))
                            .await;
                    client_side_sharding(
                        cl,
                        addrs,
                        opt.connections_per_client,
                        opt.interarrival_client_micros,
                        loads,
                        accesses,
                    )
                    .await?
                }
                x if x == "flatbuf" => {
                    let cl =
                        burrito_addr::flatbuf::Client::new(std::path::PathBuf::from(root)).await?;
                    client_side_sharding(
                        cl,
                        addrs,
                        opt.connections_per_client,
                        opt.interarrival_client_micros,
                        loads,
                        accesses,
                    )
                    .await?
                }
                x => Err(format!("Unknown burrito protocol {:?}", &x))?,
            }
        }
    } else if opt.addr.starts_with("http") {
        // raw tcp mode
        let mut http = hyper::client::connect::HttpConnector::new();
        http.set_nodelay(true);
        if num_shards < 2 {
            info!(&log, "TCP mode"; "addr" => ?&opt.addr, "sharding" => "server");
            let addr: hyper::Uri = hyper::Uri::from_str(&opt.addr)?;
            server_side_sharding(http, addr, opt.interarrival_client_micros, loads, accesses)
                .await?
        } else {
            let addrs = tcp_shard_addrs(num_shards, opt.addr);
            info!(&log, "TCP mode"; "addr" => ?&addrs, "sharding" => "client");
            client_side_sharding(
                http,
                addrs,
                opt.connections_per_client,
                opt.interarrival_client_micros,
                loads,
                accesses,
            )
            .await?
        }
    } else {
        // raw unix mode
        if num_shards < 2 {
            info!(&log, "UDP mode"; "addr" => ?&opt.addr, "sharding" => "server");
            let addr: hyper::Uri = hyper_unix_connector::Uri::new(opt.addr, "/").into();
            server_side_sharding(
                hyper_unix_connector::UnixClient,
                addr,
                opt.interarrival_client_micros,
                loads,
                accesses,
            )
            .await?
        } else {
            let addrs = shard_addrs(num_shards, &opt.addr, |a| {
                hyper_unix_connector::Uri::new(a, "/").into()
            });
            info!(&log, "UDP mode"; "addr" => ?&addrs, "sharding" => "client");
            client_side_sharding(
                hyper_unix_connector::UnixClient,
                addrs,
                opt.connections_per_client,
                opt.interarrival_client_micros,
                loads,
                accesses,
            )
            .await?
        }
    };

    // done
    durs.sort();
    let len = durs.len() as f64;
    let quantile_idxs = [0.25, 0.5, 0.75, 0.95];
    let quantiles: Vec<_> = quantile_idxs
        .iter()
        .map(|q| (len * q) as usize)
        .map(|i| durs[i])
        .collect();
    info!(&log, "Did accesses"; "num" => ?&durs.len(), "elapsed" => ?time,
        "min" => ?durs[0], "p25" => ?quantiles[0], "p50" => ?quantiles[1], "p75" => ?quantiles[2], "p95" => ?quantiles[3], "max" => ?durs[durs.len() - 1],
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

async fn do_loads<R, E, C>(
    resolver: &mut R,
    addr: hyper::Uri,
    loads: Vec<Op>,
) -> Result<(), StdError>
where
    R: tower_service::Service<hyper::Uri, Response = C, Error = E>,
    C: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    E: Into<StdError>,
{
    futures_util::future::poll_fn(|cx| resolver.poll_ready(cx))
        .await
        .map_err(|e| e.into())?;
    let st = resolver.call(addr.clone()).await.map_err(|e| e.into())?;
    let mut cl = kvstore::Client::from_stream(st);

    // don't need to time the loads.
    for o in loads {
        tracing::trace!("starting load");
        match o {
            Op::Get(_, k) => cl.get(k).await?,
            Op::Update(_, k, v) => cl.update(k, v).await?,
        };

        tracing::trace!("finished load");
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
async fn server_side_sharding<R, E, C>(
    mut resolver: R,
    addr: hyper::Uri,
    interarrival_micros: usize,
    loads: Vec<Op>,
    accesses: Vec<Op>,
) -> Result<(Vec<std::time::Duration>, std::time::Duration), StdError>
where
    R: tower_service::Service<hyper::Uri, Response = C, Error = E>,
    C: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    E: Into<StdError>,
{
    do_loads(&mut resolver, addr.clone(), loads).await?;

    // now, measure the accesses.
    let st = resolver.call(addr).await.map_err(|e| e.into())?;
    let access_by_client = group_by_client(accesses);
    let num_clients = access_by_client.len();
    let srv = kvstore::Client::from(tower_buffer::Buffer::new(kvstore::client(st), num_clients));

    let access_by_client = access_by_client
        .into_iter()
        .map(move |(id, ops)| (id, (vec![srv.clone()], ops)))
        .collect();

    do_requests(access_by_client, interarrival_micros as u64, |_| 0).await
}

//#[tracing::instrument(level = "debug", skip(resolver, loads, accesses))]
async fn client_side_sharding<R, E, C>(
    mut resolver: R,
    addrs: Vec<hyper::Uri>,
    connections_per_client: bool,
    interarrival_micros: usize,
    loads: Vec<Op>,
    accesses: Vec<Op>,
) -> Result<(Vec<std::time::Duration>, std::time::Duration), StdError>
where
    R: tower_service::Service<hyper::Uri, Response = C, Error = E>,
    C: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    E: Into<StdError>,
{
    if addrs.len() == 0 {
        Err(String::from("Need at least one address."))?;
    } else if addrs.len() < 3 {
        // passing only one address means no sharding, which is equivalent to server sharding.
        // two addresses is a sharder and one shard, which is the same thing.
        let mut addrs = addrs;
        return server_side_sharding(
            resolver,
            addrs.pop().unwrap(),
            interarrival_micros,
            loads,
            accesses,
        )
        .await;
    };

    do_loads(&mut resolver, addrs[0].clone(), loads).await?;

    // now, measure the accesses.
    let num_shards = addrs.len() - 1;
    let shard_fn = move |o: &Op| {
        use std::hash::{Hash, Hasher};
        let mut hasher = ahash::AHasher::default();
        o.key().hash(&mut hasher);
        hasher.finish() as usize % num_shards
    };

    if !connections_per_client {
        // one connection per shard
        let mut cls = vec![];
        for a in &addrs[1..] {
            let st = resolver.call(a.clone()).await.map_err(|e| e.into())?;
            let srv = tower_buffer::Buffer::new(kvstore::client(st), 100_000);
            cls.push(kvstore::Client::from(srv));
        }

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
                let mut cls = vec![];
                for a in &addrs[1..] {
                    let st = resolver.call(a.clone()).await.map_err(|e| e.into())?;
                    cls.push(kvstore::Client::from_stream(st));
                }

                access_by_client.insert(k, (cls, vec![o]));
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
            return Poll::Pending;
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
            None => return Poll::Pending, // there are waiting requests, but none of the clients are ready.
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
                tracing::info!(id = client_id, "starting");
                loop {
                    tokio::select!(
                        Some((_, o)) = ops.next() => {
                            let shard = shard_fn(&o);
                            pending.push(shard, o);
                            tracing::trace!(id = client_id, inflight = inflight.len(), shard_id = shard, pending = pending.len(shard), "new request");
                        }
                        Some(Ok((then, fut))) = pending.next() => {
                            inflight.push(async move {
                                fut.await?;
                                Ok::<_, StdError>(then.elapsed())
                            });
                        }
                        Some(Ok(d)) = inflight.next() => {
                            tracing::trace!(id = client_id, inflight = inflight.len(), "request done");
                            durs.push(d);
                        }
                        else => {
                            tracing::debug!(id = client_id, completed = durs.len(), "finished requests");
                            break;
                        }
                    );

                    // This can't be inside the select because then else would never be
                    // triggered.
                    if *done.borrow() {
                        tracing::debug!(id = client_id, completed = durs.len(), "stopping");
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
    tracing::debug!("broadcasting done");
    done_tx.broadcast(true)?;

    // collect all the requests that have completed.
    let rest_durs: Vec<Vec<_>> = reqs.try_collect().await?;
    assert!(!rest_durs.is_empty());
    tracing::debug!("all clients reported");
    durs.extend(rest_durs.into_iter().flat_map(|x| x.into_iter()));
    Ok((durs, access_end))
}
