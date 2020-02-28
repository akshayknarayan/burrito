use kvstore_ycsb::{ops, Op};
use slog::info;
use std::collections::HashMap;
use std::error::Error;
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

    #[structopt(long)]
    accesses: std::path::PathBuf,

    #[structopt(short, long)]
    out_file: Option<std::path::PathBuf>,
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
                    server_side_sharding(cl, addr, loads, accesses).await?
                }
                x if x == "bincode" => {
                    let cl =
                        burrito_addr::bincode::StaticClient::new(std::path::PathBuf::from(root))
                            .await;
                    server_side_sharding(cl, addr, loads, accesses).await?
                }
                x if x == "flatbuf" => {
                    let cl =
                        burrito_addr::flatbuf::Client::new(std::path::PathBuf::from(root)).await?;
                    server_side_sharding(cl, addr, loads, accesses).await?
                }
                x => Err(format!("Unknown burrito protocol {:?}", &x))?,
            }
        } else {
            let addrs = shard_addrs(num_shards, &opt.addr, |a| burrito_addr::Uri::new(a).into());
            info!(&log, "burrito mode"; "sharding" => "client", "proto" => &opt.burrito_proto, "burrito_root" => ?root, "addr" => ?&addrs);
            match opt.burrito_proto {
                x if x == "tonic" => {
                    let cl = burrito_addr::tonic::Client::new(root).await?;
                    client_side_sharding(cl, addrs, loads, accesses).await?
                }
                x if x == "bincode" => {
                    let cl =
                        burrito_addr::bincode::StaticClient::new(std::path::PathBuf::from(root))
                            .await;
                    client_side_sharding(cl, addrs, loads, accesses).await?
                }
                x if x == "flatbuf" => {
                    let cl =
                        burrito_addr::flatbuf::Client::new(std::path::PathBuf::from(root)).await?;
                    client_side_sharding(cl, addrs, loads, accesses).await?
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
            server_side_sharding(http, addr, loads, accesses).await?
        } else {
            let addrs = tcp_shard_addrs(num_shards, opt.addr);
            info!(&log, "TCP mode"; "addr" => ?&addrs, "sharding" => "client");
            client_side_sharding(http, addrs, loads, accesses).await?
        }
    } else {
        // raw unix mode
        if num_shards < 2 {
            info!(&log, "UDP mode"; "addr" => ?&opt.addr, "sharding" => "server");
            let addr: hyper::Uri = hyper_unix_connector::Uri::new(opt.addr, "/").into();
            server_side_sharding(hyper_unix_connector::UnixClient, addr, loads, accesses).await?
        } else {
            let addrs = shard_addrs(num_shards, &opt.addr, |a| {
                hyper_unix_connector::Uri::new(a, "/").into()
            });
            info!(&log, "UDP mode"; "addr" => ?&addrs, "sharding" => "client");
            client_side_sharding(hyper_unix_connector::UnixClient, addrs, loads, accesses).await?
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

    Ok(())
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
    loads: Vec<Op>,
    accesses: Vec<Op>,
) -> Result<(Vec<std::time::Duration>, std::time::Duration), StdError>
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

    // now, measure the accesses.
    let st = resolver.call(addr).await.map_err(|e| e.into())?;

    // accesses group_by client
    let mut access_by_client: HashMap<usize, Vec<Op>> = Default::default();
    for o in accesses {
        let k = o.client_id();
        access_by_client.entry(k).or_default().push(o);
    }

    let srv = tower_buffer::Buffer::new(kvstore::client(st), access_by_client.len());

    // each client issues its requests closed-loop. So we get one future per client, resolving to a
    // Result<Vec<durations, _>>.
    use futures_util::stream::{FuturesUnordered, TryStreamExt};
    let reqs: FuturesUnordered<_> = access_by_client
        .into_iter()
        .map(|(_, ops)| {
            let mut cl = kvstore::Client::from(srv.clone());
            let mut durs = vec![];
            async move {
                for o in ops {
                    tracing::trace!("starting access");
                    let then = tokio::time::Instant::now();
                    match o {
                        Op::Get(_, k) => cl.get(k).await?,
                        Op::Update(_, k, v) => cl.update(k, v).await?,
                    };

                    tracing::trace!("finished access");
                    durs.push(then.elapsed())
                }

                Ok::<_, StdError>(durs)
            }
        })
        .collect();

    // do the accesses.
    let access_start = tokio::time::Instant::now();
    let durs: Vec<Vec<_>> = reqs.try_collect().await?;
    let access_end = access_start.elapsed();
    let durs = durs.into_iter().flat_map(|x| x.into_iter()).collect();
    Ok((durs, access_end))
}

#[tracing::instrument(level = "debug", skip(resolver, loads, accesses))]
async fn client_side_sharding<R, E, C>(
    mut resolver: R,
    addrs: Vec<hyper::Uri>,
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
        return server_side_sharding(resolver, addrs.pop().unwrap(), loads, accesses).await;
    };

    futures_util::future::poll_fn(|cx| resolver.poll_ready(cx))
        .await
        .map_err(|e| e.into())?;
    let st = resolver
        .call(addrs[0].clone())
        .await
        .map_err(|e| e.into())?;
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

    // now, measure the accesses.
    let num_shards = addrs.len() - 1;
    let shard_fn = move |o: &Op| {
        use std::hash::{Hash, Hasher};
        let mut hasher = ahash::AHasher::default();
        o.key().hash(&mut hasher);
        hasher.finish() as usize % num_shards
    };

    // one client per shard
    let mut cls = vec![];
    for a in addrs {
        let st = resolver.call(a).await.map_err(|e| e.into())?;
        let srv = tower_buffer::Buffer::new(kvstore::client(st), 100_000);
        cls.push(kvstore::Client::from(srv));
    }

    // accesses group_by client
    let mut access_by_client: HashMap<usize, Vec<Op>> = Default::default();
    for o in accesses {
        let k = o.client_id();
        access_by_client.entry(k).or_default().push(o);
    }

    // each client issues its requests closed-loop. So we get one future per client, resolving to a
    // Result<Vec<durations, _>>.
    use futures_util::stream::{FuturesUnordered, TryStreamExt};
    let reqs: FuturesUnordered<_> = access_by_client
        .into_iter()
        .map(|(_, ops)| {
            let cls = cls.clone();
            let mut durs = vec![];
            async move {
                for o in ops {
                    tracing::trace!("starting access");
                    let then = tokio::time::Instant::now();
                    // +1 because the first address is the sharder
                    let shard = shard_fn(&o) + 1;
                    let mut cl = cls[shard].clone();
                    match o {
                        Op::Get(_, k) => cl.get(k).await?,
                        Op::Update(_, k, v) => cl.update(k, v).await?,
                    };

                    tracing::trace!("finished access");
                    durs.push(then.elapsed())
                }

                Ok::<_, StdError>(durs)
            }
        })
        .collect();

    // do the accesses.
    let access_start = tokio::time::Instant::now();
    let durs: Vec<Vec<_>> = reqs.try_collect().await?;
    let access_end = access_start.elapsed();
    let durs = durs.into_iter().flat_map(|x| x.into_iter()).collect();
    Ok((durs, access_end))
}
