use kvstore_ycsb::{ops, Op};
use slog::info;
use std::collections::HashMap;
use std::error::Error;
use std::str::FromStr;
use structopt::StructOpt;
use tracing_timing::{Builder, Histogram};

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

    #[structopt(long)]
    accesses: std::path::PathBuf,

    #[structopt(short, long)]
    out_file: Option<std::path::PathBuf>,
}

#[tracing::instrument(level = "debug", skip(resolver, loads, accesses))]
async fn start<R, E, C>(
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
    let srv = tower_buffer::Buffer::new(kvstore::client(st), 2_000);

    // make all the futures at once, tower_buffer will deal with the rest.
    use futures_util::stream::{FuturesUnordered, TryStreamExt};
    let reqs: FuturesUnordered<_> = accesses
        .into_iter()
        .map(|o| {
            let mut cl = kvstore::Client::from(srv.clone());
            async move {
                tracing::trace!("starting access");
                let then = tokio::time::Instant::now();
                match o {
                    Op::Get(_, k) => cl.get(k).await?,
                    Op::Update(_, k, v) => cl.update(k, v).await?,
                };

                tracing::trace!("finished access");
                Ok::<_, StdError>(then.elapsed())
            }
        })
        .collect();

    // do the accesses.
    let access_start = tokio::time::Instant::now();
    let durs: Vec<_> = reqs.try_collect().await?;
    let access_end = access_start.elapsed();
    Ok((durs, access_end))

    //// now, measure the accesses.
    //let st = resolver.call(addr).await.map_err(|e| e.into())?;

    // accesses group_by client
    //let mut access_by_client: HashMap<usize, Vec<Op>> = Default::default();
    //for o in accesses {
    //    let k = o.client_id();
    //    access_by_client.entry(k).or_default().push(o);
    //}

    //let srv = tower_buffer::Buffer::new(kvstore::client(st), access_by_client.len());

    //// each client issues its requests closed-loop. So we get one future per client, resolving to a
    //// Result<Vec<durations, _>>.
    //use futures_util::stream::{FuturesUnordered, TryStreamExt};
    //let reqs: FuturesUnordered<_> = access_by_client
    //    .into_iter()
    //    .map(|(_, ops)| {
    //        let mut cl = kvstore::Client::from(srv.clone());
    //        let mut durs = vec![];
    //        async move {
    //            for o in ops {
    //                tracing::trace!("starting access");
    //                let then = tokio::time::Instant::now();
    //                match o {
    //                    Op::Get(_, k) => cl.get(k).await?,
    //                    Op::Update(_, k, v) => cl.update(k, v).await?,
    //                };

    //                tracing::trace!("finished access");
    //                durs.push(then.elapsed())
    //            }

    //            Ok::<_, StdError>(durs)
    //        }
    //    })
    //    .collect();

    //// do the accesses.
    //let access_start = tokio::time::Instant::now();
    //let durs: Vec<Vec<_>> = reqs.try_collect().await?;
    //let access_end = access_start.elapsed();
    //let durs = durs.into_iter().flat_map(|x| x.into_iter()).collect();
    //Ok((durs, access_end))
}

#[tokio::main]
async fn main() -> Result<(), StdError> {
    let log = burrito_ctl::logger();
    let opt = Opt::from_args();

    let subscriber = Builder::default()
        .no_span_recursion()
        .build(|| Histogram::new_with_max(10_000_000, 2).unwrap());
    let sid = subscriber.downcaster();
    let d = tracing::Dispatch::new(subscriber);

    if let None = opt.out_file {
        tracing_subscriber::fmt::init();
    } else {
        tracing::dispatcher::set_global_default(d.clone()).expect("set tracing global default");
    }

    let loads = ops(opt.accesses.with_extension("load"))?;
    let accesses = ops(opt.accesses)?;

    let (mut durs, time) = if let Some(root) = opt.burrito_root {
        // burrito mode
        let addr: hyper::Uri = burrito_addr::Uri::new(&opt.addr).into();
        info!(&log, "burrito mode"; "proto" => &opt.burrito_proto, "burrito_root" => ?root, "addr" => ?&opt.addr);
        match opt.burrito_proto {
            x if x == "tonic" => {
                let cl = burrito_addr::tonic::Client::new(root).await?;
                start(cl, addr, loads, accesses).await?
            }
            x if x == "bincode" => {
                let cl =
                    burrito_addr::bincode::StaticClient::new(std::path::PathBuf::from(root)).await;
                start(cl, addr, loads, accesses).await?
            }
            x if x == "flatbuf" => {
                let cl = burrito_addr::flatbuf::Client::new(std::path::PathBuf::from(root)).await?;
                start(cl, addr, loads, accesses).await?
            }
            x => Err(format!("Unknown burrito protocol {:?}", &x))?,
        }
    } else if opt.addr.starts_with("http") {
        // raw tcp mode
        info!(&log, "TCP mode"; "addr" => ?&opt.addr);
        let mut http = hyper::client::connect::HttpConnector::new();
        http.set_nodelay(true);
        let addr: hyper::Uri = hyper::Uri::from_str(&opt.addr)?;
        start(http, addr, loads, accesses).await?
    } else {
        // raw unix mode
        info!(&log, "UDS mode"; "addr" => ?&opt.addr);
        let addr: hyper::Uri = hyper_unix_connector::Uri::new(opt.addr, "/").into();
        start(hyper_unix_connector::UnixClient, addr, loads, accesses).await?
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

    sid.downcast(&d).unwrap().force_synchronize();

    if let Some(ref path) = opt.out_file {
        use std::io::Write;
        let path = path.with_extension("trace");
        let mut f = std::fs::File::create(path)?;
        sid.downcast(&d).unwrap().with_histograms(|hs| {
            for (span_group, hs) in hs {
                for (event_group, h) in hs {
                    write!(
                        &mut f,
                        "{}:{}: {} {} {} {} {}\n",
                        span_group,
                        event_group,
                        h.min(),
                        h.value_at_quantile(0.25),
                        h.value_at_quantile(0.5),
                        h.value_at_quantile(0.75),
                        h.max(),
                    )
                    .expect("write to trace file");
                }
            }
        });
    }

    if let Some(path) = opt.out_file {
        use std::io::Write;
        let mut f = std::fs::File::create(path)?;
        write!(&mut f, "Latency_us\n")?;
        for d in durs {
            write!(&mut f, "{}\n", d.as_micros())?;
        }
    }

    Ok(())
}
