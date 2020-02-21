use slog::info;
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
    loads: std::path::PathBuf,
    #[structopt(long)]
    accesses: std::path::PathBuf,

    #[structopt(short, long)]
    out_file: Option<std::path::PathBuf>,
}

#[derive(Debug, Clone)]
enum Op {
    Get(usize, String),
    Update(usize, String, String),
}

impl std::str::FromStr for Op {
    type Err = StdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let sp: Vec<&str> = s.split_whitespace().collect();
        Ok(if sp.len() == 3 && sp[1] == "GET" {
            Op::Get(sp[0].parse()?, sp[2].into())
        } else if sp.len() == 4 && sp[1] == "UPDATE" {
            Op::Update(sp[0].parse()?, sp[1].into(), sp[2].into())
        } else {
            Err(format!("Invalid line: {:?}", s))?
        })
    }
}

fn ops(f: std::path::PathBuf) -> Result<Vec<Op>, StdError> {
    use std::io::BufRead;
    let f = std::fs::File::open(f)?;
    let f = std::io::BufReader::new(f);
    Ok(f.lines().filter_map(|l| l.ok()?.parse().ok()).collect())
}

#[tracing::instrument(level = "debug", skip(resolver, loads, accesses))]
async fn start<R, E, C>(
    mut resolver: R,
    addr: hyper::Uri,
    loads: Vec<Op>,
    accesses: Vec<Op>,
) -> Result<Vec<std::time::Duration>, StdError>
where
    R: tower_service::Service<hyper::Uri, Response = C, Error = E>,
    C: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    E: Into<StdError>,
{
    futures_util::future::poll_fn(|cx| resolver.poll_ready(cx))
        .await
        .map_err(|e| e.into())?;
    let st = resolver.call(addr).await.map_err(|e| e.into())?;
    let mut cl = kvstore::Client::from(st);

    tracing::trace!("starting loads");
    // don't need to time the loads.
    for o in loads {
        match o {
            Op::Get(_, k) => cl.get(k).await?,
            Op::Update(_, k, v) => cl.update(k, v).await?,
        };
    }

    tracing::trace!("finished loads");

    // now, measure latency and throughput for the accesses.
    let mut durs = vec![];
    let num = accesses.len();
    tracing::trace!("starting accesses");
    for o in accesses {
        let span = tracing::span!(tracing::Level::TRACE, "do_operation", op = ?&o);
        let _enter = span.enter();
        let then = tokio::time::Instant::now();
        match o {
            Op::Get(_, k) => cl.get(k).await?,
            Op::Update(_, k, v) => cl.update(k, v).await?,
        };

        tracing::trace!("did operation");
        durs.push(then.elapsed());
    }

    tracing::trace!("finished {:?} accesses", num);
    Ok(durs)
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

    let loads = ops(opt.loads)?;
    let accesses = ops(opt.accesses)?;

    let durs = if let Some(root) = opt.burrito_root {
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
        write!(&mut f, "Latency\n")?;
        for d in durs {
            write!(&mut f, "{}\n", d.as_micros())?;
        }
    }

    Ok(())
}
