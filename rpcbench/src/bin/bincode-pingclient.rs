use slog::{debug, info};
use structopt::StructOpt;
use tower::Service;
use tracing_timing::{Builder, Histogram};

#[derive(Debug, StructOpt)]
#[structopt(name = "ping_client")]
struct Opt {
    #[structopt(short, long)]
    burrito_root: Option<String>,
    #[structopt(long)]
    addr: String,
    #[structopt(short, long)]
    work: i32,
    #[structopt(long)]
    amount: i64,
    #[structopt(short, long)]
    iters: usize,
    #[structopt(short, long)]
    size_of_req: Option<usize>,
    #[structopt(long)]
    reqs_per_iter: usize,
    #[structopt(short, long)]
    out_file: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), failure::Error> {
    let log = burrito_util::logger();
    let opt = Opt::from_args();

    let pp = rpcbench::PingParams {
        work: rpcbench::Work::from_i32(opt.work)
            .ok_or_else(|| failure::format_err!("Invalid work value"))? as i32,
        amount: opt.amount,
        padding: vec![0u8; opt.size_of_req.unwrap_or_default()],
    };

    let per_iter = opt.reqs_per_iter;
    let subscriber = Builder::default()
        .no_span_recursion()
        .events(|e: &tracing::Event| {
            let mut val = String::new();
            let mut f = |field: &tracing::field::Field, value: &dyn std::fmt::Debug| {
                if field.name() == "message" {
                    val.push_str(&format!(" {:?} ", value));
                } else if field.name() == "which" {
                    val.push_str(&format!(" which={:?} ", value));
                };
            };
            e.record(&mut f);
            val
        })
        .build(|| Histogram::new_with_max(10_000_000, 2).unwrap());
    let sid = subscriber.downcaster();
    let d = tracing::Dispatch::new(subscriber);

    if let None = opt.out_file {
        tracing_subscriber::fmt::init();
    } else {
        //tracing_subscriber::fmt::init();
        tracing::dispatcher::set_global_default(d.clone()).expect("set tracing global default");
    }

    let durs = if let Some(root) = opt.burrito_root {
        // burrito mode
        let addr: hyper::Uri = burrito_addr::Uri::new(&opt.addr).into();
        info!(&log, "burrito mode"; "burrito_root" => ?root, "addr" => ?&opt.addr);
        let cl = burrito_addr::Client::new(root).await?;
        let fncl = |addr| {
            let mut cl = cl.clone();
            async move {
                futures_util::future::poll_fn(|cx| cl.poll_ready(cx))
                    .await
                    .map_err(|e| e.compat())?;
                cl.call(addr).await.map_err(|e| e.compat())
            }
        };
        debug!(&log, "Connecting to rpcserver"; "addr" => ?&addr);
        rpcbench::bincode_client_ping(addr, fncl, pp.into(), opt.iters, opt.reqs_per_iter).await?
    } else if opt.addr.starts_with("http") {
        // raw tcp mode
        info!(&log, "TCP mode"; "addr" => ?&opt.addr);
        let addr: std::net::SocketAddr = opt.addr.parse()?;
        let http = |addr| async move { tokio::net::TcpStream::connect(addr).await };
        rpcbench::bincode_client_ping(addr, http, pp.into(), opt.iters, opt.reqs_per_iter).await?
    } else {
        // raw unix mode
        info!(&log, "UDS mode"; "addr" => ?&opt.addr);
        let addr: std::path::PathBuf = opt.addr.parse()?;
        let ctr = |addr| async move { tokio::net::UnixStream::connect(addr).await };
        rpcbench::bincode_client_ping(addr, ctr, pp.into(), opt.iters, opt.reqs_per_iter).await?
    };

    sid.downcast(&d).unwrap().force_synchronize();

    tracing::info!("done");
    if let Some(ref path) = opt.out_file {
        tracing::debug!("writing trace file");
        use std::io::Write;
        let path = path.with_extension("trace");
        let mut f = std::fs::File::create(path)?;
        // these values are in nanoseconds
        sid.downcast(&d).unwrap().with_histograms(|hs| {
            for (span_group, hs) in hs {
                for (event_group, h) in hs {
                    write!(
                        &mut f,
                        "{} {}:{}: {} {} {} {} {}\n",
                        per_iter,
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
        tracing::debug!("writing latencies file");
        use std::io::Write;
        let mut f = std::fs::File::create(path)?;
        write!(&mut f, "Elapsed_us,Total_us,Server_us\n")?;
        for (time, t, s) in durs {
            write!(&mut f, "{},{},{}\n", time.as_micros(), t, s)?;
        }
    }

    Ok(())
}
