use slog::{info, trace};
use structopt::StructOpt;
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
    #[structopt(long)]
    reqs_per_iter: usize,
    #[structopt(short, long)]
    out_file: Option<std::path::PathBuf>,
    #[structopt(long, default_value = "flatbuf")]
    burrito_proto: String,
}

#[tokio::main]
async fn main() -> Result<(), failure::Error> {
    let log = burrito_util::logger();
    let opt = Opt::from_args();

    let pp = rpcbench::PingParams {
        work: rpcbench::Work::from_i32(opt.work)
            .ok_or_else(|| failure::format_err!("Invalid work value"))? as i32,
        amount: opt.amount,
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
        tracing::dispatcher::set_global_default(d.clone()).expect("set tracing global default");
    }

    let durs = if let Some(root) = opt.burrito_root {
        // burrito mode
        let addr: hyper::Uri = burrito_addr::Uri::new(&opt.addr).into();
        info!(&log, "burrito mode"; "proto" => &opt.burrito_proto, "burrito_root" => ?root, "addr" => ?&opt.addr);
        match opt.burrito_proto {
            x if x == "tonic" => {
                let cl = burrito_addr::tonic::Client::new(root).await?;
                trace!(&log, "Connecting to rpcserver"; "addr" => ?&addr);
                rpcbench::client_ping(addr, cl, pp, opt.iters, opt.reqs_per_iter).await?
            }
            x if x == "bincode" => {
                let cl =
                    burrito_addr::bincode::StaticClient::new(std::path::PathBuf::from(root)).await;
                trace!(&log, "Connecting to rpcserver"; "addr" => ?&addr);
                rpcbench::client_ping(addr, cl, pp, opt.iters, opt.reqs_per_iter).await?
            }
            x if x == "flatbuf" => {
                let cl = burrito_addr::flatbuf::Client::new(std::path::PathBuf::from(root)).await?;
                trace!(&log, "Connecting to rpcserver"; "addr" => ?&addr);
                rpcbench::client_ping(addr, cl, pp, opt.iters, opt.reqs_per_iter).await?
            }
            x => failure::bail!("Unknown burrito protocol {:?}", &x),
        }
    } else if opt.addr.starts_with("http") {
        // raw tcp mode
        info!(&log, "TCP mode"; "addr" => ?&opt.addr);
        use std::str::FromStr;
        let mut http = hyper::client::connect::HttpConnector::new();
        http.set_nodelay(true);
        let addr: hyper::Uri = hyper::Uri::from_str(&opt.addr)?;

        rpcbench::client_ping(addr, http, pp, opt.iters, opt.reqs_per_iter).await?
    } else {
        // raw unix mode
        info!(&log, "UDS mode"; "addr" => ?&opt.addr);
        let addr: hyper::Uri = hyper_unix_connector::Uri::new(opt.addr, "/").into();
        rpcbench::client_ping(
            addr,
            hyper_unix_connector::UnixClient,
            pp,
            opt.iters,
            opt.reqs_per_iter,
        )
        .await?
    };

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
        use std::io::Write;
        let mut f = std::fs::File::create(path)?;
        write!(&mut f, "Total_us,Server_us\n")?;
        for (t, s) in durs {
            write!(&mut f, "{},{}\n", t, s)?;
        }
    }

    Ok(())
}
