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
}

#[tokio::main]
async fn main() -> Result<(), failure::Error> {
    let log = burrito_ctl::logger();
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
        .build(|| Histogram::new_with_max(1_000_000, 2).unwrap());
    let sid = subscriber.downcaster();
    let d = tracing::Dispatch::new(subscriber);

    //tracing_subscriber::fmt::init();
    tracing::dispatcher::set_global_default(d.clone()).expect("set tracing global default");

    let durs = if let Some(root) = opt.burrito_root {
        // burrito mode
        info!(&log, "burrito mode"; "burrito_root" => ?root, "addr" => ?&opt.addr);
        let cl = burrito_addr::Client::new(root).await?;
        let addr: hyper::Uri = burrito_addr::Uri::new(&opt.addr).into();
        trace!(&log, "Connecting to rpcserver"; "addr" => ?&addr);
        rpcbench::client_ping(addr, cl, pp, opt.iters, opt.reqs_per_iter).await?
    } else if opt.addr.starts_with("http") {
        // raw tcp mode
        info!(&log, "TCP mode"; "addr" => ?&opt.addr);
        use std::str::FromStr;
        let http = hyper::client::connect::HttpConnector::new();
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
    sid.downcast(&d).unwrap().with_histograms(|hs| {
        for (span_group, hs) in hs {
            for (event_group, h) in hs {
                println!(
                    "{} {}:{}: {} {} {} {} {}",
                    per_iter,
                    span_group,
                    event_group,
                    h.min(),
                    h.value_at_quantile(0.25),
                    h.value_at_quantile(0.5),
                    h.value_at_quantile(0.75),
                    h.max(),
                );
            }
        }
    });

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
