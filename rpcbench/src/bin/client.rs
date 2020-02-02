use slog::{info, trace};
use structopt::StructOpt;

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

    let durs = if let Some(root) = opt.burrito_root {
        // burrito mode
        info!(&log, "burrito mode"; "burrito_root" => ?root, "addr" => ?&opt.addr);
        let cl = burrito_addr::Client::new(root, &log).await?;
        let addr: hyper::Uri = burrito_addr::Uri::new(&opt.addr).into();
        trace!(&log, "Connecting to rpcserver"; "addr" => ?&addr);
        rpcbench::client_ping(addr, cl, pp, opt.iters).await?
    } else if opt.addr.starts_with("http") {
        // raw tcp mode
        info!(&log, "TCP mode"; "addr" => ?&opt.addr);
        use std::str::FromStr;
        let http = hyper::client::connect::HttpConnector::new();
        let addr: hyper::Uri = hyper::Uri::from_str(&opt.addr)?;

        rpcbench::client_ping(addr, http, pp, opt.iters).await?
    } else {
        // raw unix mode
        info!(&log, "UDS mode"; "addr" => ?&opt.addr);
        let addr: hyper::Uri = hyper_unix_connector::Uri::new(opt.addr, "/").into();
        rpcbench::client_ping(addr, hyper_unix_connector::UnixClient, pp, opt.iters).await?
    };

    println!("Total_us,Server_us");
    for (t, s) in durs {
        println!("{},{}", t, s);
    }

    Ok(())
}
