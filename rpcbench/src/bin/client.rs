use slog::trace;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "ping_client")]
struct Opt {
    #[structopt(short, long)]
    burrito_root: Option<String>,
    #[structopt(short, long)]
    addr: String,
    #[structopt(short, long)]
    work: i32,
    #[structopt(short, long)]
    amount: i64,
    #[structopt(short, long)]
    iters: usize,
}

#[tokio::main]
async fn main() -> Result<(), failure::Error> {
    let log = burrito_ctl::logger();
    let opt = Opt::from_args();

    let durs = if let Some(root) = opt.burrito_root {
        let cl = burrito_addr::Client::new(root, &log).await?;
        let addr: hyper::Uri = burrito_addr::Uri::new(&opt.addr).into();
        trace!(&log, "Connecting to rpcserver"; "addr" => ?&addr);
        rpcbench::client_ping(
            addr,
            cl,
            rpcbench::PingParams {
                work: rpcbench::Work::from_i32(opt.work)
                    .ok_or_else(|| failure::format_err!("Invalid work value"))?
                    as i32,
                amount: opt.amount,
            },
            opt.iters,
        )
        .await?
    } else {
        let addr: hyper::Uri = hyper_unix_connector::Uri::new(opt.addr, "/").into();
        trace!(&log, "Connecting to rpcserver"; "addr" => ?&addr);
        rpcbench::client_ping(
            addr,
            hyper_unix_connector::UnixClient,
            rpcbench::PingParams {
                work: rpcbench::Work::from_i32(opt.work)
                    .ok_or_else(|| failure::format_err!("Invalid work value"))?
                    as i32,
                amount: opt.amount,
            },
            opt.iters,
        )
        .await?
    };

    println!("Total_us,Server_us");
    for (t, s) in durs {
        println!("{},{}", t, s);
    }

    Ok(())
}
