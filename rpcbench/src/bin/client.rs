use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "ping_client")]
struct Opt {
    #[structopt(short, long)]
    enable_burrito: bool,
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
    let opt = Opt::from_args();

    let addr = if opt.enable_burrito {
        let cl = burrito_addr::Client::new("/tmp/burrito/controller").await?;
        let a = burrito_addr::Uri::new("rpcbench", "/");
        let d = hyper::client::connect::Destination::try_from_uri(a.into())?;
        cl.resolve(d).await?
    } else {
        opt.addr
    };

    let durs = rpcbench::client_ping(
        addr,
        rpcbench::PingParams {
            work: rpcbench::Work::from_i32(opt.work)
                .ok_or_else(|| failure::format_err!("Invalid work value"))?
                as i32,
            amount: opt.amount,
        },
        opt.iters,
    )
    .await?;

    println!("Total_us,Server_us");

    for (t, s) in durs {
        println!("{},{}", t, s);
    }

    Ok(())
}
