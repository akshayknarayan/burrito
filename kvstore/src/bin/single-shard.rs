use color_eyre::eyre::Report;
use kvstore::bin::tracing_init;
use kvstore::single_shard;
use std::net::SocketAddr;
use structopt::StructOpt;
use tracing::info;

#[derive(Debug, StructOpt)]
#[structopt(name = "burrito-lb")]
struct Opt {
    #[structopt(short, long)]
    addr: SocketAddr,

    #[structopt(short, long)]
    internal_addr: Option<SocketAddr>,

    #[structopt(short, long)]
    shenango_cfg: std::path::PathBuf,

    #[structopt(short, long)]
    log: bool,

    #[structopt(short, long)]
    trace_time: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Report> {
    let opt = Opt::from_args();
    color_eyre::install()?;
    tracing_init(opt.log, opt.trace_time, std::time::Duration::from_secs(5)).await;

    info!(addr = ?&opt.addr, internal_addr = ?&opt.internal_addr, "starting shard");
    let listener = shenango_chunnel::ShenangoUdpReqChunnel(
        shenango_chunnel::ShenangoUdpSkChunnel::new(&opt.shenango_cfg),
    );

    single_shard(
        opt.addr,
        listener.clone(),
        opt.internal_addr,
        listener,
        true,
        None,
    )
    .await;
    Ok(())
}
