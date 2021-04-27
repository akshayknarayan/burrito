use color_eyre::eyre::Report;
use kvstore::bin::tracing_init;
use kvstore::serve_lb;
use std::net::SocketAddr;
use structopt::StructOpt;
use tracing::info;

#[derive(Debug, StructOpt)]
#[structopt(name = "burrito-lb")]
struct Opt {
    #[structopt(short, long, default_value = "0.0.0.0:4242")]
    addr: SocketAddr,

    #[structopt(long)]
    shards: Vec<SocketAddr>,

    #[structopt(short, long)]
    redis_addr: SocketAddr,

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

    let shard_internal_addr_from_external = |sa: SocketAddr| {
        let mut internal_addr = sa;
        internal_addr.set_port(sa.port() + 100);
        internal_addr
    };

    info!(addr = ?&opt.addr, shards = ?&opt.shards, "starting load balancer");
    let sk = shenango_chunnel::ShenangoUdpSkChunnel::new(&opt.shenango_cfg);
    let listener = shenango_chunnel::ShenangoUdpReqChunnel(sk.clone());

    let shards_internal = opt
        .shards
        .iter()
        .copied()
        .map(shard_internal_addr_from_external)
        .collect();
    serve_lb(
        opt.addr,
        opt.shards,
        listener,
        shards_internal,
        sk,
        opt.redis_addr,
        None,
    )
    .await
}
