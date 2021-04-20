use color_eyre::eyre::Report;
use kvstore::serve_lb;
use std::net::SocketAddr;
use structopt::StructOpt;
use tracing::info;
use tracing_error::ErrorLayer;
use tracing_subscriber::prelude::*;

#[derive(Debug, StructOpt)]
#[structopt(name = "burrito-lb")]
struct Opt {
    #[structopt(short, long, default_value = "0.0.0.0:4242")]
    addr: SocketAddr,

    #[structopt(short, long)]
    shards: Vec<SocketAddr>,

    #[structopt(short, long)]
    redis_addr: SocketAddr,

    #[structopt(short, long)]
    shenango_cfg: std::path::PathBuf,

    #[structopt(short, long)]
    log: bool,
}

#[tokio::main]
async fn main() -> Result<(), Report> {
    let opt = Opt::from_args();
    color_eyre::install()?;
    if opt.log {
        let subscriber = tracing_subscriber::registry();
        let subscriber = subscriber
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let d = tracing::Dispatch::new(subscriber);
        d.init();
    }

    let shard_internal_addr_from_external = |sa: SocketAddr| {
        let mut internal_addr = sa;
        internal_addr.set_port(sa.port() + 100);
        internal_addr
    };

    info!(addr = ?&opt.addr, shards = ?&opt.shards, "starting load balancer");
    let listener = shenango_chunnel::ShenangoUdpReqChunnel(
        shenango_chunnel::ShenangoUdpSkChunnel::new(&opt.shenango_cfg),
    );

    let shards_internal = opt
        .shards
        .iter()
        .copied()
        .map(shard_internal_addr_from_external)
        .collect();
    serve_lb(
        listener,
        opt.addr,
        opt.shards,
        shards_internal,
        opt.redis_addr,
        None,
    )
    .await
}
