use color_eyre::eyre::Report;
use kvstore::single_shard;
use std::net::SocketAddr;
use structopt::StructOpt;
use tracing::info;
use tracing_error::ErrorLayer;
use tracing_subscriber::prelude::*;

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
