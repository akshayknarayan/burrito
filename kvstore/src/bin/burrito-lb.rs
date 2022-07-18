use color_eyre::eyre::Report;
use kvstore::bin::{tracing_init, Datapath};
use kvstore::serve_lb;
use std::net::SocketAddr;
use std::path::PathBuf;
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
    skip_negotiation: bool,

    #[structopt(short, long)]
    log: bool,

    #[structopt(short, long)]
    trace_time: Option<PathBuf>,

    #[structopt(short, long)]
    datapath: Datapath,

    #[structopt(short, long)]
    cfg: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Report> {
    let opt = Opt::from_args();
    color_eyre::install()?;
    tracing_init(opt.log, opt.trace_time, std::time::Duration::from_secs(5));
    opt.datapath
        .validate_cfg(opt.cfg.as_ref().map(PathBuf::as_path))?;

    info!(addr = ?&opt.addr, shards = ?&opt.shards, skip_negotiation = ?&opt.skip_negotiation, "starting load balancer");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    rt.block_on(async move {
        match opt.datapath {
            Datapath::Kernel => {
                let listener = bertha::udp::UdpReqChunnel::default();
                serve_lb(
                    opt.addr,
                    opt.shards.clone(),
                    listener,
                    opt.shards,
                    bertha::udp::UdpSkChunnel,
                    opt.redis_addr,
                    None, // no ready notification
                )
                .await
            }
            Datapath::Shenango => {
                let cfg = opt.cfg.unwrap();
                let sk = shenango_chunnel::ShenangoUdpSkChunnel::new(&cfg);
                let listener = shenango_chunnel::ShenangoUdpReqChunnel(sk.clone());
                serve_lb(
                    opt.addr,
                    opt.shards.clone(),
                    listener,
                    opt.shards,
                    sk,
                    opt.redis_addr,
                    None, // no ready notification
                )
                .await
            }
            Datapath::DpdkSingleThread => {
                let cfg = opt.cfg.unwrap();
                let sk = dpdk_direct::DpdkUdpSkChunnel::new(&cfg)?;
                let listener = dpdk_direct::DpdkUdpReqChunnel(sk.clone());
                serve_lb(
                    opt.addr,
                    opt.shards.clone(),
                    listener,
                    opt.shards,
                    sk,
                    opt.redis_addr,
                    None, // no ready notification
                )
                .await
            }
            Datapath::DpdkMultiThread => {
                let cfg = opt.cfg.unwrap();
                let sk = dpdk_direct::DpdkInlineChunnel::new(cfg, 1)?;
                let listener = dpdk_direct::DpdkInlineReqChunnel::from(sk.clone());
                serve_lb(
                    opt.addr,
                    opt.shards.clone(),
                    listener,
                    opt.shards,
                    sk,
                    opt.redis_addr,
                    None, // no ready notification
                )
                .await
            }
        }
    })?;
    Ok(())
}
