use color_eyre::eyre::{bail, Report};
use kvstore::bin::{tracing_init, Datapath};
use kvstore::single_shard;
use std::net::SocketAddr;
use std::path::PathBuf;
use structopt::StructOpt;
use tracing::info;

#[derive(Debug, StructOpt)]
#[structopt(name = "single-shard")]
struct Opt {
    #[structopt(short, long)]
    addr: SocketAddr,

    #[structopt(short, long)]
    skip_negotiation: bool,

    #[structopt(short, long)]
    log: bool,

    #[structopt(short, long)]
    trace_time: Option<std::path::PathBuf>,

    #[structopt(short, long)]
    datapath: Datapath,

    #[structopt(short, long)]
    cfg: Option<std::path::PathBuf>,
}

fn main() -> Result<(), Report> {
    let opt = Opt::from_args();
    color_eyre::install()?;
    tracing_init(opt.log, opt.trace_time, std::time::Duration::from_secs(5));
    opt.datapath
        .validate_cfg(opt.cfg.as_ref().map(PathBuf::as_path))?;

    info!(addr = ?&opt.addr, skip_negotiation = ?&opt.skip_negotiation, "starting shard");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async move {
        match opt.datapath {
            Datapath::Kernel => {
                let listener = bertha::udp::UdpReqChunnel::default();
                single_shard(
                    opt.addr,
                    listener,
                    None,                               // no internal addr
                    None::<bertha::udp::UdpReqChunnel>, // no internal listener
                    false,                              // don't need address embedding
                    None,                               // no ready notification
                    opt.skip_negotiation,
                )
                .await?;
            }
            #[cfg(features = "shenango-chunnel")]
            Datapath::Shenango if cfg!(features = "shenango-chunnel") => {
                let cfg = opt.cfg.unwrap();
                let listener = shenango_chunnel::ShenangoUdpReqChunnel(
                    shenango_chunnel::ShenangoUdpSkChunnel::new(&cfg),
                );

                single_shard(
                    opt.addr,
                    listener,
                    None,                               // no internal addr
                    None::<bertha::udp::UdpReqChunnel>, // no internal listener
                    false,                              // don't need address embedding
                    None,                               // no ready notification
                    opt.skip_negotiation,
                )
                .await?;
            }
            Datapath::Shenango => {
                bail!("This binary was not compiled with shenango-chunnel support.")
            }
            #[cfg(features = "dpdk-direct")]
            Datapath::DpdkSingleThread if cfg!(features = "dpdk-direct") => {
                let cfg = opt.cfg.unwrap();
                let s = dpdk_direct::DpdkUdpSkChunnel::new(&cfg)?;
                let listener = dpdk_direct::DpdkUdpReqChunnel(s);
                single_shard(
                    opt.addr,
                    listener,
                    None,                               // no internal addr
                    None::<bertha::udp::UdpReqChunnel>, // no internal listener
                    false,                              // don't need address embedding
                    None,                               // no ready notification
                    opt.skip_negotiation,
                )
                .await?;
            }
            #[cfg(features = "dpdk-direct")]
            Datapath::DpdkMultiThread if cfg!(features = "dpdk-direct") => {
                let cfg = opt.cfg.unwrap();
                let ch = dpdk_direct::DpdkInlineChunnel::new(cfg, 1)?;
                let listener = dpdk_direct::DpdkInlineReqChunnel::from(ch);
                single_shard(
                    opt.addr,
                    listener,
                    None,                               // no internal addr
                    None::<bertha::udp::UdpReqChunnel>, // no internal listener
                    false,                              // don't need address embedding
                    None,                               // no ready notification
                    opt.skip_negotiation,
                )
                .await?;
            }
            Datapath::DpdkSingleThread | Datapath::DpdkMultiThread => {
                bail!("This binary was not compiled with dpdk-direct support.")
            }
        }

        Ok(())
    })
}
