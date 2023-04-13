use color_eyre::eyre::{bail, Report};
use kvstore::bin::{tracing_init, Datapath};
use kvstore::serve_lb;
use std::net::SocketAddr;
use std::path::PathBuf;
use structopt::StructOpt;
use tracing::{debug_span, info};
use tracing_futures::Instrument;

#[derive(Debug, StructOpt)]
#[structopt(name = "burrito-lb")]
struct Opt {
    #[structopt(long, default_value = "0.0.0.0:4242")]
    addr: SocketAddr,

    #[structopt(long)]
    shards: Vec<SocketAddr>,

    #[structopt(long)]
    redis_addr: SocketAddr,

    #[structopt(long)]
    skip_negotiation: bool,

    #[structopt(long)]
    log: bool,

    #[structopt(long)]
    trace_time: Option<PathBuf>,

    #[structopt(long)]
    datapath: Datapath,

    #[structopt(long)]
    cfg: Option<PathBuf>,

    #[structopt(long)]
    num_threads: usize,
}

macro_rules! spawn_n {
    ($listener: expr, $connector: expr, $opt: expr) => {{
    for i in 1..$opt.num_threads {
        let l = $listener.clone();
        let c = $connector.clone();
        let s = $opt.shards.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build().unwrap();
            rt.block_on(
                serve_lb(
                    $opt.addr,
                    s.clone(),
                    l,
                    s,
                    c,
                    $opt.redis_addr,
                    None, // no ready notification
                )
                .instrument(debug_span!("serve_lb thread", thread=?i)),
            )
            .unwrap();
        });
    }

    // thread 0
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(
        serve_lb(
            $opt.addr,
            $opt.shards.clone(),
            $listener,
            $opt.shards,
            $connector,
            $opt.redis_addr,
            None, // no ready notification
        )
        .instrument(debug_span!("serve_lb thread", thread = 0)),
    )
    .unwrap();
   }}
}

fn main() -> Result<(), Report> {
    let opt = Opt::from_args();
    color_eyre::install()?;
    tracing_init(opt.log, opt.trace_time, std::time::Duration::from_secs(5));
    #[cfg(feature = "shenango-chunnel")]
    info!("shenango feature is enabled");
    #[cfg(feature = "dpdk-direct")]
    info!("dpdk-direct feature is enabled");
    opt.datapath
        .validate_cfg(opt.cfg.as_ref().map(PathBuf::as_path))?;

    info!(addr = ?&opt.addr, shards = ?&opt.shards, skip_negotiation = ?&opt.skip_negotiation, "starting load balancer");

    match opt.datapath {
        Datapath::Kernel => {
            spawn_n!(bertha::udp::UdpReqChunnel, bertha::udp::UdpSkChunnel, opt);
        }
        #[cfg(feature = "shenango-chunnel")]
        Datapath::Shenango if cfg!(feature = "shenango-chunnel") => {
            let cfg = opt.cfg.unwrap();
            let sk = shenango_chunnel::ShenangoUdpSkChunnel::new(&cfg);
            let listener = shenango_chunnel::ShenangoUdpReqChunnel(sk.clone());
            spawn_n!(listener, sk, opt);
        }
        Datapath::Shenango => {
            bail!("This binary was not compiled with shenango-chunnel support.");
        }
        #[cfg(feature = "dpdk-direct")]
        Datapath::DpdkSingleThread if cfg!(feature = "dpdk-direct") => {
            let cfg = opt.cfg.unwrap();
            let sk = dpdk_direct::DpdkUdpSkChunnel::new(&cfg)?;
            let listener = dpdk_direct::DpdkUdpReqChunnel(sk.clone());
            spawn_n!(listener, sk, opt);
        }
        #[cfg(feature = "dpdk-direct")]
        Datapath::DpdkMultiThread if cfg!(feature = "dpdk-direct") => {
            let cfg = opt.cfg.unwrap();
            let sk = dpdk_direct::DpdkInlineChunnel::new(cfg, opt.num_threads)?;
            let listener = dpdk_direct::DpdkInlineReqChunnel::from(sk.clone());
            spawn_n!(listener, sk, opt);
        }
        Datapath::DpdkSingleThread | Datapath::DpdkMultiThread => {
            bail!("This binary was not compiled with dpdk-direct support.")
        }
    }

    Ok(())
}
