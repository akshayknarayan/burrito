use color_eyre::eyre::{bail, eyre, Report};
use kvstore::bin::{tracing_init, Datapath};
use kvstore::single_shard;
use std::net::SocketAddr;
use std::path::PathBuf;
use structopt::StructOpt;
use tracing::{debug_span, info};
use tracing_futures::Instrument;

#[derive(Debug, StructOpt)]
#[structopt(name = "single-shard")]
struct Opt {
    #[structopt(long)]
    addr: Vec<SocketAddr>,

    #[structopt(long)]
    skip_negotiation: bool,

    #[structopt(long)]
    log: bool,

    #[structopt(long)]
    trace_time: Option<std::path::PathBuf>,

    #[structopt(long)]
    datapath: Datapath,

    #[structopt(long)]
    cfg: Option<std::path::PathBuf>,
}

macro_rules! spawn_n {
    ($listener: expr, $opt: expr) => {{
        let mut addrs = $opt.addr.into_iter();
        let last = addrs.next().ok_or_else(|| eyre!("need at least one addr"))?;
        let mut i = 0;
        while let Some(addr) = addrs.next() {
            i += 1;
            let l = $listener.clone();
            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build().unwrap();
                rt.block_on(
                    single_shard(
                        addr,
                        l,
                        None,                               // no internal addr
                        None::<bertha::udp::UdpReqChunnel>, // no internal listener
                        false,                              // don't need address embedding
                        None,                               // no ready notification
                        $opt.skip_negotiation,
                    )
                    .instrument(debug_span!( "serve_lb thread", thread=?i)),
                )
                    .unwrap();
                });
        }

    // thread 0
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build().unwrap();
    rt.block_on(
                single_shard(
                    last,
                    $listener,
                    None,                               // no internal addr
                    None::<bertha::udp::UdpReqChunnel>, // no internal listener
                    false,                              // don't need address embedding
                    None,                               // no ready notification
                    $opt.skip_negotiation,
                )
                .instrument(debug_span!("serve_lb thread", thread=?i)),
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

    info!(addr = ?&opt.addr, skip_negotiation = ?&opt.skip_negotiation, "starting shard");

    match opt.datapath {
        Datapath::Kernel => {
            let listener = bertha::udp::UdpReqChunnel::default();
            spawn_n!(listener, opt);
        }
        #[cfg(feature = "shenango-chunnel")]
        Datapath::Shenango if cfg!(feature = "shenango-chunnel") => {
            let cfg = opt.cfg.unwrap();
            let listener = shenango_chunnel::ShenangoUdpReqChunnel(
                shenango_chunnel::ShenangoUdpSkChunnel::new(&cfg),
            );

            spawn_n!(listener, opt);
        }
        Datapath::Shenango => {
            bail!("This binary was not compiled with shenango-chunnel support.")
        }
        #[cfg(feature = "dpdk-direct")]
        Datapath::DpdkSingleThread if cfg!(feature = "dpdk-direct") => {
            let cfg = opt.cfg.unwrap();
            let s = dpdk_direct::DpdkUdpSkChunnel::new(&cfg)?;
            let listener = dpdk_direct::DpdkUdpReqChunnel(s);
            spawn_n!(listener, opt);
        }
        #[cfg(feature = "dpdk-direct")]
        Datapath::DpdkMultiThread if cfg!(feature = "dpdk-direct") => {
            let cfg = opt.cfg.unwrap();
            let ch = dpdk_direct::DpdkInlineChunnel::new(cfg, opt.addr.len())?;
            let listener = dpdk_direct::DpdkInlineReqChunnel::from(ch);
            spawn_n!(listener, opt);
        }
        Datapath::DpdkSingleThread | Datapath::DpdkMultiThread => {
            bail!("This binary was not compiled with dpdk-direct support.")
        }
    }

    Ok(())
}
