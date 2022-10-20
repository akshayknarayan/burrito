use color_eyre::eyre::{bail, eyre, Report, WrapErr};
use kvstore::bin::{tracing_init, Datapath};
use kvstore::serve;
use std::path::PathBuf;
use structopt::StructOpt;
use tracing::info;

#[derive(Debug, StructOpt)]
#[structopt(name = "kvserver")]
struct Opt {
    #[structopt(short, long)]
    port: u16,

    #[structopt(short, long, default_value = "0.0.0.0")]
    ip_addr: std::net::IpAddr,

    #[structopt(short, long)]
    redis_addr: std::net::SocketAddr,

    #[structopt(short, long)]
    num_shards: u16,

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

fn main() -> Result<(), Report> {
    let opt = Opt::from_args();
    color_eyre::install()?;
    tracing_init(
        opt.log,
        opt.trace_time.clone(),
        std::time::Duration::from_secs(5),
    );

    opt.datapath
        .validate_cfg(opt.cfg.as_ref().map(PathBuf::as_path))?;

    info!("KV Server");
    match opt.datapath {
        Datapath::Kernel => run_server_kernel(opt),
        #[cfg(features = "shenango-chunnel")]
        Datapath::Shenango if cfg!(features = "shenango-chunnel") => run_server_shenango(opt),
        Datapath::Shenango => bail!("This binary was not compiled with shenango-chunnel support."),
        #[cfg(features = "dpdk-direct")]
        Datapath::DpdkSingleThread if cfg!(features = "dpdk-direct") => {
            run_server_dpdk_singlethread(opt)
        }
        #[cfg(features = "dpdk-direct")]
        Datapath::DpdkMultiThread if cfg!(features = "dpdk-direct") => {
            run_server_dpdk_multithread(opt)
        }
        Datapath::DpdkSingleThread | Datapath::DpdkMultiThread => Err(eyre!(
            "This binary was not compiled with dpdk-direct support."
        )),
    }
}

fn run_server_kernel(opt: Opt) -> Result<(), Report> {
    info!("using kernel datapath");
    serve(
        bertha::udp::UdpReqChunnel::default(),
        opt.redis_addr,
        opt.ip_addr,
        opt.port,
        opt.num_shards,
        None,
        opt.skip_negotiation,
    )
    .wrap_err(eyre!("serve errored"))
}

#[cfg(features = "shenango-chunnel")]
fn run_server_shenango(opt: Opt) -> Result<(), Report> {
    info!("using shenango datapath");

    let s = shenango_chunnel::ShenangoUdpSkChunnel::new(&opt.cfg.unwrap());
    let l = shenango_chunnel::ShenangoUdpReqChunnel(s);
    serve(
        l,
        opt.redis_addr,
        opt.ip_addr,
        opt.port,
        opt.num_shards,
        None,
        opt.skip_negotiation,
    )
    .wrap_err(eyre!("serve errored"))
}

#[cfg(features = "dpdk-direct")]
fn run_server_dpdk_singlethread(opt: Opt) -> Result<(), Report> {
    info!("using dpdk single-thread datapath");

    let s = dpdk_direct::DpdkUdpSkChunnel::new(&opt.cfg.unwrap())?;
    let l = dpdk_direct::DpdkUdpReqChunnel(s);
    serve(
        l,
        opt.redis_addr,
        opt.ip_addr,
        opt.port,
        opt.num_shards,
        None,
        opt.skip_negotiation,
    )
    .wrap_err(eyre!("serve errored"))
}

#[cfg(features = "dpdk-direct")]
fn run_server_dpdk_multithread(opt: Opt) -> Result<(), Report> {
    info!("using dpdk multi-thread datapath");
    let s = dpdk_direct::DpdkInlineChunnel::new(opt.cfg.unwrap(), (opt.num_shards + 1) as _)?;
    let l = dpdk_direct::DpdkInlineReqChunnel::from(s);
    serve(
        l,
        opt.redis_addr,
        opt.ip_addr,
        opt.port,
        opt.num_shards,
        None,
        opt.skip_negotiation,
    )
    .wrap_err(eyre!("serve errored"))
}
