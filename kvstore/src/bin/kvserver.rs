use color_eyre::eyre::{eyre, Report};
use kvstore::bin::tracing_init;
use kvstore::serve;
use structopt::StructOpt;
use tracing::{info, info_span};
use tracing_futures::Instrument;

#[cfg(all(feature = "use-shenango", feature = "use-dpdk-direct"))]
compile_error!("Features \"use-shenango\" and \"use-dpdk-direct\" are incompatible");

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
    shenango_cfg: Option<std::path::PathBuf>,

    #[structopt(short, long, default_value = "none")]
    batch_mode: kvstore::BatchMode,

    #[structopt(short, long)]
    num_shards: u16,

    #[structopt(short, long)]
    fragment_stack: bool,

    #[structopt(short, long)]
    skip_negotiation: bool,

    #[structopt(short, long)]
    log: bool,

    #[structopt(short, long)]
    trace_time: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Report> {
    let opt = Opt::from_args();
    color_eyre::install()?;
    tracing_init(
        opt.log,
        opt.trace_time.clone(),
        std::time::Duration::from_secs(5),
    )
    .await;

    info!("KV Server");
    run_server(opt).await?;
    Ok(())
}

#[cfg(all(not(feature = "use-shenango"), not(feature = "use-dpdk-direct")))]
async fn run_server(opt: Opt) -> Result<(), Report> {
    info!("using default feature");
    if opt.shenango_cfg.is_some() {
        tracing::warn!(cfg_file = ?opt.shenango_cfg, "Shenango is disabled, ignoring config");
    }

    serve(
        bertha::udp::UdpReqChunnel::default(),
        opt.redis_addr,
        opt.ip_addr,
        opt.port,
        opt.num_shards,
        None,
        opt.batch_mode,
        opt.fragment_stack,
        opt.skip_negotiation,
    )
    .instrument(info_span!("server"))
    .await
}

#[cfg(feature = "use-shenango")]
async fn run_server(opt: Opt) -> Result<(), Report> {
    info!("using shenango feature");
    if opt.shenango_cfg.is_none() {
        return Err(eyre!(
            "If shenango feature is enabled, shenango_cfg must be specified"
        ));
    }

    let s = shenango_chunnel::ShenangoUdpSkChunnel::new(&opt.shenango_cfg.unwrap());
    let l = shenango_chunnel::ShenangoUdpReqChunnel(s);
    serve(
        l,
        opt.redis_addr,
        opt.ip_addr,
        opt.port,
        opt.num_shards,
        None,
        opt.batch_mode,
        opt.fragment_stack,
        opt.skip_negotiation,
    )
    .instrument(info_span!("server"))
    .await
}

#[cfg(feature = "use-dpdk-direct")]
async fn run_server(opt: Opt) -> Result<(), Report> {
    info!("using dpdk feature");
    if opt.shenango_cfg.is_none() {
        return Err(eyre!(
            "If use-dpdk-direct feature is enabled, shenango_cfg must be specified"
        ));
    }

    let s = dpdk_direct::DpdkUdpSkChunnel::new(&opt.shenango_cfg.unwrap())?;
    let l = dpdk_direct::DpdkUdpReqChunnel(s);
    serve(
        l,
        opt.redis_addr,
        opt.ip_addr,
        opt.port,
        opt.num_shards,
        None,
        opt.batch_mode,
        opt.fragment_stack,
        opt.skip_negotiation,
    )
    .instrument(info_span!("server"))
    .await
}
