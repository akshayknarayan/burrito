use color_eyre::eyre::{eyre, Report};
use kvstore::bin::tracing_init;
use kvstore::serve;
use structopt::StructOpt;
use tracing::{info, info_span};
use tracing_futures::Instrument;

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

    #[structopt(short, long)]
    num_shards: u16,

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

#[cfg(not(feature = "use-shenango"))]
async fn run_server(opt: Opt) -> Result<(), Report> {
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
    )
    .instrument(info_span!("server"))
    .await
}

#[cfg(feature = "use-shenango")]
async fn run_server(opt: Opt) -> Result<(), Report> {
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
    )
    .instrument(info_span!("server"))
    .await
}
