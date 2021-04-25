use color_eyre::eyre::{eyre, Report};
use kvstore::serve;
use structopt::StructOpt;
use tracing::{info, info_span};
use tracing_error::ErrorLayer;
use tracing_futures::Instrument;
use tracing_subscriber::prelude::*;

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
    let subscriber = tracing_subscriber::registry();
    let timing_downcaster = if let Some(ref trace_file) = &opt.trace_time {
        let timing_layer = tracing_timing::Builder::default()
            .no_span_recursion()
            .layer(|| tracing_timing::Histogram::new_with_max(1_000_000, 2).unwrap());
        let timing_downcaster = timing_layer.downcaster();
        let subscriber = subscriber
            .with(timing_layer)
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let d = tracing::Dispatch::new(subscriber);
        d.clone().init();
        Some((trace_file.clone(), timing_downcaster, d))
    } else {
        let subscriber = subscriber
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let d = tracing::Dispatch::new(subscriber);
        d.init();
        None
    };

    if let Some((trace_file, timing_downcaster, d)) = timing_downcaster {
        tokio::spawn(async move {
            let timing = timing_downcaster
                .downcast(&d)
                .expect("downcast timing layer");

            loop {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                let mut f = std::fs::File::create(&trace_file).unwrap();
                info!("writing tracing info");
                dump_tracing(timing, &mut f).unwrap();
            }
        });
    }

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

fn dump_tracing(
    timing: &'_ tracing_timing::TimingLayer,
    f: &mut std::fs::File,
) -> Result<(), Report> {
    use std::io::prelude::*;
    timing.force_synchronize();
    timing.with_histograms(|hs| {
        for (span_group, hs) in hs {
            for (event_group, h) in hs {
                let tag = format!("{}.{}", span_group, event_group);
                write!(
                    f,
                    "tracing: \
                        event = {}, \
                        min   = {}, \
                        p25   = {}, \
                        p50   = {}, \
                        p75   = {}, \
                        p95   = {}, \
                        max   = {}, \
                        cnt   = {} \
                        ",
                    &tag,
                    h.min(),
                    h.value_at_quantile(0.25),
                    h.value_at_quantile(0.5),
                    h.value_at_quantile(0.75),
                    h.value_at_quantile(0.95),
                    h.max(),
                    h.len(),
                )?;
            }
        }

        Ok::<_, Report>(())
    })?;

    Ok(())
}
