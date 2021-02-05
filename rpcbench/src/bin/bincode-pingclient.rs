use bertha::{
    bincode::SerializeChunnelProject, either::Either, negotiate_client, udp::UdpSkChunnel,
    uds::UnixSkChunnel, util::ProjectLeft, ChunnelConnector, CxList,
};
use burrito_localname_ctl::LocalNameChunnel;
use color_eyre::eyre::{bail, Report};
use kvstore::reliability::KvReliabilityChunnel;
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use structopt::StructOpt;
use tracing::info;
use tracing_error::ErrorLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, StructOpt)]
#[structopt(name = "ping_client")]
struct Opt {
    #[structopt(short, long)]
    burrito_root: Option<PathBuf>,
    #[structopt(long)]
    addr: Option<SocketAddr>,
    #[structopt(long)]
    unix_addr: Option<PathBuf>,
    #[structopt(short, long)]
    work: rpcbench::Work,
    #[structopt(short, long)]
    size_of_req: Option<usize>,
    #[structopt(short, long)]
    iters: usize,
    #[structopt(long)]
    reqs_per_iter: usize,
    #[structopt(short, long)]
    out_file: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Report> {
    let opt = Opt::from_args();
    let subscriber = tracing_subscriber::registry();
    let timing_downcaster = if opt.out_file.is_some() {
        let timing_layer = tracing_timing::Builder::default()
            .no_span_recursion()
            .events(|e: &tracing::Event| {
                let mut val = String::new();
                let mut f = |field: &tracing::field::Field, value: &dyn std::fmt::Debug| {
                    if field.name() == "message" {
                        val.push_str(&format!(" {:?} ", value));
                    } else if field.name() == "which" {
                        val.push_str(&format!(" which={:?} ", value));
                    };
                };
                e.record(&mut f);
                val
            })
            .layer(|| tracing_timing::Histogram::new_with_max(1_000_000, 2).unwrap());
        let timing_downcaster = timing_layer.downcaster();
        let subscriber = subscriber
            .with(timing_layer)
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let d = tracing::Dispatch::new(subscriber);
        d.clone().init();
        Some((timing_downcaster, d))
    } else {
        let subscriber = subscriber
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let d = tracing::Dispatch::new(subscriber);
        d.init();
        None
    };

    color_eyre::install().unwrap();

    let pp = rpcbench::PingParams {
        work: opt.work,
        padding: vec![0u8; opt.size_of_req.unwrap_or_default()],
    };

    let per_iter = opt.reqs_per_iter;
    let durs = match opt {
        Opt {
            burrito_root: Some(root),
            addr: Some(addr),
            iters,
            reqs_per_iter,
            ..
        } => {
            let fncl = |addr| {
                let r = root.clone();
                async move {
                    let lch = LocalNameChunnel::new(
                        r.clone(),
                        None,
                        UnixSkChunnel::with_root(r),
                        SerializeChunnelProject::default(),
                    )
                    .await?;
                    let stack = CxList::from(KvReliabilityChunnel::default())
                        .wrap(SerializeChunnelProject::default())
                        .wrap(lch);
                    let cn = negotiate_client(stack, UdpSkChunnel.connect(()).await?, addr).await?;
                    Ok(ProjectLeft::new(Either::Left(addr), cn))
                }
            };
            info!(?root, ?addr, "Burrito mode");
            rpcbench::client_ping(addr, fncl, pp, iters, reqs_per_iter).await?
        }

        Opt {
            burrito_root: None,
            addr: Some(addr),
            iters,
            reqs_per_iter,
            ..
        } => {
            // raw udp mode
            info!(?addr, "UDP mode");
            let http = |addr| async move {
                let cn = negotiate_client(
                    CxList::from(KvReliabilityChunnel::default())
                        .wrap(SerializeChunnelProject::default()),
                    UdpSkChunnel.connect(()).await?,
                    addr,
                )
                .await?;
                Ok(ProjectLeft::new(addr, cn))
            };
            rpcbench::client_ping(addr, http, pp, iters, reqs_per_iter).await?
        }
        Opt {
            unix_addr: Some(addr),
            burrito_root,
            iters,
            reqs_per_iter,
            ..
        } => {
            // raw unix mode
            info!(?addr, "uds mode");
            let ctr = |addr: PathBuf| {
                let br = burrito_root.clone();
                async move {
                    let u = if let Some(r) = br {
                        UnixSkChunnel::with_root(r).connect(()).await?
                    } else {
                        UnixSkChunnel::default().connect(()).await?
                    };

                    let cn = negotiate_client(
                        CxList::from(KvReliabilityChunnel::default())
                            .wrap(SerializeChunnelProject::default()),
                        u,
                        addr.clone(),
                    )
                    .await?;
                    Ok(ProjectLeft::new(addr, cn))
                }
            };
            rpcbench::client_ping(addr, ctr, pp, iters, reqs_per_iter).await?
        }
        _ => {
            bail!("Bad option set");
        }
    };

    tracing::info!("done");
    if let Some(ref path) = opt.out_file {
        tracing::debug!("writing latencies file");
        let mut f = std::fs::File::create(path)?;
        writeln!(&mut f, "Elapsed_us,Total_us,Server_us")?;
        for (time, t, s) in durs.iter() {
            writeln!(&mut f, "{},{},{}", time.as_micros(), t, s)?;
        }

        tracing::debug!("writing trace file");
        let (downcaster, d) = timing_downcaster.unwrap();
        let path = path.with_extension("trace");
        let mut f = std::fs::File::create(path)?;
        let timing = downcaster.downcast(&d).expect("downcast timing layer");
        timing.force_synchronize();
        // these values are in nanoseconds
        timing.with_histograms(|hs| {
            for (span_group, hs) in hs {
                for (event_group, h) in hs {
                    writeln!(
                        &mut f,
                        "{} {}:{} (ns:min,p25,p50,p75,p95): {} {} {} {} {} {} {}",
                        per_iter,
                        span_group,
                        event_group,
                        h.min(),
                        h.value_at_quantile(0.25),
                        h.value_at_quantile(0.5),
                        h.value_at_quantile(0.75),
                        h.value_at_quantile(0.95),
                        h.max(),
                        h.len(),
                    )
                    .expect("write to trace file");
                }
            }
        });
    }
    Ok(())
}
