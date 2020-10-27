use color_eyre::eyre::Report;
use kvstore::serve;
use structopt::StructOpt;
use tracing::{info, info_span};
use tracing_error::ErrorLayer;
use tracing_futures::Instrument;
use tracing_subscriber::prelude::*;
//use tracing_timing::{Builder, Histogram};

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

    #[structopt(long, default_value = "/tmp/burrito")]
    burrito_root: std::path::PathBuf,

    #[structopt(short, long)]
    log: bool,
}

#[tokio::main]
async fn main() -> Result<(), Report> {
    let opt = Opt::from_args();
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(ErrorLayer::default());
    subscriber.init();
    color_eyre::install()?;

    info!("KV Server");
    serve(opt.redis_addr, opt.ip_addr, opt.port, opt.num_shards, None)
        .instrument(info_span!("server"))
        .await?;

    Ok(())
}

//fn write_tracing<S>(s: S) -> tracing_subscriber::layer::Layered<tracing_timing::TimingLayer, S>
//where
//    S: tracing_subscriber::layer::SubscriberExt,
//{
//    let layer = Builder::default()
//        .no_span_recursion()
//        .layer(|| Histogram::new_with_max(10_000_000, 2).unwrap());
//    let sid = layer.downcaster();
//    let d = tracing::Dispatch::new(
//        layer
//            .clone()
//            .with_subscriber(tracing_subscriber::Registry::default()),
//    );
//    std::thread::spawn(move || loop {
//        std::thread::sleep(std::time::Duration::from_secs(1));
//        sid.downcast(&d).unwrap().force_synchronize();
//        sid.downcast(&d).unwrap().with_histograms(|hs| {
//            for (span_group, hs) in hs {
//                for (event_group, h) in hs {
//                    let tag = format!("{}:{}", span_group, event_group);
//                    info!(
//                        event = %&tag,
//                        min   = %h.min(),
//                        p25   = %h.value_at_quantile(0.25),
//                        p50   = %h.value_at_quantile(0.5),
//                        p75   = %h.value_at_quantile(0.75),
//                        max   = %h.max(),
//                        cnt   = %h.len(),
//                        "tracing"
//                    );
//                }
//            }
//        });
//    });
//
//    s.with(layer)
//}
