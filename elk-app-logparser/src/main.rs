//! Take in messages containing one or more server log lines, and publish messages grouped by
//! client IP, ordered by timestamp.
//!
//! Listen on HTTP/HTTPS/QUIC (with load balancing across multiple instances / threads) and publish
//! on pub/sub chunnel with reconfigurable ordering.

use std::net::SocketAddr;

use color_eyre::eyre::{Report, WrapErr};
use structopt::StructOpt;
use tracing::{info, instrument, warn};
use tracing_error::ErrorLayer;
use tracing_subscriber::prelude::*;

#[derive(Debug, StructOpt)]
#[structopt(name = "logparser")]
struct Opt {
    #[structopt(long)]
    redis_addr: String,

    #[structopt(long)]
    kafka_addr: Option<String>,

    #[structopt(long)]
    gcp_project_name: String,

    #[structopt(long)]
    listen_addr: SocketAddr,

    #[structopt(long)]
    num_workers: usize,

    #[structopt(long)]
    logging: bool,
}

fn main() -> Result<(), Report> {
    color_eyre::install().unwrap();
    let opt = Opt::from_args();
    if opt.logging {
        let subscriber = tracing_subscriber::registry();
        let subscriber = subscriber
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let d = tracing::Dispatch::new(subscriber);
        d.init();
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .wrap_err("Building tokio runtime")?;
    rt.block_on(logparser(opt))
}

#[instrument(level = "info", skip(opt))]
async fn logparser(opt: Opt) -> Result<(), Report> {
    todo!()
}
