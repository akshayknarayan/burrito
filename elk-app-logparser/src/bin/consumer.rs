//! Receive rate histograms and keep a running histogram.

use std::{future::Future, net::SocketAddr, path::PathBuf, pin::Pin};

use color_eyre::eyre::{Report, WrapErr};
use elk_app_logparser::{
    listen::{serve_local, ProcessLine},
    parse_log::EstOutputRateHist,
};
use futures_util::future::ready;
use structopt::StructOpt;
use tracing::{info, instrument};
use tracing_error::ErrorLayer;
use tracing_subscriber::prelude::*;

#[derive(Debug, StructOpt)]
#[structopt(name = "consumer")]
struct Opt {
    #[structopt(long)]
    local_root: PathBuf,

    #[structopt(long)]
    listen_addr: SocketAddr,

    #[structopt(long)]
    hostname: String,

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
    rt.block_on(consumer(opt))
}

struct Process;

impl ProcessLine<EstOutputRateHist> for Process {
    type Error = Report;
    type Future<'a> = Pin<Box<dyn Future<Output = Result<(), Self::Error>> + 'a>>;
    fn process_lines<'a>(
        &'a self,
        line_batch: &'a mut [Option<(SocketAddr, EstOutputRateHist)>],
    ) -> Self::Future<'a> {
        for (_, est_output) in line_batch.iter_mut().map_while(Option::take) {
            let num_records = est_output.len();
            if num_records > 0 {
                let msg = est_output
                    .iter_quantiles(2)
                    .map(|iv| {
                        let quantile = iv.quantile_iterated_to();
                        let value = iv.value_iterated_to();
                        let cnt = iv.count_since_last_iteration();
                        (quantile, value, cnt)
                    })
                    .fold(format!("Hist({})", num_records), |mut acc, (q, v, c)| {
                        let m = format!("[{}]({}): {}", q, v, c);
                        acc.push_str(&m);
                        acc
                    });

                info!(?msg, "got histogram update");
            }
        }

        Box::pin(ready(Ok(())))
    }
}

#[instrument(level = "info", skip(opt))]
async fn consumer(opt: Opt) -> Result<(), Report> {
    info!(?opt, "starting consumer");
    serve_local(opt.listen_addr, opt.hostname, opt.local_root, Process).await
}
