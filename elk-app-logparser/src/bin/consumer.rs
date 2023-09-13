//! Receive rate histograms and keep a running histogram.

use std::{future::Future, net::SocketAddr, path::PathBuf, pin::Pin};

use color_eyre::eyre::{eyre, Report, WrapErr};
use elk_app_logparser::{
    listen::{serve_local, ProcessLine},
    parse_log::EstOutputRateHist,
};
use futures_util::future::ready;
use structopt::StructOpt;
use tokio::fs::File;
use tracing::{debug, error, info, instrument};
use tracing_error::ErrorLayer;
use tracing_subscriber::prelude::*;

#[derive(Debug, StructOpt)]
#[structopt(name = "consumer")]
struct Opt {
    #[structopt(long)]
    listen_addr: SocketAddr,

    #[structopt(long)]
    hostname: String,

    #[structopt(long)]
    local_root: Option<Option<PathBuf>>,

    #[structopt(long)]
    out_file: Option<PathBuf>,

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

#[derive(Clone, Copy, Debug, Default)]
struct ProcessRecord {
    recv_ts: u64,
    num_records_observed: usize,
}

#[derive(Debug)]
struct Process {
    clk: quanta::Clock,
    s: flume::Sender<ProcessRecord>,
}

impl ProcessLine<EstOutputRateHist> for Process {
    type Error = Report;
    type Future<'a> = Pin<Box<dyn Future<Output = Result<(), Self::Error>> + 'a>>;
    fn process_lines<'a>(
        &'a self,
        line_batch: &'a mut [Option<EstOutputRateHist>],
    ) -> Self::Future<'a> {
        let mut num_records_observed = 0;
        for est_output in line_batch.iter_mut().map_while(Option::take) {
            let client_ip = est_output.client_ip;
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
                    .fold(
                        format!("[{}]: Hist({}) | ", client_ip, num_records),
                        |mut acc, (q, v, c)| {
                            let m = format!("[{}]({}): {} | ", q, v, c);
                            acc.push_str(&m);
                            acc
                        },
                    );
                info!(?msg, "got histogram update");
            }

            num_records_observed += num_records;
        }

        if num_records_observed > 0 {
            let recv_ts = self.clk.raw();
            // blocking not possible on unbounded channel
            if let Err(err) = self.s.try_send(ProcessRecord {
                recv_ts,
                num_records_observed: num_records_observed as _,
            }) {
                error!(?err, "Receiver dropped, exiting");
                return Box::pin(ready(Err(eyre!("Exiting due to channel send failure"))));
            }
        } else {
            debug!("empty update");
        }

        Box::pin(ready(Ok(())))
    }
}

#[instrument(level = "info", skip(opt))]
async fn consumer(opt: Opt) -> Result<(), Report> {
    if let Some(lr) = opt.local_root.as_ref() {
        tokio::spawn(burrito_localname_ctl::ctl::serve_ctl(lr.clone(), true));
    }

    info!(?opt, "starting consumer");
    let local_root = opt
        .local_root
        .map(|o| o.unwrap_or_else(|| "/tmp/burrito".parse().unwrap()));
    let (s, r) = flume::unbounded();
    let clk = quanta::Clock::new();
    let p = Process {
        clk: clk.clone(),
        s,
    };

    let of = if let Some(p) = opt.out_file {
        Some(File::create(p).await?)
    } else {
        None
    };

    tokio::spawn(stats(r, clk, of));
    serve_local(opt.listen_addr, opt.hostname, local_root, p).await
}

#[instrument(level = "info", skip(r, clk, outf), err)]
async fn stats(
    r: flume::Receiver<ProcessRecord>,
    clk: quanta::Clock,
    mut outf: Option<tokio::fs::File>,
) -> Result<(), Report> {
    use tokio::io::AsyncWriteExt;
    info!("starting");
    let start = clk.now();
    let mut last_recv_time: Option<u64> = None;
    let mut records_observed = 0;
    if let Some(ref mut f) = outf {
        f.write_all(b"since_start_us,tot_records_observed,records_observed,elapsed_us,rate_records_per_sec\n")
            .await?;
    }

    while let Ok(ProcessRecord {
        recv_ts,
        num_records_observed,
    }) = r.recv_async().await
    {
        records_observed += num_records_observed;
        if last_recv_time.is_none() {
            last_recv_time = Some(recv_ts);
            continue;
        }

        let el = clk.delta(last_recv_time.unwrap(), clk.raw());
        let samp = (num_records_observed as f64) / el.as_secs_f64();
        if let Some(ref mut f) = outf {
            let line = format!(
                "{},{},{},{},{}\n",
                start.elapsed().as_micros(),
                records_observed,
                num_records_observed,
                el.as_micros(),
                samp,
            );
            f.write_all(line.as_bytes())
                .await
                .wrap_err("write to out file")?;
        } else {
            info!(time = ?start.elapsed(), ?records_observed, ?el, ?samp, "stats");
        }

        last_recv_time = Some(recv_ts);
    }

    info!("exiting");
    Ok(())
}
