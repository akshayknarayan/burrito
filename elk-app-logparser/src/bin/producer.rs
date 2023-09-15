//! Produce messages containing server log lines.

use std::{net::SocketAddr, path::PathBuf, time::Duration};

use bertha::ChunnelConnection;
use color_eyre::{eyre::WrapErr, Report};
use futures_util::{
    future::{ready, Either},
    Stream, StreamExt,
};
use structopt::StructOpt;
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
};
use tokio_stream::wrappers::LinesStream;
use tracing::{debug, info, warn};
use tracing_error::ErrorLayer;
use tracing_subscriber::prelude::*;

use elk_app_logparser::{
    connect,
    parse_log::{sample_logentry_lines, Line},
    EncrSpec,
};

#[derive(Debug, StructOpt)]
#[structopt(name = "logproducer")]
struct Opt {
    #[structopt(long)]
    connect_addr: SocketAddr,

    #[structopt(long)]
    redis_addr: Option<String>,

    #[structopt(long)]
    log_file: Option<PathBuf>,

    #[structopt(long)]
    produce_interarrival_ms: Option<usize>,

    #[structopt(long)]
    tot_message_limit: Option<usize>,

    #[structopt(long, default_value = "allow-none")]
    encr_spec: EncrSpec,

    #[structopt(long)]
    stats_file: Option<PathBuf>,

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
    rt.block_on(async move {
        info!("starting client");
        let cn = connect::connect(opt.connect_addr, opt.redis_addr, opt.encr_spec)
            .await
            .wrap_err("connect error")?;
        let producer = get_line_producer(
            opt.log_file,
            opt.produce_interarrival_ms
                .map(|i| Duration::from_millis(i as _))
                .unwrap_or_else(|| Duration::from_secs(1)),
        )
        .await?;
        let mut producer = std::pin::pin!(producer);
        let mut rem_line_count = opt.tot_message_limit;
        let mut outf = if let Some(filename) = opt.stats_file {
            let mut f = tokio::fs::File::create(filename).await?;
            f.write_all(b"since_start_us,tot_records,tot_bytes,records,bytes,elapsed_us,rate_records_per_sec,rate_bytes_per_sec\n").await?;
            Some(f)
        } else {
            None
        };

        info!(?opt.connect_addr, ?rem_line_count, "got connection, starting");
        let clk = quanta::Clock::new();
        let start = clk.raw();
        let mut then = start;
        let mut tot_bytes = 0;
        let mut tot_msgs = 0;
        let mut last_log = start;
        let mut slots: Vec<_> = (0..16).map(|_| None).collect();
        while let Some(burst) = producer.next().await {
            let burst_len = burst.len();
            let burst_bytes: usize = burst
                .iter()
                .map(|l| match l {
                    Line::Report(s) => s.len(),
                    Line::Ack => 0,
                })
                .sum();
            rem_line_count = rem_line_count.map(|c| c.saturating_sub(burst_len));
            if rem_line_count.as_ref().map(|c| *c == 0).unwrap_or(false) {
                break;
            }

            cn.send(burst.into_iter()).await?;
            debug!(?burst_len, ?burst_bytes, "sent lines");
            let ms = cn.recv(&mut slots[..]).await?;
            let now = clk.raw();
            let el = clk.delta(then, now);
            then = now;
            let num_acks = ms.iter_mut().map_while(Option::take).count();
            if num_acks != burst_len {
                warn!(
                    ?num_acks,
                    ?burst_len,
                    ?rem_line_count,
                    "wrong number of acks"
                );
            }

            tot_bytes += burst_bytes;
            tot_msgs += burst_len;
            if clk.delta(last_log, now) > Duration::from_secs(1) {
                let t = clk.delta_as_nanos(start, now) / 1_000;
                if let Some(ref mut f) = outf {
                    let line = format!("{},{},{},{},{},{},{},{}\n",
                        t,
                        tot_msgs,
                        tot_bytes,
                        burst_len,
                        burst_bytes,
                        el.as_micros(),
                        (burst_len as f64) / el.as_secs_f64(),
                        (burst_bytes as f64) / el.as_secs_f64());
                    f.write_all(line.as_bytes()).await?;
                } else {
                    info!(?t, ?tot_msgs, ?tot_bytes, "stats");
                }

                last_log = now;
            }
        }

        let tot_elapsed = clk.delta(start, clk.raw());
        if let Some(ref mut f) = outf {
            let line = format!("{},{},{},{},{}", tot_elapsed.as_micros(), tot_msgs, (tot_msgs as f64) / tot_elapsed.as_secs_f64(), tot_bytes, (tot_bytes as f64) / tot_elapsed.as_secs_f64());
            f.write_all(line.as_bytes()).await?;
        } else {
            info!(?tot_elapsed, ?tot_msgs, ?tot_bytes, "stats");
        }

        info!(?rem_line_count, "done");
        Ok(())
    })
}

async fn get_line_producer(
    log_file: Option<PathBuf>,
    interval: Duration,
) -> Result<impl Stream<Item = Vec<Line>>, Report> {
    Ok(
        futures_util::stream::unfold(tokio::time::interval(interval), |mut i| async move {
            i.tick().await;
            Some(((), i))
        })
        .zip(
            if let Some(f) = log_file {
                let reader = BufReader::new(File::open(&f).await?);
                Either::Left(
                    LinesStream::new(reader.lines())
                        .filter_map(|x| ready(x.ok().map(Line::Report))),
                )
            } else {
                Either::Right(futures_util::stream::iter(
                    sample_logentry_lines().map(|x| Line::Report(x)),
                ))
            }
            .ready_chunks(16),
        )
        .map(|(_, x)| x),
    )
}
