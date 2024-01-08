//! Produce messages containing server log lines.

use std::{
    net::SocketAddr,
    path::PathBuf,
    str::FromStr,
    time::{Duration, Instant},
};

use bertha::ChunnelConnection;
use color_eyre::{
    eyre::{ensure, eyre, WrapErr},
    Report,
};
use duration_str::parse;
use futures_util::{
    future::{ready, Either},
    Stream, StreamExt,
};
use structopt::StructOpt;
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    time::Interval,
};
use tokio_stream::wrappers::LinesStream;
use tracing::{debug, info, trace, warn};
use tracing_error::ErrorLayer;
use tracing_subscriber::prelude::*;

use elk_app_logparser::{
    connect,
    parse_log::{live_logentry_lines, Line},
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
    produce_interarrival: VariableTicker,

    #[structopt(long)]
    message_limit: Option<String>,

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
        let interval_mgr = opt.produce_interarrival;
        let (mut rem_line_count, expected_dur) = if let Some((r, d)) =
            opt.message_limit
                .as_ref()
                .map(|m| interval_mgr.parse_limit(m)).transpose()?
        {
            (Some(r), Some(d))
        } else {
            (None, None)
        };
        let producer = get_line_producer(opt.log_file, interval_mgr).await?;
        let mut producer = std::pin::pin!(producer);
        let mut outf = if let Some(filename) = opt.stats_file {
            let mut f = tokio::fs::File::create(filename).await?;
            f.write_all(b"since_start_us,tot_records,tot_bytes,records,bytes,elapsed_us,rate_records_per_sec,rate_bytes_per_sec\n").await?;
            Some(f)
        } else {
            None
        };

        info!(?opt.connect_addr, ?rem_line_count, "got connection, starting");
        let clk = quanta::Clock::new();
        let mut slots: Vec<_> = (0..16).map(|_| None).collect();
        let mut tot_bytes = 0;
        let mut tot_msgs = 0;
        let start = clk.raw();
        let mut then = start;
        while let Some(burst) = producer.next().await {
            if rem_line_count.as_ref().map(|c| *c == 0).unwrap_or(false) {
                break;
            }

            let burst_len = burst.len();
            let mut burst_bytes = 0;
            cn.send(burst.into_iter().filter(|l| match l {
                Line::Report(_) => true,
                _ => false,
            }).take_while(|l| match l {
                Line::Report(s) => {
                    burst_bytes += s.len();
                    burst_bytes < 1024
                }
                _ => unreachable!(),
            })).await?;
            rem_line_count = rem_line_count.map(|c| c.saturating_sub(burst_len));
            debug!(?burst_len, ?burst_bytes, "sent lines");
            let ms = cn.recv(&mut slots[..]).await?;
            let now = clk.raw();
            let el = clk.delta(then, now);
            then = now;
            let num_acks = ms.iter_mut().map_while(Option::take).count();
            trace!(?num_acks, ?burst_len, ?el, "got acks");
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

            if let Some(expect) = expected_dur {
                if clk.delta(start, now) > expect * 2 {
                    let time = clk.delta(start, now);
                    warn!(?time, ?expected_dur, "exiting");
                    break;
                }
            }
        }

        let tot_elapsed = clk.delta(start, clk.raw());
        if let Some(ref mut f) = outf {
            let line = format!("{},{},{},{},{},{},{},{}\n",
                tot_elapsed.as_micros(),
                tot_msgs,
                tot_bytes,
                0,
                0,
                0,
                (tot_msgs as f64) / tot_elapsed.as_secs_f64(),
                (tot_bytes as f64) / tot_elapsed.as_secs_f64());
            f.write_all(line.as_bytes()).await?;
        } else {
            info!(?tot_elapsed, ?tot_msgs, ?tot_bytes, "stats");
        }

        info!(?rem_line_count, "done");
        Ok(())
    })
}

#[derive(Debug)]
struct VariableTicker {
    intervals: Vec<(Duration, Duration)>,
    final_interval: Duration,
    curr_interval: Option<(Interval, Instant, Option<Duration>)>,
}

impl FromStr for VariableTicker {
    type Err = Report;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut intervals = s
            .split('~')
            .map(|epoch_spec| {
                let mut parts = epoch_spec.splitn(2, ':');
                let int = parse(
                    parts
                        .next()
                        .ok_or_else(|| eyre!("malformed interval spec {:?}", epoch_spec))?,
                )?;

                Ok::<_, Report>((int, parts.next().map(parse).transpose()?))
            })
            .collect::<Result<Vec<_>, _>>()?;
        ensure!(!intervals.is_empty(), "need nonempty intervals list");
        let (final_interval, d) = intervals.pop().unwrap();
        ensure!(d.is_none(), "final interval cannot have limited duration");
        let intervals = intervals
            .into_iter()
            .map(|(i, d)| {
                if d.is_none() {
                    Err(eyre!(
                        "intermediate intervals must have limited epoch duration"
                    ))
                } else {
                    Ok((i, d.unwrap()))
                }
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            intervals,
            final_interval,
            curr_interval: None,
        })
    }
}

impl VariableTicker {
    async fn tick(&mut self) {
        match &mut self.curr_interval {
            Some((ref mut i, ref mut s, d)) if d.is_some() && d.unwrap() < s.elapsed() => {
                let (next_period, next_epoch) = match self.intervals.pop() {
                    Some((int, dur)) => (int, Some(dur)),
                    None => (self.final_interval, None),
                };

                *i = tokio::time::interval(next_period);
                *s = Instant::now();
                *d = next_epoch;
                info!(?next_epoch, ?next_period, "setting tick interval");
            }
            Some(_) => (),
            None => {
                let (next_period, next_epoch) = match self.intervals.pop() {
                    Some((int, dur)) => (int, Some(dur)),
                    None => (self.final_interval, None),
                };

                self.curr_interval = Some((
                    tokio::time::interval(next_period),
                    Instant::now(),
                    next_epoch,
                ));
                info!(?next_epoch, ?next_period, "setting tick interval");
            }
        }

        self.curr_interval.as_mut().unwrap().0.tick().await;
    }

    fn parse_limit(&self, msg_limit: &str) -> Result<(usize, Duration), Report> {
        if let Ok(lim) = msg_limit.parse() {
            Ok((lim, self.expected_dur(lim)))
        } else {
            let tot_dur = parse(msg_limit)?;
            let mut rem_dur = tot_dur;
            let lim = self
                .intervals
                .iter()
                .map(|(int, dur)| {
                    if rem_dur > *dur {
                        rem_dur -= *dur;
                        ((dur.as_secs_f64() / int.as_secs_f64()) * 16.) as usize
                    } else {
                        rem_dur = Duration::ZERO;
                        ((rem_dur.as_secs_f64() / int.as_secs_f64()) * 16.) as usize
                    }
                })
                .sum::<usize>()
                + (((rem_dur.as_secs_f64() / self.final_interval.as_secs_f64()) * 16.) as usize);
            Ok((lim, tot_dur))
        }
    }

    fn expected_dur(&self, mut msg_limit: usize) -> Duration {
        let t1 = self
            .intervals
            .iter()
            .map(|(int, dur)| {
                let ticks = (dur.as_secs_f64() / int.as_secs_f64()) as usize;
                if ticks * 16 < msg_limit {
                    msg_limit -= ticks * 16;
                    *dur
                } else {
                    Duration::from_secs((int.as_secs_f64() * (msg_limit as f64) / 16.) as u64)
                }
            })
            .sum();
        if msg_limit > 0 {
            t1 + Duration::from_secs(
                (self.final_interval.as_secs_f64() * (msg_limit as f64) / 16.) as u64,
            )
        } else {
            t1
        }
    }
}

async fn get_line_producer(
    log_file: Option<PathBuf>,
    interval: VariableTicker,
) -> Result<impl Stream<Item = Vec<Line>>, Report> {
    Ok(futures_util::stream::unfold(interval, |mut i| async move {
        i.tick().await;
        Some(((), i))
    })
    .zip(
        if let Some(f) = log_file {
            let reader = BufReader::new(File::open(&f).await?);
            Either::Left(
                LinesStream::new(reader.lines()).filter_map(|x| ready(x.ok().map(Line::Report))),
            )
        } else {
            Either::Right(futures_util::stream::iter(
                live_logentry_lines().map(|x| Line::Report(x)),
            ))
        }
        .ready_chunks(16),
    )
    .map(|(_, x)| x))
}

#[cfg(test)]
mod t {
    use super::VariableTicker;
    use std::time::Duration;

    #[test]
    fn variable_ticker_parse() {
        let x: VariableTicker = "50ms".parse().unwrap();
        assert!(x.intervals.is_empty());
        assert_eq!(x.final_interval, Duration::from_millis(50));
        let x: VariableTicker = "50ms:10s~100ms:10s~50ms".parse().unwrap();
        assert_eq!(x.intervals.len(), 2);
        assert_eq!(
            x.intervals[0],
            (Duration::from_millis(50), Duration::from_secs(10))
        );
        assert_eq!(
            x.intervals[1],
            (Duration::from_millis(100), Duration::from_secs(10))
        );
        assert_eq!(x.final_interval, Duration::from_millis(50));
    }
}
