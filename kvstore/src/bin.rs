//! Utils to help the crate's binaries

use color_eyre::eyre::{bail, ensure, eyre, Report, WrapErr};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use tracing::{debug, info};
use tracing_error::ErrorLayer;
use tracing_subscriber::prelude::*;

#[derive(Clone, Copy, Debug)]
pub enum Datapath {
    Kernel,
    Shenango,
    DpdkSingleThread,
    DpdkMultiThread,
}

impl std::str::FromStr for Datapath {
    type Err = Report;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "kernel" | "Kernel" => Self::Kernel,
            "shenango" | "Shenango" => Self::Shenango,
            "dpdkthread" => Self::DpdkSingleThread,
            "dpdkmulti" | "dpdkinline" => Self::DpdkMultiThread,
            s => bail!(eyre!("Unknown datapath {:?}", s)),
        })
    }
}

fn get_first_line(x: &Path) -> Result<String, Report> {
    let mut f = BufReader::new(
        std::fs::File::open(x).wrap_err_with(|| eyre!("could not open config file {:?}", x))?,
    );
    let mut line = String::new();
    f.read_line(&mut line)
        .wrap_err("error reading config file")?;
    Ok(line)
}

impl Datapath {
    pub fn validate_cfg(&self, cfg_file: Option<&Path>) -> Result<(), Report> {
        let cfg_file: &Path = match self {
            Datapath::Kernel => {
                tracing::warn!(?cfg_file, "Using kernel datapath, ignoring datapath config");
                return Ok(());
            }
            Datapath::Shenango | Datapath::DpdkSingleThread | Datapath::DpdkMultiThread => {
                cfg_file.ok_or_else(|| eyre!("datapath cfg must be specified"))?
            }
        };

        debug!(?cfg_file, "test config file contents");
        let first_line = get_first_line(cfg_file)?;

        ensure!(
            first_line.starts_with(match self {
                Datapath::Shenango => "host_addr",
                Datapath::DpdkSingleThread | Datapath::DpdkMultiThread => "[dpdk]",
                _ => unreachable!(),
            }),
            "config file has wrong format: {:?}",
            first_line
        );
        Ok(())
    }
}

/// `log`: enable stdout logging.
/// `trace_time`: enable dumps of tracing times to file
/// `trace_write_interval`: how often to write tracing times
pub fn tracing_init(
    log: bool,
    trace_time: Option<PathBuf>,
    trace_write_interval: std::time::Duration,
) {
    let subscriber = tracing_subscriber::registry();
    let timing_downcaster = if let Some(trace_file) = trace_time {
        let timing_layer = tracing_timing::Builder::default()
            .no_span_recursion()
            .layer(|| tracing_timing::Histogram::new_with_max(1_000_000, 2).unwrap());
        let timing_downcaster = timing_layer.downcaster();
        if log {
            let subscriber = subscriber
                .with(timing_layer)
                .with(tracing_subscriber::fmt::layer())
                .with(tracing_subscriber::EnvFilter::from_default_env())
                .with(ErrorLayer::default());
            let d = tracing::Dispatch::new(subscriber);
            d.clone().init();
            Some((trace_file, timing_downcaster, d))
        } else {
            let subscriber = subscriber
                .with(timing_layer)
                .with(tracing_subscriber::EnvFilter::from_default_env())
                .with(ErrorLayer::default());
            let d = tracing::Dispatch::new(subscriber);
            d.clone().init();
            Some((trace_file, timing_downcaster, d))
        }
    } else if log {
        let subscriber = subscriber
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let d = tracing::Dispatch::new(subscriber);
        d.init();
        None
    } else {
        None
    };

    if let Some((trace_file, timing_downcaster, d)) = timing_downcaster {
        let tf = trace_file.clone();
        let down = d.clone();
        ctrlc::set_handler(move || {
            let timing = timing_downcaster
                .downcast(&down)
                .expect("downcast timing layer");
            let mut f = std::fs::File::create(&tf).unwrap();
            info!("writing tracing info");
            dump_tracing(timing, &mut f).unwrap();
            std::process::exit(0)
        })
        .unwrap();

        std::thread::spawn(move || {
            let timing = timing_downcaster
                .downcast(&d)
                .expect("downcast timing layer");

            loop {
                std::thread::sleep(trace_write_interval);
                let mut f = std::fs::File::create(&trace_file).unwrap();
                info!("writing tracing info");
                dump_tracing(timing, &mut f).unwrap();
            }
        });
    }
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
