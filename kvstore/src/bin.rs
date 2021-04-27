//! Utils to help the crate's binaries

use color_eyre::eyre::Report;
use std::path::PathBuf;
use tracing::info;
use tracing_error::ErrorLayer;
use tracing_subscriber::prelude::*;

/// `log`: enable stdout logging.
/// `trace_time`: enable dumps of tracing times to file
/// `trace_write_interval`: how often to write tracing times
pub async fn tracing_init(
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
        tokio::spawn(async move {
            let timing = timing_downcaster
                .downcast(&d)
                .expect("downcast timing layer");

            loop {
                tokio::time::sleep(trace_write_interval).await;
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
