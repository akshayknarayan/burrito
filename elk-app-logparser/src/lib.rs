use std::{path::PathBuf, str::FromStr};

use color_eyre::eyre::{bail, Report, WrapErr};
use tokio::{fs::File, io::AsyncWriteExt};
use tracing::{info, instrument};

pub mod connect;
pub mod listen;
pub mod parse_log;
pub mod publish_subscribe;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum EncrSpec {
    AllowNone,
    AutoOnly,
    TlsOnly,
    QuicOnly,
}

impl FromStr for EncrSpec {
    type Err = Report;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut c = s.to_lowercase();
        c.retain(|c| c != '-');
        Ok(match c.as_str() {
            "allownone" => EncrSpec::AllowNone,
            "autoonly" => EncrSpec::AutoOnly,
            "tlsonly" => EncrSpec::TlsOnly,
            "quiconly" => EncrSpec::QuicOnly,
            x => bail!("Unknown EncrSpec {}", x),
        })
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ProcessRecord {
    pub recv_ts: u64,
    pub num_records_observed: usize,
    pub num_bytes_observed: Option<usize>,
}

#[instrument(level = "info", skip(r, clk, out_filename), err)]
pub async fn stats(
    r: flume::Receiver<ProcessRecord>,
    clk: quanta::Clock,
    out_filename: Option<PathBuf>,
) -> Result<(), Report> {
    let mut outf = if let Some(p) = out_filename {
        Some(File::create(p).await?)
    } else {
        None
    };
    info!("starting");
    let start = clk.now();
    let mut last_recv_time: Option<u64> = None;
    let mut records_observed = 0;
    let mut bytes_observed = 0;
    if let Some(ref mut f) = outf {
        f.write_all(
            b"since_start_us,tot_records,tot_bytes,records,bytes,elapsed_us,rate_records_per_sec,rate_bytes_per_sec\n",
        )
        .await?;
    }

    while let Ok(ProcessRecord {
        recv_ts,
        num_records_observed,
        num_bytes_observed,
    }) = r.recv_async().await
    {
        records_observed += num_records_observed;
        let b = num_bytes_observed.unwrap_or(0);
        bytes_observed += b;
        if last_recv_time.is_none() {
            last_recv_time = Some(recv_ts);
            continue;
        }

        let el = clk.delta(last_recv_time.unwrap(), clk.raw());
        let samp_rec = (num_records_observed as f64) / el.as_secs_f64();
        let samp_bytes = (b as f64) / el.as_secs_f64();
        if let Some(ref mut f) = outf {
            let line = format!(
                "{},{},{},{},{},{},{},{}\n",
                start.elapsed().as_micros(),
                records_observed,
                bytes_observed,
                num_records_observed,
                b,
                el.as_micros(),
                samp_rec,
                samp_bytes,
            );
            f.write_all(line.as_bytes())
                .await
                .wrap_err("write to out file")?;
        } else {
            info!(time = ?start.elapsed(), ?records_observed, bytes=?b, ?el, ?samp_rec, ?samp_bytes, "stats");
        }

        last_recv_time = Some(recv_ts);
    }

    info!("exiting");
    Ok(())
}
