//! Bertha Shenango ping

use bertha::{ChunnelConnection, ChunnelConnector, ChunnelListener};
use color_eyre::{eyre::WrapErr, Result};
use dpdk_direct::DpdkUdpSkChunnel;
use futures_util::stream::StreamExt;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use structopt::StructOpt;
use tracing::{debug, info};
use tracing_error::ErrorLayer;
use tracing_subscriber::prelude::*;

#[derive(Debug, Clone, StructOpt)]
struct Opt {
    #[structopt(long)]
    cfg: PathBuf,

    #[structopt(short, long)]
    port: u16,

    #[structopt(short, long)]
    client: Option<Ipv4Addr>,
}

fn main() -> Result<()> {
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(ErrorLayer::default());
    let d = tracing::Dispatch::new(subscriber);
    d.init();
    color_eyre::install()?;
    let Opt { cfg, port, client } = Opt::from_args();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;

    let sk = DpdkUdpSkChunnel::new(cfg)?;
    rt.block_on(async move {
        match client {
            Some(ip) => {
                let mut times = do_client(sk, SocketAddrV4::new(ip, port)).await.unwrap();
                let (p5, p25, p50, p75, p95) = percentiles_us(&mut times);
                info!(?p5, ?p25, ?p50, ?p75, ?p95, "done");
                println!(
                    "p5={:?}, p25={:?}, p50={:?}, p75={:?}, p95={:?}",
                    p5, p25, p50, p75, p95
                );
                std::process::exit(0);
            }
            None => do_server(sk, port).await.unwrap(),
        }
    });

    Ok(())
}

#[tracing::instrument(err)]
async fn do_server(mut sk: DpdkUdpSkChunnel, port: u16) -> Result<()> {
    let addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port);
    let ch = sk.listen(addr.into()).await?.next().await.unwrap()?;
    info!(?port, "listening");
    loop {
        let (from_addr, d) = ch.recv().await?;
        debug!(?from_addr, "got msg");
        ch.send((from_addr, d)).await?;
        debug!(?from_addr, "sent echo");
    }
}

#[tracing::instrument(err)]
async fn do_client(mut sk: DpdkUdpSkChunnel, remote: SocketAddrV4) -> Result<Vec<Duration>> {
    let ch = sk.connect(()).await?;
    info!(?remote, "made client sk");
    let mut times = Vec::with_capacity(100);
    let start = Instant::now();
    for i in 0..1000 {
        let msg = bincode::serialize(&TimeMsg::new(start))?;
        ch.send((remote.into(), msg)).await?;
        info!(?i, "sent");
        let (_, buf) = ch.recv().await.wrap_err("recv")?;
        let msg: TimeMsg = bincode::deserialize(&buf[..])?;
        let elap = msg.elapsed(start);
        info!(?i, ?elap, "received response");
        times.push(elap);
    }

    Ok(times)
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
struct TimeMsg(Duration, u32);
impl TimeMsg {
    fn new(start: Instant) -> Self {
        Self(start.elapsed(), 0xdead)
    }

    fn elapsed(&self, start: Instant) -> Duration {
        assert_eq!(self.1, 0xdead);
        start.elapsed() - self.0
    }
}

fn percentiles_us(durs: &mut [Duration]) -> (Duration, Duration, Duration, Duration, Duration) {
    durs.sort();
    let len = durs.len() as f64;
    let quantile_idxs = [0.05, 0.25, 0.5, 0.75, 0.95];
    let quantiles: Vec<_> = quantile_idxs
        .iter()
        .map(|q| (len * q) as usize)
        .map(|i| durs[i])
        .collect();
    match quantiles[..] {
        [p5, p25, p50, p75, p95] => (p5, p25, p50, p75, p95),
        [..] => unreachable!(),
    }
}
