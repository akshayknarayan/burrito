//! Shenango-native ping

use color_eyre::{eyre::WrapErr, Result};
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

    shenango::runtime_init(cfg.to_str().unwrap().to_owned(), move || match client {
        Some(ip) => {
            let mut times = do_client(SocketAddrV4::new(ip, port)).unwrap();
            let (p5, p25, p50, p75, p95) = percentiles_us(&mut times);
            info!(?p5, ?p25, ?p50, ?p75, ?p95, "done");
            println!(
                "p5={:?}, p25={:?}, p50={:?}, p75={:?}, p95={:?}",
                p5, p25, p50, p75, p95
            );
            std::process::exit(0);
        }
        None => do_server(port).unwrap(),
    })
    .unwrap();

    Ok(())
}

#[tracing::instrument(err)]
fn do_server(port: u16) -> Result<()> {
    let addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port);
    let sk = shenango::udp::UdpConnection::listen(addr)
        .wrap_err("Failed to make shenango udp socket")
        .expect("make udp conn");
    info!(?port, "listening");
    let mut buf = [0u8; 1024];
    loop {
        let (read_len, from_addr) = sk.read_from(&mut buf)?;
        debug!(?from_addr, "got msg");
        sk.write_to(&buf[0..read_len], from_addr)?;
        debug!(?from_addr, "sent echo");
    }
}

#[tracing::instrument(err)]
fn do_client(remote: SocketAddrV4) -> Result<Vec<Duration>> {
    let conn = shenango::udp::UdpConnection::listen(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0))?;
    info!(?remote, "made client sk");
    let mut times = Vec::with_capacity(100);
    let mut buf = [0u8; 1024];
    let start = Instant::now();
    for i in 0..1000 {
        let msg = bincode::serialize(&TimeMsg::new(start))?;
        conn.write_to(&msg[..], remote).wrap_err("send")?;
        info!(?i, "sent");
        let (read_len, _) = conn.read_from(&mut buf).wrap_err("recv")?;
        let msg: TimeMsg = bincode::deserialize(&buf[..read_len])?;
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
