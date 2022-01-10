//! Bertha Shenango vs DPDK ping

use bertha::{ChunnelConnection, ChunnelConnector, ChunnelListener};
use color_eyre::eyre::{eyre, Report, WrapErr};
use dpdk_direct::DpdkUdpSkChunnel;
use futures_util::stream::{StreamExt, TryStreamExt};
use shenango_chunnel::ShenangoUdpSkChunnel;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use structopt::StructOpt;
use tracing::{debug, info};
use tracing_error::ErrorLayer;
use tracing_subscriber::prelude::*;

#[derive(Clone, Copy, Debug)]
enum Backend {
    Dpdk,
    Shenango,
}

impl std::str::FromStr for Backend {
    type Err = Report;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "dpdk" => Ok(Self::Dpdk),
            "shenango" => Ok(Self::Shenango),
            x => Err(eyre!("Unkown backend {:?}", x)),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Mode {
    Echo,
    ConnEcho,
}

impl std::str::FromStr for Mode {
    type Err = Report;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "echo" => Ok(Self::Echo),
            "conn" => Ok(Self::ConnEcho),
            x => Err(eyre!("Unkown mode {:?}", x)),
        }
    }
}

impl Mode {
    fn run<C>(self, sk: C, port: u16, client: Option<Ipv4Addr>) -> Result<(), Report>
    where
        C: ChunnelListener<Addr = SocketAddr, Error = Report>
            + ChunnelConnector<Addr = (), Error = Report>,
        <C as ChunnelListener>::Stream: Unpin,
        <C as ChunnelListener>::Connection:
            ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + 'static,
        <C as ChunnelConnector>::Connection:
            ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + 'static,
    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()?;

        rt.block_on(async move {
            match client {
                Some(ip) => {
                    let mut times = match self {
                        Mode::Echo => do_echo_client(sk, SocketAddrV4::new(ip, port))
                            .await
                            .unwrap(),
                        Mode::ConnEcho => do_conn_client(sk, SocketAddrV4::new(ip, port))
                            .await
                            .unwrap(),
                    };
                    let (p5, p25, p50, p75, p95) = percentiles_us(&mut times);
                    info!(?p5, ?p25, ?p50, ?p75, ?p95, "done");
                    println!(
                        "p5={:?}, p25={:?}, p50={:?}, p75={:?}, p95={:?}",
                        p5, p25, p50, p75, p95
                    );
                    std::process::exit(0);
                }
                None => match self {
                    Mode::Echo => do_echo_server(sk, port).await.unwrap(),
                    Mode::ConnEcho => do_conn_server(sk, port).await.unwrap(),
                },
            }
        });
        Ok(())
    }
}

#[derive(Debug, Clone, StructOpt)]
struct Opt {
    #[structopt(long)]
    mode: Mode,

    #[structopt(long)]
    backend: Backend,

    #[structopt(long)]
    cfg: PathBuf,

    #[structopt(short, long)]
    port: u16,

    #[structopt(short, long)]
    client: Option<Ipv4Addr>,
}

fn main() -> Result<(), Report> {
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(ErrorLayer::default());
    let d = tracing::Dispatch::new(subscriber);
    d.init();
    color_eyre::install()?;
    let Opt {
        cfg,
        port,
        client,
        mode,
        backend,
    } = Opt::from_args();

    match backend {
        Backend::Dpdk => {
            let ch = DpdkUdpSkChunnel::new(cfg)?;
            mode.run(ch, port, client)?;
        }
        Backend::Shenango => {
            let ch = ShenangoUdpSkChunnel::new(&cfg);
            mode.run(ch, port, client)?;
        }
    }
    Ok(())
}

#[tracing::instrument(err, skip(ch))]
async fn do_echo_server<C>(mut ch: C, port: u16) -> Result<(), Report>
where
    C: ChunnelListener<Addr = SocketAddr, Error = Report>,
    <C as ChunnelListener>::Stream: Unpin,
    <C as ChunnelListener>::Connection:
        ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + 'static,
{
    let addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port);
    let cn = ch.listen(addr.into()).await?.next().await.unwrap()?;
    info!(?port, "listening");
    loop {
        let (from_addr, d) = cn.recv().await?;
        debug!(?from_addr, "got msg");
        cn.send((from_addr, d)).await?;
        debug!(?from_addr, "sent echo");
    }
}

#[tracing::instrument(err, skip(ch))]
async fn do_echo_client<C>(mut ch: C, remote: SocketAddrV4) -> Result<Vec<Duration>, Report>
where
    C: ChunnelConnector<Addr = (), Error = Report>,
    <C as ChunnelConnector>::Connection:
        ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + 'static,
{
    let cn = ch.connect(()).await?;
    info!(?remote, "made client sk");
    let mut times = Vec::with_capacity(1000);
    let start = Instant::now();
    for i in 0..1000 {
        let msg = bincode::serialize(&TimeMsg::new(start))?;
        cn.send((remote.into(), msg)).await?;
        info!(?i, "sent");
        let (_, buf) = cn.recv().await.wrap_err("recv")?;
        let msg: TimeMsg = bincode::deserialize(&buf[..])?;
        let elap = msg.elapsed(start);
        info!(?i, ?elap, "received response");
        times.push(elap);
    }

    Ok(times)
}

#[tracing::instrument(err, skip(ch))]
async fn do_conn_server<C>(mut ch: C, port: u16) -> Result<(), Report>
where
    C: ChunnelListener<Addr = SocketAddr, Error = Report>,
    <C as ChunnelListener>::Stream: Unpin,
    <C as ChunnelListener>::Connection:
        ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + 'static,
{
    let addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port);
    let st = ch.listen(addr.into()).await?;
    info!(?port, "listening");
    st.try_for_each_concurrent(None, |cn| async move {
        loop {
            let (from_addr, d) = cn.recv().await?;
            debug!(?from_addr, "got msg");
            cn.send((from_addr, d)).await?;
            debug!(?from_addr, "sent echo");
        }
    })
    .await?;
    unreachable!()
}

#[tracing::instrument(err, skip(ch))]
async fn do_conn_client<C>(mut ch: C, remote: SocketAddrV4) -> Result<Vec<Duration>, Report>
where
    C: ChunnelConnector<Addr = (), Error = Report>,
    <C as ChunnelConnector>::Connection:
        ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + 'static,
{
    let mut cns = vec![];
    for _ in 0..4 {
        let cn = ch.connect(()).await?;
        cns.push(cn);
    }

    let num_conns = cns.len();
    info!(?remote, "made client sks");
    let mut times = Vec::with_capacity(1000);
    let start = Instant::now();
    for i in 0..1000 {
        let cn = &cns[i % num_conns];
        let msg = bincode::serialize(&TimeMsg::new(start))?;
        cn.send((remote.into(), msg)).await?;
        info!(?i, "sent");
        let (_, buf) = cn.recv().await.wrap_err("recv")?;
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