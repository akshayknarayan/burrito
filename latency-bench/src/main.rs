//! Bertha Shenango vs DPDK ping

use bertha::{ChunnelConnection, ChunnelConnector, ChunnelListener};
use color_eyre::eyre::{eyre, Report, WrapErr};
use dpdk_direct::{DpdkUdpReqChunnel, DpdkUdpSkChunnel};
use futures_util::stream::{StreamExt, TryStreamExt};
use shenango_chunnel::{ShenangoUdpReqChunnel, ShenangoUdpSkChunnel};
use std::fs::OpenOptions;
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
    DpdkRaw,
    Shenango,
    ShenangoRt,
}

impl std::str::FromStr for Backend {
    type Err = Report;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "dpdk_raw" | "dpdkraw" => Ok(Self::DpdkRaw),
            "dpdk" => Ok(Self::Dpdk),
            "shenango" => Ok(Self::Shenango),
            "shenangort" => Ok(Self::ShenangoRt),
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
    async fn run_client<C>(
        self,
        sk: C,
        port: u16,
        ip: Ipv4Addr,
        padding: usize,
    ) -> Result<Vec<Duration>, Report>
    where
        C: ChunnelListener<Addr = SocketAddr, Error = Report>
            + ChunnelConnector<Addr = (), Error = Report>,
        <C as ChunnelListener>::Stream: Unpin,
        <C as ChunnelListener>::Connection:
            ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + 'static,
        <C as ChunnelConnector>::Connection:
            ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + 'static,
    {
        match self {
            Mode::Echo => Ok(do_echo_client(sk, SocketAddrV4::new(ip, port), padding).await?),
            Mode::ConnEcho => Ok(do_conn_client(sk, SocketAddrV4::new(ip, port), padding).await?),
        }
    }

    async fn run_server<C>(self, sk: C, port: u16) -> Result<(), Report>
    where
        C: ChunnelListener<Addr = SocketAddr, Error = Report>,
        <C as ChunnelListener>::Stream: Unpin,
        <C as ChunnelListener>::Connection:
            ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + 'static,
    {
        match self {
            Mode::Echo => do_echo_server(sk, port).await?,
            Mode::ConnEcho => do_conn_server(sk, port).await?,
        }
        unreachable!()
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

    #[structopt(short, long, default_value = "0")]
    length_padding: usize,

    #[structopt(short, long)]
    client: Option<Ipv4Addr>,

    #[structopt(short, long)]
    out_file: Option<PathBuf>,
}

fn dump(mut times: Vec<Duration>, out_file: Option<PathBuf>) -> Result<(), Report> {
    if let Some(of) = out_file {
        use std::io::Write;
        let mut f = match OpenOptions::new().append(true).open(&of) {
            Ok(f) => f,
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => {
                    let mut f = OpenOptions::new().append(true).create(true).open(&of)?;
                    writeln!(&mut f, "Latency_us")?;
                    f
                }
                _ => Err(e)?,
            },
        };

        for t in &times {
            writeln!(&mut f, "{}", t.as_micros())?;
        }
    }

    let (p5, p25, p50, p75, p95) = percentiles_us(&mut times);
    info!(?p5, ?p25, ?p50, ?p75, ?p95, "done");
    println!(
        "p5={:?}, p25={:?}, p50={:?}, p75={:?}, p95={:?}",
        p5, p25, p50, p75, p95
    );

    Ok(())
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
        length_padding,
        out_file,
    } = Opt::from_args();

    if let Backend::ShenangoRt = backend {
        shenango::runtime_init(cfg.to_str().unwrap().to_owned(), move || {
            if let Some(ip) = client {
                let times = shenangort_client(mode, ip, port, length_padding).unwrap();
                dump(times, out_file).unwrap();
                std::process::exit(0);
            } else {
                shenangort_server(mode, port).unwrap();
            }
        })
        .unwrap();
        unreachable!();
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;

    rt.block_on(async move {
        if let Some(ip) = client {
            let times = match backend {
                Backend::Dpdk => {
                    let ch = DpdkUdpSkChunnel::new(cfg)?;
                    mode.run_client(ch, port, ip, length_padding).await?
                }
                Backend::Shenango => {
                    let ch = ShenangoUdpSkChunnel::new(&cfg);
                    mode.run_client(ch, port, ip, length_padding).await?
                }
                Backend::DpdkRaw => {
                    let handle = dpdk_raw_start_iokernel(cfg).await?;
                    dpdk_raw_client(handle, mode, port, ip, length_padding).await?
                }
                Backend::ShenangoRt => unreachable!(),
            };

            dump(times, out_file)?;
            std::process::exit(0);
        } else {
            match backend {
                Backend::Dpdk => match mode {
                    Mode::Echo => {
                        let ch = DpdkUdpSkChunnel::new(cfg)?;
                        mode.run_server(ch, port).await?;
                    }
                    Mode::ConnEcho => {
                        let ch = DpdkUdpSkChunnel::new(cfg)?;
                        let ch = DpdkUdpReqChunnel(ch);
                        mode.run_server(ch, port).await?;
                    }
                },
                Backend::DpdkRaw => {
                    let handle = dpdk_raw_start_iokernel(cfg).await?;
                    dpdk_raw_server(handle, mode, port).await?;
                }
                Backend::Shenango => match mode {
                    Mode::Echo => {
                        let ch = ShenangoUdpSkChunnel::new(&cfg);
                        mode.run_server(ch, port).await?;
                    }
                    Mode::ConnEcho => {
                        let ch = ShenangoUdpSkChunnel::new(&cfg);
                        let ch = ShenangoUdpReqChunnel(ch);
                        mode.run_server(ch, port).await?;
                    }
                },
                Backend::ShenangoRt => unreachable!(),
            }
        }

        Ok::<_, Report>(())
    })
}

async fn dpdk_raw_start_iokernel(cfg: PathBuf) -> Result<dpdk_wrapper::DpdkIoKernelHandle, Report> {
    use dpdk_wrapper::DpdkIoKernel;
    let (handle_s, handle_r) = flume::bounded(1);

    std::thread::spawn(move || {
        let (iokernel, handle) = match DpdkIoKernel::new(cfg) {
            Ok(x) => x,
            Err(err) => {
                tracing::error!(err = %format!("{:#?}", err), "Dpdk init failed");
                return;
            }
        };
        handle_s.send(handle).unwrap();
        iokernel.run();
    });

    let handle = handle_r.recv_async().await?;
    Ok(handle)
}

async fn dpdk_raw_server(
    handle: dpdk_wrapper::DpdkIoKernelHandle,
    mode: Mode,
    port: u16,
) -> Result<(), Report> {
    let incoming = handle.accept(port)?;
    info!(?port, "listening");

    async fn echo_conn(conn: dpdk_wrapper::BoundDpdkConn) -> Result<(), Report> {
        let remote = conn.remote_addr();
        loop {
            let (from, buf) = conn.recv_async().await.wrap_err("recv")?;
            debug!(?remote, ?from, "got msg");
            conn.send_async(remote, buf).await.wrap_err("send echo")?;
            debug!(?remote, ?from, "sent echo");
        }
    }

    for conn in incoming {
        let remote = conn.remote_addr();
        info!(?remote, "New bound connection");
        tokio::spawn(async move {
            if let Err(e) = echo_conn(conn).await {
                debug!(?e, "conn errored")
            } else {
                unreachable!()
            }
        });
    }

    Err(eyre!("sender for incoming messages dropped"))
}

async fn dpdk_raw_client(
    handle: dpdk_wrapper::DpdkIoKernelHandle,
    mode: Mode,
    port: u16,
    ip: Ipv4Addr,
    padding: usize,
) -> Result<Vec<Duration>, Report> {
    let conns: Result<_, _> = (0..4).map(|_| handle.socket(None)).collect();
    let conns: Vec<_> = conns?;
    let num_conns = conns.len();

    let remote = SocketAddrV4::new(ip, port);
    info!(?remote, ?num_conns, "made client connections");
    let mut times = Vec::with_capacity(100);
    let start = Instant::now();
    for i in 0..1000 {
        let conn = &conns[i % num_conns];
        let msg = bincode::serialize(&TimeMsg::new(start, padding))?;
        conn.send(remote, msg).wrap_err("send")?;
        info!(?i, "sent");
        let (from, buf) = conn.recv().wrap_err("recv")?;
        let msg: TimeMsg = bincode::deserialize(&buf)?;
        let elap = msg.elapsed(start);
        info!(?i, ?from, ?elap, "received response");
        times.push(elap);
    }

    Ok(times)
}

fn shenangort_server(mode: Mode, port: u16) -> Result<(), Report> {
    let addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port);
    shenango::udp::udp_accept(addr, move |cn| {
        let mut buf = [0u8; 2048];
        let from = cn.remote_addr();
        loop {
            let read_len = cn.recv(&mut buf).wrap_err("udp_recv").unwrap();
            let recvd = &buf[..read_len];
            debug!(?from, ?read_len, "got msg");
            cn.write_to(&recvd, from).wrap_err("udp_send").unwrap();
            debug!(?from, "sent echo");
        }
    })?;
    Ok(())
}

fn shenangort_client(
    mode: Mode,
    ip: Ipv4Addr,
    port: u16,
    padding: usize,
) -> Result<Vec<Duration>, Report> {
    let conns: Result<_, _> = (0..4)
        .map(|_| shenango::udp::UdpConnection::listen(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)))
        .collect();
    let conns: Vec<_> = conns?;
    let num_conns = conns.len();
    let remote = SocketAddrV4::new(ip, port);
    info!(?remote, ?num_conns, "made client connections");
    let mut times = Vec::with_capacity(100);
    let mut buf = [0u8; 2048];
    let start = Instant::now();
    for i in 0..1000 {
        let conn = &conns[i % num_conns];
        let msg = bincode::serialize(&TimeMsg::new(start, padding))?;
        conn.write_to(&msg[..], remote).wrap_err("send")?;
        debug!(?i, "sent");
        let (read_len, _) = conn.read_from(&mut buf).wrap_err("recv")?;
        let msg: TimeMsg = bincode::deserialize(&buf[..read_len])?;
        let elap = msg.elapsed(start);
        debug!(?i, ?elap, "received response");
        times.push(elap);
    }

    Ok(times)
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
async fn do_echo_client<C>(
    mut ch: C,
    remote: SocketAddrV4,
    padding: usize,
) -> Result<Vec<Duration>, Report>
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
        let msg = bincode::serialize(&TimeMsg::new(start, padding))?;
        cn.send((remote.into(), msg)).await?;
        debug!(?i, "sent");
        let (_, buf) = cn.recv().await.wrap_err("recv")?;
        let msg: TimeMsg = bincode::deserialize(&buf[..])?;
        let elap = msg.elapsed(start);
        debug!(?i, ?elap, "received response");
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
async fn do_conn_client<C>(
    mut ch: C,
    remote: SocketAddrV4,
    padding: usize,
) -> Result<Vec<Duration>, Report>
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
        let msg = bincode::serialize(&TimeMsg::new(start, padding))?;
        cn.send((remote.into(), msg)).await?;
        debug!(?i, "sent");
        let (_, buf) = cn.recv().await.wrap_err("recv")?;
        debug!(msg_len=?buf.len(), "received");
        let msg: TimeMsg = bincode::deserialize(&buf[..]).wrap_err("deserialize")?;
        let elap = msg.elapsed(start);
        debug!(?i, ?elap, "received response");
        times.push(elap);
    }

    Ok(times)
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct TimeMsg(Duration, u32, Vec<u8>);
impl TimeMsg {
    fn new(start: Instant, padding: usize) -> Self {
        Self(start.elapsed(), 0xdead, vec![0u8; padding])
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
