use bertha::{
    bincode::SerializeChunnelProject,
    negotiate_client, negotiate_rendezvous, negotiate_server,
    udp::{UdpReqChunnel, UdpSkChunnel},
    util::{NeverCn, ProjectLeft},
    Chunnel, ChunnelConnection, ChunnelConnector, ChunnelListener, ClientNegotiator, CxList,
    Negotiate,
};
use color_eyre::eyre::{bail, ensure, eyre, Report, WrapErr};
use redis_basechunnel::RedisBase;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;
use std::{future::Future, pin::Pin};
use structopt::StructOpt;
use test_util::start_redis;
use tracing::{debug, info, instrument, trace};
use tracing_error::ErrorLayer;
use tracing_subscriber::prelude::*;

#[derive(Clone, Debug)]
enum Mode {
    Rendezvous(String),
    OneRtt,
    ZeroRtt,
    Baseline,
}

impl FromStr for Mode {
    type Err = Report;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let sp: Vec<_> = s.splitn(2, ':').collect();
        match sp.as_slice() {
            ["Rendezvous", r] | ["rendezvous", r] | ["rd", r] => {
                Ok(Mode::Rendezvous(r.to_string()))
            }
            ["OneRtt"] | ["onertt"] | ["1r"] => Ok(Mode::OneRtt),
            ["ZeroRtt"] | ["zerortt"] | ["0r"] => Ok(Mode::ZeroRtt),
            ["Baseline"] | ["baseline"] | ["b"] => Ok(Mode::Baseline),
            _ => bail!("Unkown mode {:?}", s),
        }
    }
}

#[derive(Clone, Debug, StructOpt)]
#[structopt(name = "negotiation-bench")]
struct Opt {
    #[structopt(short, long)]
    num_reqs: usize,
    #[structopt(short, long)]
    mode: Mode,
    #[structopt(short, long)]
    out_file: std::path::PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Report> {
    color_eyre::install().unwrap();
    let subscriber = tracing_subscriber::registry();
    let subscriber = subscriber
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(ErrorLayer::default());
    let d = tracing::Dispatch::new(subscriber);
    d.init();
    let opt = Opt::from_args();
    info!(?opt.num_reqs, rendezvous_mode = ?opt.mode, "starting");
    let mut addr: SocketAddr = "127.0.0.1:12315".parse().unwrap();
    match opt.mode {
        Mode::Rendezvous(redis) => {
            let redis_port = redis
                .split(':')
                .last()
                .ok_or(eyre!("Bad redis address: {:?}", redis))?
                .parse()
                .wrap_err(eyre!("Bad redis address: {:?}", redis))?;
            let _r = start_redis(redis_port);
            let mut durs = Vec::with_capacity(opt.num_reqs);
            for i in 0..opt.num_reqs {
                debug!(?i, "running iteration");
                addr.set_port(addr.port() + 1 as u16);
                let redis_base = RedisBase::new(&redis).await?;
                let srv_task = tokio::spawn(server_rendezvous(addr, redis_base));
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;

                let redis_base = RedisBase::new(&redis).await?;
                let dur = client_rendezvous(addr, redis_base).await?;
                durs.push(dur);
                srv_task.abort();
                ensure!(
                    srv_task.await.unwrap_err().is_cancelled(),
                    "Server task was not cancelled"
                );
            }
            dump(durs, "Consensus", opt.out_file).wrap_err("writing outfile")?;
        }
        Mode::OneRtt => {
            tokio::spawn(server_direct(addr));
            let durs = client_direct(addr, opt.num_reqs)
                .await
                .wrap_err("running client")?;
            dump(durs, "1rtt", opt.out_file).wrap_err("writing outfile")?;
        }
        Mode::ZeroRtt => {
            tokio::spawn(server_direct(addr));
            let durs = client_direct_zerortt(addr, opt.num_reqs)
                .await
                .wrap_err("running client")?;
            dump(durs, "0rtt", opt.out_file).wrap_err("writing outfile")?;
        }
        Mode::Baseline => {
            tokio::spawn(server_raw(addr));
            let durs = client_raw(addr, opt.num_reqs)
                .await
                .wrap_err("running client")?;
            dump(durs, "Baseline", opt.out_file).wrap_err("writing outfile")?;
        }
    }

    info!("done");
    Ok(())
}

#[instrument(skip(redis), err)]
async fn server_rendezvous(addr: SocketAddr, redis: RedisBase) -> Result<(), Report> {
    debug!("start");
    let st = bertha::Select::from((
        CxList::from(TimeMsgChunnel::default()).wrap(UdpBaseSrv { addr }),
        CxList::from(TimeMsgChunnel::default()).wrap(UdpBaseSrv { addr }),
    ))
    .into();
    let cn = negotiate_rendezvous(st, redis, format!("{:?}", addr)).await?;
    loop {
        let d = cn.recv().await?;
        cn.send(d).await?;
    }
}

#[instrument(skip(redis), err)]
async fn client_rendezvous(addr: SocketAddr, redis: RedisBase) -> Result<Duration, Report> {
    debug!("start");
    let start = std::time::Instant::now();
    let t = TimeMsg::new(start);
    let st = bertha::Select::from((
        CxList::from(TimeMsgChunnel::default()).wrap(UdpBaseCl),
        CxList::from(TimeMsgChunnel::default()).wrap(UdpBaseCl),
    ))
    .into();
    let cn = negotiate_rendezvous(st, redis, format!("{:?}", addr)).await?;
    trace!("finished negotiation");
    cn.send((addr, t)).await?;
    let (_, m) = cn.recv().await?;
    Ok(m.elapsed(start))
}

#[instrument(err, level = "info")]
async fn server_raw(addr: SocketAddr) -> Result<(), Report> {
    #[instrument(skip(cn), err, level = "debug")]
    async fn one_conn(
        cn: impl ChunnelConnection<Data = (SocketAddr, TimeMsg)>,
    ) -> Result<(), Report> {
        let d = cn.recv().await?;
        cn.send(d).await?;
        trace!("conn done");
        Ok(())
    }

    let c = TimeMsgChunnel::default();
    let mut st = UdpReqChunnel::default().listen(addr).await?;
    use futures_util::stream::TryStreamExt;
    while let Some(cn) = st.try_next().await? {
        let cn = c.clone().connect_wrap(cn).await?;
        tokio::spawn(one_conn(cn));
    }

    Ok(())
}

async fn client_raw(addr: SocketAddr, num_reqs: usize) -> Result<Vec<Duration>, Report> {
    let start = std::time::Instant::now();
    let mut durs = Vec::with_capacity(num_reqs);
    for i in 0..num_reqs {
        debug!(?i, "running iteration");
        let t = TimeMsg::new(start);
        let cn = UdpSkChunnel.connect(()).await?;
        let cn = TimeMsgChunnel::default().connect_wrap(cn).await?;
        let cn = ProjectLeft::new(addr, cn);
        trace!("finished negotiation");
        cn.send(t).await?;
        let m = cn.recv().await?;
        durs.push(m.elapsed(start));
    }

    Ok(durs)
}

#[instrument(err, level = "info")]
async fn server_direct(addr: SocketAddr) -> Result<(), Report> {
    #[instrument(skip(cn), err, level = "debug")]
    async fn one_conn(
        cn: impl ChunnelConnection<Data = (SocketAddr, TimeMsg)>,
    ) -> Result<(), Report> {
        let d = cn.recv().await?;
        cn.send(d).await?;
        trace!("conn done");
        Ok(())
    }

    let mut st = negotiate_server(
        TimeMsgChunnel::default(),
        UdpReqChunnel::default().listen(addr).await?,
    )
    .await?;
    use futures_util::stream::TryStreamExt;
    while let Some(cn) = st.try_next().await? {
        tokio::spawn(one_conn(cn));
    }

    Ok(())
}

async fn client_direct(addr: SocketAddr, num_reqs: usize) -> Result<Vec<Duration>, Report> {
    let start = std::time::Instant::now();
    let mut durs = Vec::with_capacity(num_reqs);
    for i in 0..num_reqs {
        debug!(?i, "running iteration");
        let t = TimeMsg::new(start);
        let cn = ProjectLeft::new(
            addr,
            negotiate_client(
                TimeMsgChunnel::default(),
                UdpSkChunnel.connect(()).await?,
                addr,
            )
            .await?,
        );
        trace!("finished negotiation");
        cn.send(t).await?;
        let m = cn.recv().await?;
        durs.push(m.elapsed(start));
    }

    Ok(durs)
}

#[instrument(err)]
async fn client_direct_zerortt(addr: SocketAddr, num_reqs: usize) -> Result<Vec<Duration>, Report> {
    let start = std::time::Instant::now();
    let mut durs = Vec::with_capacity(num_reqs);
    let mut cl_neg = ClientNegotiator::default();

    // do the first one
    let i = 0;
    let cn = cl_neg
        .negotiate_fetch_nonce(
            TimeMsgChunnel::default(),
            UdpSkChunnel.connect(()).await?,
            addr,
        )
        .await?;
    debug!(?i, "running iteration");
    let t = TimeMsg::new(start);
    let cn = ProjectLeft::new(addr, cn);
    trace!("finished negotiation");
    cn.send(t).await?;
    let m = cn.recv().await?;
    durs.push(m.elapsed(start));

    for i in 1..num_reqs {
        debug!(?i, elapsed = ?start.elapsed(), "running iteration");
        let t = TimeMsg::new(start);
        let cn = ProjectLeft::new(
            addr,
            cl_neg
                .negotiate_zero_rtt(
                    TimeMsgChunnel::default(),
                    UdpSkChunnel.connect(()).await?,
                    addr,
                )
                .await?,
        );
        trace!("finished negotiation");
        cn.send(t).await?;
        let m = cn.recv().await?;
        durs.push(m.elapsed(start));
    }

    Ok(durs)
}

#[derive(Clone, Debug, Copy, serde::Serialize, serde::Deserialize)]
struct TimeMsg(std::time::Duration, u32);

impl TimeMsg {
    fn new(start: std::time::Instant) -> Self {
        let t = start.elapsed();
        assert!(t < std::time::Duration::from_secs(100));
        Self(start.elapsed(), 242947)
    }

    fn elapsed(&self, start: std::time::Instant) -> Duration {
        assert_eq!(self.1, 242947);
        let t = start.elapsed();
        if let Some(d) = t.checked_sub(self.0) {
            d
        } else {
            let diff = self.0 - t;
            debug!(?diff, start = ?self.0, now = ?t, "TimeMsg was negative");
            std::time::Duration::ZERO
        }
    }
}

#[derive(Clone, Debug, Default)]
struct TimeMsgChunnel(SerializeChunnelProject<TimeMsg>);

impl<InC> Chunnel<InC> for TimeMsgChunnel
where
    SerializeChunnelProject<TimeMsg>: Chunnel<InC>,
{
    type Future = <SerializeChunnelProject<TimeMsg> as Chunnel<InC>>::Future;
    type Connection = <SerializeChunnelProject<TimeMsg> as Chunnel<InC>>::Connection;
    type Error = <SerializeChunnelProject<TimeMsg> as Chunnel<InC>>::Error;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        self.0.connect_wrap(cn)
    }
}

impl Negotiate for TimeMsgChunnel {
    type Capability = TimeMsgCap;
    fn guid() -> u64 {
        0xdf3b07a7e6a847b5
    }
    fn capabilities() -> Vec<Self::Capability> {
        vec![TimeMsgCap]
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
struct TimeMsgCap;

impl bertha::negotiate::CapabilitySet for TimeMsgCap {
    fn guid() -> u64 {
        0xdb41bc16f620d494
    }

    fn universe() -> Option<Vec<Self>> {
        None
    }
}

fn dump(mut durs: Vec<Duration>, mode: &str, out_file: std::path::PathBuf) -> Result<(), Report> {
    use std::fs::OpenOptions;
    use std::io::prelude::*;
    info!(?out_file, ?mode, "writing");
    let mut write_header = true;
    match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&out_file)
        .map_err(|e| e.kind())
    {
        Ok(_) => (),
        Err(std::io::ErrorKind::AlreadyExists) => {
            write_header = false;
        }
        Err(e) => {
            let e: std::io::Error = e.into();
            return Err(e).wrap_err(eyre!("creating file: {:?}", out_file));
        }
    }

    let mut f = OpenOptions::new()
        .append(true)
        .open(&out_file)
        .wrap_err(eyre!("opening file: {:?}", out_file))?;
    if write_header {
        writeln!(&mut f, "Mode NumReqs ReqNum Latency_us")?;
    }

    let num_reqs = durs.len();
    for (i, d) in durs.iter().enumerate() {
        writeln!(&mut f, "{} {} {} {}", mode, num_reqs, i, d.as_micros())?;
    }

    durs.sort();
    let len = durs.len() as f64;
    let quantile_idxs = [0.25, 0.5, 0.75, 0.95];
    let quantiles: Vec<_> = quantile_idxs
        .iter()
        .map(|q| (len * q) as usize)
        .map(|i| durs[i])
        .collect();
    info!(
        ?mode, num = ?&durs.len(),
        min = ?durs[0], p25 = ?quantiles[0], p50 = ?quantiles[1],
        p75 = ?quantiles[2], p95 = ?quantiles[3], max = ?durs[durs.len() - 1],
        "done"
    );

    Ok(())
}

#[derive(Clone, Debug)]
struct UdpBaseCl;
impl Chunnel<NeverCn> for UdpBaseCl {
    type Future = <UdpSkChunnel as ChunnelConnector>::Future;
    type Connection = <UdpSkChunnel as ChunnelConnector>::Connection;
    type Error = <UdpSkChunnel as ChunnelConnector>::Error;

    fn connect_wrap(&mut self, _: NeverCn) -> Self::Future {
        UdpSkChunnel::default().connect(())
    }
}
impl Negotiate for UdpBaseCl {
    type Capability = ();
    fn guid() -> u64 {
        0xf1256b8b96d31706
    }
}

#[derive(Clone, Debug)]
struct UdpBaseSrv {
    addr: SocketAddr,
}
impl Chunnel<NeverCn> for UdpBaseSrv {
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Connection = <UdpSkChunnel as ChunnelListener>::Connection;
    type Error = <UdpSkChunnel as ChunnelListener>::Error;

    fn connect_wrap(&mut self, _: NeverCn) -> Self::Future {
        let a = self.addr;
        Box::pin(async move {
            use futures_util::stream::StreamExt;
            UdpSkChunnel::default()
                .listen(a)
                .await?
                .next()
                .await
                .unwrap()
        })
    }
}

impl Negotiate for UdpBaseSrv {
    type Capability = ();
    fn guid() -> u64 {
        0xfd350a1ee135b93b
    }
}
