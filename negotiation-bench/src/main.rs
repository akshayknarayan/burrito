use bertha::{
    bincode::SerializeChunnelProject,
    negotiate_client, negotiate_rendezvous, negotiate_server,
    udp::{UdpReqChunnel, UdpSkChunnel},
    util::{NeverCn, ProjectLeft},
    Chunnel, ChunnelConnection, ChunnelConnector, ChunnelListener, CxList, Negotiate,
};
use color_eyre::eyre::{Report, WrapErr};
use redis_basechunnel::RedisBase;
use std::net::SocketAddr;
use std::time::Duration;
use std::{future::Future, pin::Pin};
use structopt::StructOpt;
use tracing::{debug, info, instrument};
use tracing_error::ErrorLayer;
use tracing_subscriber::prelude::*;

#[derive(Clone, Debug, StructOpt)]
#[structopt(name = "negotiation-bench")]
struct Opt {
    #[structopt(short, long)]
    num_reqs: usize,
    #[structopt(short, long)]
    redis: Option<String>,
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
    info!(?opt.num_reqs, rendezvous_mode = ?opt.redis.is_some(), "starting");
    let mut addr: SocketAddr = "127.0.0.1:12315".parse().unwrap();
    if let Some(redis) = opt.redis {
        let mut durs = Vec::with_capacity(opt.num_reqs);
        for i in 0..opt.num_reqs {
            addr.set_port(addr.port() + i as u16);
            let redis_base = RedisBase::new(&redis).await?;
            let srv_task = tokio::spawn(server_rendezvous(addr, redis_base));
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;

            let redis_base = RedisBase::new(&redis).await?;
            let dur = client_rendezvous(addr, redis_base).await?;
            durs.push(dur);
            srv_task.abort();
            srv_task.await.unwrap_err();
        }
        dump(durs, "Consensus", opt.out_file).wrap_err("writing outfile")?;
    } else {
        tokio::spawn(server_direct(addr));

        let durs = client_direct(addr, opt.num_reqs)
            .await
            .wrap_err("running client")?;
        dump(durs, "Simplified", opt.out_file).wrap_err("writing outfile")?;
    }

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
    cn.send((addr, t)).await?;
    let (_, m) = cn.recv().await?;
    Ok(m.elapsed(start))
}

async fn server_direct(addr: SocketAddr) -> Result<(), Report> {
    let mut st = negotiate_server(
        TimeMsgChunnel::default(),
        UdpReqChunnel::default().listen(addr).await?,
    )
    .await?;
    use futures_util::stream::TryStreamExt;
    while let Some(cn) = st.try_next().await? {
        tokio::spawn(async move {
            loop {
                let d = cn.recv().await.unwrap();
                cn.send(d).await.unwrap();
            }
        });
    }

    Ok(())
}

async fn client_direct(addr: SocketAddr, num_reqs: usize) -> Result<Vec<Duration>, Report> {
    let start = std::time::Instant::now();
    let mut durs = Vec::with_capacity(num_reqs);
    for _ in 0..num_reqs {
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
        cn.send(t).await?;
        let m = cn.recv().await?;
        durs.push(m.elapsed(start));
    }

    Ok(durs)
}

#[derive(Clone, Debug, Copy, serde::Serialize, serde::Deserialize)]
struct TimeMsg(std::time::Duration);

impl TimeMsg {
    fn new(start: std::time::Instant) -> Self {
        Self(start.elapsed())
    }

    fn elapsed(&self, start: std::time::Instant) -> Duration {
        start.elapsed() - self.0
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

fn dump(durs: Vec<Duration>, mode: &str, out_file: std::path::PathBuf) -> Result<(), Report> {
    use std::io::prelude::*;
    let mut f = std::fs::File::create(&out_file)?;
    writeln!(&mut f, "Mode NumReqs Latency_us")?;
    let num_reqs = durs.len();
    for d in durs {
        writeln!(&mut f, "{} {} {}", mode, num_reqs, d.as_micros())?;
    }

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
