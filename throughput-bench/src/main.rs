//! Achieved throughput as number of connections and file size increases.
//!
//! One connection per request, n simultaneous clients looping on establishing connections that
//! each download m bytes.

use bertha::{
    bincode::SerializeChunnelProject, reliable::ReliabilityProjChunnel, tagger::OrderedChunnelProj,
    ChunnelConnection, ChunnelConnector, ChunnelListener, CxList, Select,
};
use color_eyre::eyre::{bail, eyre, Report, WrapErr};
use dpdk_direct::{DpdkUdpReqChunnel, DpdkUdpSkChunnel};
use futures_util::stream::TryStreamExt;
use kvstore::reliability::{KvReliabilityChunnel, KvReliabilityServerChunnel};
use rand::{Rng, SeedableRng};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use structopt::StructOpt;
use tracing::{info, info_span};
use tracing_error::ErrorLayer;
use tracing_futures::Instrument;
use tracing_subscriber::prelude::*;

#[derive(Debug, Clone, StructOpt)]
struct Opt {
    #[structopt(long)]
    cfg: PathBuf,

    #[structopt(short, long)]
    port: u16,

    #[structopt(subcommand)]
    mode: Mode,
}

#[derive(Debug, Clone, StructOpt)]
struct Client {
    #[structopt(long)]
    addr: Ipv4Addr,

    #[structopt(long)]
    num_clients: usize,

    #[structopt(long)]
    download_size: usize,

    #[structopt(long)]
    duration_secs: u64,
}

#[derive(Debug, Clone, StructOpt)]
enum Mode {
    Client(Client),
    Server,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
enum Msg {
    Request(usize, usize),
    Response(usize, Vec<u8>),
}

impl bertha::util::MsgId for Msg {
    fn id(&self) -> usize {
        match self {
            &Msg::Request(i, _) | &Msg::Response(i, _) => i,
        }
    }
}

fn main() -> Result<(), Report> {
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(ErrorLayer::default());
    let d = tracing::Dispatch::new(subscriber);
    d.init();
    color_eyre::install()?;
    let Opt { cfg, port, mode } = Opt::from_args();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;

    rt.block_on(async move {
        if let Mode::Client(cl) = mode {
            let ch = DpdkUdpSkChunnel::new(cfg)?;
            run_clients(ch, cl, port).await?;
        } else {
            let ch = DpdkUdpSkChunnel::new(cfg)?;
            let ch = DpdkUdpReqChunnel(ch);
            run_server(ch, port).await?;
        }

        Ok::<_, Report>(())
    })?;
    Ok(())
}

async fn run_clients<C, Cn, E>(ctr: C, c: Client, port: u16) -> Result<usize, Report>
where
    C: ChunnelConnector<Addr = (), Connection = Cn, Error = E> + Clone + Send + Sync + 'static,
    Cn: ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
    E: Into<Report> + Send + Sync + 'static,
{
    let addr = SocketAddr::from(SocketAddrV4::new(c.addr, port));

    let start = Instant::now();
    let end = start + Duration::from_secs(c.duration_secs);

    let clients: futures_util::stream::FuturesUnordered<_> = (0..c.num_clients)
        .map(|_| tokio::spawn(run_client(ctr.clone(), addr, c.download_size, end)))
        .collect();

    let joined: Vec<Result<usize, Report>> = clients.try_collect().await?;
    joined.into_iter().sum()
}

async fn run_client<C, Cn, E /*F*/>(
    mut ctr: C,
    addr: SocketAddr,
    download_size: usize,
    end_time: Instant,
) -> Result<usize, Report>
where
    C: ChunnelConnector<Addr = (), Connection = Cn, Error = E> + Send + Sync + 'static,
    Cn: ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
    E: Into<Report> + Send + Sync + 'static,
{
    info!(?addr, ?download_size, "starting client");
    let stack = Select::from((
        CxList::from(OrderedChunnelProj::default())
            .wrap(ReliabilityProjChunnel::default())
            .wrap(SerializeChunnelProject::default()),
        CxList::from(KvReliabilityChunnel::default()).wrap(SerializeChunnelProject::default()),
    ))
    .prefer_right();
    let mut tot_bytes = 0;
    let mut num_msgs = 0;
    while Instant::now() < end_time {
        // 1. connect
        let s = stack.clone();
        let cn = ctr.connect(()).await.map_err(Into::into)?;
        let cn = bertha::negotiate_client(s, cn, addr).await?;

        // 2. get bytes
        cn.send((addr, Msg::Request(num_msgs, download_size)))
            .await?;
        match cn.recv().await? {
            (_, Msg::Response(id, payload)) if id == num_msgs => {
                tot_bytes += payload.len();
                num_msgs += 1;
            }
            (_, Msg::Response(id, _)) => bail!("msg id {:?} != {:?}", id, num_msgs),
            _ => bail!("got request at client"),
        }
    }

    Ok(tot_bytes)
}

async fn run_server<L, Cn, E>(mut listener: L, port: u16) -> Result<(), Report>
where
    L: ChunnelListener<Addr = SocketAddr, Connection = Cn, Error = E>,
    Cn: ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
    E: Into<Report> + Send + Sync + 'static,
{
    info!(?port, "starting server");
    let st = listener
        .listen(SocketAddr::from(SocketAddrV4::new(
            std::net::Ipv4Addr::UNSPECIFIED,
            port,
        )))
        .await
        .map_err(Into::into)?;
    let stack = Select::from((
        CxList::from(OrderedChunnelProj::default())
            .wrap(ReliabilityProjChunnel::default())
            .wrap(SerializeChunnelProject::default()),
        CxList::from(KvReliabilityServerChunnel::default())
            .wrap(SerializeChunnelProject::default()),
    ))
    .prefer_right();
    let st = bertha::negotiate::negotiate_server(stack, st)
        .instrument(info_span!("negotiate_server"))
        .await
        .wrap_err("negotiate_server")?;

    tokio::pin!(st);
    while let Some(cn) = st
        .try_next()
        .instrument(info_span!("negotiate_server"))
        .await?
    {
        tokio::spawn(async move {
            let mut rng = rand::rngs::SmallRng::from_entropy();
            loop {
                let (a, msg) = cn.recv().await?;
                match msg {
                    Msg::Request(id, resp_size) => {
                        let mut buf = vec![0u8; resp_size];
                        rng.fill(&mut buf[..]);
                        cn.send((a, Msg::Response(id, buf))).await?;
                    }
                    _ => break Err::<(), _>(eyre!("Got response at server")),
                }
            }
        });
    }

    unreachable!() // negotiate_server never returns None
}
