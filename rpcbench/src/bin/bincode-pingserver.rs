use bertha::{
    bincode::SerializeChunnelProject,
    negotiate_server,
    udp::UdpReqChunnel,
    uds::{UnixReqChunnel, UnixSkChunnel},
    ChunnelListener, CxList,
};
use burrito_localname_ctl::LocalNameChunnel;
use color_eyre::eyre::{bail, Report};
use kvstore::reliability::KvReliabilityServerChunnel;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use structopt::StructOpt;
use tracing::info;
use tracing_error::ErrorLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, StructOpt)]
#[structopt(name = "ping_server")]
struct Opt {
    #[structopt(short, long)]
    unix_addr: Option<PathBuf>,

    #[structopt(short, long)]
    port: Option<u16>,

    #[structopt(long)]
    burrito_root: Option<PathBuf>,
}

#[tracing::instrument(skip(srv))]
async fn unix(srv: rpcbench::Server, addr: PathBuf) -> Result<(), Report> {
    info!(?addr, "Serving unix-only mode");
    let st = negotiate_server(
        CxList::from(KvReliabilityServerChunnel::default())
            .wrap(SerializeChunnelProject::default()),
        UnixReqChunnel.listen(addr).await?,
    )
    .await?;
    srv.serve(st).await
}

#[tracing::instrument(skip(srv))]
async fn burrito(srv: rpcbench::Server, port: u16, root: PathBuf) -> Result<(), Report> {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port);
    let lch = LocalNameChunnel::new(
        root.clone(),
        Some(addr),
        UnixSkChunnel::default(),
        SerializeChunnelProject::default(),
    )
    .await?;
    let stack = CxList::from(KvReliabilityServerChunnel::default())
        .wrap(SerializeChunnelProject::default())
        .wrap(lch);
    info!(?port, ?root, "Serving localname mode");
    let st = negotiate_server(stack, UdpReqChunnel.listen(addr).await?).await?;
    srv.serve(st).await
}

#[tracing::instrument(skip(srv))]
async fn udp(srv: rpcbench::Server, port: u16) -> Result<(), Report> {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port);
    info!(?port, "Serving udp mode");
    let st = negotiate_server(
        CxList::from(KvReliabilityServerChunnel::default())
            .wrap(SerializeChunnelProject::default()),
        UdpReqChunnel::default().listen(addr).await?,
    )
    .await?;
    srv.serve(st).await
}

#[tokio::main]
async fn main() -> Result<(), Report> {
    let opt = Opt::from_args();
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(ErrorLayer::default());
    let _guard = subscriber.set_default();
    color_eyre::install().unwrap();

    let srv = rpcbench::Server::default();
    if let Some(path) = opt.unix_addr {
        return unix(srv, path).await;
    }

    if opt.port.is_none() {
        bail!("Must specify port if not using unix address");
    }

    let port = opt.port.unwrap();

    if let Some(root) = opt.burrito_root {
        return burrito(srv, port, root).await;
    }

    udp(srv, port).await
}
