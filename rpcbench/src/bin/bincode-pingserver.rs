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
use tls_tunnel::{TLSChunnel, TlsConnAddr};
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

    #[structopt(long, default_value = "/tmp")]
    encr_unix_root: PathBuf,

    #[structopt(long)]
    encr_ghostunnel_root: Option<PathBuf>,
}

#[derive(Debug, Clone)]
struct EncryptOpt {
    unix_root: PathBuf,
    ghostunnel_root: PathBuf,
}

impl EncryptOpt {
    fn from(o: &Opt) -> Option<Self> {
        if let Some(ref gt) = o.encr_ghostunnel_root {
            Some(EncryptOpt {
                ghostunnel_root: gt.clone(),
                unix_root: o.encr_unix_root.clone(),
            })
        } else {
            None
        }
    }

    fn unix_root(&self) -> PathBuf {
        self.unix_root.clone()
    }

    fn bin_path(&self) -> PathBuf {
        self.ghostunnel_root.join("ghostunnel")
    }

    fn cert_dir_path(&self) -> PathBuf {
        self.ghostunnel_root.join("test-keys/")
    }
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
async fn udp(srv: rpcbench::Server, port: u16, enc: Option<EncryptOpt>) -> Result<(), Report> {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port);
    info!(?port, encrypt = enc.is_some(), "Serving udp mode");
    if let Some(enc) = enc {
        let tls =
            TLSChunnel::<TlsConnAddr>::new(enc.unix_root(), enc.bin_path(), enc.cert_dir_path())
                .listen(addr);
        let st = negotiate_server(
            CxList::from(KvReliabilityServerChunnel::default())
                .wrap(SerializeChunnelProject::default())
                .wrap(tls),
            UdpReqChunnel::default().listen(addr).await?,
        )
        .await?;
        srv.serve(st).await
    } else {
        let st = negotiate_server(
            CxList::from(KvReliabilityServerChunnel::default())
                .wrap(SerializeChunnelProject::default()),
            UdpReqChunnel::default().listen(addr).await?,
        )
        .await?;
        srv.serve(st).await
    }
}

#[tracing::instrument(skip(srv))]
async fn burrito(
    srv: rpcbench::Server,
    port: u16,
    root: PathBuf,
    enc: Option<EncryptOpt>,
) -> Result<(), Report> {
    use tls_tunnel::TlsConnAddr;
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port);

    if let Some(enc) = enc {
        let lch = LocalNameChunnel::<_, _, (SocketAddr, TlsConnAddr)>::new(
            root.clone(),
            Some((addr, TlsConnAddr::Request)),
            UnixSkChunnel::with_root(root.clone()),
            SerializeChunnelProject::default(),
        )
        .await?;
        let tls = TLSChunnel::<(SocketAddr, TlsConnAddr)>::new(
            enc.unix_root(),
            enc.bin_path(),
            enc.cert_dir_path(),
        )
        .listen(addr);
        let stack = CxList::from(KvReliabilityServerChunnel::default())
            .wrap(SerializeChunnelProject::default())
            .wrap(lch)
            .wrap(tls);

        info!(?port, ?root, "Serving localname mode with encryption");
        let st = negotiate_server(stack, UdpReqChunnel.listen(addr).await?).await?;
        srv.serve(st).await
    } else {
        let lch = LocalNameChunnel::<_, _, SocketAddr>::new(
            root.clone(),
            Some(addr),
            UnixSkChunnel::with_root(root.clone()),
            SerializeChunnelProject::default(),
        )
        .await?;
        let stack = CxList::from(KvReliabilityServerChunnel::default())
            .wrap(SerializeChunnelProject::default())
            .wrap(lch);

        info!(?port, ?root, "Serving localname mode without encryption");
        let st = negotiate_server(stack, UdpReqChunnel.listen(addr).await?).await?;
        srv.serve(st).await
    }
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

    let encrypt = EncryptOpt::from(&opt);
    let port = opt.port.unwrap();
    if let Some(root) = opt.burrito_root {
        return burrito(srv, port, root, encrypt).await;
    }

    udp(srv, port, encrypt).await
}
