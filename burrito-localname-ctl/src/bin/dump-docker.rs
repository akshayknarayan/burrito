use burrito_localname_ctl::docker_proxy;
use structopt::StructOpt;
use tracing::{error, info};

#[derive(Debug, StructOpt)]
#[structopt(name = "dump-docker-proxy")]
struct Opt {
    #[structopt(short, long)]
    in_addr: std::path::PathBuf,

    #[structopt(short, long)]
    out_addr: std::path::PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), eyre::Error> {
    let opt = Opt::from_args();
    color_eyre::install()?;
    tracing_subscriber::fmt::init();

    let out_addr = opt.out_addr.clone();

    std::fs::remove_file(&opt.in_addr).unwrap_or_default(); // ignore error if file was not present
    use hyper_unix_connector::UnixConnector;
    let uc: UnixConnector = tokio::net::UnixListener::bind(&opt.in_addr)?.into();
    let make_service = docker_proxy::MakeDockerProxy {
        out_addr: out_addr.clone(),
    };
    let server = hyper::server::Server::builder(uc).serve(make_service);

    info!(listening_at = ?&opt.in_addr, proxying_to = ?&opt.out_addr, "starting");
    if let Err(e) = server.await {
        error!(err = ?e, "server crashed");
    }

    Ok(())
}
