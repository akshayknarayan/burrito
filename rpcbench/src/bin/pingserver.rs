use slog::{info, warn};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "ping_server")]
struct Opt {
    #[structopt(short, long)]
    unix_addr: Option<std::path::PathBuf>,

    #[structopt(long)]
    burrito_addr: Option<String>,

    #[structopt(short, long)]
    port: Option<u16>,

    #[structopt(long, default_value = "/tmp/burrito")]
    burrito_root: String,
}

#[tokio::main]
async fn main() -> Result<(), failure::Error> {
    let log = burrito_util::logger();
    let opt = Opt::from_args();

    let srv_impl = rpcbench::Server::default();

    if let Some(path) = opt.unix_addr {
        info!(&log, "UDS mode"; "addr" => ?&path);
        let l = tokio::net::UnixListener::bind(&path)?;
        let srv = hyper_unix_connector::UnixConnector::from(l);
        let ping_srv = rpcbench::PingServer::new(srv_impl);
        hyper::server::Server::builder(srv)
            .serve(hyper::service::make_service_fn(move |_| {
                let ps = ping_srv.clone();
                async move { Ok::<_, hyper::Error>(ps) }
            }))
            .await?;
        return Ok(());
    }

    if opt.port.is_none() {
        warn!(&log, "Must specify port if not using unix address");
        failure::bail!("Must specify port if not using unix address");
    }

    let port = opt.port.unwrap();

    if let Some(addr) = opt.burrito_addr {
        info!(&log, "burrito mode";  "burrito_root" => ?&opt.burrito_root, "addr" => ?&addr, "tcp port" => port);
        let srv = burrito_addr::Server::start(&addr, ("tcp", port), &opt.burrito_root).await?;
        let ping_srv = rpcbench::PingServer::new(srv_impl);
        hyper::server::Server::builder(hyper::server::accept::from_stream(srv))
            .serve(hyper::service::make_service_fn(move |_| {
                let ps = ping_srv.clone();
                async move { Ok::<_, hyper::Error>(ps) }
            }))
            .await?;

        return Ok(());
    }

    info!(&log, "TCP mode"; "port" => port);
    let addr = format!("0.0.0.0:{}", port).parse()?;
    tonic::transport::Server::builder()
        .tcp_nodelay(true)
        .add_service(rpcbench::PingServer::new(srv_impl))
        .serve(addr)
        .await?;

    Ok(())
}
