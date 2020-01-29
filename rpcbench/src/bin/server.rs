use slog::info;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "ping_server")]
struct Opt {
    #[structopt(short, long)]
    unix_addr: Option<std::path::PathBuf>,

    #[structopt(short, long)]
    burrito_addr: Option<String>,

    #[structopt(short, long)]
    port: u16,

    #[structopt(short, long, default_value = "/tmp/burrito")]
    burrito_root: String,
}

#[tokio::main]
async fn main() -> Result<(), failure::Error> {
    let log = burrito_ctl::logger();
    let opt = Opt::from_args();

    if let Some(path) = opt.unix_addr {
        info!(&log, "UDS mode"; "addr" => ?&path);
        let l = tokio::net::UnixListener::bind(&path)?;
        let srv = hyper_unix_connector::UnixConnector::from(l);
        let ping_srv = rpcbench::PingServer::new(rpcbench::Server);
        hyper::server::Server::builder(srv)
            .serve(hyper::service::make_service_fn(move |_| {
                let ps = ping_srv.clone();
                async move { Ok::<_, hyper::Error>(ps) }
            }))
            .await?;
        return Ok(());
    }

    if let Some(addr) = opt.burrito_addr {
        info!(&log, "burrito mode"; "burrito_root" => ?&opt.burrito_root, "addr" => ?&addr, "tcp port" => opt.port);
        let srv =
            burrito_addr::Server::start(&addr, opt.port, &opt.burrito_root, Some(&log)).await?;
        let ping_srv = rpcbench::PingServer::new(rpcbench::Server);
        hyper::server::Server::builder(srv)
            .serve(hyper::service::make_service_fn(move |_| {
                let ps = ping_srv.clone();
                async move { Ok::<_, hyper::Error>(ps) }
            }))
            .await?;
        return Ok(());
    }

    info!(&log, "TCP mode"; "port" => opt.port);
    let addr = format!("0.0.0.0:{}", opt.port).parse()?;
    tonic::transport::Server::builder()
        .add_service(rpcbench::PingServer::new(rpcbench::Server))
        .serve(addr)
        .await?;

    Ok(())
}
