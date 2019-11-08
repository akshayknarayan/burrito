use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "burrito")]
struct Opt {
    #[structopt(short, long)]
    docker_in_addr: std::path::PathBuf,

    #[structopt(short, long)]
    docker_out_addr: std::path::PathBuf,

    #[structopt(short, long)]
    burrito_coordinator_addr: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), failure::Error> {
    let opt = Opt::from_args();
    let log = burrito::logger();

    let docker_out_addr = opt.docker_out_addr.clone();

    use hyper_unix_connector::UnixConnector;
    let uc: UnixConnector = tokio::net::UnixListener::bind(&opt.docker_in_addr)?.into();
    let make_service = burrito::MakeDockerProxy {
        out_addr: docker_out_addr.clone(),
        log: log.new(slog::o!("server" => "docker_proxy")),
    };
    let docker_proxy_server = hyper::server::Server::builder(uc).serve(make_service);
    slog::info!(log, "docker proxy starting"; "listening at" => ?&opt.docker_in_addr, "proxying to" => ?&opt.docker_out_addr);

    let burrito = burrito::BurritoNet::new(
        opt.burrito_coordinator_addr,
        log.new(slog::o!("server" => "burrito_net")),
    );
    let burrito_addr = burrito.listen_path();
    let uc: UnixConnector = tokio::net::UnixListener::bind(&burrito_addr)?.into();
    let burrito_service = burrito.start_burritonet()?;
    let burrito_rpc_server =
        hyper::server::Server::builder(uc).serve(hyper::service::make_service_fn(move |_| {
            let bs = burrito_service.clone();
            async move { Ok::<_, hyper::Error>(bs) }
        }));
    slog::info!(log, "burrito net starting"; "listening at" => ?&burrito_addr);

    let both_servers = futures_util::future::join(docker_proxy_server, burrito_rpc_server);
    match both_servers.await {
        (Err(e1), Err(e2)) => {
            slog::crit!(log, "crash"; "docker_proxy" => ?e1, "burrito_rpc" => ?e2);
        }
        (Err(e), _) => {
            slog::crit!(log, "crash"; "docker_proxy" => ?e);
        }
        (_, Err(e)) => {
            slog::crit!(log, "crash"; "burrito_rpc" => ?e);
        }
        _ => (),
    }

    Ok(())
}
