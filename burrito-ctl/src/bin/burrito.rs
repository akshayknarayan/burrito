use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "burrito")]
struct Opt {
    #[structopt(short, long)]
    in_addr_docker: std::path::PathBuf,

    #[structopt(short, long)]
    out_addr_docker: std::path::PathBuf,

    #[structopt(short, long)]
    burrito_coordinator_addr: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), failure::Error> {
    let opt = Opt::from_args();
    let log = burrito_ctl::logger();

    let out_addr_docker = opt.out_addr_docker.clone();

    use hyper_unix_connector::UnixConnector;
    let uc: UnixConnector = tokio::net::UnixListener::bind(&opt.in_addr_docker)?.into();
    let make_service = burrito_ctl::MakeDockerProxy {
        out_addr: out_addr_docker.clone(),
        log: log.new(slog::o!("server" => "docker_proxy")),
    };
    let docker_proxy_server = hyper::server::Server::builder(uc).serve(make_service);
    slog::info!(log, "docker proxy starting"; "listening at" => ?&opt.in_addr_docker, "proxying to" => ?&opt.out_addr_docker);

    let burrito = burrito_ctl::BurritoNet::new(
        opt.burrito_coordinator_addr,
        log.new(slog::o!("server" => "burrito_net")),
    );
    let burrito_addr = burrito.listen_path();
    let uc: UnixConnector = tokio::net::UnixListener::bind(&burrito_addr)?.into();
    let burrito_service = burrito.start()?;
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
