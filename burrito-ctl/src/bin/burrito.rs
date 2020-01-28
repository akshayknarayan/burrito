use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "burrito")]
struct Opt {
    #[structopt(short, long)]
    in_addr_docker: std::path::PathBuf,

    #[structopt(short, long)]
    out_addr_docker: std::path::PathBuf,

    #[structopt(short, long)]
    force_burrito: bool,

    #[structopt(short, long)]
    burrito_coordinator_addr: Option<std::path::PathBuf>,

    #[structopt(short, long)]
    redis_addr: String,

    #[structopt(short, long = "net_addr")]
    net_addrs: Option<Vec<std::net::IpAddr>>,
}

fn get_net_addrs() -> Result<Vec<std::net::IpAddr>, failure::Error> {
    let hostname_ret = std::process::Command::new("hostname")
        .arg("-I")
        .output()
        .expect("failed to exec hostname")
        .stdout;

    let ips = String::from_utf8(hostname_ret)?;
    ips.split_whitespace()
        .map(|s| s.parse().map_err(failure::Error::from))
        .collect()
}

#[tokio::main]
async fn main() -> Result<(), failure::Error> {
    let opt = Opt::from_args();
    let log = burrito_ctl::logger();

    let out_addr_docker = opt.out_addr_docker.clone();

    std::fs::remove_file(&opt.in_addr_docker).unwrap_or_default(); // ignore error if file was not present
    use hyper_unix_connector::UnixConnector;
    let uc: UnixConnector = tokio::net::UnixListener::bind(&opt.in_addr_docker).map_err(|e| {
        slog::error!(log, "Could not bind to docker proxy address"; "addr" => ?&opt.in_addr_docker, "err" => ?e);
        e
    })?.into();
    let make_service = burrito_ctl::MakeDockerProxy {
        out_addr: out_addr_docker.clone(),
        log: log.new(slog::o!("server" => "docker_proxy")),
    };
    let docker_proxy_server = hyper::server::Server::builder(uc).serve(make_service);
    slog::info!(log, "docker proxy starting"; "listening at" => ?&opt.in_addr_docker, "proxying to" => ?&opt.out_addr_docker);

    let net_addrs = if let None = opt.net_addrs {
        get_net_addrs()?
    } else {
        opt.net_addrs.unwrap()
    };

    let burrito = burrito_ctl::BurritoNet::new(
        opt.burrito_coordinator_addr,
        net_addrs.into_iter().map(|a| a.to_string()),
        &opt.redis_addr,
        log.new(slog::o!("server" => "burrito_net")),
    )
    .await?;
    let burrito_addr = burrito.listen_path();

    // if force_burrito, then we are ok with hijacking /controller, potentially from another
    // instance of burrito. Might cause bad things.
    // TODO docker-proxy above might want a similar option, although things are stateless there (except for attached ttys)
    if opt.force_burrito {
        std::fs::remove_file(&burrito_addr).unwrap_or_default(); // ignore error if file was not present
    }

    let ba = burrito_addr.clone();
    ctrlc::set_handler(move || {
        std::fs::remove_file(&ba).expect("Remove file for currently listening controller");
        std::process::exit(0);
    })?;

    let uc: UnixConnector = tokio::net::UnixListener::bind(&burrito_addr).map_err(|e| {
        slog::error!(log, "Could not bind to burrito controller address"; "addr" => ?&burrito_addr, "err" => ?e);
        e
    })?.into();
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
