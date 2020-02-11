use structopt::StructOpt;
use tracing_timing::{Builder, Histogram};

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
    burrito_coordinator_addr: std::path::PathBuf,

    #[structopt(long)]
    resolve_to: std::net::SocketAddr,

    #[structopt(long)]
    tracing_file: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), failure::Error> {
    let opt = Opt::from_args();
    let log = burrito_ctl::logger();

    let out_addr_docker = opt.out_addr_docker.clone();

    if let Some(ref p) = opt.tracing_file {
        write_tracing(p.clone(), &log);
    }

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

    let burrito = burrito_ctl::StaticResolver::new(
        opt.burrito_coordinator_addr,
        &format!("{}", opt.resolve_to),
    );
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

    let burrito_service = burrito.start();
    slog::info!(log, "burrito net starting"; "listening at" => ?&burrito_addr);

    let both_servers = futures_util::future::join(docker_proxy_server, burrito_service);
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

// write tracing quantiles to file once a second
fn write_tracing(p: std::path::PathBuf, _log: &slog::Logger) {
    let subscriber = Builder::default()
        .no_span_recursion()
        .build(|| Histogram::new_with_max(10_000_000, 2).unwrap());
    let sid = subscriber.downcaster();
    let d = tracing::Dispatch::new(subscriber);

    tracing::dispatcher::set_global_default(d.clone()).expect("set tracing global default");

    std::thread::spawn(move || loop {
        use std::io::Write;

        std::thread::sleep(std::time::Duration::from_secs(1));

        // overwrite the file
        // TODO do this atomically with a tmpfile
        let mut f = match std::fs::File::create(p.as_path()) {
            Ok(f) => f,
            Err(_) => return,
        };

        sid.downcast(&d).unwrap().force_synchronize();

        sid.downcast(&d).unwrap().with_histograms(|hs| {
            for (span_group, hs) in hs {
                for (event_group, h) in hs {
                    let tag = format!("{}:{}", span_group, event_group);
                    write!(
                        &mut f,
                        "{}: {} {} {} {} {}\n",
                        tag,
                        h.min(),
                        h.value_at_quantile(0.25),
                        h.value_at_quantile(0.5),
                        h.value_at_quantile(0.75),
                        h.max(),
                    )
                    .expect("write to trace file");
                }
            }

            f.sync_all().expect("sync data to trace file");
        });
    });
}
