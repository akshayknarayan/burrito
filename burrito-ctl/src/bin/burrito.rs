use structopt::StructOpt;
use tracing_futures::Instrument;
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
    burrito_coordinator_addr: Option<std::path::PathBuf>,

    #[structopt(short, long)]
    redis_addr: String,

    #[structopt(short, long = "net-addr")]
    net_addrs: Option<Vec<std::net::IpAddr>>,

    #[structopt(long)]
    tracing_file: Option<std::path::PathBuf>,
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
    let burrito_service = burrito.into_hyper_service()?;
    let burrito_rpc_server = hyper::server::Server::builder(uc)
        .serve(hyper::service::make_service_fn(move |_| {
            let bs = burrito_service.clone();
            async move { Ok::<_, hyper::Error>(bs) }
        }))
        .instrument(tracing::span!(tracing::Level::DEBUG, "burrito-ctl"));
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

// write tracing quantiles to file once a second
fn write_tracing(p: std::path::PathBuf, log: &slog::Logger) {
    let subscriber = Builder::default()
        .no_span_recursion()
        //.events(|e: &tracing::Event| {
        //    let mut val = String::new();
        //    let mut f = |field: &tracing::field::Field, value: &dyn std::fmt::Debug| {
        //        if field.name() == "message" {
        //            val.push_str(&format!(" {:?} ", value));
        //        } else if field.name() == "which" {
        //            val.push_str(&format!(" which={:?} ", value));
        //        };
        //    };
        //    e.record(&mut f);
        //    val
        //})
        .build(|| Histogram::new_with_max(10_000_000, 2).unwrap());
    let sid = subscriber.downcaster();
    let d = tracing::Dispatch::new(subscriber);

    //tracing_subscriber::fmt::init();
    tracing::dispatcher::set_global_default(d.clone()).expect("set tracing global default");

    let log = log.clone();
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
            //for (_, hs) in hs.iter_mut() {
            //    for (_, h) in hs {
            //        h.refresh();
            //    }
            //}

            for (span_group, hs) in hs {
                for (event_group, h) in hs {
                    let tag = format!("{}:{}", span_group, event_group);
                    slog::debug!(&log, "tracing"; "event" => &tag,
                        "min" => h.min(),
                        "p25" => h.value_at_quantile(0.25),
                        "p50" => h.value_at_quantile(0.5),
                        "p75" => h.value_at_quantile(0.75),
                        "max" => h.max(),
                        "cnt" => h.len(),
                    );

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
