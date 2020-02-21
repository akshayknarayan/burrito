use failure::Error;
use slog::{debug, error, trace};

#[test]
// The sleeps are unfortunate, but there's not really a way to tell when
// a server has started serving :(
fn local_tonic_ping() -> Result<(), Error> {
    let log = test_logger();
    let mut rt = tokio::runtime::Runtime::new()?;

    rt.block_on(async move {
        let l = log.clone();
        let redis = start_redis(&l, 42423);

        tokio::spawn(async move {
            start_tonic_burrito_ctl(&redis.get_addr(), &l)
                .await
                .expect("Burrito Ctl")
        });
        block_for(std::time::Duration::from_millis(200)).await;
        let l = log.clone();
        tokio::spawn(async move {
            start_rpcbench_tonic_server_burrito(&l)
                .await
                .expect("RPC Server")
        });
        block_for(std::time::Duration::from_millis(200)).await;

        trace!(&log, "connecting to burrito controller"; "burrito_root" => "./tmp-tonic-bn");
        let cl = burrito_addr::tonic::Client::new("./tmp-tonic-bn").await?;
        let a: hyper::Uri = burrito_addr::Uri::new("test-rpcbench").into();
        rpcbench::client_ping(
            a,
            cl,
            rpcbench::PingParams {
                work: rpcbench::Work::Immediate as i32,
                amount: 0,
            },
            1, // iters
            1, // reqs per iter
        )
        .await?;

        Ok(())
    })
}

#[test]
fn local_flatbuf_ping() -> Result<(), Error> {
    let log = test_logger();
    let mut rt = tokio::runtime::Runtime::new()?;
    tracing_subscriber::fmt::init();

    rt.block_on(async move {
        let l = log.clone();
        let redis = start_redis(&l, 42422);

        tokio::spawn(async move {
            start_flatbuf_burrito_ctl(&redis.get_addr(), &l)
                .await
                .expect("burrito-ctl")
        });
        block_for(std::time::Duration::from_millis(200)).await;
        let l = log.clone();
        tokio::spawn(async move {
            start_rpcbench_flatbuf_server_burrito(&l)
                .await
                .expect("RPC Server")
        });
        block_for(std::time::Duration::from_millis(200)).await;

        debug!(&log, "connecting to burrito controller"; "burrito_root" => "./tmp-flatbuf-bn");
        let cl = burrito_addr::flatbuf::Client::new("./tmp-flatbuf-bn").await?;
        let a: hyper::Uri = burrito_addr::Uri::new("test-rpcbench").into();
        rpcbench::client_ping(
            a,
            cl,
            rpcbench::PingParams {
                work: rpcbench::Work::Immediate as i32,
                amount: 0,
            },
            1, // iters
            1, // reqs per iter
        )
        .await?;

        Ok(())
    })
}

async fn start_rpcbench_flatbuf_server_burrito(log: &slog::Logger) -> Result<(), Error> {
    let l2 = log.clone();

    // get serving address
    let srv =
        burrito_addr::flatbuf::Server::start("test-rpcbench", 42426, "./tmp-flatbuf-bn").await?;
    let ping_srv = rpcbench::PingServer::new(rpcbench::Server);

    trace!(l2, "spawning test-rpcbench");

    hyper::server::Server::builder(hyper::server::accept::from_stream(srv))
        .serve(hyper::service::make_service_fn(move |_| {
            let ps = ping_srv.clone();
            async move { Ok::<_, hyper::Error>(ps) }
        }))
        .await?;

    Ok(())
}

pub async fn start_flatbuf_burrito_ctl(redis_addr: &str, log: &slog::Logger) -> Result<(), Error> {
    trace!(&log, "removing"; "dir" => "./tmp-flatbuf-bn/");
    std::fs::remove_dir_all("./tmp-flatbuf-bn/").unwrap_or_default();
    trace!(&log, "creating"; "dir" => "./tmp-flatbuf-bn/");
    std::fs::create_dir_all("./tmp-flatbuf-bn/")?;

    let bn = burrito_ctl::BurritoNet::new(
        Some(std::path::PathBuf::from("./tmp-flatbuf-bn/")),
        vec!["127.0.0.1".to_string()],
        redis_addr,
        log.clone(),
    )
    .await?;

    let burrito_addr = bn.listen_path();
    debug!(&log, "burrito-ctl addr"; "addr" => ?&burrito_addr);
    let mut uc = tokio::net::UnixListener::bind(&burrito_addr)?;
    let burrito_rpc_server = bn.serve_flatbuf_on(uc.incoming());

    trace!(&log, "spawning flatbuf burrito_rpc_server");
    let s = burrito_rpc_server.await;
    if let Err(e) = s {
        error!(&log, "burrito_rpc_server crashed"; "err" => ?e);
        panic!(e)
    }

    Ok(())
}

async fn start_rpcbench_tonic_server_burrito(log: &slog::Logger) -> Result<(), Error> {
    let l2 = log.clone();

    // get serving address
    let srv = burrito_addr::tonic::Server::start("test-rpcbench", 42425, "./tmp-tonic-bn").await?;
    let ping_srv = rpcbench::PingServer::new(rpcbench::Server);

    trace!(l2, "spawning test-rpcbench");

    hyper::server::Server::builder(hyper::server::accept::from_stream(srv))
        .serve(hyper::service::make_service_fn(move |_| {
            let ps = ping_srv.clone();
            async move { Ok::<_, hyper::Error>(ps) }
        }))
        .await?;

    Ok(())
}

pub async fn start_tonic_burrito_ctl(redis_addr: &str, log: &slog::Logger) -> Result<(), Error> {
    trace!(&log, "removing"; "dir" => "./tmp-tonic-bn/");
    std::fs::remove_dir_all("./tmp-tonic-bn/").unwrap_or_default();
    trace!(&log, "creating"; "dir" => "./tmp-tonic-bn/");
    std::fs::create_dir_all("./tmp-tonic-bn/")?;

    let bn = burrito_ctl::BurritoNet::new(
        Some(std::path::PathBuf::from("./tmp-tonic-bn/")),
        vec!["127.0.0.1".to_string()],
        redis_addr,
        log.clone(),
    )
    .await?;

    let burrito_addr = bn.listen_path();
    debug!(&log, "burrito_addr"; "addr" => ?&burrito_addr);
    use hyper_unix_connector::UnixConnector;
    let uc: UnixConnector = tokio::net::UnixListener::bind(&burrito_addr)?.into();
    let bn = bn.into_hyper_service()?;
    let burrito_rpc_server =
        hyper::server::Server::builder(uc).serve(hyper::service::make_service_fn(move |_| {
            let bs = bn.clone();
            async move { Ok::<_, hyper::Error>(bs) }
        }));

    trace!(&log, "spawning burrito_rpc_server");
    let s = burrito_rpc_server.await;
    if let Err(e) = s {
        error!(&log, "burrito_rpc_server crashed"; "err" => ?e);
        panic!(e)
    }

    Ok(())
}

pub fn start_redis(log: &slog::Logger, port: u16) -> Redis {
    Redis::start(log, port).expect("starting redis")
}

#[must_use]
pub struct Redis {
    port: u16,
}

impl Redis {
    pub fn start(log: &slog::Logger, port: u16) -> Result<Self, Error> {
        let name = format!("test-burritoctl-redis-{:?}", port);
        kill_redis(port);

        let mut redis = std::process::Command::new("sudo")
            .args(&[
                "DOCKER_HOST=unix:///var/run/burrito-docker.sock",
                "docker",
                "run",
                "--name",
                &name,
                "-d",
                "-p",
                &format!("{}:6379", port),
                "redis:5",
            ])
            .spawn()?;

        std::thread::sleep(std::time::Duration::from_millis(100));

        if let Ok(Some(_)) = redis.try_wait() {
            failure::bail!("Could not start redis");
        }

        let red_conn_string = format!("redis://localhost:{}", port);
        let cl = redis::Client::open(red_conn_string.as_str())?;
        while let Err(_) = cl.get_connection() {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        let s = Self { port };
        debug!(&log, "started redis"; "url" => s.get_addr());
        Ok(s)
    }

    pub fn get_port(&self) -> u16 {
        self.port
    }

    pub fn get_addr(&self) -> String {
        format!("redis://localhost:{}", self.get_port())
    }
}

impl Drop for Redis {
    fn drop(&mut self) {
        kill_redis(self.port);
    }
}
fn kill_redis(port: u16) {
    let name = format!("test-burritoctl-redis-{:?}", port);
    let mut kill = std::process::Command::new("sudo")
        .args(&[
            "DOCKER_HOST=unix:///var/run/burrito-docker.sock",
            "docker",
            "rm",
            "-f",
            &name,
        ])
        .spawn()
        .expect("Could not spawn docker rm");

    kill.wait().expect("Error waiting on docker rm");
}

pub fn test_logger() -> slog::Logger {
    use slog::Drain;
    let plain = slog_term::PlainSyncDecorator::new(slog_term::TestStdoutWriter);
    let drain = slog_term::FullFormat::new(plain).build().fuse();
    slog::Logger::root(drain, slog::o!())
}

pub async fn block_for(d: std::time::Duration) {
    tokio::time::delay_for(d).await;
}
