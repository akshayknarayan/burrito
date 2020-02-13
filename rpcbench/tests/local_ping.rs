use failure::Error;
use slog::{debug, error, trace};

#[test]
// The sleeps are unfortunate, but there's not really a way to tell when
// a server has started serving :(
fn local_ping() -> Result<(), Error> {
    let log = test_logger();
    let mut rt = tokio::runtime::Runtime::new()?;

    rt.block_on(async move {
        let l = log.clone();
        let redis = start_redis(&l, 42423);

        tokio::spawn(async move {
            start_burrito_ctl(&redis.get_addr(), &l)
                .await
                .expect("Burrito Ctl")
        });
        block_for(std::time::Duration::from_millis(200)).await;
        let l = log.clone();
        tokio::spawn(async move { start_sever_burrito(&l).await.expect("RPC Server") });
        block_for(std::time::Duration::from_millis(200)).await;

        trace!(&log, "connecting to burrito controller"; "burrito_root" => "./tmp-test-bn");
        let cl = burrito_addr::tonic::Client::new("./tmp-test-bn").await?;
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

async fn start_sever_burrito(log: &slog::Logger) -> Result<(), Error> {
    let l2 = log.clone();

    // get serving address
    let srv = burrito_addr::tonic::Server::start("test-rpcbench", 42425, "./tmp-test-bn").await?;
    let ping_srv = rpcbench::PingServer::new(rpcbench::Server);

    trace!(l2, "spawning test-rpcbench");

    hyper::server::Server::builder(srv)
        .serve(hyper::service::make_service_fn(move |_| {
            let ps = ping_srv.clone();
            async move { Ok::<_, hyper::Error>(ps) }
        }))
        .await?;

    Ok(())
}

pub async fn start_burrito_ctl(redis_addr: &str, log: &slog::Logger) -> Result<(), Error> {
    trace!(&log, "removing"; "dir" => "./tmp-test-bn/");
    std::fs::remove_dir_all("./tmp-test-bn/").unwrap_or_default();
    trace!(&log, "creating"; "dir" => "./tmp-test-bn/");
    std::fs::create_dir_all("./tmp-test-bn/")?;

    let bn = burrito_ctl::BurritoNet::new(
        Some(std::path::PathBuf::from("./tmp-test-bn/")),
        vec!["127.0.0.1".to_string()],
        redis_addr,
        log.clone(),
    )
    .await?;

    let burrito_addr = bn.listen_path();
    debug!(&log, "burrito_addr"; "addr" => ?&burrito_addr);
    use hyper_unix_connector::UnixConnector;
    let uc: UnixConnector = tokio::net::UnixListener::bind(&burrito_addr)?.into();
    let bn = bn.start()?;
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
        let mut kill = std::process::Command::new("sudo")
            .args(&[
                "DOCKER_HOST=unix:///var/run/burrito-docker.sock",
                "docker",
                "rm",
                "-f",
                "test-burritoctl-redis",
            ])
            .spawn()
            .expect("Could not spawn docker rm");

        kill.wait().expect("Error waiting on docker rm");

        let mut redis = std::process::Command::new("sudo")
            .args(&[
                "DOCKER_HOST=unix:///var/run/burrito-docker.sock",
                "docker",
                "run",
                "--name",
                "test-burritoctl-redis",
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
        let mut kill = std::process::Command::new("sudo")
            .args(&[
                "DOCKER_HOST=unix:///var/run/burrito-docker.sock",
                "docker",
                "rm",
                "-f",
                "test-burritoctl-redis",
            ])
            .spawn()
            .expect("Could not spawn docker rm");

        kill.wait().expect("Error waiting on docker rm");
    }
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
