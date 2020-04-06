use failure::{Error, ResultExt};
use slog::debug;
use std::path::Path;

#[test]
fn discname_lookup() -> Result<(), Error> {
    let log = test_logger();
    let root: std::path::PathBuf = "./tmp-bn-discname-lookup".parse().unwrap();
    reset_root_dir(&root, &log);

    let mut rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async move {
        let l = log.clone();
        let redis = start_redis(&l, 63339);
        let redis_addr = redis.get_addr();
        let root2 = root.clone();
        block_for(std::time::Duration::from_millis(400)).await;
        tokio::spawn(async move {
            burrito_discovery_ctl::ctl::serve_ctl(
                Some(root2.clone()),
                &redis_addr,
                vec!["127.0.0.1".parse().unwrap()],
                true,
            )
            .await
        });
        block_for(std::time::Duration::from_millis(400)).await;
        tokio::spawn(burrito_localname_ctl::ctl::serve_ctl(
            Some(root.clone()),
            true,
            l,
        ));
        block_for(std::time::Duration::from_millis(400)).await;

        let l = log.clone();
        let root2 = root.clone();
        tokio::spawn(async move {
            start_rpcbench_server_burrito("lookuptest", 42428, &root2, &l)
                .await
                .expect("RPC Server")
        });
        block_for(std::time::Duration::from_millis(200)).await;

        debug!(&log, "connecting to burrito controller"; "burrito_root" => ?&root);
        let mut dcl = burrito_discovery_ctl::client::DiscoveryClient::new(root.clone())
            .await
            .expect("Connect client discoveryclient");

        let rsp = dcl
            .query("lookuptest".into())
            .await
            .map_err(|e| failure::format_err!("Could not query discovery-ctl: {}", e))?;

        assert_eq!(rsp.name, "lookuptest");
        debug!(&log, "Got services"; "services" => ?&rsp.services);

        let (dsc, lcl) = rsp.services.into_iter().fold((None, None), |acc, srv| {
            let burrito_discovery_ctl::proto::Service { ref service, .. } = srv;
            match service.as_str() {
                burrito_discovery_ctl::CONTROLLER_ADDRESS => (acc.0.or_else(|| Some(srv)), acc.1),
                burrito_localname_ctl::CONTROLLER_ADDRESS => {
                    if let Some(_) = acc.1 {
                        panic!("Duplicate entry");
                    }

                    (acc.0, Some(srv))
                }
                _ => acc,
            }
        });

        assert!(matches!(
            dsc,
            Some(burrito_discovery_ctl::proto::Service { name, address, .. }) if name == "lookuptest" && address == burrito_discovery_ctl::proto::Addr::Tcp("127.0.0.1:42428".parse().unwrap())
        ));
        assert!(matches!(
            lcl,
            Some(burrito_discovery_ctl::proto::Service { name, address, .. }) if name == "lookuptest" && address == burrito_discovery_ctl::proto::Addr::Burrito("lookuptest".into())
        ));
        Ok(())
    })
}

#[test]
fn discname_ping() -> Result<(), Error> {
    let log = test_logger();
    let root: std::path::PathBuf = "./tmp-bn-discname".parse().unwrap();
    reset_root_dir(&root, &log);

    let mut rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async move {
        let l = log.clone();
        let redis = start_redis(&l, 63379);
        let redis_addr = redis.get_addr();
        let root2 = root.clone();
        block_for(std::time::Duration::from_millis(400)).await;
        tokio::spawn(async move {
            burrito_discovery_ctl::ctl::serve_ctl(
                Some(root2.clone()),
                &redis_addr,
                vec!["127.0.0.1".parse().unwrap()],
                true,
            )
            .await
        });
        block_for(std::time::Duration::from_millis(400)).await;
        tokio::spawn(burrito_localname_ctl::ctl::serve_ctl(
            Some(root.clone()),
            true,
            l,
        ));
        block_for(std::time::Duration::from_millis(400)).await;

        let l = log.clone();
        let root2 = root.clone();
        tokio::spawn(async move {
            start_rpcbench_server_burrito("burrito://pingtest", 42427, &root2, &l)
                .await
                .expect("RPC Server")
        });
        block_for(std::time::Duration::from_millis(200)).await;

        debug!(&log, "connecting to burrito controller"; "burrito_root" => ?&root);
        let cl = burrito_addr::Client::new(&root).await?;
        let a: hyper::Uri = burrito_addr::Uri::new("burrito://pingtest").into();
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
fn localname_ping() -> Result<(), Error> {
    let log = test_logger();
    let root: std::path::PathBuf = "./tmp-bn-localname".parse().unwrap();
    reset_root_dir(&root, &log);

    let mut rt = tokio::runtime::Runtime::new()?;

    rt.block_on(async move {
        let l = log.clone();
        tokio::spawn(burrito_localname_ctl::ctl::serve_ctl(
            Some(root.clone()),
            true,
            l.clone(),
        ));
        block_for(std::time::Duration::from_millis(200)).await;
        let l = log.clone();
        let root2 = root.clone();
        tokio::spawn(async move {
            start_rpcbench_server_burrito("tcp://127.0.0.1:42426", 42426, &root2, &l)
                .await
                .expect("RPC Server")
        });
        block_for(std::time::Duration::from_millis(200)).await;

        debug!(&log, "connecting to burrito controller"; "burrito_root" => ?&root);
        let cl = burrito_addr::Client::new(&root).await?;
        let a: hyper::Uri = burrito_addr::Uri::new("tcp://127.0.0.1:42426").into();
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

async fn start_rpcbench_server_burrito(
    addr: &str,
    port: u16,
    root: &Path,
    log: &slog::Logger,
) -> Result<(), Error> {
    let l2 = log.clone();

    // get serving address
    let srv = burrito_addr::Server::start(addr, ("tcp", port), root)
        .await
        .context("burrito_addr server")?;
    let ping_srv = rpcbench::PingServer::new(rpcbench::Server::default());

    debug!(l2, "spawning test-rpcbench");

    hyper::server::Server::builder(hyper::server::accept::from_stream(srv))
        .serve(hyper::service::make_service_fn(move |_| {
            let ps = ping_srv.clone();
            async move { Ok::<_, hyper::Error>(ps) }
        }))
        .await?;

    Ok(())
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

fn reset_root_dir(path: &std::path::Path, log: &slog::Logger) {
    debug!(log, "removing"; "dir" => ?&path);
    std::fs::remove_dir_all(&path).unwrap_or_default();
    debug!(log, "creating"; "dir" => ?&path);
    std::fs::create_dir_all(&path).unwrap();
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
