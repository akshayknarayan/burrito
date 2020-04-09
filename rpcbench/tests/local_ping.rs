use failure::{Error, ResultExt};
use slog::debug;
use std::path::Path;
use test_util::*;

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
