use anyhow::Error;
use slog::debug;
use test_util::*;

#[cfg(feature = "bin")]
#[test]
fn recursive_shardctl_test() -> Result<(), Error> {
    use anyhow::Context;

    let log = test_logger();
    let root: std::path::PathBuf = "./tmp-bn-shardctl-rec".parse().unwrap();
    reset_root_dir(&root, &log);

    let mut rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async move {
        let l = log.clone();
        let redis = start_redis(&l, 63949);
        let redis_addr = redis.get_addr();
        let ra = redis_addr.clone();
        let root2 = root.clone();
        block_for(std::time::Duration::from_millis(400)).await; // wait for redis

        tokio::spawn(async move {
            burrito_discovery_ctl::ctl::serve_ctl(
                Some(root2.clone()),
                &ra,
                vec!["127.0.0.1".parse().unwrap()],
                true,
            )
            .await
        });
        block_for(std::time::Duration::from_millis(400)).await; // wait for discovery-ctl

        let shardctl = burrito_shard_ctl::ShardCtl::new(&redis_addr).await?;
        let ul = tokio::net::UnixListener::bind(root.join(burrito_shard_ctl::CONTROLLER_ADDRESS))?;
        tokio::spawn(shardctl.serve_on(ul, None));
        block_for(std::time::Duration::from_millis(400)).await; // wait for shard-ctl

        // start server
        start_rec_server(root.as_path(), 59422).await;
        block_for(std::time::Duration::from_millis(200)).await;

        debug!(&log, "connecting to burrito controller"; "burrito_root" => ?&root);
        let mut dcl = burrito_discovery_ctl::client::DiscoveryClient::new(&root)
            .await
            .expect("Connect client discoveryclient");
        let mut shardctl = burrito_shard_ctl::ShardCtlClient::new(&root).await.unwrap();

        let si = shardctl
            .query_recursive(&mut dcl, "burrito://shardtest")
            .await
            .context("Could not query shard-ctl")?;

        assert!(matches!(
            si,
            burrito_shard_ctl::proto::ShardInfo {
                canonical_addr: burrito_shard_ctl::proto::Addr::Udp(ca),
                shard_info: burrito_shard_ctl::proto::SimpleShardPolicy {
                    packet_data_offset: 18,
                    packet_data_length: 4,
                },
                ..
            } if ca.ip().is_loopback() && ca.port() == 59422
        ));

        debug!(&log, "Checking shard addrs"; "shard_addrs" => ?&si.shard_addrs);

        let mut ps: Vec<u16> = si
            .shard_addrs
            .iter()
            .map(|a| match a {
                burrito_shard_ctl::proto::Addr::Udp(sa) if sa.ip().is_loopback() => sa.port(),
                _ => panic!("shard addresses wrong"),
            }).collect();
        ps.sort();

        for (p, r) in ps.iter().zip(59423..=59429) {
            assert_eq!(*p, r);
        }

        Ok(())
    })
}

async fn start_rec_server(root: &std::path::Path, start: u16) {
    use burrito_shard_ctl::proto;
    let addrs: Vec<proto::Addr> = (0..8)
        .map(|i| format!("udp://127.0.0.1:{}", start + i).parse().unwrap())
        .collect();

    // shardtest -> (shard-1, shard-2)
    // shard-1 -> (p1, p2)
    // shard-2 -> (shard-3, shard-4, p3)
    // shard-3 -> (p4, p5)
    // shard-4 -> (p6, p7)
    let n0: proto::Addr = "burrito://shardtest".parse().unwrap();
    let n1: proto::Addr = "burrito://shard-1".parse().unwrap();
    let n2: proto::Addr = "burrito://shard-2".parse().unwrap();
    let n3: proto::Addr = "burrito://shard-3".parse().unwrap();
    let n4: proto::Addr = "burrito://shard-4".parse().unwrap();
    let p1: proto::Addr = "burrito://shard-1-1".parse().unwrap();
    let p2: proto::Addr = "burrito://shard-1-2".parse().unwrap();
    let p3: proto::Addr = "burrito://shard-2-3".parse().unwrap();
    let p4: proto::Addr = "burrito://shard-3-1".parse().unwrap();
    let p5: proto::Addr = "burrito://shard-3-2".parse().unwrap();
    let p6: proto::Addr = "burrito://shard-4-1".parse().unwrap();
    let p7: proto::Addr = "burrito://shard-4-2".parse().unwrap();

    let ns = vec![n0.clone(), n1.clone(), n2.clone(), n3.clone(), n4.clone()];
    let ps = vec![p1.clone(), p2.clone(), p3.clone(), p4.clone(), p5.clone(), p6.clone(), p7.clone()];

    {
        let mut dcl = burrito_discovery_ctl::client::DiscoveryClient::new(root.clone())
            .await
            .expect("Connect client discoveryclient");

        dcl.register(burrito_discovery_ctl::proto::Service {
            name: "shardtest".to_owned(),
            scope: burrito_discovery_ctl::proto::Scope::Global,
            service: burrito_discovery_ctl::CONTROLLER_ADDRESS.to_owned(),
            address: addrs[0].clone(),
        })
        .await
        .expect("Could not register discovery-ctl");

        // register all the shards
        for (n, a) in ps.iter().zip(addrs[1..].iter()) {
            let s = match n {
                burrito_discovery_ctl::proto::Addr::Burrito(s) => s,
                _ => unreachable!(),
            };

            dcl.register(burrito_discovery_ctl::proto::Service {
                name: s.clone(),
                scope: burrito_discovery_ctl::proto::Scope::Global,
                service: burrito_discovery_ctl::CONTROLLER_ADDRESS.to_owned(),
                address: a.clone(),
            })
            .await
            .expect("Could not register discovery-ctl");
        }

        for n in ns.iter() {
            dcl.register(burrito_discovery_ctl::proto::Service {
                name: match n {
                    proto::Addr::Burrito(s) => s.clone(),
                    _ => unreachable!(),
                },
                scope: burrito_discovery_ctl::proto::Scope::Global,
                service: burrito_shard_ctl::CONTROLLER_ADDRESS.to_owned(),
                address: n.clone(),
            })
            .await
            .expect("Could not register discovery-ctl");
        }
    }

    let si = proto::ShardInfo {
        canonical_addr: n0,
        shard_addrs: vec![n1.clone(), n2.clone()],
        shard_info: proto::SimpleShardPolicy {
            packet_data_offset: 18,
            packet_data_length: 4,
        },
    };

    let si1 = proto::ShardInfo {
        canonical_addr: n1,
        shard_addrs: vec![p1, p2],
        shard_info: proto::SimpleShardPolicy {
            packet_data_offset: 18,
            packet_data_length: 4,
        },
    };

    let si2 = proto::ShardInfo {
        canonical_addr: n2,
        shard_addrs: vec![n3.clone(), n4.clone(), p3],
        shard_info: proto::SimpleShardPolicy {
            packet_data_offset: 18,
            packet_data_length: 4,
        },
    };

    let si3 = proto::ShardInfo {
        canonical_addr: n3,
        shard_addrs: vec![p4, p5],
        shard_info: proto::SimpleShardPolicy {
            packet_data_offset: 18,
            packet_data_length: 4,
        },
    };

    let si4 = proto::ShardInfo {
        canonical_addr: n4,
        shard_addrs: vec![p6, p7],
        shard_info: proto::SimpleShardPolicy {
            packet_data_offset: 18,
            packet_data_length: 4,
        },
    };

    // register the shards
    {
        let mut shardctl = burrito_shard_ctl::ShardCtlClient::new(root).await.unwrap();
        shardctl.register(si).await.unwrap();
        shardctl.register(si1).await.unwrap();
        shardctl.register(si2).await.unwrap();
        shardctl.register(si3).await.unwrap();
        shardctl.register(si4).await.unwrap();
    } // drop shardctl connection
}

#[cfg(feature = "bin")]
#[test]
fn shardctl_test() -> Result<(), Error> {
    use anyhow::Context;

    let log = test_logger();
    let root: std::path::PathBuf = "./tmp-bn-shardctl".parse().unwrap();
    reset_root_dir(&root, &log);

    let mut rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async move {
        let l = log.clone();
        let redis = start_redis(&l, 63349);
        let redis_addr = redis.get_addr();
        let ra = redis_addr.clone();
        let root2 = root.clone();
        block_for(std::time::Duration::from_millis(400)).await; // wait for redis

        tokio::spawn(async move {
            burrito_discovery_ctl::ctl::serve_ctl(
                Some(root2.clone()),
                &ra,
                vec!["127.0.0.1".parse().unwrap()],
                true,
            )
            .await
        });
        block_for(std::time::Duration::from_millis(400)).await; // wait for discovery-ctl

        let shardctl = burrito_shard_ctl::ShardCtl::new(&redis_addr).await?;
        let ul = tokio::net::UnixListener::bind(root.join(burrito_shard_ctl::CONTROLLER_ADDRESS))?;
        tokio::spawn(shardctl.serve_on(ul, None));
        block_for(std::time::Duration::from_millis(400)).await; // wait for shard-ctl

        // start server
        start_disc_server(root.as_path(), 29422).await;
        block_for(std::time::Duration::from_millis(200)).await;

        debug!(&log, "connecting to burrito controller"; "burrito_root" => ?&root);
        let mut dcl = burrito_discovery_ctl::client::DiscoveryClient::new(&root)
            .await
            .expect("Connect client discoveryclient");
        let mut shardctl = burrito_shard_ctl::ShardCtlClient::new(&root).await.unwrap();

        let si = shardctl
            .query_recursive(&mut dcl, "burrito://shardtest")
            .await
            .context("Could not query shard-ctl")?;

        assert!(matches!(
            si,
            burrito_shard_ctl::proto::ShardInfo {
                canonical_addr: burrito_shard_ctl::proto::Addr::Udp(ca),
                shard_info: burrito_shard_ctl::proto::SimpleShardPolicy {
                    packet_data_offset: 18,
                    packet_data_length: 4,
                },
                ..
            } if ca.ip().is_loopback() && ca.port() == 29422
        ));

        for (p, r) in si
            .shard_addrs
            .iter()
            .map(|a| match a {
                burrito_shard_ctl::proto::Addr::Udp(sa) if sa.ip().is_loopback() => sa.port(),
                _ => panic!("shard addresses wrong"),
            })
            .zip(29423..29430)
        {
            assert_eq!(p, r);
        }

        Ok(())
    })
}

async fn start_disc_server(root: &std::path::Path, start: u16) {
    use burrito_shard_ctl::proto;
    let addrs: Vec<proto::Addr> = (0..8)
        .map(|i| format!("udp://127.0.0.1:{}", start + i).parse().unwrap())
        .collect();
    let names: Vec<proto::Addr> = (0..7)
        .map(|i| format!("burrito://shard-{}", i).parse().unwrap())
        .collect();
    let si = proto::ShardInfo {
        canonical_addr: "burrito://shardtest".parse().unwrap(),
        shard_addrs: names[0..].into(),
        shard_info: proto::SimpleShardPolicy {
            packet_data_offset: 18,
            packet_data_length: 4,
        },
    };

    // register the shards
    {
        let mut shardctl = burrito_shard_ctl::ShardCtlClient::new(root).await.unwrap();
        shardctl.register(si).await.unwrap();
    } // drop shardctl connection

    {
        let mut dcl = burrito_discovery_ctl::client::DiscoveryClient::new(root.clone())
            .await
            .expect("Connect client discoveryclient");

        dcl.register(burrito_discovery_ctl::proto::Service {
            name: "shardtest".to_owned(),
            scope: burrito_discovery_ctl::proto::Scope::Global,
            service: burrito_discovery_ctl::CONTROLLER_ADDRESS.to_owned(),
            address: addrs[0].clone(),
        })
        .await
        .expect("Could not register discovery-ctl");

        // register all the shards
        for (n, a) in names.iter().zip(addrs[1..].iter()) {
            let s = match n {
                burrito_discovery_ctl::proto::Addr::Burrito(s) => s,
                _ => unreachable!(),
            };

            dcl.register(burrito_discovery_ctl::proto::Service {
                name: s.clone(),
                scope: burrito_discovery_ctl::proto::Scope::Global,
                service: burrito_discovery_ctl::CONTROLLER_ADDRESS.to_owned(),
                address: a.clone(),
            })
            .await
            .expect("Could not register discovery-ctl");
        }

        dcl.register(burrito_discovery_ctl::proto::Service {
            name: "shardtest".to_owned(),
            scope: burrito_discovery_ctl::proto::Scope::Global,
            service: burrito_shard_ctl::CONTROLLER_ADDRESS.to_owned(),
            address: "burrito://shardtest".parse().unwrap(),
        })
        .await
        .expect("Could not register discovery-ctl");
    }
}

#[cfg(feature = "bin")]
#[test]
fn basic_shardctl_test() -> Result<(), Error> {
    use anyhow::Context;

    let log = test_logger();
    let root: std::path::PathBuf = "./tmp-bn-shardctl-local".parse().unwrap();
    reset_root_dir(&root, &log);

    let mut rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async move {
        let l = log.clone();
        let redis = start_redis(&l, 64349);
        let redis_addr = redis.get_addr();
        block_for(std::time::Duration::from_millis(400)).await; // wait for redis

        let shardctl = burrito_shard_ctl::ShardCtl::new(&redis_addr)
            .await
            .context("make shardctl")?;
        let ul = tokio::net::UnixListener::bind(root.join(burrito_shard_ctl::CONTROLLER_ADDRESS))
            .context("bind unix listener")?;
        tokio::spawn(shardctl.serve_on(ul, None));
        block_for(std::time::Duration::from_millis(400)).await; // wait for shard-ctl

        // start server
        start_local_server(root.as_path()).await;
        block_for(std::time::Duration::from_millis(200)).await;

        let mut cl = burrito_shard_ctl::ShardCtlClient::new(&root)
            .await
            .context("shardctl client")?;
        let a: burrito_shard_ctl::proto::Addr = format!("udp://127.0.0.1:19422").parse().unwrap();
        let r = cl.query(a).await.context("query shardctl")?;

        assert!(matches!(r, burrito_shard_ctl::proto::ShardInfo {
            canonical_addr: burrito_shard_ctl::proto::Addr::Udp(ca), 
            shard_info: burrito_shard_ctl::proto::SimpleShardPolicy {
                packet_data_offset: 18, packet_data_length: 4,
            },
            ..
        } if ca.ip().is_loopback() && ca.port() == 19422));

        Ok(())
    })
}

async fn start_local_server(root: &std::path::Path) {
    use burrito_shard_ctl::proto;
    let addrs: Vec<proto::Addr> = (19422..19426)
        .map(|i| format!("udp://127.0.0.1:{}", i).parse().unwrap())
        .collect();
    let si = proto::ShardInfo {
        canonical_addr: addrs[0].clone(),
        shard_addrs: addrs[1..].into(),
        shard_info: proto::SimpleShardPolicy {
            packet_data_offset: 18,
            packet_data_length: 4,
        },
    };

    // register the shards
    {
        let mut shardctl = burrito_shard_ctl::ShardCtlClient::new(root).await.unwrap();
        shardctl.register(si).await.unwrap();
    } // drop shardctl connection
}
