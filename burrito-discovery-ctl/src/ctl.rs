use crate::proto;
use anyhow::Error;
use std::collections::HashMap;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info, trace, warn};

/// Serve the ctl.
///
/// See also [`Ctl`].
///
/// `force`: If true, root will be removed before attempting to listen.
pub async fn serve_ctl(
    root: Option<PathBuf>,
    redis_addr: &str,
    local_addrs: impl IntoIterator<Item = std::net::IpAddr>,
    force: bool,
) -> Result<(), Error> {
    // get burrito-localname-ctl serving future
    let burrito = Ctl::new(redis_addr, local_addrs).await?;

    let burrito_addr = root
        .unwrap_or_else(|| "/tmp/burrito".parse().unwrap())
        .join(crate::CONTROLLER_ADDRESS);

    // if force_burrito, then we are ok with hijacking /controller, potentially from another
    // instance of burrito. Might cause bad things.
    // TODO docker-proxy might want a similar option, although things are stateless there (except for attached ttys)
    if force {
        std::fs::remove_file(&burrito_addr).unwrap_or_default(); // ignore error if file was not present
    }

    //let ba = burrito_addr.clone();
    //ctrlc::set_handler(move || {
    //    std::fs::remove_file(&ba).expect("Remove file for currently listening controller");
    //    std::process::exit(0);
    //})?;

    info!(listening = ?&burrito_addr, "burrito net starting");
    let uc = tokio::net::UnixListener::bind(&burrito_addr).map_err(|e| {
        error!(addr = ?&burrito_addr, err = ?e, "Could not bind to burrito controller address");
        e
    })?;

    burrito.serve_on(uc).await?;
    Ok(())
}

#[derive(Clone)]
pub struct Ctl {
    service_table: Arc<RwLock<HashMap<String, Vec<proto::Service>>>>,
    local_public_addrs: Vec<std::net::IpAddr>,
    redis_client: redis::Client,
    redis_listen_connection: Arc<Mutex<redis::aio::Connection>>,
}

impl Ctl {
    /// redis_addr: The address, in redis format:
    ///   From https://docs.rs/redis/0.15.1/redis/
    ///   `The URL format is redis://[:<passwd>@]<hostname>[:port][/<db>]`
    ///   If Unix socket support is available you can use a unix URL in this format:
    ///   `redis+unix:///[:<passwd>@]<path>[?db=<db>]`
    ///
    ///   Why is this necessary?
    ///   The redis instance needs to be accessible from all hosts running burrito-ctl. So, we
    ///   can't start it ourselves because then a coordination mechanism would be needed to pick a common redis
    ///   server among each host's burrito-ctl, which is what we are using redis for in the first
    ///   place.
    ///
    /// local_public_addrs: What is/are the public IP address(es) of this machine?
    ///   Callers should be able to get this information either because:
    ///   1. BurritoNet is running on the host/baremetal.
    ///   2. It's possible to fetch the information from k8s/docker/cloud provider in an
    ///      environment-specific way.
    ///
    ///   Why is this necessary?
    ///   Server logic generally knows what port it's listening on, so we can ask for that info in
    ///   ListenRequest.
    ///   But for IP, the usual trick of binding to 0.0.0.0 means server logic doesn't have
    ///   to know. In the case of containers, it can't even easily figure out what the real public ip
    ///   is because of the docker bridge.
    ///
    ///   So, we have to figure it out for the service.
    pub async fn new(
        redis_addr: &str,
        local_addrs: impl IntoIterator<Item = std::net::IpAddr>,
    ) -> Result<Self, Error> {
        let redis_client = redis::Client::open(redis_addr)?;
        let redis_listen_connection =
            Arc::new(Mutex::new(redis_client.get_async_connection().await?));

        let s = Self {
            local_public_addrs: local_addrs.into_iter().collect(),
            redis_client,
            redis_listen_connection,
            service_table: Default::default(),
        };

        Ok(s)
    }

    pub async fn serve_on<S, E>(self, sk: S) -> Result<(), Error>
    where
        S: tokio::stream::Stream<Item = Result<tokio::net::UnixStream, E>>,
        E: std::error::Error,
    {
        use futures_util::{sink::Sink, stream::StreamExt};
        let con = self
            .redis_client
            .get_async_connection()
            .await
            .expect("get redis connection");
        tokio::spawn(listen_updates(self.service_table.clone(), con));

        sk.for_each_concurrent(None, |st| async {
            let st = st.expect("accept failed");
            let peer = st.peer_addr();
            let st: async_bincode::AsyncBincodeStream<_, proto::Request, proto::Reply, _> =
                st.into();
            let mut st = st.for_async();
            loop {
                let req = st.next().await;
                match req {
                    Some(Err(e)) => {
                        warn!(err = ?e, "Error processing message");
                        break;
                    }
                    None => {
                        trace!("st.next() gave None");
                        break;
                    }
                    _ => (),
                }

                let rep = async {
                    match req {
                        Some(Ok(proto::Request::Register(si))) => {
                            let rep = self.register(si).await;
                            proto::Reply::Register(rep)
                        }
                        Some(Ok(proto::Request::Query(sa))) => {
                            let rep = self.query(sa).await;
                            proto::Reply::Query(rep)
                        }
                        _ => unreachable!(),
                    }
                };

                let (rep, rdy): (proto::Reply, _) = futures_util::future::join(
                    rep,
                    futures_util::future::poll_fn(|cx| {
                        let pst = Pin::new(&mut st);
                        pst.poll_ready(cx)
                    }),
                )
                .await;
                rdy.expect("poll_ready");

                trace!(response = ?&rep, "start_send");
                let pst = Pin::new(&mut st);
                pst.start_send(rep).expect("start_send");

                trace!("poll_flush");
                futures_util::future::poll_fn(|cx| {
                    let pst = Pin::new(&mut st);
                    pst.poll_flush(cx)
                })
                .await
                .expect("poll_flush");
            }

            debug!(client = ?peer, "Stream done");
        })
        .await;
        Ok(())
    }

    // Instead of tower_service::call(), do this, which lets us have async fns
    // instead of fn foo() -> Pin<Box<dyn Future<..>>> everywhere
    pub async fn register(&self, req: proto::Service) -> proto::RegisterReply {
        match req.address {
            proto::Addr::Tcp(mut sa) if sa.ip().is_unspecified() => {
                // if address is Ip::unspec, need to make n entries, one for each self.local_public_addrs
                for ip in self.local_public_addrs.iter() {
                    sa.set_ip(*ip);
                    let r: Result<_, _> = self
                        .do_register(proto::Service {
                            address: proto::Addr::Tcp(sa),
                            ..req.clone()
                        })
                        .await
                        .into();
                    match r {
                        Ok(_) => (),
                        e @ Err(_) => return e.into(),
                    }
                }

                Ok(()).into()
            }
            proto::Addr::Udp(mut sa) if sa.ip().is_unspecified() => {
                // if address is Ip::unspec, need to make n entries, one for each self.local_public_addrs
                for ip in self.local_public_addrs.iter() {
                    sa.set_ip(*ip);
                    let r: Result<_, _> = self
                        .do_register(proto::Service {
                            address: proto::Addr::Udp(sa),
                            ..req.clone()
                        })
                        .await
                        .into();
                    match r {
                        Ok(_) => (),
                        e @ Err(_) => return e.into(),
                    }
                }

                Ok(()).into()
            }
            _ => self.do_register(req).await,
        }
    }

    async fn do_register(&self, req: proto::Service) -> proto::RegisterReply {
        let redis_conn = self.redis_listen_connection.clone();
        if let Err(e) = self.service_table_insert(req.clone()).await {
            return Err(format!("Could not register service: {}", e)).into();
        }

        debug!(req = ?&req, "Registered service");

        match req.scope {
            proto::Scope::Local | proto::Scope::Global => {
                let r = req.clone();
                tokio::spawn(async move {
                    if let Err(e) = redis_insert(redis_conn, &r).await {
                        warn!(req = ?&r, err = ?e, "Could not do redis insert");
                    }
                });
            }
            _ => (),
        }

        Ok(()).into()
    }

    pub async fn query(&self, req: proto::QueryNameRequest) -> proto::QueryNameReply {
        if let Some(srvs) = self.service_table.read().await.get(&req.name) {
            // TODO implement service filters?
            // or: remove service filters entirely
            let proto::QueryNameRequest { name, .. } = req;
            Ok(proto::QueryNameReplyOk {
                name,
                services: srvs.clone(),
            })
            .into()
        } else {
            Err(format!("Could not find {}", &req.name)).into()
        }
    }

    async fn service_table_insert(&self, req: proto::Service) -> Result<(), Error> {
        let sn = req.name.clone();
        self.service_table
            .write()
            .await
            .entry(sn)
            .or_insert_with(Vec::new)
            .push(req);
        Ok(())
    }
}

async fn listen_updates(
    service_table: Arc<RwLock<HashMap<String, Vec<proto::Service>>>>,
    mut con: redis::aio::Connection,
) {
    // usual blah blah warning about polling intervals
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
    loop {
        interval.tick().await;
        if let Err(e) = poll_updates(&service_table, &mut con).await {
            warn!(err = ?e, "Failed to poll discovery-service updates");
        } else {
            trace!("Updated discovery-services");
        }
    }
}

async fn poll_updates(
    service_table: &RwLock<HashMap<String, Vec<proto::Service>>>,
    con: &mut redis::aio::Connection,
) -> Result<(), Error> {
    use redis::AsyncCommands;
    let srvs: Vec<String> = con.smembers("names").await?;

    // TODO fast-path check if a write is even necessary?

    let mut tbl = service_table.write().await;
    for srv in srvs {
        let k = srv.trim_start_matches("name:");
        if !tbl.contains_key(k) {
            let serv_info_blobs = con.smembers::<_, Vec<Vec<u8>>>(&srv).await?;
            let serv_info: Vec<proto::Service> = serv_info_blobs
                .iter()
                .filter_map(|b| bincode::deserialize(b).ok())
                .collect();
            if !serv_info.is_empty() {
                tbl.insert(k.to_owned(), serv_info);
            } else {
                warn!("Failed to deserialize one or more services");
            }
        }
    }

    Ok(())
}

async fn redis_insert(
    conn: Arc<Mutex<redis::aio::Connection>>,
    serv: &proto::Service,
) -> Result<(), Error> {
    let name = format!("name:{}", serv.name);
    let mut r = redis::pipe();
    r.atomic().cmd("SADD").arg("names").arg(&name).ignore();

    let shard_blob = bincode::serialize(serv)?;
    r.cmd("SADD").arg(&name).arg(shard_blob).ignore();

    r.query_async(&mut *conn.lock().await).await?;
    Ok(())
}
