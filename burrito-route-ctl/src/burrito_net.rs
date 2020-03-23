use burrito_flatbuf as flatbuf;
use failure::Error;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::trace;
use tracing_futures::Instrument;

pub mod rpc {
    tonic::include_proto!("burrito");
}

pub const CONTROLLER_ADDRESS: &str = "controller";

/// Manages the inter-container network.
///
/// Jobs:
/// 1. Maintain service addresses
/// 2. if local, return local pipe address to connect to: "unix://<addr>"
/// 3. if remote, return remote tcp address: "http(s)://<addr>"
///
/// Services register with the listen() RPC.
/// Clients call the open() RPC with a service-level address to get the address to connect to.
///
/// Note: the returned path must be joined with the burrito root path to be useful.
#[derive(Clone)]
pub struct BurritoNet {
    root: std::path::PathBuf,
    local_public_addrs: Vec<String>,
    route_table: Arc<RwLock<HashMap<String, (String, rpc::open_reply::AddrType)>>>,
    log: slog::Logger,
    redis_client: redis::Client,
    redis_listen_connection: Arc<Mutex<redis::aio::Connection>>,
}

use rpc::connection_server::{Connection, ConnectionServer};
use rpc::{ListenReply, ListenRequest, OpenReply, OpenRequest};

impl BurritoNet {
    /// Make a new BurritoNet.
    ///
    /// # Arguments
    /// root: The filesystem root of BurritoNet's unix pipes. Default is /tmp/burrito
    ///
    /// log: Logger instance.
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
    ///
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
    pub async fn new(
        root: Option<std::path::PathBuf>,
        local_public_addrs: impl IntoIterator<Item = String>,
        redis_addr: &str,
        log: slog::Logger, // TODO make optional
    ) -> Result<Self, Error> {
        let redis_client = redis::Client::open(redis_addr)?;
        let redis_listen_connection =
            Arc::new(Mutex::new(redis_client.get_async_connection().await?));
        Ok(BurritoNet {
            root: root.unwrap_or_else(|| std::path::PathBuf::from("/tmp/burrito")),
            local_public_addrs: local_public_addrs.into_iter().collect(),
            route_table: Default::default(),
            log,
            redis_client,
            redis_listen_connection,
        })
    }

    /// Get burrito-ctl's listening path.
    pub fn listen_path(&self) -> std::path::PathBuf {
        self.root.join(CONTROLLER_ADDRESS)
    }

    /// Returns a `Service` which can be passed to `hyper::service::make_service_fn`.
    pub fn into_hyper_service(self) -> Result<ConnectionServer<BurritoNet>, Error> {
        let s = self.clone();
        tokio::spawn(s.listen_updates());
        Ok(ConnectionServer::new(self))
    }

    /// Returns a future which will serve BurritoNet RPCs over hyper/tonic/gRPC.
    ///
    /// Calls [`BurritoNet::into_hyper_service`] internally.
    pub async fn serve_tonic_on<L>(self, uc: L) -> Result<(), Error>
    where
        L: hyper::server::accept::Accept<Conn = tokio::net::UnixStream>,
        L::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let s = self.into_hyper_service()?;
        hyper::server::Server::builder(uc)
            .serve(hyper::service::make_service_fn(move |_| {
                let s = s.clone();
                async move { Ok::<_, hyper::Error>(s) }
            }))
            .instrument(tracing::span!(tracing::Level::DEBUG, "burrito-ctl"))
            .await
            .map_err(|e| e.into())
    }

    pub async fn serve_flatbuf_on<S, E>(self, sk: S) -> Result<(), Error>
    where
        S: tokio::stream::Stream<Item = Result<tokio::net::UnixStream, E>>,
        E: std::error::Error,
    {
        use futures_util::stream::StreamExt;
        tokio::spawn(self.clone().listen_updates());
        sk.for_each_concurrent(None, |st| {
            async {
                let mut st = st.expect("Accept failed");
                let mut write_buf = burrito_flatbuf::FlatBufferBuilder::new_with_capacity(1024);
                let mut read_buf = [0u8; 512];

                // service this connection indefinitely
                loop {
                    // read req
                    let (len, msg_type) =
                        match burrito_util::read_msg_with_type(&mut st, &mut read_buf).await {
                            Ok(s) => s,
                            Err(_) => return,
                        };

                    let msg = &read_buf[..len];

                    match msg_type as usize {
                        flatbuf::LISTEN_REQUEST => {
                            let msg = flatbuf::ListenRequest::from(msg);
                            let addr = self.do_listen(&msg.service_addr, msg.port).await.unwrap();

                            let msg = flatbuf::ListenReply { addr };
                            msg.onto(&mut write_buf);
                            let msg = write_buf.finished_data();
                            if burrito_util::write_msg(
                                &mut st,
                                Some(flatbuf::LISTEN_REPLY as u32),
                                msg,
                            )
                            .await
                            .is_err()
                            {
                                return;
                            }
                        }
                        flatbuf::OPEN_REQUEST => {
                            let msg = flatbuf::OpenRequest::from(msg);
                            let (send_addr, addr_type) =
                                self.route_table_get(&msg.dst_addr).await.unwrap();
                            let msg = match addr_type {
                                rpc::open_reply::AddrType::Unix => {
                                    flatbuf::OpenReply::Unix(send_addr)
                                }
                                rpc::open_reply::AddrType::Tcp => {
                                    flatbuf::OpenReply::Tcp(send_addr)
                                }
                            };

                            msg.onto(&mut write_buf);
                            let msg = write_buf.finished_data();
                            if burrito_util::write_msg(
                                &mut st,
                                Some(flatbuf::OPEN_REPLY as u32),
                                msg,
                            )
                            .await
                            .is_err()
                            {
                                return;
                            }
                        }
                        _ => unreachable!(),
                    }

                    write_buf.reset();
                }
            }
        })
        .await;
        Ok(())
    }
}

#[tonic::async_trait]
impl Connection for BurritoNet {
    #[allow(clippy::cognitive_complexity)]
    async fn listen(
        &self,
        request: tonic::Request<ListenRequest>,
    ) -> Result<tonic::Response<ListenReply>, tonic::Status> {
        trace!("listen() start");
        // service_addr is what other services will call open() with
        let req = request.into_inner();
        let service_addr = req.service_addr;
        let listen_port = req.listen_port;

        let listen_addr = self
            .do_listen(&service_addr, listen_port.parse().unwrap())
            .await
            .map_err(|e| tonic::Status::new(tonic::Code::AlreadyExists, format!("{}", e)))?;

        Ok(tonic::Response::new(ListenReply { listen_addr }))
    }

    #[allow(clippy::cognitive_complexity)]
    async fn open(
        &self,
        request: tonic::Request<OpenRequest>,
    ) -> Result<tonic::Response<OpenReply>, tonic::Status> {
        let dst_addr = request.into_inner().dst_addr;

        trace!("open() start");
        let (send_addr, addr_type) = self.route_table_get(&dst_addr).await.unwrap(); // TODO NOW
        trace!("open() done");

        slog::trace!(self.log, "Service connection request";
            "service" => &dst_addr,
            "addr" => ?send_addr,
        );

        trace!("open() returning");

        Ok(tonic::Response::new(OpenReply {
            send_addr,
            addr_type: addr_type as _,
        }))
    }
}

impl BurritoNet {
    async fn do_listen(&self, service_addr: &str, listen_port: u16) -> Result<String, Error> {
        let net_addrs: Vec<String> = self
            .local_public_addrs
            .iter()
            .map(|ref a| format!("{}:{}", a, &listen_port))
            .collect();

        let listen_addr = get_addr();
        self.route_table_insert(&service_addr, &listen_addr).await?;

        tokio::spawn(redis_insert(
            self.redis_listen_connection.clone(),
            service_addr.to_string(),
            net_addrs.into_iter(),
            listen_port,
            self.log.clone(),
        ));
        trace!("listen() done");

        slog::info!(self.log, "New service listening";
            "service" => &service_addr,
            "addr" => ?listen_addr,
        );

        trace!("listen() returning");
        Ok(listen_addr)
    }

    // redis schema:
    //
    // key "services": a set. keys are service names.
    // key "<service name": a set. keys are addresses.
    async fn listen_updates(self) -> Result<(), Error> {
        let mut con = self.redis_client.get_async_connection().await?;

        // How often do new services call listen()? Probably not too often?
        // TODO use redis-rs pub/sub once there is async support for it
        // https://github.com/mitsuhiko/redis-rs/issues/183
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
        loop {
            interval.tick().await;
            if let Err(e) = self.poll_updates(&mut con).await {
                slog::warn!(self.log, "Failed to poll services updates"; "err" => ?e);
            } else {
                slog::trace!(self.log, "Updated services");
            }
        }
    }

    async fn poll_updates(&self, con: &mut redis::aio::Connection) -> Result<(), Error> {
        use redis::AsyncCommands;
        let srvs: Vec<String> = con.smembers("route-services").await?;

        let mut tbl = self.route_table.write().await;
        for srv in srvs {
            if !tbl.contains_key(&srv) {
                let addrs = con
                    .smembers::<_, Vec<String>>(&srv)
                    .await?
                    .into_iter()
                    .map(|a| {
                        let s = srv.trim_start_matches("route:").to_string();
                        (s, (a, rpc::open_reply::AddrType::Tcp))
                    });
                tbl.extend(addrs);
            }
        }

        Ok(())
    }

    #[allow(clippy::cognitive_complexity)]
    #[tracing::instrument(level = "debug", skip(self))]
    async fn route_table_insert(
        &self,
        service_addr: &str,
        listen_addr: &str,
    ) -> Result<(), failure::Error> {
        trace!("routetable insert start");
        if let Some(s) = self.route_table.write().await.insert(
            service_addr.to_string(),
            (listen_addr.to_string(), rpc::open_reply::AddrType::Unix),
        ) {
            trace!("routetable insert end");
            Err(failure::format_err!(
                "Service address {} already in use at {}",
                &service_addr,
                &s.0
            ))
        } else {
            trace!("routetable insert end");
            Ok(())
        }
    }

    #[allow(clippy::cognitive_complexity)]
    #[tracing::instrument(level = "debug", skip(self))]
    async fn route_table_get(
        &self,
        dst_addr: &str,
    ) -> Result<(String, rpc::open_reply::AddrType), failure::Error> {
        trace!("routetable get start");
        // Look up the service addr to translate.
        //
        // It's possible that a recently-registered remote service has not yet been polled into
        // route_table (see `listen_updates`) when this is called.
        // Treat as a cache miss, or treat route_table as truth? Treat as truth for now.
        let tbl = self.route_table.read().await;
        trace!("routetable get locked");
        let (send_addr, addr_type) = tbl
            .get(dst_addr)
            .ok_or_else(|| failure::format_err!("Unknown service address {}", dst_addr))?;

        trace!("routetable get end");
        Ok((send_addr.clone(), *addr_type))
    }
}

#[tracing::instrument(level = "debug", skip(conn, net_addrs, log))]
async fn redis_insert(
    conn: Arc<Mutex<redis::aio::Connection>>,
    sa: String,
    net_addrs: impl Iterator<Item = String>,
    listen_port: u16,
    log: slog::Logger,
) {
    trace!("redis insert start");
    let name = format!("route:{}", sa);
    let mut r = redis::pipe();
    r.atomic()
        .cmd("SADD")
        .arg("route-services")
        .arg(&name)
        .ignore();
    for net_addr in net_addrs {
        r.cmd("SADD").arg(&name).arg(&net_addr);
    }

    let r: redis::RedisResult<()> = r.query_async(&mut *conn.lock().await).await;
    if let Err(e) = r {
        slog::warn!(log, "Could not write new service to redis"; "err" => ?e);
    }

    trace!("redis insert done");
}

fn get_addr() -> String {
    use rand::Rng;
    let rng = rand::thread_rng();

    let listen_addr: String = rng
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(10)
        .collect();
    listen_addr
}
