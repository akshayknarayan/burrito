use failure::Error;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

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
    route_table: Arc<Mutex<HashMap<String, (String, rpc::open_reply::AddrType)>>>,
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
    pub fn start(self) -> Result<ConnectionServer<BurritoNet>, Error> {
        let s = self.clone();
        tokio::spawn(s.listen_updates());
        Ok(ConnectionServer::new(self))
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
        let srvs: Vec<String> = con.smembers("services").await?;

        let mut tbl = self.route_table.lock().await;
        for srv in srvs {
            if !tbl.contains_key(&srv) {
                let addrs = con
                    .smembers::<_, Vec<String>>(&srv)
                    .await?
                    .into_iter()
                    .map(|a| (srv.clone(), (a, rpc::open_reply::AddrType::Tcp)));
                tbl.extend(addrs);
            }
        }

        Ok(())
    }
}

#[tonic::async_trait]
impl Connection for BurritoNet {
    async fn listen(
        &self,
        request: tonic::Request<ListenRequest>,
    ) -> Result<tonic::Response<ListenReply>, tonic::Status> {
        // service_addr is what other services will call open() with
        let req = request.into_inner();
        let service_addr = req.service_addr;
        let listen_port = req.listen_port;
        let net_addrs: Vec<String> = self
            .local_public_addrs
            .iter()
            .map(|ref a| format!("{}:{}", a, &listen_port))
            .collect();

        let listen_addr = {
            use rand::Rng;
            let rng = rand::thread_rng();

            let listen_addr: String = rng
                .sample_iter(&rand::distributions::Alphanumeric)
                .take(10)
                .collect();
            listen_addr
        };

        let sa = service_addr.clone();
        let l2 = self.log.clone();
        let l_conn = self.redis_listen_connection.clone();
        tokio::spawn(async move {
            let mut r = redis::pipe();
            r.atomic().cmd("SADD").arg("services").arg(&sa).ignore();
            for net_addr in net_addrs {
                r.cmd("SADD").arg(&sa).arg(&net_addr);
            }

            let r: redis::RedisResult<()> = r.query_async(&mut *l_conn.lock().await).await;
            if let Err(e) = r {
                slog::warn!(l2, "Could not write new service to redis"; "err" => ?e);
            }
        });

        {
            if let Some(s) = self.route_table.lock().await.insert(
                service_addr.clone(),
                (listen_addr.clone(), rpc::open_reply::AddrType::Unix),
            ) {
                Err(tonic::Status::new(
                    tonic::Code::AlreadyExists,
                    format!(
                        "Service address {} already in use at {}",
                        &service_addr, &s.0
                    ),
                ))
            } else {
                Ok(())
            }
        }?; // release route_table lock

        slog::info!(self.log, "New service listening";
            "service" => &service_addr,
            "addr" => ?listen_addr,
        );

        Ok(tonic::Response::new(ListenReply { listen_addr }))
    }

    async fn open(
        &self,
        request: tonic::Request<OpenRequest>,
    ) -> Result<tonic::Response<OpenReply>, tonic::Status> {
        let dst_addr = request.into_inner().dst_addr;

        // Look up the service addr to translate.
        //
        // It's possible that a recently-registered remote service has not yet been polled into
        // route_table (see `listen_updates`) when this is called.
        // Treat as a cache miss, or treat route_table as truth? Treat as truth for now.
        let (send_addr, addr_type): (String, rpc::open_reply::AddrType) = {
            let tbl = self.route_table.lock().await;
            let (send_addr, addr_type) = tbl.get(&dst_addr).ok_or_else(|| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Unknown service address {}", dst_addr),
                )
            })?;

            (send_addr.clone(), *addr_type)
        };

        slog::info!(self.log, "Service connection request";
            "service" => &dst_addr,
            "addr" => ?send_addr,
        );

        Ok(tonic::Response::new(OpenReply {
            send_addr,
            addr_type: addr_type as _,
        }))
    }
}
