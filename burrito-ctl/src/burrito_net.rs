use failure::Error;
use std::collections::HashMap;

pub mod rpc {
    tonic::include_proto!("burrito");
}

pub const CONTROLLER_ADDRESS: &str = "controller";

/// Manages the inter-container network.
///
/// Jobs:
/// 1. Maintain service addresses
/// 2. if local, return remote pipe address
/// 3. TODO if remote, establish a connection with the right machine and splice the pipe with that
///
/// Services register with the listen() RPC.
/// Clients call the open() RPC with a service-level address to get the address to connect to.
///
/// Note: the returned path must be joined with the burrito root path to be useful.
pub struct BurritoNet {
    root: std::path::PathBuf,
    route_table: std::sync::Mutex<HashMap<String, String>>,
    log: slog::Logger,
}

use rpc::connection_server::{Connection, ConnectionServer};
use rpc::{ListenReply, ListenRequest, OpenReply, OpenRequest};

impl BurritoNet {
    pub fn new(root: Option<std::path::PathBuf>, log: slog::Logger) -> Self {
        BurritoNet {
            root: root.unwrap_or_else(|| std::path::PathBuf::from("/tmp/burrito")),
            route_table: Default::default(),
            log,
        }
    }

    pub fn listen_path(&self) -> std::path::PathBuf {
        self.root.join(CONTROLLER_ADDRESS)
    }

    pub fn start(self) -> Result<ConnectionServer<BurritoNet>, Error> {
        Ok(ConnectionServer::new(self))
    }
}

#[tonic::async_trait]
impl Connection for BurritoNet {
    async fn listen(
        &self,
        request: tonic::Request<ListenRequest>,
    ) -> Result<tonic::Response<ListenReply>, tonic::Status> {
        // service_addr is what other services will call open() with
        let service_addr = request.into_inner().service_addr;

        use rand::Rng;
        let rng = rand::thread_rng();

        let listen_addr: String = rng
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(10)
            .collect();

        {
            if let Some(s) = self
                .route_table
                .lock()
                .expect("Route table lock poisoned")
                .insert(service_addr.clone(), listen_addr.clone())
            {
                Err(tonic::Status::new(
                    tonic::Code::AlreadyExists,
                    format!("Service address {} already in use at {}", &service_addr, &s),
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

        // look up the service addr to translate
        let send_addr = {
            self.route_table
                .lock()
                .expect("Route table lock poisoned")
                .get(&dst_addr)
                .ok_or_else(|| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Unknown service address {}", dst_addr),
                    )
                })?
                .to_string()
        }; // release route_table lock

        slog::info!(self.log, "Service connection request";
            "service" => &dst_addr,
            "addr" => ?send_addr,
        );

        Ok(tonic::Response::new(OpenReply { send_addr }))
    }
}
