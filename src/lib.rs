//! Container manager

use bollard::{container::Config, Docker};
use failure::Error;
use std::collections::HashMap;

/// Main role this plays is establishing /burrito in the containers.
/// Could plausibly replace this with a proxy for kubernetes 
/// connections to the docker api.
pub struct Manager {
    docker: Docker,
    containers: Vec<String>,
}

impl Default for Manager {
    fn default() -> Self {
        Manager {
            docker: Docker::connect_with_unix_defaults().unwrap(),
            containers: Default::default(),
        }
    }
}

impl Manager {
    pub async fn spawn_container(
        &mut self,
        name: String,
        mut cfg: Config<String>,
    ) -> Result<(), Error> {
        // add our custom mountpoint
        let new_bind = "/tmp/burrito:/burrito".to_string();
        match cfg.host_config {
            Some(bollard::container::HostConfig {
                binds: Some(ref mut c),
                ..
            }) => {
                c.push(new_bind);
            }
            Some(ref mut hc) => hc.binds = Some(vec![new_bind]),
            _ => {
                cfg.host_config = Some(bollard::container::HostConfig {
                    binds: Some(vec![new_bind]),
                    ..Default::default()
                });
            }
        };

        let create_results = self.docker.create_container(
            Some(bollard::container::CreateContainerOptions { name: name.clone() }),
            cfg,
        ).await?;

        self.docker.start_container(
            &name, 
            None::<bollard::container::StartContainerOptions<String>>,
        ).await?;

        self.containers.push(create_results.id);
        Ok(())
    }

    pub async fn start_burritonet(&self) -> Result<(),Error> {
        let mut burrito = BurritoNet::default();
        burrito.root = std::path::PathBuf::from("/tmp/burrito");

        use hyper_unix_connector::UnixConnector;
        let uc: UnixConnector = tokio::net::UnixListener::bind(burrito.listen_path())?.into();
        hyper::server::Server::builder(uc).serve(ConnectionServer::new(burrito)).await?;
        
        Ok(())
    }
}

pub mod burritonet {
    tonic::include_proto!("burrito");
}

/// Manages the inter-container network.
///
/// Jobs:
/// 1. Maintain service addresses
/// 2. if local, return remote pipe address
/// 3. TODO if remote, establish a connection with the right machine and splice the pipe with that
///
/// Services register with the listen() RPC.
/// Clients call the open() RPC with a service-level address to get the address to connect to.
#[derive(Default)]
pub struct BurritoNet {
    root: std::path::PathBuf,
    route_table: std::sync::Mutex<HashMap<String, String>>,
}

impl BurritoNet {
    fn listen_path(&self) -> std::path::PathBuf {
        self.root.join("controller")
    }
}

use burritonet::server::{Connection, ConnectionServer};
use burritonet::{ListenRequest, OpenRequest, ListenReply, OpenReply};

#[tonic::async_trait]
impl Connection for BurritoNet {
    async fn listen(&self, request: tonic::Request<ListenRequest>) -> Result<tonic::Response<ListenReply>, tonic::Status> {
        // service_addr is what other services will call open() with
        let service_addr = request.into_inner().service_addr;

        use rand::Rng;
        let rng = rand::thread_rng();

        let p: String = rng.sample_iter(&rand::distributions::Alphanumeric).take(10).collect();
        let listen_addr = self.root.join(p).into_os_string().into_string().expect("Generating pipe address failed");
        
        {
            self.route_table.lock().expect("Route table lock poisoned")
                .insert(service_addr.clone(), listen_addr.clone())
                .ok_or_else(|| tonic::Status::new(tonic::Code::AlreadyExists, format!("Service address {} already in use", &service_addr)))
        }?; // release route_table lock

        let reply = burritonet::ListenReply { listen_addr: listen_addr.clone() };
        Ok(tonic::Response::new(reply))
    }

    async fn open(&self, request: tonic::Request<OpenRequest>) -> Result<tonic::Response<OpenReply>, tonic::Status> {
        let dst_addr = request.into_inner().dst_addr;
        
        // look up the service addr to translate
        let send_addr = {
            self.route_table.lock().expect("Route table lock poisoned")
                .get(&dst_addr).ok_or_else(|| 
                    tonic::Status::new(tonic::Code::Unknown, format!("Unknown service address {}", dst_addr))
            )?.to_string()
        }; // release route_table lock

        let reply = burritonet::OpenReply { send_addr };
        Ok(tonic::Response::new(reply))
    }
}
