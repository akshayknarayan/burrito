//! Addr::Burrito() -> Addr::Unix with local scope.

use crate::proto;
use burrito_discovery_ctl::proto::Addr;
use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Mutex, RwLock};
use tower_service as tower;
use tracing::{info, trace, warn};

/// Serve the localname-ctl, with support for communicating inside docker containers.
#[cfg(feature = "docker")]
pub async fn serve_ctl_and_docker(
    root: Option<PathBuf>,
    force: bool,
    in_addr_docker: PathBuf,
    out_addr_docker: PathBuf,
    log: slog::Logger,
) {
    let docker = crate::docker_proxy::serve(in_addr_docker, out_addr_docker, log.clone());
    let ctl = serve_ctl(root, force, log.clone());
    let both_servers = { futures_util::future::join(docker, ctl).await };
    match both_servers {
        (Err(e), _) => {
            slog::crit!(log, "crash"; "docker_proxy" => ?e);
        }
        _ => (),
    }
}

/// Serve just the localname-ctl, without support for docker containers.
///
/// See also [`BurritoNet`].
///
/// `force`: If true, root will be removed before attempting to listen.
pub async fn serve_ctl(
    root: Option<PathBuf>,
    force: bool,
    log: slog::Logger,
) -> Result<(), failure::Error> {
    // get burrito-localname-ctl serving future
    let burrito = BurritoNet::new(root).await;
    let burrito_addr = burrito.listen_path();

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

    slog::info!(log, "burrito net starting"; "listening at" => ?&burrito_addr);
    let uc = tokio::net::UnixListener::bind(&burrito_addr).map_err(|e| {
        slog::error!(log, "Could not bind to burrito controller address"; "addr" => ?&burrito_addr, "err" => ?e);
        e
    })?;

    burrito.serve_on(uc).await;
    Ok(())
}

/// Manages the inter-container network.
///
/// Jobs:
/// 1. Maintain local addresses
/// 2. register local address service presence with discovery
/// 3. return local pipe address to connect to: "unix://<addr>"
///
/// Note: the returned path must be joined with the burrito root path to be useful.
#[derive(Debug, Clone)]
pub struct BurritoNet {
    root: PathBuf,
    name_table: Arc<RwLock<HashMap<Addr, Addr>>>,
    disc_cl: Option<Arc<Mutex<burrito_discovery_ctl::client::DiscoveryClient>>>,
}

impl BurritoNet {
    /// Make a new BurritoNet.
    ///
    /// Will attempt to connect to discovery-ctl as well. If unavailable, then recursively
    /// registering Addr::Tcp will be disabled.
    ///
    /// # Arguments
    /// root: The filesystem root of BurritoNet's unix pipes. Default is /tmp/burrito
    pub async fn new(root: Option<PathBuf>) -> Self {
        let root = root.unwrap_or_else(|| std::path::PathBuf::from("/tmp/burrito"));
        match burrito_discovery_ctl::client::DiscoveryClient::new(root.clone()).await {
            Ok(cl) => {
                let disc_cl = Some(Arc::new(Mutex::new(cl)));
                BurritoNet {
                    root,
                    name_table: Default::default(),
                    disc_cl,
                }
            }
            Err(e) => {
                warn!(err = ?e, "Could not connect to DiscoveryClient");
                BurritoNet {
                    root,
                    name_table: Default::default(),
                    disc_cl: None,
                }
            }
        }
    }

    /// Get burrito-ctl's listening path.
    pub fn listen_path(&self) -> std::path::PathBuf {
        self.root.join(crate::CONTROLLER_ADDRESS)
    }

    /// Serve the Ctl on the given stream.
    pub async fn serve_stream(self, st: impl AsyncWrite + AsyncRead + Unpin) {
        use async_bincode::AsyncBincodeStream;
        use tokio_tower::pipeline;

        let st: AsyncBincodeStream<_, proto::Request, proto::Reply, _> =
            AsyncBincodeStream::from(st).for_async();
        pipeline::Server::new(st, self).await.unwrap();
    }

    pub async fn serve_on<S, E>(self, inc: impl futures_util::stream::Stream<Item = Result<S, E>>)
    where
        S: AsyncWrite + AsyncRead + Unpin + Send,
        E: std::error::Error + Send + Sync + 'static,
    {
        use futures_util::stream::StreamExt;
        inc.for_each_concurrent(None, |st| {
            let srv = self.clone();
            srv.serve_stream(st.unwrap())
        })
        .await
    }
}

impl tower::Service<proto::Request> for BurritoNet {
    type Response = proto::Reply;
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + Sync>>;

    fn poll_ready(&mut self, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: proto::Request) -> Self::Future {
        let this = self.clone();
        Box::pin(async move {
            Ok(match req {
                proto::Request::Register(proto::RegisterRequest { name, register }) => {
                    proto::Reply::Register(this.do_register(name, register).await.into())
                }
                proto::Request::Query(ba @ proto::Addr::Tcp(_))
                | proto::Request::Query(ba @ proto::Addr::Udp(_))
                | proto::Request::Query(ba @ proto::Addr::Burrito(_)) => {
                    match this.query(&ba).await {
                        Ok(rep) => proto::Reply::Query(
                            Ok(proto::QueryNameReplyOk {
                                addr: ba,
                                local_addr: rep,
                            })
                            .into(),
                        ),
                        Err(e) => proto::Reply::Query(Err(e).into()),
                    }
                }
                proto::Request::Query(ba @ proto::Addr::Unix(_)) => proto::Reply::Query(
                    Ok(proto::QueryNameReplyOk {
                        addr: ba.clone(),
                        local_addr: ba,
                    })
                    .into(),
                ),
            })
        })
    }
}

impl BurritoNet {
    async fn do_register(
        &self,
        name: Addr,
        register: Option<Addr>,
    ) -> Result<proto::RegisterReplyOk, String> {
        match &name {
            proto::Addr::Burrito(_) => (),
            _ => {
                return Err(format!("Must provide Addr::Burrito as name: {}", name));
            }
        }

        match register {
            Some(b @ proto::Addr::Tcp(_)) => {
                // recursively call discovery-ctl
                if let Some(cl) = self.disc_cl.clone() {
                    info!(
                        service = ?&name,
                        addr = ?&b,
                        "Recursively registering with discovery-ctl"
                    );

                    let service_addr = match name.clone() {
                        proto::Addr::Burrito(s) => s,
                        _ => unreachable!(),
                    };

                    let n = name.clone();

                    use burrito_discovery_ctl::proto::Service;
                    tokio::spawn(async move {
                        let mut c = cl.lock().await;
                        if let Err(e) = c
                            .register(Service {
                                name: service_addr.clone(),
                                scope: burrito_discovery_ctl::proto::Scope::Global,
                                service: burrito_discovery_ctl::CONTROLLER_ADDRESS.to_owned(),
                                address: b.clone(),
                            })
                            .await
                        {
                            warn!(err = ?e, "Failed to do recursive insert to discovery-ctl");
                        }

                        if let Err(e) = c
                            .register(Service {
                                name: service_addr.clone(),
                                scope: burrito_discovery_ctl::proto::Scope::Local,
                                service: crate::CONTROLLER_ADDRESS.to_owned(),
                                address: n,
                            })
                            .await
                        {
                            warn!(err = ?e, addr = ?&b, "Failed to do recursive insert to discovery-ctl");
                        }
                    });
                }
            }
            Some(b @ proto::Addr::Unix(_)) => {
                match self.name_table_insert(name.clone(), b.clone()).await {
                    Ok(_) => {
                        info!(
                            service = ?&name,
                            addr = ?&b,
                            "New service listening"
                        );

                        return Ok(proto::RegisterReplyOk {
                            register_addr: name,
                            local_addr: b,
                        });
                    }
                    Err(e) => return Err(e),
                }
            }
            _ => (),
        };

        match self.assign_insert(name.clone()).await {
            Ok(rep) => Ok(proto::RegisterReplyOk {
                register_addr: name,
                local_addr: rep,
            }),
            Err(e) => Err(e),
        }
    }

    async fn assign_insert(&self, service_addr: Addr) -> Result<Addr, String> {
        let a = get_addr();
        let p = [".", &a].iter().collect();
        let listen_addr = Addr::Unix(p);
        let sa = service_addr.clone();
        let la = listen_addr.clone();
        self.name_table_insert(service_addr, listen_addr).await?;

        info!(
            service = ?&sa,
            addr = ?&la,
            "New service listening"
        );

        Ok(la)
    }

    #[allow(clippy::cognitive_complexity)]
    async fn name_table_insert(&self, service_addr: Addr, listen_addr: Addr) -> Result<(), String> {
        trace!("routetable insert start");
        let mut tbl = self.name_table.write().await;
        if tbl.contains_key(&service_addr) {
            trace!("routetable insert end");
            Err(format!(
                "Service address {} already in use at {}",
                &service_addr,
                tbl.get(&listen_addr).unwrap(),
            ))
        } else if tbl.insert(service_addr, listen_addr).is_none() {
            trace!("routetable insert end");
            Ok(())
        } else {
            unreachable!()
        }
    }

    #[allow(clippy::cognitive_complexity)]
    async fn query(&self, dst_addr: &Addr) -> Result<Addr, String> {
        trace!("routetable get start");
        // Look up the service addr to translate.
        let tbl = self.name_table.read().await;
        trace!("routetable get locked");
        let addr = tbl
            .get(dst_addr)
            .ok_or_else(|| format!("Unknown service address {}", dst_addr))?;

        trace!("routetable get end");
        Ok(addr.clone())
    }
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
