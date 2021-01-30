//! SocketAddr -> Path with local scope.

use crate::proto;
use color_eyre::eyre::Error;
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::RwLock;
use tower_service as tower;
use tracing::{error, info, trace};

/// Serve the localname-ctl, with support for communicating inside docker containers.
#[cfg(feature = "docker")]
pub async fn serve_ctl_and_docker(
    root: Option<PathBuf>,
    force: bool,
    in_addr_docker: PathBuf,
    out_addr_docker: PathBuf,
) {
    let docker = crate::docker_proxy::serve(in_addr_docker, out_addr_docker);
    let ctl = serve_ctl(root, force);
    let both_servers = { futures_util::future::join(docker, ctl).await };
    match both_servers {
        (Err(e), _) => {
            error!(docker_proxy = ?e, "crash" );
        }
        _ => (),
    }
}

/// Serve just the localname-ctl, without support for docker containers.
///
/// See also [`BurritoNet`].
///
/// `force`: If true, root will be removed before attempting to listen.
pub async fn serve_ctl(root: Option<PathBuf>, force: bool) -> Result<(), Error> {
    // get burrito-localname-ctl serving future
    let burrito = BurritoNet::new(root);
    let burrito_addr = burrito.listen_path();

    // if force_burrito, then we are ok with hijacking /controller, potentially from another
    // instance of burrito. Might cause bad things.
    // TODO docker-proxy might want a similar option, although things are stateless there (except for attached ttys)
    if force {
        std::fs::remove_file(&burrito_addr).unwrap_or_default(); // ignore error if file was not present
    }

    let ba = burrito_addr.clone();
    ctrlc::set_handler(move || {
        std::fs::remove_file(&ba).expect("Remove file for currently listening controller");
        std::process::exit(0);
    })?;

    info!(listening_addr = ?&burrito_addr, "burrito net starting" );
    let uc = tokio::net::UnixListener::bind(&burrito_addr).map_err(|e| {
        error!(addr = ?&burrito_addr, err = ?e, "Could not bind to burrito controller address" );
        e
    })?;

    use tokio_stream::wrappers::UnixListenerStream;
    burrito.serve_on(UnixListenerStream::new(uc)).await;
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
    name_table: Arc<RwLock<HashMap<SocketAddr, PathBuf>>>,
}

impl BurritoNet {
    /// Make a new BurritoNet.
    ///
    /// # Arguments
    /// root: The filesystem root of BurritoNet's unix pipes. Default is /tmp/burrito
    pub fn new(root: Option<PathBuf>) -> Self {
        let root = root.unwrap_or_else(|| std::path::PathBuf::from("/tmp/burrito"));
        BurritoNet {
            root,
            name_table: Default::default(),
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
                proto::Request::Register(proto::RegisterRequest { name }) => {
                    proto::Reply::Register(this.do_register(name).await.into())
                }
                proto::Request::Query(sk) => match this.query(&sk).await {
                    Ok(rep) => proto::Reply::Query(
                        Ok(proto::QueryNameReplyOk {
                            addr: sk,
                            local_addr: rep,
                        })
                        .into(),
                    ),
                    Err(e) => proto::Reply::Query(Err(e).into()),
                },
            })
        })
    }
}

impl BurritoNet {
    async fn do_register(&self, register: SocketAddr) -> Result<proto::RegisterReplyOk, String> {
        self.assign_insert(register)
            .await
            .map(|rep| proto::RegisterReplyOk {
                register_addr: register,
                local_addr: rep,
            })
    }

    async fn assign_insert(&self, service_addr: SocketAddr) -> Result<PathBuf, String> {
        let a = get_addr();
        let p: PathBuf = [".", &a].iter().collect();
        self.name_table_insert(service_addr, p.clone()).await?;

        info!(
            service = ?&service_addr,
            addr = ?&p,
            "New service listening"
        );

        Ok(p)
    }

    async fn name_table_insert(
        &self,
        service_addr: SocketAddr,
        listen_addr: PathBuf,
    ) -> Result<(), String> {
        trace!("routetable insert start");
        let mut tbl = self.name_table.write().await;
        if tbl.contains_key(&service_addr) {
            trace!("routetable insert end");
            Err(format!("Service address {} already in use", &service_addr,))
        } else if tbl.insert(service_addr, listen_addr).is_none() {
            trace!("routetable insert end");
            Ok(())
        } else {
            unreachable!()
        }
    }

    async fn query(&self, dst_addr: &SocketAddr) -> Result<Option<PathBuf>, String> {
        trace!("routetable get start");
        // Look up the service addr to translate.
        let tbl = self.name_table.read().await;
        trace!("routetable get locked");
        let addr = tbl.get(dst_addr).cloned();
        trace!("routetable get end");
        Ok(addr)
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
