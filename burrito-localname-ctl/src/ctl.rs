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
use tracing::{error, info};

/// Serve localname-ctl.
///
/// See also [`BurritoNet`].
///
/// `force`: If true, root will be removed before attempting to listen.
#[tracing::instrument]
pub async fn serve_ctl(root: Option<PathBuf>, force: bool) -> Result<(), Error> {
    // get burrito-localname-ctl serving future
    let burrito = BurritoNet::new(root);
    let burrito_addr = burrito.listen_path();

    // if force_burrito, then we are ok with hijacking /controller, potentially from another
    // instance of burrito. Might cause bad things.
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
                proto::Request::Register(proto::RegisterRequest { addrs }) => {
                    proto::Reply::Register(this.do_register(addrs).await.into())
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
    async fn do_register(
        &self,
        register: Vec<SocketAddr>,
    ) -> Result<proto::RegisterReplyOk, String> {
        self.assign_insert(register.clone())
            .await
            .map(|rep| proto::RegisterReplyOk {
                register_addr: register,
                local_addr: rep,
            })
    }

    async fn assign_insert(&self, service_addrs: Vec<SocketAddr>) -> Result<PathBuf, String> {
        let a = get_addr();
        let p = PathBuf::from(&a);
        for service_addr in &service_addrs {
            self.name_table_insert(*service_addr, p.clone()).await?;
        }

        info!(
            service = ?&service_addrs,
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
        let mut tbl = self.name_table.write().await;
        tbl.insert(service_addr, listen_addr);
        Ok(())
    }

    async fn query(&self, dst_addr: &SocketAddr) -> Result<Option<PathBuf>, String> {
        // Look up the service addr to translate.
        let tbl = self.name_table.read().await;
        let addr = tbl.get(dst_addr).cloned();
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

#[cfg(test)]
mod test {
    use crate::LocalNameChunnel;
    use bertha::{
        either::Either, negotiate_client, negotiate_server, udp::UdpSkChunnel, uds::UnixSkChunnel,
        util::Nothing, ChunnelConnection, ChunnelConnector, ChunnelListener,
    };
    use color_eyre::eyre::Report;
    use futures_util::stream::TryStreamExt;
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use tracing::{info, info_span};
    use tracing_error::ErrorLayer;
    use tracing_futures::Instrument;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    #[allow(clippy::unit_arg)] // https://github.com/tokio-rs/tracing/issues/1093
    #[tracing::instrument(err)]
    async fn server(addr: SocketAddr, root: PathBuf) -> Result<(), Report> {
        let lch_s = LocalNameChunnel::new(
            root.clone(),
            Some(addr),
            UnixSkChunnel::with_root(root.clone()),
            Nothing::<()>::default(),
        )
        .await?;

        let st = negotiate_server(lch_s, UdpSkChunnel.listen(addr).await?).await?;
        st.try_for_each_concurrent(None, |cn| async move {
            loop {
                let m = cn.recv().await?;
                info!(?m, "got msg");
                cn.send(m).await?;
            }
        })
        .await?;
        unreachable!()
    }

    #[test]
    fn with_ctl() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        color_eyre::install().unwrap_or(());

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let addr = "127.0.0.1:17052".parse().unwrap();
        let root = PathBuf::from("./tmp-test-with-ctl/");
        test_util::reset_root_dir(&root);

        rt.block_on(
            async move {
                // start ctl
                tokio::spawn(super::serve_ctl(Some(root.clone()), true));
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;

                // start server
                tokio::spawn(server(addr, root.clone()));
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;

                let lch = LocalNameChunnel::new(
                    root.clone(),
                    None,
                    UnixSkChunnel::with_root(root),
                    Nothing::<()>::default(),
                )
                .await
                .unwrap();
                let cn = negotiate_client(lch, UdpSkChunnel.connect(()).await.unwrap(), addr)
                    .await
                    .unwrap();

                cn.send((Either::Left(addr), vec![0u8; 10])).await.unwrap();
                let m = cn.recv().await.unwrap();
                assert_eq!(m, (Either::Left(addr), vec![0u8; 10]));
            }
            .instrument(info_span!("with_ctl")),
        );
    }
}
