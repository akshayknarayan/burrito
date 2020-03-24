use anyhow::Error;
use std::collections::HashMap;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::{trace, warn};

pub mod proto;

pub const CONTROLLER_ADDRESS: &str = "shard-controller";

pub struct ShardCtlClient {
    uc: async_bincode::AsyncBincodeStream<
        tokio::net::UnixStream,
        proto::Reply,
        proto::Request,
        async_bincode::AsyncDestination,
    >,
}

impl ShardCtlClient {
    pub async fn new(burrito_root: impl AsRef<Path>) -> Result<Self, Error> {
        let controller_addr = burrito_root.as_ref().join(CONTROLLER_ADDRESS);
        let uc: async_bincode::AsyncBincodeStream<_, proto::Reply, proto::Request, _> =
            tokio::net::UnixStream::connect(controller_addr)
                .await?
                .into();
        let uc = uc.for_async();

        Ok(ShardCtlClient { uc })
    }

    pub async fn register(&mut self, req: proto::ShardInfo) -> Result<(), Error> {
        use futures_util::{sink::Sink, stream::StreamExt};
        futures_util::future::poll_fn(|cx| {
            let pst = Pin::new(&mut self.uc);
            pst.poll_ready(cx)
        })
        .await?;

        let pst = Pin::new(&mut self.uc);
        pst.start_send(proto::Request::Register(req))?;

        futures_util::future::poll_fn(|cx| {
            let pst = Pin::new(&mut self.uc);
            pst.poll_flush(cx)
        })
        .await?;

        // now wait for the response
        match self.uc.next().await {
            Some(Ok(proto::Reply::Register(proto::RegisterShardReply::Ok))) => Ok(()),
            Some(Ok(proto::Reply::Register(proto::RegisterShardReply::Err(e)))) => {
                Err(anyhow::anyhow!(e))
            }
            Some(Err(e)) => Err(anyhow::Error::from(e)),
            None => Err(anyhow::anyhow!("Stream done")),
            Some(Ok(proto::Reply::Query(_))) => unreachable!(),
        }
    }

    pub async fn query(&mut self, req: &str) -> Result<proto::ShardInfo, Error> {
        use futures_util::{sink::Sink, stream::StreamExt};
        futures_util::future::poll_fn(|cx| {
            let pst = Pin::new(&mut self.uc);
            pst.poll_ready(cx)
        })
        .await?;

        let pst = Pin::new(&mut self.uc);
        pst.start_send(proto::Request::Query(proto::QueryShardRequest {
            service_name: req.into(),
        }))?;

        futures_util::future::poll_fn(|cx| {
            let pst = Pin::new(&mut self.uc);
            pst.poll_flush(cx)
        })
        .await?;

        // now wait for the response
        match self.uc.next().await {
            Some(Ok(proto::Reply::Query(proto::QueryShardReply::Ok(si)))) => Ok(si),
            Some(Ok(proto::Reply::Query(proto::QueryShardReply::Err(e)))) => {
                Err(anyhow::anyhow!(e))
            }
            Some(Err(e)) => Err(anyhow::Error::from(e)),
            None => Err(anyhow::anyhow!("Stream done")),
            Some(Ok(proto::Reply::Register(_))) => unreachable!(),
        }
    }
}

/// Keep track of sharded services.
#[derive(Clone)]
pub struct ShardCtl {
    shard_table: Arc<RwLock<HashMap<String, proto::ShardInfo>>>,
    redis_client: redis::Client,
    redis_listen_connection: Arc<Mutex<redis::aio::Connection>>,
}

impl ShardCtl {
    pub async fn new(redis_addr: &str) -> Result<Self, Error> {
        let redis_client = redis::Client::open(redis_addr)?;
        let redis_listen_connection =
            Arc::new(Mutex::new(redis_client.get_async_connection().await?));

        let s = ShardCtl {
            redis_client,
            redis_listen_connection,
            shard_table: Default::default(),
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
        tokio::spawn(listen_updates(self.shard_table.clone(), con));

        sk.for_each_concurrent(None, |st| async {
            let st: async_bincode::AsyncBincodeStream<_, proto::Request, proto::Reply, _> =
                st.expect("accept failed").into();
            let mut st = st.for_async();
            loop {
                let req = st.next().await;
                let rep = async {
                    match req {
                        Some(Ok(proto::Request::Register(si))) => {
                            let rep = self.register(si).await;
                            Ok(proto::Reply::Register(rep))
                        }
                        Some(Ok(proto::Request::Query(sa))) => {
                            let rep = self.query(sa).await;
                            Ok(proto::Reply::Query(rep))
                        }
                        Some(Err(e)) => Err(anyhow::Error::from(e)),
                        None => Err(anyhow::anyhow!("Stream done")),
                    }
                };

                let (rep, rdy): (Result<proto::Reply, Error>, _) = futures_util::future::join(
                    rep,
                    futures_util::future::poll_fn(|cx| {
                        let pst = Pin::new(&mut st);
                        pst.poll_ready(cx)
                    }),
                )
                .await;
                rdy.unwrap();

                if let Err(_) = rep {
                    break;
                }

                let pst = Pin::new(&mut st);
                pst.start_send(rep.unwrap()).unwrap();
            }
        })
        .await;
        Ok(())
    }

    // Instead of tower_service::call(), do this, which lets us have async fns
    // instead of fn foo() -> Pin<Box<dyn Future<..>>> everywhere
    pub async fn register(&self, req: proto::ShardInfo) -> proto::RegisterShardReply {
        let redis_conn = self.redis_listen_connection.clone();
        if let Err(e) = self.shard_table_insert(req.clone()).await {
            return proto::RegisterShardReply::Err(format!(
                "Could not insert shard-service: {}",
                e
            ));
        }

        tokio::spawn(async move {
            let r = req.clone();
            if let Err(e) = redis_insert(redis_conn, &r).await {
                warn!(req = ?&r, err = ?e, "Could not do redis insert");
            }
        });

        proto::RegisterShardReply::Ok
    }

    pub async fn query(&self, req: proto::QueryShardRequest) -> proto::QueryShardReply {
        if let Some(s) = self.shard_table.read().await.get(&req.service_name) {
            proto::QueryShardReply::Ok(s.clone())
        } else {
            proto::QueryShardReply::Err(format!("Could not find {:?}", &req.service_name))
        }
    }

    async fn shard_table_insert(&self, serv: proto::ShardInfo) -> Result<(), Error> {
        let sn = serv.service_name.clone();
        if let Some(s) = self.shard_table.write().await.insert(sn.clone(), serv) {
            Err(anyhow::anyhow!(
                "Shard service address {} already in use at: {:?}",
                sn,
                s.canonical_addr
            ))?;
        };

        Ok(())
    }
}
async fn listen_updates(
    shard_table: Arc<RwLock<HashMap<String, proto::ShardInfo>>>,
    mut con: redis::aio::Connection,
) {
    // usual blah blah warning about polling intervals
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
    loop {
        interval.tick().await;
        if let Err(e) = poll_updates(&shard_table, &mut con).await {
            warn!(err = ?e, "Failed to poll shard-services updates");
        } else {
            trace!("Updated shard-services");
        }
    }
}

async fn poll_updates(
    shard_table: &RwLock<HashMap<String, proto::ShardInfo>>,
    con: &mut redis::aio::Connection,
) -> Result<(), Error> {
    use redis::AsyncCommands;
    let srvs: Vec<String> = con.smembers("shard-services").await?;

    // TODO fast-path check if a write is even necessary?

    let mut tbl = shard_table.write().await;
    for srv in srvs {
        if !tbl.contains_key(&srv) {
            let sh_info_blob = con.get::<_, Vec<u8>>(&srv).await?;
            let sh_info = bincode::deserialize(&sh_info_blob)?;
            let k = srv.trim_start_matches("shard:");
            tbl.insert(k.to_owned(), sh_info);
        }
    }

    Ok(())
}

async fn redis_insert(
    conn: Arc<Mutex<redis::aio::Connection>>,
    serv: &proto::ShardInfo,
) -> Result<(), Error> {
    let name = format!("shard:{}", serv.service_name);
    let mut r = redis::pipe();
    r.atomic()
        .cmd("SADD")
        .arg("shard-services")
        .arg(&name)
        .ignore();

    let shard_blob = bincode::serialize(serv)?;
    r.cmd("SET").arg(&name).arg(shard_blob).ignore();

    r.query_async(&mut *conn.lock().await).await?;
    Ok(())
}
