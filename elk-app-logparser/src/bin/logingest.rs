//! Take in messages containing one or more server log lines, and publish messages grouped by
//! client IP, ordered by timestamp.
//!
//! Listen on HTTP/HTTPS/QUIC (with load balancing across multiple instances / threads) and publish
//! on pub/sub chunnel with reconfigurable ordering.

use std::{future::Future, net::SocketAddr, pin::Pin};

use bertha::{ChunnelConnection, Either};
use color_eyre::eyre::{eyre, Report, WrapErr};
use elk_app_logparser::{
    listen::{self, ProcessLine},
    parse_log::{parse_raw, Line, ParsedLine},
    publish_subscribe::{self, make_topic, ConnState},
};
use gcp_pubsub::GcpClient;
use queue_steer::MessageQueueAddr;
use redis_basechunnel::RedisBase;
use structopt::StructOpt;
use tokio::sync::watch::Receiver;
use tracing::{info, instrument, trace, warn};
use tracing_error::ErrorLayer;
use tracing_subscriber::prelude::*;

#[derive(Debug, StructOpt)]
#[structopt(name = "logingest")]
struct Opt {
    #[structopt(long)]
    redis_addr: String,

    #[structopt(long)]
    kafka_addr: Option<String>,

    #[structopt(long)]
    gcp_project_name: String,

    #[structopt(long)]
    listen_addr: SocketAddr,

    #[structopt(long)]
    hostname: String,

    #[structopt(long)]
    num_workers: usize,

    #[structopt(long)]
    topic_name: String,

    #[structopt(long)]
    encr_only: bool,

    #[structopt(long)]
    logging: bool,
}

fn main() -> Result<(), Report> {
    color_eyre::install().unwrap();
    let opt = Opt::from_args();
    if opt.logging {
        let subscriber = tracing_subscriber::registry();
        let subscriber = subscriber
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let d = tracing::Dispatch::new(subscriber);
        d.init();
    }

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .wrap_err("Building tokio runtime")?;
    let redis_addr = opt.redis_addr.clone();
    let topic_name = opt.topic_name.clone();
    let (cn, mut cn_state_watcher) = rt.block_on(async move {
        let (redis, gcp_client) = setup_subscriber(
            redis_addr,
            opt.gcp_project_name,
            opt.kafka_addr.clone(),
            &topic_name,
        )
        .await?;
        if let Some(ref kafka_addr) = opt.kafka_addr {
            let (cn_state_watcher, conn) =
                publish_subscribe::connect(&topic_name, redis, gcp_client, kafka_addr).await?;
            Ok::<_, Report>((Either::Left(conn), cn_state_watcher))
        } else {
            let (cn_state_watcher, conn) =
                publish_subscribe::connect_gcp_only(&topic_name, redis, gcp_client).await?;
            Ok::<_, Report>((Either::Right(conn), cn_state_watcher))
        }
    })?;

    let curr_publish_state = *cn_state_watcher.borrow_and_update();
    let handler = PublishLines {
        inner: cn,
        topic_id: opt.topic_name,
        cn_state_watcher,
    };

    info!(?curr_publish_state, "starting server");
    listen::serve(
        opt.listen_addr,
        opt.hostname,
        opt.num_workers,
        opt.redis_addr,
        handler,
        opt.encr_only,
        Some(rt),
    )
}

struct PublishLines<C> {
    inner: C,
    topic_id: String,
    cn_state_watcher: Receiver<ConnState>,
}

impl<C> ProcessLine<(SocketAddr, Line)> for PublishLines<C>
where
    C: ChunnelConnection<Data = (MessageQueueAddr, ParsedLine)> + Send + Sync + 'static,
{
    type Error = Report;
    type Future<'a> = Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>>;

    fn process_lines<'a>(
        &'a self,
        line_batch: &'a mut [Option<(SocketAddr, Line)>],
    ) -> Self::Future<'a> {
        let str_msgs = line_batch
            .iter_mut()
            .map_while(Option::take)
            .filter_map(|x| match x.1 {
                Line::Report(s) => Some(s),
                Line::Ack => None,
            });
        let lines = parse_raw(str_msgs);
        Box::pin(async move {
            let parsed_lines = lines.filter_map(|p| {
                Some((
                    MessageQueueAddr {
                        topic_id: self.topic_id.clone(),
                        group: Some(p.client_ip.to_string()),
                    },
                    p,
                ))
            });
            let cn_state = *self.cn_state_watcher.borrow();
            self.inner.send(parsed_lines).await?;
            trace!(?cn_state, ?self.topic_id,  "sent lines");
            Ok(())
        })
    }
}

#[instrument(level = "debug", err)]
async fn setup_subscriber(
    redis_addr: String,
    gcp_project_name: String,
    kafka_addr: Option<String>,
    topic_name: &str,
) -> Result<(RedisBase, GcpClient), Report> {
    let redis = RedisBase::new(&redis_addr)
        .await
        .wrap_err_with(|| eyre!("Connect to redis at {}", redis_addr))?;
    let mut gcp_client = gcp_pubsub::GcpCreds::default()
        .with_project_name(gcp_project_name)
        .creds_path_env()
        .finish()
        .await?;

    if kafka_addr.is_some() {
        if let Err(err) = make_topic(
            ConnState::KafkaOrdering,
            &kafka_addr,
            &mut gcp_client,
            topic_name,
        )
        .await
        {
            warn!(?err, "make kafka topic errored");
        }
    }

    if let Err(err) = make_topic(
        ConnState::GcpClientSideOrdering,
        &kafka_addr,
        &mut gcp_client,
        topic_name,
    )
    .await
    {
        warn!(?err, "make gcp topic errored");
    }

    Ok((redis, gcp_client))
}
