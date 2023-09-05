//! Take in messages containing one or more server log lines, and publish messages grouped by
//! client IP, ordered by timestamp.
//!
//! Listen on HTTP/HTTPS/QUIC (with load balancing across multiple instances / threads) and publish
//! on pub/sub chunnel with reconfigurable ordering.

use bertha::ChunnelConnection;
use color_eyre::eyre::{Context, Report};
use elk_app_logparser::{
    parse_log::{self, ParsedLine},
    publish_subscribe::ConnState,
};
use gcp_pubsub::GcpClient;
use queue_steer::MessageQueueAddr;
use redis_basechunnel::RedisBase;
use structopt::StructOpt;
use tokio::sync::watch;
use tracing::{info, instrument, warn};
use tracing_error::ErrorLayer;
use tracing_subscriber::prelude::*;

#[derive(Debug, StructOpt)]
#[structopt(name = "kvclient")]
struct Opt {
    #[structopt(long)]
    redis_addr: String,

    #[structopt(long)]
    kafka_addr: Option<String>,

    #[structopt(long)]
    gcp_project_name: String,

    #[structopt(long)]
    subscriber: bool,

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

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .wrap_err("Building tokio runtime")?;
    rt.block_on(logparser(opt))
}

const TOPIC_NAME: &str = "server-logs";

#[instrument(level = "info")]
async fn logparser(opt: Opt) -> Result<(), Report> {
    let redis = RedisBase::new(&opt.redis_addr).await?;
    let mut gcp_client = gcp_pubsub::GcpCreds::default()
        .with_project_name(&opt.gcp_project_name)
        .creds_path_env()
        .finish()
        .await?;

    if opt.kafka_addr.is_some() {
        if let Err(err) =
            make_topic(ConnState::KafkaOrdering, &opt.kafka_addr, &mut gcp_client).await
        {
            warn!(?err, "make kafka topic errored");
        }
    }

    if let Err(err) = make_topic(
        ConnState::GcpClientSideOrdering,
        &opt.kafka_addr,
        &mut gcp_client,
    )
    .await
    {
        warn!(?err, "make gcp topic errored");
    }

    if let Some(ref kafka_addr) = opt.kafka_addr {
        let (cn_state_watcher, conn) = elk_app_logparser::publish_subscribe::connect(
            TOPIC_NAME,
            redis,
            gcp_client.clone(),
            kafka_addr,
        )
        .await?;
        inner(opt, gcp_client, conn, cn_state_watcher).await
    } else {
        let (cn_state_watcher, conn) = elk_app_logparser::publish_subscribe::connect_gcp_only(
            TOPIC_NAME,
            redis,
            gcp_client.clone(),
        )
        .await?;
        inner(opt, gcp_client, conn, cn_state_watcher).await
    }
}

async fn inner(
    opt: Opt,
    gcp_client: GcpClient,
    conn: impl ChunnelConnection<Data = (MessageQueueAddr, ParsedLine)> + Send,
    mut cn_state_watcher: watch::Receiver<ConnState>,
) -> Result<(), Report> {
    if opt.subscriber {
        let mut curr_cn_state = *cn_state_watcher.borrow_and_update();
        //make_topic(curr_cn_state, &opt.kafka_addr, &mut gcp_client).await?;
        let mut slots = vec![None; 16];
        loop {
            tokio::select! {
                ms = conn.recv(&mut slots[..]) => {
                    for m in ms?.iter_mut().map_while(Option::take) {
                        info!(?m, "received");
                    }
                }
                _ = cn_state_watcher.changed() => {
                    let cn_state = *cn_state_watcher.borrow_and_update();
                    if curr_cn_state.is_kafka() && cn_state.is_gcp() || curr_cn_state.is_gcp() && cn_state.is_kafka() {
                        info!(?cn_state, "reconfiguring connection");
                        //make_topic(cn_state, &opt.kafka_addr, &mut gcp_client).await?;
                        //delete_topic(curr_cn_state, &opt.kafka_addr, &mut gcp_client).await?;
                    }

                    curr_cn_state = cn_state;
                }
            }
        }
    } else {
        use itertools::Itertools;
        let lines = parse_log::sample_lines();
        let line_groups = lines
            .map(|i| {
                (
                    MessageQueueAddr {
                        topic_id: TOPIC_NAME.to_owned(),
                        group: Some(i.client_ip.to_string()),
                    },
                    i,
                )
            })
            .batching(|i| Some(i.take(16).collect::<Vec<_>>()));

        for batch in line_groups {
            conn.send(batch).await?;
        }

        unreachable!()
    }
}

async fn make_topic(
    cn: ConnState,
    kafka_addr: &Option<String>,
    gcp_client: &mut GcpClient,
) -> Result<(), Report> {
    match cn {
        ConnState::KafkaOrdering => {
            info!(?TOPIC_NAME, "creating kafka topic");
            kafka::make_topics(
                    kafka_addr.clone().expect("if kafka_addr was not provided, we cannot have ended up with ConnState::KafkaOrdering"),
                    vec![TOPIC_NAME.to_owned()]
                ).await.wrap_err("make kafka topic")?;
        }
        ConnState::GcpClientSideOrdering | ConnState::GcpServiceSideOrdering => {
            info!(?TOPIC_NAME, "creating GCP Pub/Sub topic");
            gcp_pubsub::make_topic(gcp_client, TOPIC_NAME.to_owned())
                .await
                .wrap_err("make GCP Pub/Sub topic")?;
        }
    }

    Ok(())
}

async fn delete_topic(
    cn: ConnState,
    kafka_addr: &Option<String>,
    gcp_client: &mut GcpClient,
) -> Result<(), Report> {
    match cn {
        ConnState::KafkaOrdering => {
            info!(?TOPIC_NAME, "delete kafka topic");
            kafka::delete_topic(
                    kafka_addr.clone().expect("if kafka_addr was not provided, we cannot have ended up with ConnState::KafkaOrdering"),
                    vec![TOPIC_NAME.to_owned()]
                ).await.wrap_err("delete kafka topic")?;
        }
        ConnState::GcpClientSideOrdering | ConnState::GcpServiceSideOrdering => {
            info!(?TOPIC_NAME, "delete GCP Pub/Sub topic");
            gcp_pubsub::delete_topic(gcp_client, TOPIC_NAME.to_owned())
                .await
                .wrap_err("delete GCP Pub/Sub topic")?;
        }
    }

    Ok(())
}
