//! Switch between Kafka, GCP Pub/Sub with client-side ordering, and GCP Pub/Sub with service-side
//! ordering.

use std::time::Duration;

use bertha::ChunnelConnection;
use color_eyre::eyre::{Report, WrapErr};
use elk_app_logparser::{
    parse_log::{self, ParsedLine},
    publish_subscribe::{delete_topic, make_topic, ConnState},
};
use gcp_pubsub::GcpClient;
use queue_steer::MessageQueueAddr;
use redis_basechunnel::RedisBase;
use structopt::StructOpt;
use tokio::sync::{oneshot, watch};
use tracing::{info, instrument, warn};
use tracing_error::ErrorLayer;
use tracing_subscriber::prelude::*;

#[derive(Debug, StructOpt)]
#[structopt(name = "logparser")]
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

#[instrument(level = "info", skip(opt))]
async fn logparser(opt: Opt) -> Result<(), Report> {
    info!(?opt, "starting logparser");
    let redis = RedisBase::new(&opt.redis_addr).await?;
    let mut gcp_client = gcp_pubsub::GcpCreds::default()
        .with_project_name(&opt.gcp_project_name)
        .creds_path_env()
        .finish()
        .await?;

    if opt.kafka_addr.is_some() {
        if let Err(err) = make_topic(
            ConnState::KafkaOrdering,
            &opt.kafka_addr,
            &mut gcp_client,
            TOPIC_NAME,
        )
        .await
        {
            warn!(?err, "make kafka topic errored");
        }
    }

    if let Err(err) = make_topic(
        ConnState::GcpClientSideOrdering,
        &opt.kafka_addr,
        &mut gcp_client,
        TOPIC_NAME,
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
    mut gcp_client: GcpClient,
    conn: impl ChunnelConnection<Data = (MessageQueueAddr, ParsedLine)> + Send,
    mut cn_state_watcher: watch::Receiver<ConnState>,
) -> Result<(), Report> {
    let mut curr_cn_state = *cn_state_watcher.borrow_and_update();
    if opt.subscriber {
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
                    } else {
                        info!(?cn_state, ?curr_cn_state, "connection update");
                    }

                    curr_cn_state = cn_state;
                }
            }
        }
    } else {
        use itertools::Itertools;
        let lines = parse_log::sample_parsed_lines();
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
        let (s, mut r) = oneshot::channel();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.unwrap();
            s.send(()).unwrap();
        });

        for batch in line_groups {
            if r.try_recv().is_ok() {
                break;
            }

            if cn_state_watcher.has_changed()? {
                curr_cn_state = *cn_state_watcher.borrow_and_update();
            }

            conn.send(batch).await?;
            info!("sent");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        delete_topic(curr_cn_state, &opt.kafka_addr, &mut gcp_client, TOPIC_NAME).await?;
        Ok(())
    }
}
