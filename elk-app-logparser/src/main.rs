//! Take in messages containing one or more server log lines, and publish messages grouped by
//! client IP, ordered by timestamp.
//!
//! Listen on HTTP/HTTPS/QUIC (with load balancing across multiple instances / threads) and publish
//! on pub/sub chunnel with reconfigurable ordering.

use bertha::ChunnelConnection;
use color_eyre::eyre::{Context, Report};
use elk_app_logparser::parse_log;
use queue_steer::MessageQueueAddr;
use redis_basechunnel::RedisBase;
use structopt::StructOpt;
use tracing::{info, instrument};
use tracing_error::ErrorLayer;
use tracing_subscriber::prelude::*;

#[derive(Debug, StructOpt)]
#[structopt(name = "kvclient")]
struct Opt {
    #[structopt(long)]
    redis_addr: String,

    #[structopt(long)]
    kafka_addr: String,

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

#[instrument]
async fn logparser(opt: Opt) -> Result<(), Report> {
    let gcp_client = gcp_pubsub::GcpCreds::default()
        .with_project_name(&opt.gcp_project_name)
        .creds_path_env()
        .finish()
        .await?;
    let redis = RedisBase::new(&opt.redis_addr).await?;

    let conn = elk_app_logparser::publish_subscribe::connect(
        TOPIC_NAME,
        redis,
        gcp_client,
        &opt.kafka_addr,
    )
    .await?;

    if opt.subscriber {
        let mut slots = vec![None; 16];
        loop {
            let ms = conn.recv(&mut slots[..]).await?;
            for m in ms.iter_mut().map_while(Option::take) {
                info!(?m, "received");
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
