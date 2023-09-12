//! Subscribe to a pub/sub topic with logs ordered by client and calculate statistics.

use std::{net::SocketAddr, path::PathBuf, time::Duration};

use bertha::{ChunnelConnection, Either};
use color_eyre::eyre::{Report, WrapErr};
use elk_app_logparser::{
    connect::{self},
    parse_log::{self, EstOutputRate, ParsedLine},
    publish_subscribe::{make_topic, ConnState},
};
use queue_steer::MessageQueueAddr;
use redis_basechunnel::RedisBase;
use structopt::StructOpt;
use tokio::sync::watch;
use tracing::{debug, info, instrument, warn};
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
    topic_name: String,

    #[structopt(long)]
    local_root: PathBuf,

    #[structopt(long)]
    forward_addr: SocketAddr,

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
            &opt.topic_name,
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
        &opt.topic_name,
    )
    .await
    {
        warn!(?err, "make gcp topic errored");
    }

    if let Some(ref kafka_addr) = opt.kafka_addr {
        let (cn_state_watcher, conn) = elk_app_logparser::publish_subscribe::connect(
            &opt.topic_name,
            redis,
            gcp_client.clone(),
            kafka_addr,
        )
        .await?;
        inner(opt, conn, cn_state_watcher).await
    } else {
        let (cn_state_watcher, conn) = elk_app_logparser::publish_subscribe::connect_gcp_only(
            &opt.topic_name,
            redis,
            gcp_client.clone(),
        )
        .await?;
        inner(opt, conn, cn_state_watcher).await
    }
}

async fn inner(
    opt: Opt,
    conn: impl ChunnelConnection<Data = (MessageQueueAddr, ParsedLine)> + Send,
    mut cn_state_watcher: watch::Receiver<ConnState>,
) -> Result<(), Report> {
    let mut curr_cn_state = *cn_state_watcher.borrow_and_update();

    let fwd_conn = connect::connect_local(opt.forward_addr, opt.local_root).await?;

    let mut slots = vec![None; 16];
    let mut processed_entries = 0;
    let mut est_output_rate: EstOutputRate = Default::default();
    let mut send_wait = Either::Left(futures_util::future::pending());
    loop {
        tokio::select! {
            ms = conn.recv(&mut slots[..]) => {
                let log_entries = parse_log::parse_lines(
                    ms?.iter_mut().map_while(|m| m.take().map(|(_, pl)| pl)),
                )
                .filter_map(Result::ok);
                if processed_entries == 0 {
                    send_wait = Either::Right(Box::pin(tokio::time::sleep(Duration::from_secs(30))));
                }

                processed_entries += est_output_rate.new_entries(log_entries);
                debug!(?processed_entries, "received");
            }
            // dump output 30 seconds after we see an entry, then reset the counter
            _ = &mut send_wait, if send_wait.is_right() => {
                let hist = est_output_rate.take_hist();
                info!(?processed_entries, "sending histogram");
                fwd_conn.send(std::iter::once(hist)).await?;
                processed_entries = 0;
                send_wait = Either::Left(futures_util::future::pending());
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
}
