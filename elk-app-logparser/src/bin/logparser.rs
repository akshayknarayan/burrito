//! Subscribe to a pub/sub topic with logs ordered by client and calculate statistics.

use std::{
    collections::HashMap,
    future::Future,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    pin::Pin,
    time::Duration,
};

use bertha::{ChunnelConnection, Either};
use color_eyre::eyre::{eyre, Report, WrapErr};
use elk_app_logparser::{
    connect::{self},
    parse_log::{self, EstOutputRate, EstOutputRateHist, ParsedLine},
    publish_subscribe::{make_topic, ConnState},
    stats, EncrSpec, ProcessRecord,
};
use queue_steer::MessageQueueAddr;
use redis_basechunnel::RedisBase;
use structopt::StructOpt;
use tokio::sync::watch;
use tracing::{debug, error, info, instrument, warn};
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
    local_root: Option<Option<PathBuf>>,

    #[structopt(long)]
    forward_addr: SocketAddr,

    #[structopt(long, default_value = "allow-none")]
    encr_spec: EncrSpec,

    #[structopt(long)]
    interval_ms: u64,

    #[structopt(long)]
    stats_file: Option<PathBuf>,

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
    let redis = RedisBase::new(&opt.redis_addr)
        .await
        .wrap_err("connect to redis")?;
    let mut gcp_client = gcp_pubsub::GcpCreds::default()
        .with_project_name(&opt.gcp_project_name)
        .creds_path_env()
        .finish()
        .await
        .wrap_err("make gcp client")?;

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

async fn do_recv<C: ChunnelConnection<Data = (MessageQueueAddr, ParsedLine)> + Send>(
    conn: &C,
    mut buf: Vec<Option<(MessageQueueAddr, ParsedLine)>>,
) -> Result<Vec<Option<(MessageQueueAddr, ParsedLine)>>, Report> {
    conn.recv(&mut buf[..]).await?;
    Ok(buf)
}

async fn inner(
    opt: Opt,
    conn: impl ChunnelConnection<Data = (MessageQueueAddr, ParsedLine)> + Send,
    mut cn_state_watcher: watch::Receiver<ConnState>,
) -> Result<(), Report> {
    let (s, r) = flume::unbounded();
    let clk = quanta::Clock::new();
    tokio::spawn(stats(r, clk.clone(), opt.stats_file));
    let mut curr_cn_state = *cn_state_watcher.borrow_and_update();

    let local_root = opt
        .local_root
        .map(|o| o.unwrap_or_else(|| "/tmp/burrito".parse().unwrap()));
    let fwd_conn = connect::connect_local(opt.forward_addr, local_root, opt.encr_spec).await?;

    info!(?curr_cn_state, "ready");
    let mut processed_entries = 0;
    let mut received_lines: HashMap<IpAddr, Vec<ParsedLine>> = Default::default();
    let mut est_output_rates: HashMap<IpAddr, EstOutputRate> = Default::default();
    let mut send_wait = Either::Left(futures_util::future::pending());
    let mut recv_fut = Box::pin(do_recv(&conn, vec![None; 64]));
    let interval = Duration::from_millis(opt.interval_ms);
    loop {
        tokio::select! {
            ms = &mut recv_fut => {
                let mut b = ms?;
                let processed = handle_recv(&mut b[..], &mut received_lines, &mut est_output_rates, &mut send_wait, &mut processed_entries, interval, clk.raw(), &s)?;
                if processed >= (b.len() * 3 / 4) {
                    b.resize(std::cmp::min(b.len() * 2, 1024), None);
                }
                recv_fut = Box::pin(do_recv(&conn, b));
            }
            // dump output <interval> after we see an entry, then reset the counter
            _ = &mut send_wait, if send_wait.is_right() => {
                info!(?processed_entries, "sending histograms");
                fwd_conn
                    .send(est_output_rates.drain().filter_map(|(group, est_output)| {
                        if est_output.len() > 0 {
                            Some(EstOutputRateHist::new(group, est_output))
                        } else {
                            None
                        }
                    }))
                    .await?;
                processed_entries = 0;
                send_wait = Either::Left(futures_util::future::pending());
            }
            _ = cn_state_watcher.changed() => {
                let cn_state = *cn_state_watcher.borrow_and_update();
                info!(?cn_state, old_cn_state = ?curr_cn_state, "connection update");
                curr_cn_state = cn_state;
            }
        }
    }

    fn handle_recv(
        ms: &mut [Option<(MessageQueueAddr, ParsedLine)>],
        received_lines: &mut HashMap<IpAddr, Vec<ParsedLine>>,
        est_output_rates: &mut HashMap<IpAddr, EstOutputRate>,
        send_wait: &mut Either<
            futures_util::future::Pending<()>,
            Pin<Box<dyn Future<Output = ()>>>,
        >,
        tot_processed_entries: &mut usize,
        interval: Duration,
        recv_ts: u64,
        sender: &flume::Sender<ProcessRecord>,
    ) -> Result<usize, Report> {
        let mut processed_entries = 0;
        for v in received_lines.values_mut() {
            v.clear();
        }

        for (group, pl) in ms.iter_mut().map_while(|m| {
            m.take().and_then(|(MessageQueueAddr { group, .. }, pl)| {
                group.and_then(|g| {
                    if g.parse().ok() != Some(pl.client_ip) {
                        warn!(group = ?g, line = ?pl, "group and line mismatched");
                        None
                    } else {
                        Some((pl.client_ip, pl))
                    }
                })
            })
        }) {
            received_lines.entry(group).or_default().push(pl);
        }

        for (group, pls) in received_lines.iter_mut() {
            let log_entries = parse_log::parse_lines(pls.drain(..)).filter_map(Result::ok);
            let h = est_output_rates.entry(*group).or_default();
            processed_entries += h.new_entries(log_entries);
        }

        *tot_processed_entries += processed_entries;
        if *tot_processed_entries > 0 && send_wait.is_left() {
            *send_wait = Either::Right(Box::pin(tokio::time::sleep(interval)));
        }

        debug!(?processed_entries, "received");
        if let Err(err) = sender.try_send(ProcessRecord {
            recv_ts,
            num_records_observed: processed_entries,
            num_bytes_observed: None,
        }) {
            error!(?err, "Receiver dropped, exiting");
            return Err(eyre!("Exiting due to channel send failure"));
        }

        Ok(processed_entries)
    }
}
