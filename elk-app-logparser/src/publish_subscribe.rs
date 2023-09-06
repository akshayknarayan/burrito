use bertha::{
    bincode::{Base64Chunnel, SerializeChunnel},
    negotiate_rendezvous,
    tagger::OrderedChunnel,
    ChunnelConnection, CxList, Either, Select, StackUpgradeHandle, UpgradeHandle, UpgradeSelect,
};
use color_eyre::{eyre::eyre, Report};
use gcp_pubsub::{GcpClient, OrderedPubSubChunnel, PubSubChunnel};
use kafka::KafkaChunnel;
use queue_steer::{MessageQueueAddr, Ordered};
use redis_basechunnel::RedisBase;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::{info, instrument, warn};

use crate::parse_log::ParsedLine;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ConnState {
    KafkaOrdering,
    GcpClientSideOrdering,
    GcpServiceSideOrdering,
}

impl ConnState {
    pub fn is_kafka(&self) -> bool {
        match self {
            Self::KafkaOrdering => true,
            Self::GcpClientSideOrdering | Self::GcpServiceSideOrdering => false,
        }
    }

    pub fn is_gcp(&self) -> bool {
        !self.is_kafka()
    }
}

macro_rules! gcp_stack {
    ($topic: expr, $gcloud_client: expr) => {{
        // We repeat SerializeChunnel in both sides because the SerializeChunnel is actually different in either case due to the different data types.
        let mut ord = OrderedChunnel::default();
        ord.ordering_threshold(10);
        let ord: Ordered = ord.into();
        UpgradeSelect::from_select(Select::from((
            CxList::from(ord)
                .wrap(SerializeChunnel::default())
                .wrap(Base64Chunnel::default())
                .wrap(PubSubChunnel::new($gcloud_client.clone(), [$topic])),
            CxList::from(SerializeChunnel::default())
                .wrap(Base64Chunnel::default())
                .wrap(OrderedPubSubChunnel::from(PubSubChunnel::new(
                    $gcloud_client.clone(),
                    [$topic],
                ))),
        )))
    }};
}

pub async fn connect(
    topic: &str,
    redis: RedisBase,
    gcloud_client: GcpClient,
    kafka_addr: &str,
) -> Result<
    (
        watch::Receiver<ConnState>,
        impl ChunnelConnection<Data = (MessageQueueAddr, ParsedLine)> + Send + 'static,
    ),
    Report,
> {
    // the chunnel stack we want:
    // serialize |> base64 |> select(
    //   select(
    //     ordering |> besteffort_gcp,
    //     nothing  |> ordered_gcp,
    //   ),
    //   kafka
    // )

    // 1. gcp-only part (no kafka option)
    let (gcp_st, gcp_switch_ordering_handle) = gcp_stack!(topic, gcloud_client);
    // 2. kafka option.
    // kafka guarantees ordering within a partition, and in the case in which client-side ordering
    // might help - a single consumer - that consumer would pull from one partition at a time
    // only anyway. so, there's no point trying to get lower latency by increasing the number
    // of partitions.
    let (st, kafka_gcp_handle) = UpgradeSelect::from_select(Select::from((
        CxList::from(SerializeChunnel::default())
            .wrap(Base64Chunnel::default())
            .wrap(KafkaChunnel::new(kafka_addr, [topic])),
        gcp_st,
    )));

    // 3. initial negotiation and spawn the manager task.
    let (cn, stack_negotiation_manager) =
        negotiate_rendezvous(st, redis, topic.to_owned(), |np, select| {
            if Arc::ptr_eq(select, &gcp_switch_ordering_handle) {
                match np {
                    1 | 2 => Some(Either::Left(())),
                    3.. => Some(Either::Right(())),
                    _ => unreachable!(),
                }
            } else {
                None
            }
        })
        .await?;
    let cn_state = gcp_switch_ordering_handle
        .current()
        .map(|e| match e {
            Either::Left(()) => ConnState::GcpClientSideOrdering,
            Either::Right(()) => ConnState::GcpServiceSideOrdering,
        })
        .or(Some(ConnState::KafkaOrdering))
        .unwrap();
    info!(?cn_state, "Established connection");
    let (cn_state_watcher_s, cn_state_watcher_r) = watch::channel(cn_state);
    tokio::spawn(conn_negotiation_manager(
        topic.to_owned(),
        cn_state_watcher_s,
        stack_negotiation_manager,
        Some(kafka_gcp_handle),
        gcp_switch_ordering_handle,
    ));
    Ok((cn_state_watcher_r, cn))
}

pub async fn connect_gcp_only(
    topic: &str,
    redis: RedisBase,
    gcloud_client: GcpClient,
) -> Result<
    (
        watch::Receiver<ConnState>,
        impl ChunnelConnection<Data = (MessageQueueAddr, ParsedLine)> + Send + 'static,
    ),
    Report,
> {
    let (gcp_st, gcp_switch_ordering_handle) = gcp_stack!(topic, gcloud_client);
    let (cn, stack_negotiation_manager) =
        negotiate_rendezvous(gcp_st, redis, topic.to_owned(), |np, select| {
            if Arc::ptr_eq(select, &gcp_switch_ordering_handle) {
                info!(?np, "prefer Right when np >= 3");
                match np {
                    1 | 2 => Some(Either::Left(())),
                    3.. => Some(Either::Right(())),
                    _ => unreachable!(),
                }
            } else {
                None
            }
        })
        .await?;
    let cn_state = gcp_switch_ordering_handle
        .current()
        .map(|e| match e {
            Either::Left(()) => ConnState::GcpClientSideOrdering,
            Either::Right(()) => ConnState::GcpServiceSideOrdering,
        })
        .ok_or_else(|| eyre!("GCP stack should be active"))?;
    info!(?cn_state, "Established connection");
    let (cn_state_watcher_s, cn_state_watcher_r) = watch::channel(cn_state);
    tokio::spawn(conn_negotiation_manager(
        topic.to_owned(),
        cn_state_watcher_s,
        stack_negotiation_manager,
        None,
        gcp_switch_ordering_handle,
    ));
    Ok((cn_state_watcher_r, cn))
}

#[instrument(
    skip(
        cn_state_watcher,
        stack_negotiation_manager,
        kafka_gcp_handle,
        gcp_switch_ordering_handle,
    ),
    level = "debug"
)]
async fn conn_negotiation_manager(
    topic: String,
    cn_state_watcher: watch::Sender<ConnState>,
    mut stack_negotiation_manager: StackUpgradeHandle<RedisBase>,
    kafka_gcp_handle: Option<Arc<UpgradeHandle>>,
    gcp_switch_ordering_handle: Arc<UpgradeHandle>,
) {
    let mut num_participants_changed_listener = stack_negotiation_manager
        .conn_participants_changed_receiver
        .clone();
    let mut gcp_changed = Box::pin(gcp_switch_ordering_handle.stack_changed());
    let monitor_connection_negotiation_state =
        stack_negotiation_manager.monitor_connection_negotiation_state();
    let mut monitor_connection_negotiation_state =
        std::pin::pin!(monitor_connection_negotiation_state);
    let mut transition_in_progress = Either::Left(futures_util::future::pending());
    loop {
        tokio::select! {
            exit = &mut monitor_connection_negotiation_state => {
                if let Err(e) = exit {
                    warn!(negotiation_manager_exit = ?e, "Exiting negotiation manager");
                }

                return;
            }
            res = &mut transition_in_progress, if transition_in_progress.is_right() => {
                transition_in_progress = Either::Left(futures_util::future::pending());
                if let Err(err) = res {
                    warn!(?err, "stack transition failed");
                } else {
                    let cn_state = gcp_switch_ordering_handle
                        .current()
                        .map(|e| match e {
                            Either::Left(()) => ConnState::GcpClientSideOrdering,
                            Either::Right(()) => ConnState::GcpServiceSideOrdering,
                        })
                        .or(Some(ConnState::KafkaOrdering))
                        .unwrap();
                    info!(?cn_state, "did transition");
                }
            }
            // if the kafka stack is available and we are using it, we don't need to switch between
            // ordering implementations based on the number of participants.
            _ = num_participants_changed_listener.changed(), if !matches!(kafka_gcp_handle.as_ref().and_then(|h| h.current()), Some(Either::Left(()))) => {
                let new_num_participants = *num_participants_changed_listener.borrow_and_update();
                let cn_state = gcp_switch_ordering_handle
                    .current()
                    .map(|e| match e {
                        Either::Left(()) => ConnState::GcpClientSideOrdering,
                        Either::Right(()) => ConnState::GcpServiceSideOrdering,
                    })
                    .or(Some(ConnState::KafkaOrdering))
                    .unwrap();
                info!(
                    ?new_num_participants,
                    ?cn_state,
                    "num participants changed"
                );
                let trans_fut: Pin<Box<dyn Future<Output = Result<(), Report>> + Send>> = match new_num_participants {
                    1 | 2 => Box::pin(gcp_switch_ordering_handle.trigger_left()) as Pin<Box<_>>,
                    3.. => Box::pin(gcp_switch_ordering_handle.trigger_right()) as Pin<Box<_>>,
                    _ => unreachable!(),
                };
                transition_in_progress = Either::Right(trans_fut);
            }
            _ = (&mut gcp_changed), if kafka_gcp_handle.is_some() => {
                let cn_state = gcp_switch_ordering_handle
                    .current()
                    .map(|e| match e {
                        Either::Left(()) => ConnState::GcpClientSideOrdering,
                        Either::Right(()) => ConnState::GcpServiceSideOrdering,
                    })
                    .or(Some(ConnState::KafkaOrdering))
                    .unwrap();
                cn_state_watcher.send_replace(cn_state);

                // make a new future
                gcp_changed = Box::pin(gcp_switch_ordering_handle.stack_changed());
            }
        }
    }
}
