use bertha::{
    bincode::{Base64Chunnel, SerializeChunnel},
    negotiate_rendezvous, ChunnelConnection, CxList, Select, StackUpgradeHandle, UpgradeHandle,
    UpgradeSelect,
};
use color_eyre::Report;
use gcp_pubsub::{GcpClient, OrderedPubSubChunnel, PubSubChunnel};
use kafka::KafkaChunnel;
use queue_steer::{MessageQueueAddr, Ordered};
use redis_basechunnel::RedisBase;
use std::sync::Arc;
use tracing::{info, instrument, warn};

use crate::parse_log::ParsedLine;

pub async fn connect(
    topic: &str,
    redis: RedisBase,
    gcloud_client: GcpClient,
    kafka_addr: &str,
) -> Result<impl ChunnelConnection<Data = (MessageQueueAddr, ParsedLine)>, Report> {
    // the chunnel stack we want:
    // serialize |> base64 |> select(
    //   select(
    //     ordering |> besteffort_gcp,
    //     nothing  |> ordered_gcp,
    //   ),
    //   kafka
    // )

    // 1. gcp-only part (no kafka option)
    // We repeat SerializeChunnel in both sides because the SerializeChunnel is actually different
    // in either case due to the different data types.
    let (gcp_st, gcp_switch_ordering_handle) = UpgradeSelect::from_select(Select::from((
        CxList::from(Ordered::default())
            .wrap(SerializeChunnel::default())
            .wrap(Base64Chunnel::default())
            .wrap(PubSubChunnel::new(gcloud_client.clone(), [topic])),
        CxList::from(SerializeChunnel::default())
            .wrap(Base64Chunnel::default())
            .wrap(OrderedPubSubChunnel::from(PubSubChunnel::new(
                gcloud_client.clone(),
                [topic],
            ))),
    )));
    // 2. kafka option.
    // kafka guarantees ordering within a partition, and in the case in which client-side ordering
    // might help - a single consumer - that consumer would pull from one partition at a time
    // only anyway. so, there's no point trying to get lower latency by increasing the number
    // of partitions.
    //
    // Since kafka will either be available or not, we don't need an UpgradeSelect here, we just
    // need to decide once. Select by default prefers the left (first) option.
    let st = Select::from((
        CxList::from(SerializeChunnel::default())
            .wrap(Base64Chunnel::default())
            .wrap(KafkaChunnel::new(kafka_addr, [topic])),
        gcp_st,
    ));

    // 3. initial negotiation and spawn the manager task.
    let (cn, stack_negotiation_manager) = negotiate_rendezvous(st, redis, topic.to_owned()).await?;
    tokio::spawn(conn_negotiation_manager(
        topic.to_owned(),
        stack_negotiation_manager,
        gcp_switch_ordering_handle,
    ));
    Ok(cn)
}

#[instrument(
    skip(stack_negotiation_manager, gcp_switch_ordering_handle,),
    level = "debug"
)]
async fn conn_negotiation_manager(
    topic: String,
    mut stack_negotiation_manager: StackUpgradeHandle<RedisBase>,
    gcp_switch_ordering_handle: Arc<UpgradeHandle>,
) {
    let mut num_participants_changed_listener = stack_negotiation_manager
        .conn_participants_changed_receiver
        .clone();

    loop {
        tokio::select! {
            exit = stack_negotiation_manager.monitor_connection_negotiation_state() => {
                if let Err(e) = exit {
                    warn!(negotiation_manager_exit = ?e, "Exiting negotiation manager");
                }

                return;
            }
            _ = num_participants_changed_listener.changed() => {
                let new_num_participants = *num_participants_changed_listener.borrow_and_update();
                info!(
                    ?new_num_participants,
                    "num participants change, transitioning"
                );

                let res = match new_num_participants {
                    0 => unreachable!(),
                    1 | 2 => gcp_switch_ordering_handle.trigger_left().await,
                    3.. => gcp_switch_ordering_handle.trigger_right().await,
                    _ => unreachable!(),
                };

                if let Err(err) = res {
                    warn!(?err, "stack transition failed");
                }
            }
        }
    }
}
